import { StreamdError, StreamdNetworkError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
import { UNSIGNED_INTEGER } from "./decode.js";
import { RequestLimiter } from "./limiter.js";
import { requireAddress } from "./adapters/validate.js";
import type {
  EventAdapter,
  ModuleEventDescriptor,
  StreamdEvent,
  Transfer,
  ViewRequest,
} from "./types.js";

const FA_WITHDRAW = "0x1::fungible_asset::Withdraw";
const FA_DEPOSIT = "0x1::fungible_asset::Deposit";
const COIN_WITHDRAW = "0x1::coin::CoinWithdraw";
const COIN_DEPOSIT = "0x1::coin::CoinDeposit";
const TOKEN_WITHDRAW = "0x3::token::TokenWithdraw";
const TOKEN_DEPOSIT = "0x3::token::TokenDeposit";
const OBJECT_TRANSFER = "0x1::object::Transfer";
// Aborts unless a 0x4::token::Token is stored at the object; object_exists is not a view.
const TOKEN_CREATOR = "0x4::token::creator";

const TOKEN_EVENTS = new Set([OBJECT_TRANSFER, TOKEN_WITHDRAW, TOKEN_DEPOSIT]);

interface AssetInfo {
  symbol: string;
  decimals: number;
}

/** Every event a transfer is built from, served by one scan. */
export const TRANSFER_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [
    FA_WITHDRAW,
    FA_DEPOSIT,
    COIN_WITHDRAW,
    COIN_DEPOSIT,
    TOKEN_WITHDRAW,
    TOKEN_DEPOSIT,
    OBJECT_TRANSFER,
  ],
};

const MAX_LOOKUPS = 4;
// A store's metadata and whether an object is a token never change, so
// overflowing only costs a repeat read.
const CACHE_SIZE = 10_000;
const HEX = /0x[0-9a-fA-F]+/g;
const COIN_TYPE = /^0x[0-9a-fA-F]+::\w+::\w+(<.+>)?$/;
const LEGACY_TOKEN = /^(0x[0-9a-fA-F]+)::(.+::.+)::(\d+)$/s;

/** One side of a transfer: a withdraw or a deposit, with its asset and owner resolved. */
interface Leg {
  event: StreamdEvent;
  deposit: boolean;
  asset: string;
  amount: bigint;
  owner: string;
}

type Parsed =
  | { deposit: boolean; amount: bigint; store: string }
  /** A coin or legacy token leg; `key` is `asset` as the filter compares it. */
  | { deposit: boolean; amount: bigint; account: string; asset: string; key: string }
  | { object: string; from: string; to: string };

const address = (field: unknown) => (typeof field === "string" ? normalizeAddress(field) : undefined);

/** A legacy `0x3::token::TokenId` as `creator::collection::name::property_version`. */
function legacyTokenId(id: unknown): string | undefined {
  if (typeof id !== "object" || id === null) return undefined;
  const { token_data_id: data, property_version: version } = id as Record<string, unknown>;
  if (typeof data !== "object" || data === null) return undefined;
  if (typeof version !== "string" || !UNSIGNED_INTEGER.test(version)) return undefined;
  const { creator, collection, name } = data as Record<string, unknown>;
  const owner = address(creator);
  if (owner === undefined || typeof collection !== "string" || typeof name !== "string") return undefined;
  return `${owner}::${collection}::${name}::${BigInt(version)}`;
}

/**
 * `amount` in whole units as a decimal string, every digit kept and trailing
 * zeros dropped. It never passes through a float, so a `u128` stays exact.
 */
export function formatAmount(amount: bigint, decimals: number): string {
  if (typeof amount !== "bigint") throw new StreamdError(`Not a valid amount: ${String(amount)}`);
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 255) {
    throw new StreamdError(`Not a valid decimals: ${String(decimals)} (expected 0..255)`);
  }
  const digits = (amount < 0n ? -amount : amount).toString().padStart(decimals + 1, "0");
  const split = digits.length - decimals;
  const fraction = digits.slice(split).replace(/0+$/, "");
  return `${amount < 0n ? "-" : ""}${digits.slice(0, split)}${fraction === "" ? "" : `.${fraction}`}`;
}

/** Addresses inside a coin type or an asset address, in long form, for comparing. */
export function assetKey(asset: string): string {
  return asset.replace(HEX, (a) => normalizeAddress(a) ?? a);
}

export function requireAsset(asset: unknown): string {
  if (typeof asset !== "string" || asset === "") {
    throw new StreamdError("asset must be a fungible-asset metadata or token object address, a coin type, or a legacy token id");
  }
  if (!asset.includes("::")) return requireAddress(asset);
  if (COIN_TYPE.test(asset)) return assetKey(asset);
  const legacy = LEGACY_TOKEN.exec(asset);
  if (legacy === null) throw new StreamdError(`Not a valid coin type or legacy token id: ${asset}`);
  return `${requireAddress(legacy[1])}::${legacy[2]}::${BigInt(legacy[3]!)}`;
}

function parse(event: StreamdEvent): Parsed | undefined {
  const data = event.data as Record<string, unknown> | null;
  if (typeof data !== "object" || data === null) return undefined;
  if (event.type === OBJECT_TRANSFER) {
    const [object, from, to] = [address(data.object), address(data.from), address(data.to)];
    return object && from && to ? { object, from, to } : undefined;
  }
  const { amount } = data;
  if (typeof amount !== "string" || !UNSIGNED_INTEGER.test(amount)) return undefined;
  const value = BigInt(amount);
  const deposit = event.type === FA_DEPOSIT || event.type === COIN_DEPOSIT || event.type === TOKEN_DEPOSIT;
  if (event.type === FA_WITHDRAW || event.type === FA_DEPOSIT) {
    const store = address(data.store);
    return store === undefined ? undefined : { deposit, amount: value, store };
  }
  const account = address(data.account);
  if (account === undefined) return undefined;
  if (event.type === COIN_WITHDRAW || event.type === COIN_DEPOSIT) {
    const coinType = data.coin_type;
    if (typeof coinType !== "string" || coinType === "") return undefined;
    return { deposit, amount: value, account, asset: coinType, key: assetKey(coinType) };
  }
  if (event.type === TOKEN_WITHDRAW || event.type === TOKEN_DEPOSIT) {
    const asset = legacyTokenId(data.id);
    return asset === undefined ? undefined : { deposit, amount: value, account, asset, key: asset };
  }
  return undefined;
}

/**
 * A bounded least-recently-used promise cache that drops rejections and answers
 * `keep` refuses. `pending` hears of each load, with a way to drop it unless it
 * has settled by then.
 */
function memo<T>(keep: (value: T) => boolean, pending?: (drop: () => void) => void) {
  const entries = new Map<string, Promise<T>>();
  return (key: string, load: () => Promise<T>): Promise<T> => {
    let answer = entries.get(key);
    if (answer !== undefined) {
      entries.delete(key);
    } else {
      const loaded = load();
      let settled = false;
      const forget = () => entries.get(key) === loaded && entries.delete(key);
      loaded.then((value) => keep(value) || forget(), forget).finally(() => (settled = true));
      pending?.(() => settled || forget());
      if (entries.size >= CACHE_SIZE) entries.delete(entries.keys().next().value!);
      answer = loaded;
    }
    entries.set(key, answer);
    return answer;
  };
}

/**
 * Pairs one transaction's legs, in event order: a deposit takes the most
 * recent waiting withdraw of the same asset and amount; withdraws nothing took
 * stand alone. Each record keeps its anchor event's identity.
 */
function pair(legs: Leg[]): StreamdEvent<Transfer>[] {
  const waiting: Leg[] = [];
  const out: StreamdEvent<Transfer>[] = [];
  const record = (anchor: Leg, data: Transfer) => out.push({ ...anchor.event, data });
  for (const leg of legs) {
    if (!leg.deposit) {
      waiting.push(leg);
      continue;
    }
    let i = waiting.length - 1;
    while (i >= 0 && !(waiting[i]!.asset === leg.asset && waiting[i]!.amount === leg.amount)) i--;
    const from = i === -1 ? undefined : waiting.splice(i, 1)[0]!.owner;
    record(leg, { asset: leg.asset, amount: leg.amount, from, to: leg.owner });
  }
  for (const w of waiting) record(w, { asset: w.asset, amount: w.amount, from: w.owner, to: undefined });
  return out;
}

const byIdentity = (a: StreamdEvent, b: StreamdEvent) =>
  a.version === b.version ? a.eventIndex - b.eventIndex : a.version < b.version ? -1 : 1;

/**
 * `adapter` as a source of transfer records: each page of coin,
 * fungible-asset and token events becomes the transfers its transactions
 * made. Store assets and owners, and whether a transferred object is a token,
 * are read through `adapter.view` inside the page fetch, so a failed read
 * fails the page and a listener retries it.
 *
 * Pairing sees one page, so a transaction split across pages (never on
 * `FullnodeScanAdapter`) pairs per page.
 *
 * `account` resolves to the long-form address whose records to keep. It filters
 * finished records, since a deposit needs its withdraw to report `from`.
 */
export function transferSource(
  adapter: EventAdapter,
  asset?: string,
  account?: () => Promise<string>,
  withAssetInfo?: boolean,
  requestTimeout?: number,
): EventAdapter {
  if (typeof adapter.view !== "function") {
    throw new StreamdError("transfers needs an adapter that implements view");
  }
  const wanted = asset === undefined ? undefined : requireAsset(asset);
  // The timeout matters here as a memoized read that never settled would hang every retry.
  const limiter = new RequestLimiter(MAX_LOOKUPS, Infinity, requestTimeout);
  // Pages are read one at a time, so a read still pending when the next page
  // starts belongs to one the caller gave up on, perhaps at its timeout before
  // this read's own: joining it would fail the retry too.
  let loads: (() => void)[] = [];
  let generation = 0;
  // Aborted when the next page starts: reads a given-up page queued leave the queue, and
  // those it had started free their slots, though the requests themselves run on.
  let page = new AbortController();
  const track = (drop: () => void) => void loads.push(drop);
  const metadata = memo<string | undefined>((asset) => asset !== undefined, track);
  const infos = memo<AssetInfo | undefined>(() => true, track);
  const tokens = memo<boolean>(() => true, track);

  /** The address `fn` returns for `object`, before the transaction if it was gone after it. */
  const read = async (
    fn: ViewRequest["function"],
    object: string,
    version: bigint,
    signal: AbortSignal,
  ): Promise<string | undefined> => {
    const request: ViewRequest = {
      function: fn,
      typeArguments: ["0x1::object::ObjectCore"],
      functionArguments: [object],
    };
    for (const at of version > 0n ? [version, version - 1n] : [version]) {
      let result: unknown[];
      try {
        result = await limiter.run(() => adapter.view!(request, at, TRANSFER_DESCRIPTOR), signal, signal);
      } catch (err) {
        if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) continue;
        throw err;
      }
      let value = Array.isArray(result) ? result[0] : undefined;
      if (typeof value === "object" && value !== null) value = (value as { inner?: unknown }).inner;
      const address = typeof value === "string" ? normalizeAddress(value) : undefined;
      if (address === undefined) {
        throw new StreamdError(`${fn} of ${object} at version ${at} returned no address`);
      }
      return address;
    }
    return undefined;
  };

  const assetOf = (store: string, version: bigint, signal: AbortSignal) =>
    metadata(store, () => read("0x1::fungible_asset::store_metadata", store, version, signal));

  const isToken = (object: string, version: bigint, signal: AbortSignal) =>
    tokens(object, async () => (await read(TOKEN_CREATOR, object, version, signal)) !== undefined);

  const readInfo = async (asset: string, version: bigint, signal: AbortSignal): Promise<AssetInfo | undefined> => {
    const coin = asset.includes("::");
    const module = coin ? "0x1::coin" : "0x1::fungible_asset";
    const view = (name: string): Promise<unknown[]> => {
      const request: ViewRequest = {
        function: `${module}::${name}` as ViewRequest["function"],
        typeArguments: [coin ? asset : "0x1::object::ObjectCore"],
        functionArguments: coin ? [] : [asset],
      };
      return limiter.run(() => adapter.view!(request, version, TRANSFER_DESCRIPTOR), signal, signal);
    };
    let symbol: unknown[];
    let decimals: unknown[];
    try {
      [symbol, decimals] = await Promise.all([view("symbol"), view("decimals")]);
    } catch (err) {
      if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) return undefined;
      throw err;
    }
    const raw = decimals?.[0];
    const parsed = typeof raw === "string" && UNSIGNED_INTEGER.test(raw) ? Number(raw) : raw;
    if (typeof symbol?.[0] !== "string" || !Number.isInteger(parsed)) {
      throw new StreamdError(`The metadata of ${asset} at version ${version} is malformed`);
    }
    return { symbol: symbol[0], decimals: parsed as number };
  };

  const withInfo = async (event: StreamdEvent<Transfer>, signal: AbortSignal): Promise<StreamdEvent<Transfer>> => {
    if (TOKEN_EVENTS.has(event.type)) return event;
    const { asset } = event.data;
    const info = await infos(assetKey(asset), () => readInfo(asset, event.version, signal));
    return info === undefined ? event : { ...event, data: { ...event.data, ...info } };
  };

  /** A leg to pair, or a finished record for an object transfer, which names both sides. */
  const legOf = async (
    event: StreamdEvent,
    signal: AbortSignal,
  ): Promise<Leg | StreamdEvent<Transfer> | undefined> => {
    const parsed = parse(event);
    if (parsed === undefined) return undefined;
    if ("object" in parsed) {
      const { object, from, to } = parsed;
      if (wanted !== undefined && object !== wanted) return undefined;
      if (!(await isToken(object, event.version, signal))) return undefined;
      return { ...event, data: { asset: object, amount: 1n, from, to } };
    }
    const { deposit, amount } = parsed;
    if ("account" in parsed) {
      if (wanted !== undefined && parsed.key !== wanted) return undefined;
      return { event, deposit, amount, asset: parsed.asset, owner: parsed.account };
    }
    // A coin type or legacy id never names a fungible asset, so its filter needs no store read.
    if (wanted?.includes("::")) return undefined;
    const asset = await assetOf(parsed.store, event.version, signal);
    if (asset === undefined || (wanted !== undefined && asset !== wanted)) return undefined;
    const owner = await read("0x1::object::owner", parsed.store, event.version, signal);
    return owner === undefined ? undefined : { event, deposit, amount, asset, owner };
  };

  const toTransfers = async (events: StreamdEvent[], signal: AbortSignal): Promise<StreamdEvent<Transfer>[]> => {
    const transactions = new Map<bigint, StreamdEvent[]>();
    for (const event of [...events].sort(byIdentity)) {
      const txn = transactions.get(event.version);
      if (txn === undefined) transactions.set(event.version, [event]);
      else txn.push(event);
    }
    const legs = await Promise.all(
      [...transactions.values()].map((txn) => Promise.all(txn.map((event) => legOf(event, signal)))),
    );
    return legs
      .flatMap((txn) => {
        const records = txn.filter((r): r is StreamdEvent<Transfer> => r !== undefined && !("deposit" in r));
        return [...records, ...pair(txn.filter((r): r is Leg => r !== undefined && "deposit" in r))];
      })
      .sort(byIdentity);
  };

  const source: EventAdapter = {
    fetchEvents: async (descriptor, options) => {
      const mine = ++generation;
      // A page overtaken while it waited was abandoned; its reads would only be joined.
      const overtaken = () => {
        if (generation !== mine) throw new StreamdNetworkError("Transfer page abandoned for a newer one");
      };
      for (const drop of loads.splice(0)) drop();
      page.abort(new StreamdNetworkError("Transfer page abandoned for a newer one"));
      page = new AbortController();
      const { signal } = page;
      const who = await account?.();
      const fetched = await adapter.fetchEvents(descriptor, options);
      overtaken();
      let events = await toTransfers(fetched.events, signal);
      if (who !== undefined) events = events.filter(({ data }) => data.from === who || data.to === who);
      // The store reads above can outlast the page, and these reads are memoized too.
      overtaken();
      if (withAssetInfo) events = await Promise.all(events.map((event) => withInfo(event, signal)));
      if (options.orderBy === "desc") events.reverse();
      return { ...fetched, events };
    },
  };
  for (const name of ["getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const) {
    const method = adapter[name];
    if (method !== undefined) Object.assign(source, { [name]: method.bind(adapter) });
  }
  return source;
}
