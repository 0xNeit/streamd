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

/** Every event a transfer is built from, served by one scan. */
export const TRANSFER_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [FA_WITHDRAW, FA_DEPOSIT, COIN_WITHDRAW, COIN_DEPOSIT],
};

const MAX_LOOKUPS = 4;
// A store's metadata never changes, so overflowing only costs a repeat read.
const METADATA_CACHE_SIZE = 10_000;
const HEX = /0x[0-9a-fA-F]+/g;

/** One side of a transfer: a withdraw or a deposit, with its asset and owner resolved. */
interface Leg {
  event: StreamdEvent;
  deposit: boolean;
  asset: string;
  amount: bigint;
  owner: string;
}

type Parsed = { deposit: boolean; amount: bigint } & (
  | { store: string }
  | { account: string; coinType: string }
);

/** Addresses inside a coin type or an asset address, in long form, for comparing. */
function assetKey(asset: string): string {
  return asset.replace(HEX, (a) => normalizeAddress(a) ?? a);
}

function requireAsset(asset: unknown): string {
  if (typeof asset !== "string" || asset === "") {
    throw new StreamdError("asset must be a fungible-asset metadata address or a coin type");
  }
  if (!asset.includes("::")) return requireAddress(asset);
  if (!/^0x[0-9a-fA-F]+::\w+::\w+/.test(asset)) {
    throw new StreamdError(`Not a valid coin type: ${asset}`);
  }
  return assetKey(asset);
}

function parse(event: StreamdEvent): Parsed | undefined {
  const data = event.data as Record<string, unknown> | null;
  if (typeof data !== "object" || data === null) return undefined;
  const { amount } = data;
  if (typeof amount !== "string" || !UNSIGNED_INTEGER.test(amount)) return undefined;
  const value = BigInt(amount);
  const address = (field: unknown) => (typeof field === "string" ? normalizeAddress(field) : undefined);
  if (event.type === FA_WITHDRAW || event.type === FA_DEPOSIT) {
    const store = address(data.store);
    return store === undefined ? undefined : { deposit: event.type === FA_DEPOSIT, amount: value, store };
  }
  if (event.type === COIN_WITHDRAW || event.type === COIN_DEPOSIT) {
    const account = address(data.account);
    const coinType = data.coin_type;
    if (account === undefined || typeof coinType !== "string" || coinType === "") return undefined;
    return { deposit: event.type === COIN_DEPOSIT, amount: value, account, coinType };
  }
  return undefined;
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
 * `adapter` as a source of transfer records: each page of coin and
 * fungible-asset events becomes the transfers its transactions made. Store
 * assets and owners are read through `adapter.view` inside the page fetch, so
 * a failed read fails the page and a listener retries it.
 *
 * Pairing sees one page, so a transaction split across pages (never on
 * `FullnodeScanAdapter`) pairs per page.
 */
export function transferSource(adapter: EventAdapter, asset?: string): EventAdapter {
  if (typeof adapter.view !== "function") {
    throw new StreamdError("transfers needs an adapter that implements view");
  }
  const wanted = asset === undefined ? undefined : requireAsset(asset);
  const limiter = new RequestLimiter(MAX_LOOKUPS);
  const metadata = new Map<string, Promise<string | undefined>>();

  /** The address `fn` returns for `store`, before the transaction if the store was gone after it. */
  const read = async (fn: ViewRequest["function"], store: string, version: bigint): Promise<string | undefined> => {
    const request: ViewRequest = {
      function: fn,
      typeArguments: ["0x1::object::ObjectCore"],
      functionArguments: [store],
    };
    for (const at of version > 0n ? [version, version - 1n] : [version]) {
      let result: unknown[];
      try {
        result = await limiter.run(() => adapter.view!(request, at, TRANSFER_DESCRIPTOR));
      } catch (err) {
        if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) continue;
        throw err;
      }
      let value = Array.isArray(result) ? result[0] : undefined;
      if (typeof value === "object" && value !== null) value = (value as { inner?: unknown }).inner;
      const address = typeof value === "string" ? normalizeAddress(value) : undefined;
      if (address === undefined) {
        throw new StreamdError(`${fn} of ${store} at version ${at} returned no address`);
      }
      return address;
    }
    return undefined;
  };

  const assetOf = (store: string, version: bigint): Promise<string | undefined> => {
    let answer = metadata.get(store);
    if (answer !== undefined) {
      metadata.delete(store);
    } else {
      answer = read("0x1::fungible_asset::store_metadata", store, version);
      const forget = () => metadata.get(store) === answer && metadata.delete(store);
      answer.then((a) => a === undefined && forget(), forget);
      if (metadata.size >= METADATA_CACHE_SIZE) metadata.delete(metadata.keys().next().value!);
    }
    metadata.set(store, answer);
    return answer;
  };

  const legOf = async (event: StreamdEvent): Promise<Leg | undefined> => {
    const parsed = parse(event);
    if (parsed === undefined) return undefined;
    const { deposit, amount } = parsed;
    if ("coinType" in parsed) {
      if (wanted !== undefined && assetKey(parsed.coinType) !== wanted) return undefined;
      return { event, deposit, amount, asset: parsed.coinType, owner: parsed.account };
    }
    const asset = await assetOf(parsed.store, event.version);
    if (asset === undefined || (wanted !== undefined && asset !== wanted)) return undefined;
    const owner = await read("0x1::object::owner", parsed.store, event.version);
    return owner === undefined ? undefined : { event, deposit, amount, asset, owner };
  };

  const toTransfers = async (events: StreamdEvent[]): Promise<StreamdEvent<Transfer>[]> => {
    const transactions = new Map<bigint, StreamdEvent[]>();
    for (const event of [...events].sort(byIdentity)) {
      const txn = transactions.get(event.version);
      if (txn === undefined) transactions.set(event.version, [event]);
      else txn.push(event);
    }
    const legs = await Promise.all(
      [...transactions.values()].map((txn) => Promise.all(txn.map(legOf))),
    );
    return legs.flatMap((txn) => pair(txn.filter((l): l is Leg => l !== undefined))).sort(byIdentity);
  };

  const source: EventAdapter = {
    fetchEvents: async (descriptor, options) => {
      const page = await adapter.fetchEvents(descriptor, options);
      const events = await toTransfers(page.events);
      if (options.orderBy === "desc") events.reverse();
      return { ...page, events };
    },
  };
  for (const name of ["getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const) {
    const method = adapter[name];
    if (method !== undefined) Object.assign(source, { [name]: method.bind(adapter) });
  }
  return source;
}
