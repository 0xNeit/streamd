import { StreamdError, StreamdNetworkError, UNSIGNED_INTEGER, requireAddress, requireLedgerVersion, sleep, withRetries } from "@streamd/core";
import type { EventAdapter, ViewRequest } from "@streamd/core";
import type { FungibleActivityRow, FungibleStoreRow } from "./indexer.js";
import { TRANSFER_DESCRIPTOR } from "./transfers.js";
import type { FungibleAssetIndexer, FungibleHolder, HolderSnapshot, HoldersAtOptions, HolderVerification } from "./types.js";

const PAGE = 100;
const PROCESSOR_READS = 10;
const PROCESSOR_WAIT = 1000;
const APT = `0x${"a".padStart(64, "0")}`;
const DEPOSITS = new Set(["0x1::fungible_asset::Deposit", "0x1::fungible_asset::DepositEvent"]);
const WITHDRAWS = new Set(["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::WithdrawEvent"]);
const METADATA = "0x1::fungible_asset::Metadata";
const VERIFICATIONS = new Set<unknown>(["none", "supply", "stores"]);

/** What `holdersAt` reads through: the Indexer, the adapter's `view`, and the client's limiter. */
export interface HoldersHost {
  indexer: FungibleAssetIndexer;
  adapter: EventAdapter;
  run: <T>(task: () => Promise<T>) => Promise<T>;
}

type Call = <T>(task: () => Promise<T>) => Promise<T>;

interface Checked {
  verify: HolderVerification;
  maxRequests: number;
  retries: number;
  signal: AbortSignal | undefined;
}

/** The metadata address, long form; APT and coin types are refused. */
export function requireFungibleAsset(asset: unknown): string {
  if (typeof asset !== "string" || asset.includes("::")) {
    throw new StreamdError("holdersAt needs a fungible asset's metadata address, not a coin type");
  }
  const metadata = requireAddress(asset);
  if (metadata === APT) {
    throw new StreamdError("holdersAt does not fold APT: gas fees and storage refunds move it in ways the fold was never checked against");
  }
  return metadata;
}

function checkOptions(options: HoldersAtOptions): Checked {
  if (typeof options !== "object" || options === null) throw new StreamdError("holdersAt options must be an object");
  const { verify = "supply", maxRequests = 1000, retries = 0, signal } = options;
  if (!VERIFICATIONS.has(verify)) throw new StreamdError(`holdersAt verify must be "none", "supply" or "stores"`);
  if (!Number.isSafeInteger(maxRequests) || maxRequests < 1) throw new StreamdError("holdersAt maxRequests must be a positive integer");
  if (!Number.isSafeInteger(retries) || retries < 0) throw new StreamdError("holdersAt retries must be a non-negative integer");
  return { verify, maxRequests, retries, signal };
}

/** Every request through the limiter, retried, and counted against `max`. */
function counter(run: Call, { maxRequests, retries, signal }: Checked) {
  let requests = 0;
  const call: Call = (task) =>
    withRetries(
      () => {
        if (requests >= maxRequests) {
          return Promise.reject(new StreamdError(`holdersAt spent its maxRequests of ${maxRequests}`));
        }
        requests++;
        return run(task);
      },
      retries,
      signal,
    );
  return { call, requests: () => requests, left: () => maxRequests - requests };
}

async function currentStores(call: Call, indexer: FungibleAssetIndexer, asset: string): Promise<Map<string, FungibleStoreRow>> {
  const stores = new Map<string, FungibleStoreRow>();
  let after: string | undefined;
  for (;;) {
    const page = await call(() => indexer.listFungibleStores(asset, after, PAGE));
    for (const row of page) stores.set(row.store, row);
    if (page.length < PAGE) return stores;
    after = page[PAGE - 1]!.store;
  }
}

/** The processor's version once it reaches `newest`, since rows can run ahead of what it reports. */
async function processedThrough(call: Call, indexer: FungibleAssetIndexer, newest: bigint, signal: AbortSignal | undefined): Promise<bigint> {
  for (let read = 1; ; read++) {
    const version = await call(() => indexer.getFungibleAssetVersion());
    if (version >= newest) return version;
    if (read >= PROCESSOR_READS) {
      throw new StreamdNetworkError(`The Indexer has not processed through version ${newest}, which its balances already reflect`);
    }
    await sleep(PROCESSOR_WAIT, signal);
  }
}

/**
 * Every activity row in `[from, to]`. A full page may end partway through a
 * version, so its last version is read again from the start of the next
 * page; a version filling a whole page is read through by offset.
 */
async function activitiesIn(
  call: Call,
  indexer: FungibleAssetIndexer,
  asset: string,
  from: bigint,
  to: bigint,
): Promise<FungibleActivityRow[]> {
  const rows: FungibleActivityRow[] = [];
  const read = (start: bigint, end: bigint, offset: number) =>
    call(() => indexer.listFungibleActivities(asset, start, end, PAGE, offset));
  while (from <= to) {
    const page = await read(from, to, 0);
    if (page.length < PAGE) {
      rows.push(...page);
      break;
    }
    const last = page[PAGE - 1]!.version;
    if (page[0]!.version !== last) {
      rows.push(...page.filter((row) => row.version < last));
      from = last;
      continue;
    }
    rows.push(...page);
    for (let offset = PAGE; ; offset += PAGE) {
      const more = await read(last, last, offset);
      rows.push(...more);
      if (more.length < PAGE) break;
    }
    from = last + 1n;
  }
  return rows;
}

/** What undoing `row` adds to its store's balance. */
function undo(row: FungibleActivityRow): bigint {
  if (row.gasFee) return row.amount;
  if (!row.success) return 0n;
  if (DEPOSITS.has(row.type)) return -row.amount;
  if (WITHDRAWS.has(row.type)) return row.amount;
  if (row.amount === 0n) return 0n;
  throw new StreamdError(`holdersAt cannot undo a ${row.type} row at version ${row.version}`);
}

/**
 * Each store's balance at `version`. A current row read after `u0` reflects
 * every change through its `lastVersion` and none after, so only those are
 * undone. A store seen only in activities was either deleted before the walk
 * reached it, and is folded from 0, or created after, so after `version`.
 */
async function fold(
  call: Call,
  indexer: FungibleAssetIndexer,
  asset: string,
  u0: bigint,
  stores: Map<string, FungibleStoreRow>,
  activities: FungibleActivityRow[],
): Promise<Map<string, FungibleHolder>> {
  const balances = new Map<string, FungibleHolder>();
  for (const { store, owner, amount } of stores.values()) balances.set(store, { store, owner, balance: amount });
  const orphans = new Map<string, FungibleActivityRow[]>();
  for (const row of activities) {
    const current = stores.get(row.store);
    if (current === undefined) {
      const rows = orphans.get(row.store);
      if (rows === undefined) orphans.set(row.store, [row]);
      else rows.push(row);
    }
    else if (row.version <= current.lastVersion) balances.get(row.store)!.balance += undo(row);
  }
  const ids = [...orphans.keys()];
  const created = new Set<string>();
  for (let i = 0; i < ids.length; i += PAGE) {
    const batch = ids.slice(i, i + PAGE);
    for (const row of await call(() => indexer.getFungibleStores(asset, batch))) created.add(row.store);
  }
  for (const [store, rows] of orphans) {
    if (created.has(store)) {
      if (rows.some((row) => row.version <= u0)) {
        throw new StreamdError(`The Indexer's walk missed store ${store}, which changed at or before version ${u0}`);
      }
      continue;
    }
    balances.set(store, { store, owner: rows[0]!.owner, balance: rows.reduce((sum, row) => sum + undo(row), 0n) });
  }
  return balances;
}

function ranked(balances: Map<string, FungibleHolder>, version: bigint): FungibleHolder[] {
  const holders: FungibleHolder[] = [];
  for (const holder of balances.values()) {
    if (holder.balance < 0n) {
      throw new StreamdError(`The Indexer's history folds store ${holder.store} to a negative balance at version ${version}`);
    }
    if (holder.balance > 0n) holders.push(holder);
  }
  return holders.sort((a, b) => (a.balance === b.balance ? (a.store < b.store ? -1 : 1) : a.balance > b.balance ? -1 : 1));
}

function view(call: Call, adapter: EventAdapter, request: ViewRequest, version: bigint): Promise<unknown[]> {
  return call(() => adapter.view!(request, version, TRANSFER_DESCRIPTOR));
}

/** A Move `Option` a view returned first: its value, or `undefined` for none. */
function option(result: unknown[], what: string): unknown {
  const value = Array.isArray(result) ? (result[0] as { vec?: unknown } | undefined) : undefined;
  const vec = typeof value === "object" && value !== null ? value.vec : undefined;
  if (!Array.isArray(vec) || vec.length > 1) throw new StreamdNetworkError(`${what} returned no option`);
  return vec[0];
}

function amount(value: unknown, what: string): bigint {
  if (typeof value !== "string" || !UNSIGNED_INTEGER.test(value)) throw new StreamdNetworkError(`${what} returned no amount`);
  return BigInt(value);
}

/** Refuses a coin-paired asset, and a dispatchable one unless every store will be checked. */
async function refuseUnproven(call: Call, adapter: EventAdapter, asset: string, at: bigint, verify: HolderVerification): Promise<boolean> {
  const paired = await view(call, adapter, { function: "0x1::coin::paired_coin", typeArguments: [], functionArguments: [asset] }, at);
  if (option(paired, "0x1::coin::paired_coin") !== undefined) {
    throw new StreamdError(`holdersAt does not fold ${asset}: it is paired with a coin, and CoinStore balances are not in fungible stores`);
  }
  const request = { function: "0x1::fungible_asset::is_asset_type_dispatchable", typeArguments: [], functionArguments: [asset] } as const;
  const [dispatchable] = await view(call, adapter, request, at);
  if (typeof dispatchable !== "boolean") throw new StreamdNetworkError(`${request.function} returned no boolean`);
  if (dispatchable && verify !== "stores") {
    throw new StreamdError(
      `${asset} is dispatchable: its hooks can move balances in ways the fold was never checked against, so holdersAt needs verify: "stores"`,
    );
  }
  return dispatchable;
}

/** The checks `verify` asks for, each a view at `version`; returns the supply when it was read. */
async function verifyAt(
  call: Call,
  adapter: EventAdapter,
  asset: string,
  version: bigint,
  holders: FungibleHolder[],
  total: bigint,
  verify: HolderVerification,
  left: () => number,
): Promise<bigint | undefined> {
  if (verify === "none") return undefined;
  const fn = "0x1::fungible_asset::supply";
  const tracked = option(await view(call, adapter, { function: fn, typeArguments: [METADATA], functionArguments: [asset] }, version), fn);
  const supply = tracked === undefined ? undefined : amount(tracked, fn);
  if (supply === undefined && verify === "supply") {
    throw new StreamdError(`${asset} has no tracked supply at version ${version}, or is not a fungible asset there; pass verify: "stores" or "none"`);
  }
  if (supply !== undefined && supply !== total) {
    throw new StreamdError(`The fold of ${asset} sums to ${total} at version ${version}, but its supply there is ${supply}`);
  }
  if (verify !== "stores") return supply;
  if (holders.length > left()) {
    throw new StreamdError(`holdersAt has ${left()} of its maxRequests left, too few to check ${holders.length} stores`);
  }
  for (const { store, balance } of holders) {
    const request: ViewRequest = { function: "0x1::fungible_asset::balance", typeArguments: ["0x1::fungible_asset::FungibleStore"], functionArguments: [store] };
    const [held] = await view(call, adapter, request, version);
    const actual = amount(held, request.function);
    if (actual !== balance) {
      throw new StreamdError(`The fold puts store ${store} at ${balance} at version ${version}, but it held ${actual}`);
    }
  }
  return supply;
}

/**
 * Every store holding `asset` at `version`, folded backward from the
 * Indexer's current balances over its activity rows.
 */
export async function holdersAt(
  host: HoldersHost,
  asset: string,
  version: bigint,
  options: HoldersAtOptions = {},
): Promise<HolderSnapshot> {
  const checked = checkOptions(options);
  const metadata = requireFungibleAsset(asset);
  requireLedgerVersion(version);
  const { indexer, adapter } = host;
  if (typeof adapter.view !== "function") throw new StreamdError("holdersAt needs an adapter that implements view");
  const { call, requests, left } = counter(host.run, checked);
  const u0 = await call(() => indexer.getFungibleAssetVersion());
  if (version > u0) {
    throw new StreamdError(`The Indexer has processed fungible assets only through version ${u0}, before ${version}`);
  }
  // Pairing and hooks are set when an asset is created, so the processed version answers for `version` too.
  const dispatchable = await refuseUnproven(call, adapter, metadata, u0, checked.verify);
  const stores = await currentStores(call, indexer, metadata);
  let newest = u0;
  for (const row of stores.values()) if (row.lastVersion > newest) newest = row.lastVersion;
  const indexedVersion = await processedThrough(call, indexer, newest, checked.signal);
  const activities = await activitiesIn(call, indexer, metadata, version + 1n, indexedVersion);
  const holders = ranked(await fold(call, indexer, metadata, u0, stores, activities), version);
  const total = holders.reduce((sum, holder) => sum + holder.balance, 0n);
  const supply = await verifyAt(call, adapter, metadata, version, holders, total, checked.verify, left);
  return {
    asset: metadata,
    version,
    indexedVersion,
    holders,
    total,
    ...(supply !== undefined && { supply }),
    verified: checked.verify,
    dispatchable,
    requests: requests(),
  };
}
