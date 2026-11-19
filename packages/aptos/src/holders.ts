import { StreamdError, StreamdNetworkError, requireAddress, sleep, withRetries } from "@streamd/core";
import type { EventAdapter } from "@streamd/core";
import type { FungibleActivityRow, FungibleStoreRow } from "./indexer.js";
import type { FungibleAssetIndexer, FungibleHolder, HolderSnapshot, HoldersAtOptions, HolderVerification } from "./types.js";

const PAGE = 100;
const PROCESSOR_READS = 10;
const PROCESSOR_WAIT = 1000;
const APT = `0x${"a".padStart(64, "0")}`;
const DEPOSITS = new Set(["0x1::fungible_asset::Deposit", "0x1::fungible_asset::DepositEvent"]);
const WITHDRAWS = new Set(["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::WithdrawEvent"]);
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
function counter(run: HoldersHost["run"], { maxRequests, retries, signal }: Checked) {
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
  return { call, requests: () => requests };
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
export async function activitiesIn(
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
    if (current === undefined) orphans.set(row.store, [...(orphans.get(row.store) ?? []), row]);
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
        throw new StreamdNetworkError(`The Indexer's walk missed store ${store}, which changed at or before version ${u0}`);
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
      throw new StreamdNetworkError(`The Indexer's history folds store ${holder.store} to a negative balance at version ${version}`);
    }
    if (holder.balance > 0n) holders.push(holder);
  }
  return holders.sort((a, b) => (a.balance === b.balance ? (a.store < b.store ? -1 : 1) : a.balance > b.balance ? -1 : 1));
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
  const { indexer } = host;
  const { call, requests } = counter(host.run, checked);
  const u0 = await call(() => indexer.getFungibleAssetVersion());
  if (version > u0) {
    throw new StreamdError(`The Indexer has processed fungible assets only through version ${u0}, before ${version}`);
  }
  const stores = await currentStores(call, indexer, asset);
  let newest = u0;
  for (const row of stores.values()) if (row.lastVersion > newest) newest = row.lastVersion;
  const indexedVersion = await processedThrough(call, indexer, newest, checked.signal);
  const activities = await activitiesIn(call, indexer, asset, version + 1n, indexedVersion);
  const holders = ranked(await fold(call, indexer, asset, u0, stores, activities), version);
  const total = holders.reduce((sum, holder) => sum + holder.balance, 0n);
  return { asset, version, indexedVersion, holders, total, verified: "none", dispatchable: false, requests: requests() };
}
