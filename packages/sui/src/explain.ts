import { checkTimeout, committedTransaction, raceAbort } from "@streamd/core";
import type { EntryFunctionCall, EventAdapter, NameOptions, RequestLimiter, StreamdEvent, Transfer } from "@streamd/core";
import { requireDigest } from "./adapter.js";
import type { SuiAdapter, SuiGasUsage, SuiTransactionDetails } from "./adapter.js";

/** One account's net change in one coin type across a transaction's transfers. */
export interface SuiAccountBalanceChange {
  /** In long form. */
  account: string;
  /** A coin type in long form. */
  asset: string;
  /** Positive when the account gained. */
  delta: bigint;
  /** From the coin's metadata, unless `withAssetInfo: false`. */
  symbol?: string;
  decimals?: number;
}

/**
 * What one checkpointed Sui transaction did, as `SuiClient.explain` reports
 * it: `TransactionExplanation`'s fields where Sui has them, with Sui's gas.
 */
export interface SuiTransactionExplanation {
  /** Always false: only checkpointed transactions are explained. */
  simulated: false;
  version: bigint;
  /** The base58 digest. */
  transactionHash: string;
  success: boolean;
  /** `"success"`, or the execution error, as `eventsOf` reports it. */
  vmStatus: string;
  /** In long form. */
  sender?: string;
  /** Who paid the gas of a sponsored transaction, in long form; absent when the sender paid. */
  feePayer?: string;
  /** Microseconds since the Unix epoch. */
  timestamp?: bigint;
  /** The PTB's one `MoveCall`; absent when it has none or several. */
  entryFunction?: EntryFunctionCall;
  gas?: SuiGasUsage;
  /** Paired as `transfers()` pairs them; the gas fee is not a transfer. */
  transfers: Transfer[];
  /** Net per account and coin type over `transfers`, grouped by account in order of first appearance, zero nets left out. */
  balanceChanges: SuiAccountBalanceChange[];
  /** Empty: no Sui swap decoders yet. */
  swaps: never[];
  /** Empty: no Sui sale decoders yet. */
  sales: never[];
  /** Every event, in `eventIndex` order. */
  events: StreamdEvent[];
  /** Under `withNames`, the SuiNS primary name (`null` for none) of the sender, the fee payer and every transfer's `from` and `to`. */
  names?: Record<string, string | null>;
}

/** Options for `SuiClient.explain`. */
export interface SuiExplainOptions {
  /** How long to wait for the transaction to be checkpointed, in ms (default 20000), as `eventsOf` waits. */
  timeout?: number;
  /** Aborting rejects with the signal's `reason`. */
  signal?: AbortSignal;
  /** Adds `symbol` and `decimals` to transfers and balance changes, read once per coin type (default true). */
  withAssetInfo?: boolean;
  /** Attaches `names`, through the client's name cache (`true` for the default expiry, or `{ ttl }`). */
  withNames?: boolean | NameOptions;
}

/** What `explain` reads through: the client's ledger, limiter, name cache and polling interval. */
export interface ExplainHost {
  ledger: SuiAdapter;
  limiter: RequestLimiter;
  names: {
    ttl(withNames: unknown): number | undefined;
    name(address: string, ttl: number, signal?: AbortSignal): Promise<string | null>;
  };
  pollInterval: number;
}

function balanceChanges(transfers: Transfer[]): SuiAccountBalanceChange[] {
  const sums = new Map<string, Map<string, bigint>>();
  const add = (account: string | undefined, asset: string, delta: bigint) => {
    if (account === undefined) return;
    const byAsset = sums.get(account) ?? sums.set(account, new Map()).get(account)!;
    byAsset.set(asset, (byAsset.get(asset) ?? 0n) + delta);
  };
  for (const { asset, amount, from, to } of transfers) {
    add(from, asset, -amount);
    add(to, asset, amount);
  }
  return [...sums].flatMap(([account, byAsset]) =>
    [...byAsset].flatMap(([asset, delta]) => {
      if (delta === 0n) return [];
      const info = transfers.find((t) => t.asset === asset && t.symbol !== undefined);
      return [{ account, asset, delta, ...(info && { symbol: info.symbol, decimals: info.decimals }) }];
    }),
  );
}

async function withInfo(host: ExplainHost, transfers: Transfer[], signal?: AbortSignal): Promise<Transfer[]> {
  const assets = [...new Set(transfers.map((t) => t.asset))];
  const read = await Promise.all(assets.map((asset) => host.limiter.run(() => host.ledger.coinInfo(asset), signal, signal)));
  const info = new Map(assets.map((asset, i) => [asset, read[i]]));
  return transfers.map((t) => ({ ...t, ...info.get(t.asset) }));
}

async function namesOf(host: ExplainHost, e: SuiTransactionExplanation, ttl: number, signal?: AbortSignal) {
  const addresses = new Set([e.sender, e.feePayer, ...e.transfers.flatMap((t) => [t.from, t.to])]);
  addresses.delete(undefined);
  const named = await Promise.all([...addresses].map(async (a) => [a!, await host.names.name(a!, ttl, signal)] as const));
  return Object.fromEntries(named);
}

/**
 * The summary behind `SuiClient.explain`: waits for `digest` to be
 * checkpointed as `eventsOf` waits, then reads coin details and names. A
 * failed transaction is explained, not thrown. Bad input is refused before any read.
 */
export async function explain(host: ExplainHost, digest: string, options: SuiExplainOptions = {}): Promise<SuiTransactionExplanation> {
  requireDigest(digest);
  const ttl = host.names.ttl(options.withNames);
  const badTimeout = checkTimeout(options.timeout);
  if (badTimeout !== undefined) throw badTimeout;
  const lookup = { getTransactionEvents: (d: string) => host.ledger.getTransactionDetails(d) } as EventAdapter;
  const found = (await committedTransaction(lookup, host.limiter, host.pollInterval, digest, undefined, options)) as SuiTransactionDetails;
  const work = async (): Promise<SuiTransactionExplanation> => {
    const paired = found.transfers.map((t) => t.data);
    const transfers = options.withAssetInfo === false ? paired : await withInfo(host, paired, options.signal);
    const explanation: SuiTransactionExplanation = {
      simulated: false,
      version: found.version,
      transactionHash: digest,
      success: found.success,
      vmStatus: found.vmStatus,
      transfers,
      balanceChanges: balanceChanges(transfers),
      swaps: [],
      sales: [],
      events: found.events,
    };
    if (found.sender !== undefined) explanation.sender = found.sender;
    if (found.feePayer !== undefined && found.feePayer !== found.sender) explanation.feePayer = found.feePayer;
    if (found.timestamp !== undefined) explanation.timestamp = found.timestamp;
    if (found.call !== undefined) explanation.entryFunction = found.call;
    if (found.gas !== undefined) explanation.gas = found.gas;
    if (ttl !== undefined) explanation.names = await namesOf(host, explanation, ttl, options.signal);
    return explanation;
  };
  return raceAbort(work(), options.signal);
}
