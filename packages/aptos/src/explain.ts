import { StreamdError, checkTimeout, committedTransaction, normalizeAddress, raceAbort, requireTransactionHash } from "@streamd/core";
import type { EventAdapter, EventPage, RequestLimiter, StreamdEvent, TransactionEvents, Transfer } from "@streamd/core";
import { deltas } from "./activity.js";
import { u } from "./records.js";
import { toSale } from "./sales.js";
import { toSwap } from "./swaps.js";
import { simulation } from "./transaction.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import type { AccountBalanceChange, ExplainOptions, GasUsage, TransactionExplanation } from "./types.js";

const FEE_STATEMENT = "0x1::transaction_fee::FeeStatement";
const KEYS = ["signerPublicKey", "secondarySignersPublicKeys", "feePayerPublicKey"] as const;

/** What `explain` reads through: the client's adapter, limiter, name cache and timing. */
export interface ExplainHost {
  adapter: EventAdapter;
  limiter: RequestLimiter;
  /** The client's name cache: the `withNames` expiry, or a refusal, and one address's primary name. */
  names: {
    ttl(withNames: unknown): number | undefined;
    name(address: string, ttl: number, signal?: AbortSignal): Promise<string | null>;
  };
  pollInterval: number;
  requestTimeout?: number;
}

function gasOf(events: StreamdEvent[], unitPrice: bigint | undefined): GasUsage | undefined {
  const statement = events.find((e) => e.type === FEE_STATEMENT);
  if (statement === undefined) return undefined;
  const data = (statement.data ?? {}) as Record<string, unknown>;
  const gas: GasUsage = {
    units: u(data.total_charge_gas_units, statement),
    executionUnits: u(data.execution_gas_units, statement),
    ioUnits: u(data.io_gas_units, statement),
    storageFee: u(data.storage_fee_octas, statement),
    storageRefund: u(data.storage_fee_refund_octas, statement),
  };
  if (unitPrice !== undefined) {
    gas.unitPrice = unitPrice;
    gas.fee = gas.units * unitPrice;
  }
  return gas;
}

function balanceChanges(transfers: Transfer[]): AccountBalanceChange[] {
  const accounts = new Set(transfers.flatMap((t) => [t.from, t.to]));
  accounts.delete(undefined);
  return [...accounts].flatMap((account) =>
    deltas(transfers, account!).map(({ asset, delta }) => {
      const info = transfers.find((t) => t.asset === asset && t.symbol !== undefined);
      return { account: account!, asset, delta, ...(info && { symbol: info.symbol, decimals: info.decimals }) };
    }),
  );
}

/** The transaction's transfers, paired by `transferSource` over its events as one page. */
async function transfersOf(
  host: ExplainHost,
  found: TransactionEvents,
  shift: bigint,
  withAssetInfo: boolean,
): Promise<Transfer[]> {
  const page: EventPage = { events: found.events, nextCursor: undefined, hasMore: false };
  const limited = host.limiter.wrap(host.adapter);
  const feed: EventAdapter = {
    fetchEvents: async () => page,
    view: (request, version, descriptor) => limited.view!(request, version - shift, descriptor),
  };
  const source = transferSource(feed, undefined, undefined, withAssetInfo, host.requestTimeout);
  return (await source.fetchEvents(TRANSFER_DESCRIPTOR, {})).events.map((e) => e.data as Transfer);
}

/**
 * Looks the transaction up, waiting for a hash to commit, or simulates a
 * built one; `shift` is how far a simulation's version is ahead of the state
 * its views can read.
 */
async function lookUp(host: ExplainHost, target: unknown, options: ExplainOptions) {
  const { adapter, limiter } = host;
  const { signal } = options;
  if (typeof target === "string") {
    requireTransactionHash(target);
    if (KEYS.some((key) => options[key] !== undefined)) {
      throw new StreamdError("Signer keys are for simulating a built transaction, not for a transaction hash");
    }
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    const found = await committedTransaction(adapter, limiter, host.pollInterval, target, undefined, options);
    return { found, shift: 0n };
  }
  const request = simulation(target, options);
  if (adapter.simulateTransactionEvents === undefined || adapter.getHeadVersion === undefined) {
    throw new StreamdError("explain needs an adapter that implements simulateTransactionEvents and getHeadVersion to simulate");
  }
  if (signal?.aborted) throw signal.reason;
  const found = await limiter.run(() => adapter.simulateTransactionEvents!(request), signal, signal);
  // The node simulates on top of its newest state, at a version not committed
  // yet, and another node behind the same URL may not have reached it.
  const head = await limiter.run(() => adapter.getHeadVersion!(TRANSFER_DESCRIPTOR), signal, signal);
  const newest = found.version > 0n ? found.version - 1n : 0n;
  const base = newest < head ? newest : head;
  return { found, shift: found.version - base };
}

async function namesOf(host: ExplainHost, explanation: TransactionExplanation, ttl: number, signal?: AbortSignal) {
  const addresses = new Set([
    explanation.sender,
    explanation.feePayer,
    ...explanation.transfers.flatMap((t) => [t.from, t.to]),
    ...explanation.sales.flatMap((s) => [s.seller, s.buyer]),
  ]);
  addresses.delete(undefined);
  const named = await Promise.all([...addresses].map(async (a) => [a!, await host.names.name(a!, ttl, signal)] as const));
  return Object.fromEntries(named);
}

/**
 * The structured summary behind `AptosClient.explain`: `target` is a
 * transaction hash, waited for as `eventsOf` waits, or a built transaction,
 * simulated as `previewEvents` simulates it. A failed one is explained, not
 * thrown. Bad input is refused before any read.
 */
export async function explain(host: ExplainHost, target: unknown, options: ExplainOptions = {}): Promise<TransactionExplanation> {
  const ttl = host.names.ttl(options.withNames);
  if (typeof host.adapter.view !== "function") {
    throw new StreamdError("explain needs an adapter that implements view, for its transfer reads");
  }
  const { found, shift } = await lookUp(host, target, options);
  const work = async (): Promise<TransactionExplanation> => {
    const events = found.events;
    const first = events[0];
    const sender = first?.sender === undefined ? undefined : normalizeAddress(first.sender);
    const transfers = await transfersOf(host, found, shift, options.withAssetInfo !== false);
    const explanation: TransactionExplanation = {
      simulated: typeof target !== "string",
      version: found.version,
      success: found.success,
      vmStatus: found.vmStatus,
      transfers,
      balanceChanges: balanceChanges(transfers),
      swaps: events.flatMap((e) => toSwap(e)?.data ?? []),
      sales: events.flatMap((e) => toSale(e)?.data ?? []),
      events,
    };
    if (typeof target === "string") explanation.transactionHash = first?.transactionHash ?? target;
    if (sender !== undefined) explanation.sender = sender;
    if (found.feePayer !== undefined) explanation.feePayer = found.feePayer;
    if (first?.timestamp !== undefined) explanation.timestamp = first.timestamp;
    if (first?.entryFunction !== undefined) explanation.entryFunction = first.entryFunction;
    const gas = gasOf(events, found.gasUnitPrice);
    if (gas !== undefined) explanation.gas = gas;
    if (ttl !== undefined) explanation.names = await namesOf(host, explanation, ttl, options.signal);
    return explanation;
  };
  return raceAbort(work(), options.signal);
}
