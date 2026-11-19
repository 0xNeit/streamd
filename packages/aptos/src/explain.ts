import { StreamdError, checkTimeout, committedTransaction, formatAmount, normalizeAddress, raceAbort, requireTransactionHash } from "@streamd/core";
import type { AssetValue, EventAdapter, EventPage, RequestLimiter, StreamdEvent, TransactionEvents, Transfer } from "@streamd/core";
import { deltas } from "./activity.js";
import { u } from "./records.js";
import { toSale } from "./sales.js";
import { toSwap } from "./swaps.js";
import { simulation } from "./transaction.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import { assetPricer, toAssetValue } from "./values.js";
import type { Pricer } from "./values.js";
import type { AccountBalanceChange, ExplainOptions, GasUsage, Price, TransactionExplanation } from "./types.js";

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
  /** Pyth's price of a feed at a version, through the client's limiter. */
  pyth: (feedId: string, version: bigint) => Promise<Price | undefined>;
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
      const priced = transfers.find((t) => t.asset === asset && t.value !== undefined)?.value;
      const change: AccountBalanceChange = { account: account!, asset, delta, ...(info && { symbol: info.symbol, decimals: info.decimals }) };
      if (priced !== undefined) change.value = toAssetValue(priced, delta);
      return change;
    }),
  );
}

/** The transaction's transfers, paired by `transferSource` over its events as one page. */
async function transfersOf(
  host: ExplainHost,
  found: TransactionEvents,
  shift: bigint,
  options: ExplainOptions,
): Promise<Transfer[]> {
  const { signal } = options;
  const page: EventPage = { events: found.events, nextCursor: undefined, hasMore: false };
  const limited = host.limiter.wrap(host.adapter, signal, signal);
  const feed: EventAdapter = {
    fetchEvents: async () => page,
    view: (request, version, descriptor) => limited.view!(request, version - shift, descriptor),
  };
  const prices = assetPricer((request, version) => limited.view!(request, version, TRANSFER_DESCRIPTOR), host.pyth);
  const pricer: Pricer | undefined = options.withValue ? (asset, version, run) => prices(asset, version - shift, run) : undefined;
  const source = transferSource(feed, undefined, undefined, options.withAssetInfo !== false, host.requestTimeout, pricer);
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
    if (adapter.getTransactionEvents === undefined) {
      throw new StreamdError("explain needs an adapter that implements getTransactionEvents to look a hash up");
    }
    const found = await committedTransaction(adapter, limiter, host.pollInterval, target, undefined, options);
    return { found, shift: 0n };
  }
  if (options.timeout !== undefined) {
    throw new StreamdError("timeout bounds the wait for a transaction hash; a simulation does not wait");
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
    const transfers = await transfersOf(host, found, shift, options);
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
    const feePayer = found.feePayer === undefined ? undefined : normalizeAddress(found.feePayer);
    if (feePayer !== undefined) explanation.feePayer = feePayer;
    if (first?.timestamp !== undefined) explanation.timestamp = first.timestamp;
    if (first?.entryFunction !== undefined) explanation.entryFunction = first.entryFunction;
    const gas = gasOf(events, found.gasUnitPrice);
    if (gas !== undefined) explanation.gas = gas;
    if (ttl !== undefined) explanation.names = await namesOf(host, explanation, ttl, options.signal);
    return explanation;
  };
  return raceAbort(work(), options.signal);
}

// C0 and C1 controls, which some terminals obey, and the bidirectional overrides that reorder a line.
const UNSAFE = /[\u0000-\u001f\u007f-\u009f\u200e\u200f\u202a-\u202e\u2066-\u2069]/g;
const APT_DECIMALS = 8;

/**
 * `explanation` as plain text, one fact per line: the header, status, sender
 * and fee payer, function, gas, then transfers, balance changes, swaps and
 * sales, and the event count. Amounts use the asset's decimals and symbol when
 * known (a swap takes them from its transaction's transfers), else base
 * units and the asset. On-chain text has control characters replaced.
 */
export function formatExplanation(explanation: TransactionExplanation): string {
  const e = explanation;
  const clean = (text: string) => text.replace(UNSAFE, "?");
  const who = (address: string | undefined) => {
    if (address === undefined) return "an unknown account";
    const name = e.names?.[address];
    return typeof name === "string" ? `${clean(address)} (${clean(name)})` : clean(address);
  };
  const details = new Map<string, { symbol: string; decimals: number }>();
  for (const t of e.transfers) {
    const { symbol, decimals } = t;
    // formatAmount refuses decimals a u8 cannot hold; such details are not used.
    if (symbol !== undefined && decimals !== undefined && Number.isInteger(decimals) && decimals >= 0 && decimals <= 255) {
      details.set(t.asset, { symbol, decimals });
    }
  }
  const amount = (value: bigint, asset: string, worth?: AssetValue, signed = false) => {
    const info = details.get(asset);
    let text = info === undefined ? `${value} ${clean(asset)}` : `${formatAmount(value, info.decimals)} ${clean(info.symbol)}`;
    if (signed && value > 0n) text = `+${text}`;
    // Every value is quoted in USDC; see values.ts.
    return worth === undefined ? text : `${text} (${formatAmount(worth.amount, worth.decimals)} USDC)`;
  };
  const apt = (octas: bigint) => `${formatAmount(octas, APT_DECIMALS)} APT`;

  const date = e.timestamp === undefined ? undefined : new Date(Number(e.timestamp / 1000n));
  const time = date === undefined || Number.isNaN(date.getTime()) ? "" : `, ${date.toISOString()}`;
  const lines = [
    `${e.simulated ? "Simulated transaction" : `Transaction ${clean(e.transactionHash ?? "")}`} at version ${e.version}${time}`,
    `Status: ${e.success ? "success" : "failed"} (${clean(e.vmStatus)})`,
  ];
  if (e.sender !== undefined) lines.push(`Sender: ${who(e.sender)}`);
  if (e.feePayer !== undefined) lines.push(`Fee payer: ${who(e.feePayer)}`);
  if (e.entryFunction !== undefined) lines.push(`Function: ${clean(e.entryFunction.function)}`);
  if (e.gas !== undefined) {
    const { units, unitPrice, fee, storageFee, storageRefund } = e.gas;
    const price = unitPrice === undefined || fee === undefined ? "" : ` at ${unitPrice} octas, fee ${apt(fee)}`;
    lines.push(`Gas: ${units} units${price} (storage ${apt(storageFee)}, refund ${apt(storageRefund)})`);
  }
  const section = (title: string, items: string[]) => {
    if (items.length > 0) lines.push(`${title}:`, ...items.map((item) => `  ${item}`));
  };
  section(
    "Transfers",
    e.transfers.map((t) =>
      [amount(t.amount, t.asset, t.value), t.from === undefined ? "" : ` from ${who(t.from)}`, t.to === undefined ? "" : ` to ${who(t.to)}`].join(""),
    ),
  );
  section("Balance changes", e.balanceChanges.map((c) => `${who(c.account)}: ${amount(c.delta, c.asset, c.value, true)}`));
  section(
    "Swaps",
    e.swaps.map((s) => `${s.protocol}: ${amount(s.amountIn, s.assetIn)} in, ${amount(s.amountOut, s.assetOut)} out, pool ${clean(s.poolId)}`),
  );
  section(
    "Sales",
    e.sales.map((s) => `${s.marketplace} ${s.kind}: ${clean(s.token)} from ${who(s.seller)} to ${who(s.buyer)} for ${s.price} base units`),
  );
  lines.push(`Events: ${e.events.length}`);
  return `${lines.join("\n")}\n`;
}
