import { normalizeStructTag, normalizeSuiAddress } from "@mysten/sui/utils";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";

/** The part of a `ListTransactions` transaction that transfers are built from. */
export interface BalanceChangeTransaction {
  digest?: string;
  timestamp?: { seconds: bigint; nanos: number };
  transaction?: { sender?: string; gasPayment?: { owner?: string } };
  effects?: { gasUsed?: { computationCost?: bigint; storageCost?: bigint; storageRebate?: bigint } };
  balanceChanges?: { address?: string; coinType?: string; amount?: string }[];
  events?: { events: unknown[] };
}

const SUI = normalizeStructTag("0x2::sui::SUI");
const SIGNED_INTEGER = /^-?\d+$/;

/** A coin type in the long form balance changes carry. */
export function coinType(type: unknown): string {
  try {
    return normalizeStructTag(type as string);
  } catch {
    throw new StreamdError(`Not a valid Sui coin type: ${String(type)}`);
  }
}

export const micros = (stamp: { seconds: bigint; nanos: number }) => stamp.seconds * 1_000_000n + BigInt(Math.floor(stamp.nanos / 1000));

/**
 * Each address's change per coin type, with the gas fee taken back out of
 * the payer's SUI: Sui nets it into the balance change, and a fee is not a
 * transfer. A payer with no listed change paid exactly what it received.
 */
function flows(txn: BalanceChangeTransaction, asset: string | undefined): Map<string, Map<string, bigint>> {
  const out = new Map<string, Map<string, bigint>>();
  const add = (type: string, address: string, amount: bigint) => {
    if (asset !== undefined && type !== asset) return;
    const byOwner = out.get(type) ?? out.set(type, new Map()).get(type)!;
    byOwner.set(address, (byOwner.get(address) ?? 0n) + amount);
  };
  for (const { address, coinType: type, amount } of txn.balanceChanges ?? []) {
    if (typeof address !== "string" || typeof type !== "string" || typeof amount !== "string" || !SIGNED_INTEGER.test(amount)) {
      throw new StreamdError(`Sui node returned a malformed balance change in transaction ${txn.digest}`);
    }
    add(coinType(type), normalizeSuiAddress(address), BigInt(amount));
  }
  const payer = txn.transaction?.gasPayment?.owner;
  const gas = txn.effects?.gasUsed;
  if (payer !== undefined && payer !== "" && gas !== undefined) {
    add(SUI, normalizeSuiAddress(payer), (gas.computationCost ?? 0n) + (gas.storageCost ?? 0n) - (gas.storageRebate ?? 0n));
  }
  return out;
}

/**
 * One coin type's changes as transfers: each receiver, in listed order, takes
 * from the earliest sender with outflow left, as much as both have. Inflow no
 * sender covers has no `from`; outflow no receiver took has no `to`.
 */
function pair(asset: string, byOwner: Map<string, bigint>): Transfer[] {
  const senders = [...byOwner].filter(([, v]) => v < 0n).map(([owner, v]) => ({ owner, left: -v }));
  const out: Transfer[] = [];
  for (const [to, inflow] of byOwner) {
    let need = inflow;
    for (const sender of senders) {
      if (need <= 0n) break;
      if (sender.left === 0n) continue;
      const amount = sender.left < need ? sender.left : need;
      sender.left -= amount;
      need -= amount;
      out.push({ asset, amount, from: sender.owner, to });
    }
    if (need > 0n) out.push({ asset, amount: need, from: undefined, to });
  }
  for (const { owner, left } of senders) if (left > 0n) out.push({ asset, amount: left, from: owner, to: undefined });
  return out;
}

/**
 * The transfers one transaction made, at `version`, narrowed to `asset` (a
 * long-form coin type) when given. Each address's records sum to its change,
 * less gas. Records follow the coin types in listed order, and each takes
 * `eventIndex` the transaction's event count plus its position, so it never
 * shares an id with an event.
 */
export function transfersOf(txn: BalanceChangeTransaction, version: bigint, asset?: string): StreamdEvent<Transfer>[] {
  const records = [...flows(txn, asset)].flatMap(([type, byOwner]) => pair(type, byOwner));
  const first = txn.events?.events.length ?? 0;
  const sender = txn.transaction?.sender;
  return records.map((data, position) => {
    const event: StreamdEvent<Transfer> = { type: data.asset, data, version, sequenceNumber: 0n, eventIndex: first + position };
    if (txn.digest !== undefined) event.transactionHash = txn.digest;
    if (txn.timestamp !== undefined) event.timestamp = micros(txn.timestamp);
    if (sender !== undefined && sender !== "") event.sender = normalizeSuiAddress(sender);
    return event;
  });
}
