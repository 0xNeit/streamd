import { toBase58 } from "@mysten/sui/utils";
import type { SuiLedgerClient } from "../src/index.js";

/** A valid Sui digest: base58 of 32 bytes, all `n`. */
export const digest = (n: number) => toBase58(new Uint8Array(32).fill(n));

export const LONG = (n: string) => `0x${n.padStart(64, "0")}`;

export interface FakeEvent {
  cp: bigint;
  ti: bigint;
  ei: number;
  type: string;
  sender?: string;
  amount?: string;
}

/** A transaction `getTransaction` knows; `cp` undefined means executed but not yet checkpointed. */
export interface FakeTransaction {
  digest: string;
  cp?: bigint;
  ti: bigint;
  error?: string;
  events: { type: string; sender?: string; amount?: string }[];
}

export const str = (s: string) => ({ kind: { oneofKind: "stringValue" as const, stringValue: s } });

/**
 * Checkpoint n holds `sizes[n]` transactions; events are scanned in order. The
 * cursor is the next event's position as one byte; each event frame's
 * watermark is the checkpoint before it, and the last frame's the scan's end.
 */
export function fakeLedger(
  sizes: number[],
  events: FakeEvent[],
  fail?: () => unknown,
  prunedBelow = 0n,
  transactions: FakeTransaction[] = [],
): {
  client: SuiLedgerClient;
  calls: { checkpoints: string[]; list: Record<string, unknown>[]; transactions: string[] };
} {
  const totals = sizes.map((_, i) => BigInt(sizes.slice(0, i + 1).reduce((a, b) => a + b, 0)));
  const calls = { checkpoints: [] as string[], list: [] as Record<string, unknown>[], transactions: [] as string[] };
  const literalMatch = (e: FakeEvent, l: { predicate: { oneofKind: string; eventType?: { eventType: string }; sender?: { address: string } } }) =>
    l.predicate.oneofKind === "eventType" ? e.type === l.predicate.eventType!.eventType : e.sender === l.predicate.sender!.address;
  const ledger: SuiLedgerClient["ledgerService"] = {
    listEvents(input) {
      calls.list.push(input as unknown as Record<string, unknown>);
      async function* responses() {
        const err = fail?.();
        if (err !== undefined) throw err;
        if (input.startCheckpoint !== undefined && input.startCheckpoint < prunedBelow) {
          throw Object.assign(new Error("requested data below earliest available"), { code: "OUT_OF_RANGE" });
        }
        const start = input.options.after !== undefined ? input.options.after[0]! : 0;
        // The end bound is exclusive, as the proto documents.
        const last = input.endCheckpoint === undefined ? BigInt(sizes.length - 1) : input.endCheckpoint - 1n;
        let sent = 0;
        let i = start;
        for (; i < events.length; i++) {
          const e = events[i]!;
          if (input.startCheckpoint !== undefined && e.cp < input.startCheckpoint) continue;
          if (e.cp > last) break;
          if (!input.filter!.terms.some((t) => t.literals.every((l) => literalMatch(e, l)))) continue;
          if (sent === input.options.limit) {
            yield { watermark: { cursor: new Uint8Array([i]), checkpoint: e.cp - 1n }, end: { reason: 1 } };
            return;
          }
          sent++;
          yield {
            event: {
              eventType: e.type,
              ...(e.sender !== undefined && { sender: e.sender }),
              json: { kind: { oneofKind: "structValue" as const, structValue: { fields: { amount: str(e.amount ?? "1") } } } },
              checkpoint: e.cp,
              transactionDigest: `D${e.cp}t${e.ti}`,
              transactionIndex: e.ti,
              eventIndex: e.ei,
            },
            watermark: { cursor: new Uint8Array([i + 1]), checkpoint: e.cp - 1n },
          };
        }
        yield { watermark: { cursor: new Uint8Array([i]), checkpoint: last }, end: { reason: 3 } };
      }
      return { responses: responses() };
    },
    async getTransaction({ digest }) {
      calls.transactions.push(digest);
      const err = fail?.();
      if (err !== undefined) throw err;
      const t = transactions.find((x) => x.digest === digest);
      if (t === undefined) throw Object.assign(new Error(`Transaction ${digest} not found`), { code: "NOT_FOUND" });
      const status = t.error === undefined ? { success: true } : { success: false, error: { description: t.error } };
      const json = (amount = "1") => ({ kind: { oneofKind: "structValue" as const, structValue: { fields: { amount: str(amount) } } } });
      return {
        response: {
          transaction: {
            digest,
            ...(t.cp !== undefined && { checkpoint: t.cp }),
            effects: { status },
            events: { events: t.events.map((e) => ({ eventType: e.type, ...(e.sender !== undefined && { sender: e.sender }), json: json(e.amount) })) },
          },
        },
      };
    },
    async getCheckpoint({ checkpointId, readMask }) {
      const seq = checkpointId.oneofKind === "sequenceNumber" ? checkpointId.sequenceNumber : BigInt(sizes.length - 1);
      calls.checkpoints.push(String(checkpointId.oneofKind === undefined ? "latest" : seq));
      if (seq < prunedBelow || seq >= BigInt(sizes.length)) throw Object.assign(new Error(`Checkpoint ${seq} not found`), { code: "NOT_FOUND" });
      return {
        response: {
          checkpoint: {
            sequenceNumber: seq,
            summary: { totalNetworkTransactions: totals[Number(seq)]!, timestamp: { seconds: 1_700_000_000n + seq, nanos: 5_000 } },
            ...(readMask.paths.includes("transactions.digest") && {
              transactions: Array.from({ length: sizes[Number(seq)]! }, (_, i) => ({
                digest: transactions.find((t) => t.cp === seq && t.ti === BigInt(i))?.digest ?? `other${i}`,
              })),
            }),
          },
        },
      };
    },
    async getServiceInfo() {
      return { response: { checkpointHeight: BigInt(sizes.length - 1), lowestAvailableCheckpoint: 0n } };
    },
  };
  return { client: { ledgerService: ledger }, calls };
}
