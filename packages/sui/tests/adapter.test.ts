import { describe, expect, it } from "bun:test";
import { StreamdClient, StreamdError, StreamdNetworkError, checkAdapter, eventId } from "@streamd/core";
import type { EventDescriptor, MoveEventType } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiAdapter } from "../src/index.js";
import type { SuiLedgerClient } from "../src/index.js";

const LONG = (n: string) => `0x${n.padStart(64, "0")}`;
const STAKE = `${LONG("3")}::validator::StakingRequestEvent` as MoveEventType;
const SWAP = `${LONG("abc")}::pool::Swap` as MoveEventType;
const ALICE = LONG("a11ce");

interface FakeEvent {
  cp: bigint;
  ti: bigint;
  ei: number;
  type: string;
  sender?: string;
  amount?: string;
}

const str = (s: string) => ({ kind: { oneofKind: "stringValue" as const, stringValue: s } });

/**
 * Checkpoint n holds `sizes[n]` transactions; events are scanned in order. The
 * cursor is the next event's position as one byte; each event frame's
 * watermark is the checkpoint before it, and the last frame's the scan's end.
 */
function fakeLedger(sizes: number[], events: FakeEvent[], fail?: () => unknown, prunedBelow = 0n) {
  const totals = sizes.map((_, i) => BigInt(sizes.slice(0, i + 1).reduce((a, b) => a + b, 0)));
  const calls = { checkpoints: [] as string[], list: [] as Record<string, unknown>[] };
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
    async getCheckpoint({ checkpointId }) {
      const seq = checkpointId.oneofKind === "sequenceNumber" ? checkpointId.sequenceNumber : BigInt(sizes.length - 1);
      calls.checkpoints.push(String(checkpointId.oneofKind === undefined ? "latest" : seq));
      if (seq < prunedBelow) throw Object.assign(new Error(`Checkpoint ${seq} not found`), { code: "NOT_FOUND" });
      return {
        response: {
          checkpoint: {
            sequenceNumber: seq,
            summary: { totalNetworkTransactions: totals[Number(seq)]!, timestamp: { seconds: 1_700_000_000n + seq, nanos: 5_000 } },
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

// Checkpoint totals 1, 4, 6, 10: versions 0 | 1-3 | 4-5 | 6-9.
const SIZES = [1, 3, 2, 4];
const EVENTS: FakeEvent[] = [
  { cp: 0n, ti: 0n, ei: 0, type: STAKE, sender: ALICE },
  { cp: 1n, ti: 2n, ei: 0, type: SWAP, sender: LONG("b0b") },
  { cp: 1n, ti: 2n, ei: 1, type: STAKE, sender: LONG("b0b") },
  { cp: 2n, ti: 0n, ei: 0, type: STAKE, sender: ALICE, amount: "18446744073709551615" },
  { cp: 3n, ti: 3n, ei: 4, type: SWAP, sender: ALICE },
];
const stakes: EventDescriptor = { kind: "module", eventType: "0x3::validator::StakingRequestEvent" };

describe("SuiAdapter", () => {
  it("maps events field by field, with versions from checkpoint totals", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const page = await new SuiAdapter(client).fetchEvents(stakes, { fromVersion: 0n, limit: 10 });
    expect(page.events).toEqual([
      { type: STAKE, data: { amount: "1" }, version: 0n, sequenceNumber: 0n, eventIndex: 0, transactionHash: "D0t0", sender: ALICE, timestamp: 1_700_000_000_000_005n },
      { type: STAKE, data: { amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 1, transactionHash: "D1t2", sender: LONG("b0b"), timestamp: 1_700_000_001_000_005n },
      { type: STAKE, data: { amount: "18446744073709551615" }, version: 4n, sequenceNumber: 0n, eventIndex: 0, transactionHash: "D2t0", sender: ALICE, timestamp: 1_700_000_002_000_005n },
    ]);
    expect(page.hasMore).toBe(false);
    expect(page.throughVersion).toBe(9n);
  });

  it("reads each checkpoint summary once", async () => {
    const { client, calls } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    await adapter.fetchEvents(stakes, { fromVersion: 0n, limit: 10 });
    await adapter.fetchEvents(stakes, { fromVersion: 0n, limit: 10 });
    const counted = calls.checkpoints.filter((c) => c !== "latest");
    expect(counted.length).toBe(new Set(counted).size);
  });

  it("builds ORs of ANDs with long types, for type lists and account filters", async () => {
    const { client, calls } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    const both = await adapter.fetchEvents({ kind: "module", eventType: ["0x3::validator::StakingRequestEvent", SWAP] }, { fromVersion: 0n, limit: 10 });
    expect(both.events.map(eventId)).toEqual(["0:0", "3:0", "3:1", "4:0", "9:4"]);
    const sent = await adapter.fetchEvents({ kind: "account", accountAddress: "0xa11ce" }, { fromVersion: 0n, limit: 10 });
    expect(sent.events.map(eventId)).toEqual(["0:0", "4:0", "9:4"]);
    const sentSwaps = await adapter.fetchEvents({ kind: "account", accountAddress: ALICE, eventTypes: [SWAP] }, { fromVersion: 0n, limit: 10 });
    expect(sentSwaps.events.map(eventId)).toEqual(["9:4"]);
    expect(calls.list.at(-1)!.filter).toEqual({
      terms: [{ literals: [
        { negated: false, predicate: { oneofKind: "sender", sender: { address: ALICE } } },
        { negated: false, predicate: { oneofKind: "eventType", eventType: { eventType: SWAP } } },
      ] }],
    });
    expect((await adapter.fetchEvents({ kind: "account", accountAddress: ALICE, eventTypes: [] }, {})).events).toEqual([]);
  });

  it("pages by cursor with hasMore from the end reason, and throughVersion from the watermark", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    const first = await adapter.fetchEvents(stakes, { fromVersion: 0n, limit: 2 });
    expect(first.events.map(eventId)).toEqual(["0:0", "3:1"]);
    expect(first.hasMore).toBe(true);
    expect(first.throughVersion).toBe(3n);
    const rest = await adapter.fetchEvents(stakes, { cursor: first.nextCursor, limit: 2 });
    expect(rest.events.map(eventId)).toEqual(["4:0"]);
    expect(rest.hasMore).toBe(false);
    await expect(adapter.fetchEvents(stakes, { cursor: "not base64!" })).rejects.toBeInstanceOf(StreamdError);
  });

  it("refuses handle and resource descriptors and bad types, and maps gRPC failures to network errors", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    for (const d of [
      { kind: "handle", accountAddress: ALICE, creationNumber: 1n },
      { kind: "resource", accountAddress: ALICE, resourceType: "0x1::m::R" },
      { kind: "module", eventType: [] },
      { kind: "module", eventType: "not a type" },
    ] as EventDescriptor[]) {
      await expect(adapter.fetchEvents(d, {})).rejects.toBeInstanceOf(StreamdError);
      await expect(adapter.fetchEvents(d, {})).rejects.not.toBeInstanceOf(StreamdNetworkError);
    }
    const limited = fakeLedger(SIZES, EVENTS, () => Object.assign(new Error("slow down"), { code: "RESOURCE_EXHAUSTED" }));
    const err = (await new SuiAdapter(limited.client).fetchEvents(stakes, { fromVersion: 0n }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(429);
    expect(() => new SuiAdapter({} as never)).toThrow("SuiGrpcClient");
  });

  it("takes a real SuiGrpcClient", () => {
    // Compiled by typecheck: the SDK's client fits the structural type.
    const accepts = (client: SuiGrpcClient) => new SuiAdapter(client);
    expect(typeof accepts).toBe("function");
  });
});

describe("SuiAdapter versions as bounds", () => {
  it("reports the head and earliest versions from checkpoint totals", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    expect(await adapter.getHeadVersion()).toBe(9n);
    expect(await adapter.getEarliestVersion()).toBe(0n);
  });

  it("returns events up to toVersion inside its checkpoint, a single-version range, and the head's checkpoint", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    const all = { kind: "module" as const, eventType: [STAKE, SWAP] };
    expect((await adapter.fetchEvents(all, { fromVersion: 3n, toVersion: 3n, limit: 10 })).events.map(eventId)).toEqual(["3:0", "3:1"]);
    expect((await adapter.fetchEvents(all, { fromVersion: 6n, toVersion: 9n, limit: 10 })).events.map(eventId)).toEqual(["9:4"]);
    const streamd = new StreamdClient({ adapter });
    expect((await streamd.getEvents(all, { fromVersion: 4n, toVersion: "head" })).map(eventId)).toEqual(["4:0", "9:4"]);
    streamd.destroy();
  });

  it("refuses a start the node cannot place as 410, before any scan", async () => {
    // The node reports checkpoint 0 as lowest but keeps only 2 and up; 2's versions need 1.
    const { client, calls } = fakeLedger(SIZES, EVENTS, undefined, 2n);
    const adapter = new SuiAdapter(client);
    for (const fromVersion of [0n, 4n]) {
      const err = (await adapter.fetchEvents(stakes, { fromVersion }).catch((e: unknown) => e)) as StreamdNetworkError;
      expect(err.statusCode).toBe(410);
    }
    expect(calls.list).toEqual([]);
    const outOfRange = fakeLedger(SIZES, EVENTS, () =>
      Object.assign(new Error("requested data below earliest available"), { code: "OUT_OF_RANGE" }),
    );
    const err = (await new SuiAdapter(outOfRange.client).fetchEvents(stakes, { fromVersion: 0n }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err.statusCode).toBe(410);
    // Above the indexed tip is a lag to retry, not pruned history.
    const ahead = fakeLedger(SIZES, EVENTS, () =>
      Object.assign(new Error("requested data above latest available; end_checkpoint is 9"), { code: "OUT_OF_RANGE" }),
    );
    const lag = (await new SuiAdapter(ahead.client).fetchEvents(stakes, { fromVersion: 0n }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(lag).toBeInstanceOf(StreamdNetworkError);
    expect(lag.statusCode).toBeUndefined();
  });

  it("searches past checkpoints the node has pruned below its reported lowest", async () => {
    // Service info says checkpoint 0 is the lowest, but 0 and 1 are gone.
    const { client } = fakeLedger(SIZES, EVENTS, undefined, 2n);
    const adapter = new SuiAdapter(client);
    // Checkpoint 2's own versions need checkpoint 1's total, so the earliest placeable is checkpoint 3's start.
    expect(await adapter.getEarliestVersion()).toBe(6n);
    const err = (await adapter.fetchEvents(stakes, { fromVersion: 0n, limit: 10 }).catch((e: unknown) => e)) as StreamdNetworkError;
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(410);
    const page = await adapter.fetchEvents({ kind: "module", eventType: [SWAP] }, { fromVersion: 6n, limit: 10 });
    expect(page.events.map(eventId)).toEqual(["9:4"]);
  });

  it("turns fromVersion and toVersion into checkpoints and drops events outside them", async () => {
    const { client, calls } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    const page = await adapter.fetchEvents({ kind: "module", eventType: [STAKE, SWAP] }, { fromVersion: 3n, toVersion: 4n, limit: 10 });
    expect(page.events.map(eventId)).toEqual(["3:0", "3:1", "4:0"]);
    expect(calls.list.at(-1)).toMatchObject({ startCheckpoint: 1n, endCheckpoint: 3n });
    expect(page.throughVersion).toBe(4n);
    // Version 7 is inside checkpoint 3 (6 to 9), which also holds the swap at 9.
    const capped = await adapter.fetchEvents({ kind: "module", eventType: [SWAP] }, { fromVersion: 6n, toVersion: 7n, limit: 10 });
    expect(calls.list.at(-1)).toMatchObject({ endCheckpoint: 4n });
    expect(capped.events).toEqual([]);
    expect(capped.throughVersion).toBe(7n);
    // Version 5 is the last of checkpoint 2, so the scan starts there and drops version 4.
    const later = await adapter.fetchEvents(stakes, { fromVersion: 5n, limit: 10 });
    expect(later.events).toEqual([]);
    expect(calls.list.at(-1)).toMatchObject({ startCheckpoint: 2n });
    // No start and no cursor: the newest checkpoint, as a live start at the head.
    await adapter.fetchEvents(stakes, {});
    expect(calls.list.at(-1)).toMatchObject({ startCheckpoint: 3n });
  });

  it("serves a client query and a live subscription in order, none repeated", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const streamd = new StreamdClient({ adapter: new SuiAdapter(client) });
    const all = { kind: "module", eventType: [STAKE, SWAP] } as const;
    expect((await streamd.getEvents(all, { fromVersion: 0n })).map(eventId)).toEqual(["0:0", "3:0", "3:1", "4:0", "9:4"]);
    // Six versions back from head 9 is version 3.
    const recent = await streamd.getEvents(all, { fromVersion: -6n, toVersion: "head" });
    expect(recent.map(eventId)).toEqual(["3:0", "3:1", "4:0", "9:4"]);
    const seen: string[] = [];
    const sub = streamd.on(all, (e) => void seen.push(eventId(e)), { fromVersion: 0n, batchSize: 2, pollInterval: 5 });
    await sub.caughtUp;
    sub.unsubscribe();
    streamd.destroy();
    expect(seen).toEqual(["0:0", "3:0", "3:1", "4:0", "9:4"]);
  });

  it("passes checkAdapter", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const report = await checkAdapter(new SuiAdapter(client), { kind: "module", eventType: [STAKE, SWAP] }, { limit: 2, fromVersion: 0n });
    expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
  });
});
