import { describe, expect, it } from "bun:test";
import { StreamdClient, StreamdError, StreamdNetworkError, checkAdapter, eventId } from "@streamd/core";
import type { EventDescriptor, MoveEventType } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiAdapter } from "../src/index.js";
import { LONG, digest, fakeLedger, ptb, transactionBytes, withCalls, withSimulator, withTransfers, type FakeEvent, type FakeListed, type FakeTransaction } from "./helpers.js";

const STAKE = `${LONG("3")}::validator::StakingRequestEvent` as MoveEventType;
const SWAP = `${LONG("abc")}::pool::Swap` as MoveEventType;
const ALICE = LONG("a11ce");

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

// fakeLedger stamps checkpoint n at 1_700_000_000 + n seconds plus 5000 ns.
const stamp = (n: bigint) => 1_700_000_000_000_005n + n * 1_000_000n;

describe("SuiAdapter checkpoints as blocks", () => {
  it("reports checkpoint heights and maps a checkpoint to its versions and time", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const adapter = new SuiAdapter(client);
    expect(await adapter.getBlockHeights()).toEqual({ oldest: 0n, newest: 3n });
    expect(await adapter.getBlock(stakes, 0n)).toEqual({ height: 0n, firstVersion: 0n, lastVersion: 0n, timestamp: stamp(0n) });
    expect(await adapter.getBlock(stakes, 1n)).toEqual({ height: 1n, firstVersion: 1n, lastVersion: 3n, timestamp: stamp(1n) });
    expect(await adapter.getBlock(stakes, 3n)).toEqual({ height: 3n, firstVersion: 6n, lastVersion: 9n, timestamp: stamp(3n) });
  });

  it("rejects pruned checkpoints with 410, future ones with 404, and bad heights", async () => {
    const { client } = fakeLedger(SIZES, EVENTS, undefined, 2n);
    const adapter = new SuiAdapter(client);
    const status = async (height: bigint) => ((await adapter.getBlock(stakes, height).catch((e: unknown) => e)) as StreamdNetworkError).statusCode;
    // Checkpoint 1 is gone, and 2 is kept but its first version needs 1.
    expect(await status(1n)).toBe(410);
    expect(await status(2n)).toBe(410);
    expect(await status(4n)).toBe(404);
    expect((await adapter.getBlock(stakes, 3n)).firstVersion).toBe(6n);
    for (const height of [-1n, 1 as unknown as bigint]) {
      const err = await adapter.getBlock(stakes, height).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
  });

  it("refuses a checkpoint without a timestamp", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const read = client.ledgerService.getCheckpoint.bind(client.ledgerService);
    client.ledgerService.getCheckpoint = async (input) => {
      const { response } = await read(input);
      delete response.checkpoint!.summary!.timestamp;
      return { response };
    };
    await expect(new SuiAdapter(client).getBlock(stakes, 1n)).rejects.toThrow("without its timestamp");
  });

  it("passes checkAdapter's block rule", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    const report = await checkAdapter(new SuiAdapter(client), stakes, { limit: 2, fromVersion: 0n, blockHeight: 2n });
    expect(report.results.find((r) => r.name === "block")?.status).toBe("pass");
    expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
  });
});

describe("SuiAdapter time and block bounds through the client", () => {
  const all = { kind: "module", eventType: [STAKE, SWAP] } as const;

  it("starts fromTime at the first checkpoint stamped at or after it, clamped at both ends", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    using streamd = new StreamdClient({ adapter: new SuiAdapter(client) });
    const from = async (fromTime: bigint) => (await streamd.getEvents(all, { fromTime })).map(eventId);
    expect(await from(stamp(1n))).toEqual(["3:0", "3:1", "4:0", "9:4"]);
    expect(await from(stamp(1n) + 1n)).toEqual(["4:0", "9:4"]);
    expect(await from(0n)).toEqual(["0:0", "3:0", "3:1", "4:0", "9:4"]);
    expect(await from(stamp(3n) + 1n)).toEqual([]);
  });

  it("bounds a query by checkpoint with fromBlock and toBlock", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    using streamd = new StreamdClient({ adapter: new SuiAdapter(client) });
    expect((await streamd.getEvents(all, { fromBlock: 1n, toBlock: 2n })).map(eventId)).toEqual(["3:0", "3:1", "4:0"]);
    expect((await streamd.getEvents(all, { fromBlock: 3n })).map(eventId)).toEqual(["9:4"]);
  });

  it("starts a listener fromTime", async () => {
    const { client } = fakeLedger(SIZES, EVENTS);
    using streamd = new StreamdClient({ adapter: new SuiAdapter(client) });
    const seen: string[] = [];
    const sub = streamd.on(all, (e) => void seen.push(eventId(e)), { fromTime: stamp(2n), pollInterval: 5 });
    await sub.caughtUp;
    sub.unsubscribe();
    expect(seen).toEqual(["4:0", "9:4"]);
  });

  it("finds the first placeable checkpoint when the node has pruned below its reported lowest", async () => {
    const { client } = fakeLedger(SIZES, EVENTS, undefined, 2n);
    using streamd = new StreamdClient({ adapter: new SuiAdapter(client) });
    expect((await streamd.getEvents(all, { fromTime: 0n })).map(eventId)).toEqual(["9:4"]);
  });
});

describe("SuiAdapter transaction lookups", () => {
  const OK = digest(1);
  const FAILED = digest(2);
  const PENDING = digest(3);
  const TRANSACTIONS: FakeTransaction[] = [
    { digest: OK, cp: 1n, ti: 2n, events: [{ type: "0x3::validator::StakingRequestEvent", sender: "0xa11ce", amount: "7" }, { type: SWAP }] },
    { digest: FAILED, cp: 3n, ti: 1n, error: "MoveAbort(m::z, 101) in command 1", events: [] },
    { digest: PENDING, ti: 0n, events: [{ type: SWAP }] },
  ];
  const adapter = (fail?: () => unknown) => new SuiAdapter(fakeLedger(SIZES, [], fail, 0n, TRANSACTIONS).client);

  it("returns a checkpointed transaction's events, placed as fetchEvents places them", async () => {
    const found = await adapter().getTransactionEvents(OK);
    expect(found).toMatchObject({ version: 3n, success: true, vmStatus: "success" });
    expect(found!.events).toEqual([
      { type: STAKE, data: { amount: "7" }, version: 3n, sequenceNumber: 0n, eventIndex: 0, transactionHash: OK, sender: ALICE, timestamp: 1_700_000_001_000_005n },
      { type: SWAP, data: { amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 1, transactionHash: OK, timestamp: 1_700_000_001_000_005n },
    ]);
  });

  it("reports a failed transaction with its error description", async () => {
    expect(await adapter().getTransactionEvents(FAILED)).toEqual({
      version: 7n,
      success: false,
      vmStatus: "MoveAbort(m::z, 101) in command 1",
      events: [],
    });
  });

  it("answers undefined for an unknown or uncheckpointed transaction", async () => {
    expect(await adapter().getTransactionEvents(digest(9))).toBeUndefined();
    expect(await adapter().getTransactionEvents(PENDING)).toBeUndefined();
  });

  it("refuses a malformed or hex digest before any call", async () => {
    const { client, calls } = fakeLedger(SIZES, [], undefined, 0n, TRANSACTIONS);
    const sui = new SuiAdapter(client);
    for (const bad of [`0x${"ab".repeat(32)}`, "not-base58!", digest(1).slice(0, 20), 7 as unknown as string]) {
      const err = await sui.getTransactionEvents(bad).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
    expect(calls.transactions).toEqual([]);
  });

  it("turns another gRPC failure into a network error", async () => {
    const err = await adapter(() => Object.assign(new Error("slow down"), { code: "RESOURCE_EXHAUSTED" }))
      .getTransactionEvents(OK)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(429);
  });
});

describe("SuiAdapter simulations", () => {
  const simulating = (result: Parameters<typeof withSimulator>[1], fail?: () => unknown) => {
    const { client, calls } = fakeLedger(SIZES, []);
    return { ...withSimulator(client, result, fail), calls };
  };

  it("previews the events at the next version, with no hash or timestamp", async () => {
    const { client, requests } = simulating({ events: [{ type: "0x3::validator::StakingRequestEvent", sender: "0xa11ce", amount: "7" }, { type: SWAP }] });
    const bytes = await transactionBytes();
    expect(await new SuiAdapter(client).simulateTransactionEvents({ transaction: bytes })).toEqual({
      version: 10n,
      success: true,
      vmStatus: "success",
      events: [
        { type: STAKE, data: { amount: "7" }, version: 10n, sequenceNumber: 0n, eventIndex: 0, sender: ALICE },
        { type: SWAP, data: { amount: "1" }, version: 10n, sequenceNumber: 0n, eventIndex: 1 },
      ],
    });
    expect(requests).toEqual([expect.objectContaining({ transaction: { bcs: { value: bytes } }, checks: 0 })]);
  });

  it("reports a failed simulation with its execution error", async () => {
    const { client } = simulating({ error: "MoveAbort(m::z, 10) in command 1", events: [] });
    expect(await new SuiAdapter(client).simulateTransactionEvents({ transaction: await transactionBytes() })).toEqual({
      version: 10n,
      success: false,
      vmStatus: "MoveAbort(m::z, 10) in command 1",
      events: [],
    });
  });

  it("refuses anything but built transaction bytes before any call", async () => {
    const { client, requests, calls } = simulating({ events: [] });
    const sui = new SuiAdapter(client);
    for (const bad of [new Uint8Array([1, 2, 3]), "AAEC", [0, 1], undefined]) {
      const err = await sui.simulateTransactionEvents({ transaction: bad as Uint8Array }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
    expect(requests).toEqual([]);
    expect(calls.checkpoints).toEqual([]);
  });

  it("refuses a client without the execution service", async () => {
    const { client } = fakeLedger(SIZES, []);
    const err = await new SuiAdapter(client).simulateTransactionEvents({ transaction: await transactionBytes() }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain("transactionExecutionService");
  });

  it("turns a refused request into a network error", async () => {
    const { client } = simulating({ events: [] }, () => Object.assign(new Error("invalid transaction"), { code: "INVALID_ARGUMENT" }));
    const err = await new SuiAdapter(client).simulateTransactionEvents({ transaction: await transactionBytes() }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("invalid transaction");
  });
});

describe("SuiAdapter transfers and balances", () => {
  const SUI = `${LONG("2")}::sui::SUI`;
  const USDC = `${LONG("dba")}::usdc::USDC`;
  const BOB = LONG("b0b");
  const CAROL = LONG("ca401");
  // Versions: checkpoint 1 starts at 1, 2 at 4, 3 at 6.
  const LISTED: FakeListed[] = [
    { cp: 1n, ti: 0n, gas: 2n, events: 1, changes: [[ALICE, SUI, -12n], [BOB, SUI, 10n]] },
    { cp: 1n, ti: 2n, sender: BOB, changes: [[BOB, USDC, -5n], [CAROL, USDC, 5n]] },
    { cp: 2n, ti: 1n, changes: [[CAROL, USDC, -1n]] },
    { cp: 3n, ti: 3n, sender: CAROL, changes: [[CAROL, SUI, -4n], [ALICE, SUI, 4n]] },
  ];
  const adapter = (balances?: Map<string, bigint>, coins?: Map<string, { symbol: string; decimals: number }>) => {
    const { client } = fakeLedger(SIZES, []);
    const wired = withTransfers(client, SIZES, LISTED, balances, coins);
    return { adapter: new SuiAdapter(wired.client), calls: wired.calls };
  };
  const pairs = (page: { events: { version: bigint; eventIndex: number; data: { from?: string; to?: string; amount: bigint } }[] }) =>
    page.events.map((e) => [e.version, e.eventIndex, e.data.from, e.data.to, e.data.amount]);

  it("pages transfers by cursor, with hasMore and throughVersion as fetchEvents has them", async () => {
    const { adapter: sui, calls } = adapter();
    const first = await sui.fetchTransfers({ fromVersion: 0n, limit: 2 });
    expect(pairs(first)).toEqual([[1n, 1, ALICE, BOB, 10n], [3n, 0, BOB, CAROL, 5n]]);
    expect(first).toMatchObject({ hasMore: true, throughVersion: 3n });
    expect(first.events[0]).toMatchObject({ type: SUI, transactionHash: "D1t0", timestamp: 1_700_000_001_000_000n, sender: ALICE });
    const rest = await sui.fetchTransfers({ cursor: first.nextCursor, limit: 10 });
    expect(pairs(rest)).toEqual([[5n, 0, CAROL, undefined, 1n], [9n, 0, CAROL, ALICE, 4n]]);
    expect(rest).toMatchObject({ hasMore: false, throughVersion: 9n });
    expect((calls.list[0]!.readMask as { paths: string[] }).paths).toContain("balance_changes");
    expect(calls.list[0]!.filter).toBeUndefined();
  });

  it("starts at fromVersion and drops records past toVersion", async () => {
    const { adapter: sui } = adapter();
    expect(pairs(await sui.fetchTransfers({ fromVersion: 3n, toVersion: 5n }))).toEqual([[3n, 0, BOB, CAROL, 5n], [5n, 0, CAROL, undefined, 1n]]);
  });

  it("narrows by account through the node and the records, and by a short-form asset", async () => {
    const { adapter: sui, calls } = adapter();
    const carol = await sui.fetchTransfers({ fromVersion: 0n }, { account: "0xca401" });
    expect(pairs(carol)).toEqual([[3n, 0, BOB, CAROL, 5n], [5n, 0, CAROL, undefined, 1n], [9n, 0, CAROL, ALICE, 4n]]);
    expect(calls.list[0]!.filter).toEqual({ terms: [{ literals: [{ negated: false, predicate: { oneofKind: "affectedAddress", affectedAddress: { address: CAROL } } }] }] });
    const sui2 = await sui.fetchTransfers({ fromVersion: 0n }, { asset: "0x2::sui::SUI" });
    expect(sui2.events.map((e) => e.data.asset)).toEqual([SUI, SUI]);
    await expect(sui.fetchTransfers({}, { account: "nope" })).rejects.toThrow(StreamdError);
    await expect(sui.fetchTransfers({}, { asset: "nope" })).rejects.toThrow("coin type");
  });

  it("reads a balance and coin info through the state service", async () => {
    const { adapter: sui, calls } = adapter(new Map([[`${ALICE}:${SUI}`, 42n]]), new Map([[SUI, { symbol: "SUI", decimals: 9 }]]));
    expect(await sui.getBalance("0xa11ce", "0x2::sui::SUI")).toBe(42n);
    expect(calls.balances).toEqual([`${ALICE}:${SUI}`]);
    await expect(sui.getBalance(ALICE, "nope")).rejects.toThrow("coin type");
    expect(calls.balances).toHaveLength(1);
    expect(await sui.coinInfo("0x2::sui::SUI")).toEqual({ symbol: "SUI", decimals: 9 });
    expect(await sui.coinInfo(USDC)).toBeUndefined();
  });

  it("refuses a client without the services, and maps a failed read", async () => {
    const plain = new SuiAdapter(fakeLedger(SIZES, []).client);
    await expect(plain.fetchTransfers({})).rejects.toThrow("listTransactions");
    await expect(plain.getBalance(ALICE, SUI)).rejects.toThrow("stateService");
    await expect(plain.coinInfo(SUI)).rejects.toThrow("stateService");
    const { client } = fakeLedger(SIZES, []);
    const failing = new SuiAdapter({
      ...client,
      stateService: {
        getBalance: async () => Promise.reject(Object.assign(new Error("busy"), { code: "RESOURCE_EXHAUSTED" })),
        getCoinInfo: async () => Promise.reject(Object.assign(new Error("down"), { code: "UNAVAILABLE" })),
      },
    });
    await expect(failing.getBalance(ALICE, SUI)).rejects.toMatchObject({ statusCode: 429 });
    await expect(failing.coinInfo(SUI)).rejects.toThrow(StreamdNetworkError);
  });
});

describe("SuiAdapter entry functions", () => {
  const POOL = LONG("abc");
  const swap = { package: "0xabc", module: "pool", function: "swap", typeArguments: [], arguments: [{ kind: 2, input: 0 }] };
  const call = { function: `${POOL}::pool::swap`, typeArguments: [], arguments: [{ Pure: "Bw==" }] };
  const inputs = [{ kind: 1, pure: new Uint8Array([7]) }];
  const from = { pkg: POOL, module: "pool" };
  const EVENTS_WITH: FakeEvent[] = [
    { cp: 1n, ti: 2n, ei: 0, type: SWAP, sender: ALICE, ...from },
    { cp: 1n, ti: 2n, ei: 1, type: STAKE, sender: ALICE, ...from },
    { cp: 2n, ti: 0n, ei: 0, type: SWAP, sender: LONG("b0b"), ...from },
    { cp: 3n, ti: 0n, ei: 0, type: SWAP, sender: ALICE, pkg: LONG("3"), module: "validator" },
  ];
  const PTBS = {
    D1t2: ptb([swap], inputs),
    D2t0: ptb([swap, { ...swap, function: "flash" }], inputs),
    D3t0: ptb([swap], inputs),
  };
  const swaps: EventDescriptor = { kind: "module", eventType: [SWAP, STAKE] };

  it("attaches the matching command to module and account events, one batch read per page", async () => {
    const { client, batches } = withCalls(fakeLedger(SIZES, EVENTS_WITH).client, PTBS);
    const sui = new SuiAdapter(client);
    const page = await sui.fetchEvents(swaps, { fromVersion: 0n, limit: 10 });
    expect(page.events.map((e) => e.entryFunction)).toEqual([call, call, undefined, undefined]);
    expect(page.events[0]!.entryFunction).toBe(page.events[1]!.entryFunction!);
    expect(batches).toEqual([["D1t2", "D2t0", "D3t0"]]);
    const mine = await sui.fetchEvents({ kind: "account", accountAddress: ALICE }, { fromVersion: 0n, limit: 10 });
    expect(mine.events.map((e) => e.entryFunction)).toEqual([call, call, undefined]);
  });

  it("reads nothing more for an empty page, and nothing without the batch call", async () => {
    const { client, batches } = withCalls(fakeLedger(SIZES, EVENTS_WITH).client, PTBS);
    expect((await new SuiAdapter(client).fetchEvents({ kind: "module", eventType: `${LONG("9")}::m::E` }, { fromVersion: 0n })).events).toEqual([]);
    expect(batches).toEqual([]);
    const plain = await new SuiAdapter(fakeLedger(SIZES, EVENTS_WITH).client).fetchEvents(swaps, { fromVersion: 0n });
    expect(plain.events.every((e) => e.entryFunction === undefined)).toBe(true);
  });

  it("fails the page as a network error when the node leaves a transaction out", async () => {
    const { client } = withCalls(fakeLedger(SIZES, EVENTS_WITH).client, { D1t2: PTBS.D1t2 });
    const err = await new SuiAdapter(client).fetchEvents(swaps, { fromVersion: 0n }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("D2t0");
  });

  it("reads more than 200 transactions in batches of 200, and maps a failed read", async () => {
    const many: FakeEvent[] = Array.from({ length: 201 }, (_, i) => ({ cp: 3n, ti: BigInt(i), ei: 0, type: SWAP, ...from }));
    const { client, batches } = withCalls(fakeLedger([1, 3, 2, 201], many).client, Object.fromEntries(many.map((e) => [`D3t${e.ti}`, ptb([swap], inputs)])));
    const page = await new SuiAdapter(client).fetchEvents(swaps, { fromVersion: 0n, limit: 500 });
    expect(batches.map((b) => b.length)).toEqual([200, 1]);
    expect(page.events.every((e) => e.entryFunction !== undefined)).toBe(true);
    const failing = withCalls(fakeLedger(SIZES, EVENTS_WITH).client, PTBS).client;
    failing.ledgerService.batchGetTransactions = async () => { throw Object.assign(new Error("slow down"), { code: "RESOURCE_EXHAUSTED" }); };
    const err = await new SuiAdapter(failing).fetchEvents(swaps, { fromVersion: 0n }).catch((e: unknown) => e);
    expect((err as StreamdNetworkError).statusCode).toBe(429);
  });

  it("attaches calls to transaction lookups and simulations from the same response", async () => {
    const tx: FakeTransaction = { digest: digest(1), cp: 1n, ti: 2n, ptb: ptb([swap], inputs), events: [{ type: SWAP, ...from }, { type: STAKE }] };
    const found = await new SuiAdapter(fakeLedger(SIZES, [], undefined, 0n, [tx]).client).getTransactionEvents(digest(1));
    expect(found!.events.map((e) => e.entryFunction)).toEqual([call, undefined]);
    const { client, requests } = withSimulator(fakeLedger(SIZES, []).client, { ptb: ptb([swap], inputs), events: [{ type: SWAP, ...from }] });
    const preview = await new SuiAdapter(client).simulateTransactionEvents({ transaction: await transactionBytes() });
    expect(preview.events[0]!.entryFunction).toEqual(call);
    expect((requests[0] as { readMask: { paths: string[] } }).readMask.paths).toContain("transaction.transaction.kind.programmable_transaction.commands");
  });
});
