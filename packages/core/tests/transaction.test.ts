import { describe, expect, it } from "bun:test";
import {
  RequestLimiter,
  StreamdAssertionError,
  StreamdError,
  StreamdTimeoutError,
  StreamdTransactionError,
  eventsAt,
  eventsOf,
  expectEvents,
  expectPreview,
  previewEvents,
} from "../src/index.js";
import type { EventAdapter, MoveEventType, StreamdEvent, TransactionEvents, TransactionSimulation } from "../src/index.js";

const A = "0x1::m::A" as MoveEventType;
const B = "0x1::m::B" as MoveEventType;
const ev = (type: string, eventIndex: number, amount = "1"): StreamdEvent => ({
  type,
  data: { amount },
  version: 5n,
  sequenceNumber: 0n,
  eventIndex,
});

/** Answers `undefined` for the first `misses` lookups, then `found`; ids are not hex, as on Sui. */
function lookups(misses: number, found: TransactionEvents | undefined) {
  const seen: string[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }),
    async getTransactionEvents(id) {
      seen.push(id);
      return seen.length > misses ? found : undefined;
    },
  };
  return { adapter, seen };
}

const ID = "4fQ2txNotHex";
const limiter = new RequestLimiter();

describe("eventsOf in core", () => {
  it("waits through missing lookups, then narrows by descriptor, decode and where", async () => {
    const { adapter, seen } = lookups(2, {
      version: 5n,
      success: true,
      vmStatus: "success",
      events: [ev(A, 0, "3"), ev(B, 1), ev(A, 2, "9")],
    });
    const events = await eventsOf(adapter, limiter, 0, ID, { kind: "module", eventType: A }, {
      decode: ["amount"],
      where: (e) => (e.data as { amount: bigint }).amount > 5n,
    });
    expect(events.map((e) => [e.eventIndex, (e.data as { amount: bigint }).amount])).toEqual([[2, 9n]]);
    expect(seen).toEqual([ID, ID, ID]);
  });

  it("rejects a failed transaction with StreamdTransactionError", async () => {
    const { adapter } = lookups(0, { version: 7n, success: false, vmStatus: "MoveAbort 101", events: [] });
    const err = await eventsOf(adapter, limiter, 0, ID, undefined, {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: ID, version: 7n, vmStatus: "MoveAbort 101" });
  });

  it("times out on a transaction never found", async () => {
    const { adapter } = lookups(0, undefined);
    const err = await eventsOf(adapter, limiter, 0, ID, undefined, { timeout: 250 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
    expect((err as StreamdTimeoutError).timeout).toBe(250);
  });

  it("expectEvents asserts over the same lookup", async () => {
    const { adapter } = lookups(0, { version: 5n, success: true, vmStatus: "success", events: [ev(A, 0)] });
    const expectation = expectEvents(adapter, limiter, 0, ID, {});
    expect((await expectation.toEmit({ kind: "module", eventType: A })).map((e) => e.eventIndex)).toEqual([0]);
    const err = await expectation.notToEmit({ kind: "module", eventType: A }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect((err as StreamdAssertionError).transactionHash).toBe(ID);
  });
});

/** Answers every simulation with `result`, recording what it was asked. */
function simulator(result: TransactionEvents) {
  const asked: TransactionSimulation[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }),
    async simulateTransactionEvents(simulation) {
      asked.push(simulation);
      return result;
    },
  };
  return { adapter, asked };
}

const SIMULATION: TransactionSimulation = { transaction: new Uint8Array([1, 2, 3]) };

describe("previewEvents in core", () => {
  it("narrows a simulation's events by descriptor, decode and where", async () => {
    const { adapter, asked } = simulator({
      version: 9n,
      success: true,
      vmStatus: "success",
      events: [ev(A, 0, "3"), ev(B, 1), ev(A, 2, "9")],
    });
    const events = await previewEvents(adapter, limiter, SIMULATION, { kind: "module", eventType: A }, {
      decode: ["amount"],
      where: (e) => (e.data as { amount: bigint }).amount > 5n,
    });
    expect(events.map((e) => [e.eventIndex, (e.data as { amount: bigint }).amount])).toEqual([[2, 9n]]);
    expect(asked).toEqual([SIMULATION]);
  });

  it("rejects a failed simulation with StreamdTransactionError and no hash", async () => {
    const { adapter } = simulator({ version: 9n, success: false, vmStatus: "MoveAbort 10", events: [] });
    const err = await previewEvents(adapter, limiter, SIMULATION, undefined, {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: undefined, version: 9n, vmStatus: "MoveAbort 10" });
  });

  it("refuses an adapter that cannot simulate", async () => {
    const { adapter } = lookups(0, undefined);
    const err = await previewEvents(adapter, limiter, SIMULATION, undefined, {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain("simulateTransactionEvents");
  });

  it("expectPreview simulates once per assertion, failing with no hash", async () => {
    const { adapter, asked } = simulator({ version: 9n, success: true, vmStatus: "success", events: [ev(A, 0)] });
    const expectation = expectPreview(adapter, limiter, SIMULATION, {});
    expect(asked).toHaveLength(0);
    expect((await expectation.toEmit({ kind: "module", eventType: A })).map((e) => e.eventIndex)).toEqual([0]);
    const err = await expectation.toEmit({ kind: "module", eventType: B }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect((err as StreamdAssertionError).transactionHash).toBeUndefined();
    expect(asked).toHaveLength(2);
  });
});

describe("transaction lookups with a received account descriptor", () => {
  it("reject before any lookup", async () => {
    const { adapter, seen } = lookups(0, { version: 5n, success: true, vmStatus: "success", events: [] });
    const received = { kind: "account" as const, accountAddress: "0x1", received: true };
    await expect(eventsOf(adapter, limiter, 0, ID, received, {})).rejects.toThrow("received account descriptor");
    expect(seen).toEqual([]);
  });
});

describe("transaction lookups with an object descriptor", () => {
  it("reject before any lookup or simulation", async () => {
    const { adapter, seen } = lookups(0, { version: 5n, success: true, vmStatus: "success", events: [] });
    const object = { kind: "object" as const, objectId: "0x6" };
    await expect(eventsOf(adapter, limiter, 0, ID, object, {})).rejects.toThrow("not object changes");
    const simulation = simulator({ version: 9n, success: true, vmStatus: "success", events: [] });
    await expect(previewEvents(simulation.adapter, limiter, SIMULATION, object, {})).rejects.toBeInstanceOf(StreamdError);
    expect(seen).toEqual([]);
    expect(simulation.asked).toEqual([]);
  });
});

/** Answers each by-version lookup with `found`, recording what it was asked. */
function byVersion(found: TransactionEvents | undefined | (() => Promise<TransactionEvents | undefined>)) {
  const seen: unknown[][] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    async getTransactionEventsByVersion(...args) {
      seen.push(args);
      return typeof found === "function" ? found() : found;
    },
  };
  return { adapter, seen };
}

describe("eventsAt in core", () => {
  const txn: TransactionEvents = {
    version: 5n,
    success: true,
    vmStatus: "success",
    events: [ev(A, 0, "3"), ev(B, 1), ev(A, 2, "9")],
  };

  it("narrows by descriptor, decode and an async where, passing the descriptor along", async () => {
    const { adapter, seen } = byVersion(txn);
    const descriptor = { kind: "module", eventType: A } as const;
    const events = await eventsAt(adapter, limiter, 5n, descriptor, {
      decode: ["amount"],
      where: async (e) => (e.data as { amount: bigint }).amount > 5n,
    });
    expect(events.map((e) => [e.eventIndex, (e.data as { amount: bigint }).amount])).toEqual([[2, 9n]]);
    expect(seen).toEqual([[5n, descriptor]]);
    expect(await eventsAt(adapter, limiter, 5n, undefined, {})).toEqual(txn.events);
  });

  it("rejects a failed transaction with StreamdTransactionError, hash from its events", async () => {
    const fee = { ...ev("0x1::transaction_fee::FeeStatement", 0), version: 7n, transactionHash: ID };
    for (const [events, hash] of [[[fee], ID], [[], undefined]] as const) {
      const { adapter } = byVersion({ version: 7n, success: false, vmStatus: "MoveAbort 101", events: [...events] });
      const err = await eventsAt(adapter, limiter, 7n, undefined, {}).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdTransactionError);
      expect(err).toMatchObject({ transactionHash: hash, version: 7n, vmStatus: "MoveAbort 101" });
    }
  });

  it("rejects a version the source has no transaction at", async () => {
    const { adapter } = byVersion(undefined);
    const err = await eventsAt(adapter, limiter, 99n, undefined, {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toBe("No transaction at version 99");
  });

  it("refuses bad input and a missing method before any lookup", async () => {
    const { adapter, seen } = byVersion(txn);
    for (const version of [-1n, 5, "5", 2n ** 64n, undefined]) {
      const err = await eventsAt(adapter, limiter, version as bigint, undefined, {}).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    for (const [descriptor, decode] of [
      [{ kind: "resource", accountAddress: "0x1", resourceType: "0x1::m::R" }, undefined],
      [{ kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::m::S", fieldName: "f" }, undefined],
      [undefined, ["bad path.."]],
    ] as const) {
      const err = await eventsAt(adapter, limiter, 5n, descriptor as never, { decode: decode as never }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    expect(seen).toHaveLength(0);
    const bare = await eventsAt({ fetchEvents: adapter.fetchEvents }, limiter, 5n, undefined, {}).catch((e: Error) => e);
    expect((bare as Error).message).toContain("getTransactionEventsByVersion");
  });

  it("rejects with the signal's reason, before or during the lookup", async () => {
    const reason = new Error("stop");
    const { adapter, seen } = byVersion(txn);
    expect(await eventsAt(adapter, limiter, 5n, undefined, { signal: AbortSignal.abort(reason) }).catch((e) => e)).toBe(reason);
    expect(seen).toHaveLength(0);
    const hung = byVersion(() => new Promise(() => {}));
    const controller = new AbortController();
    const pending = eventsAt(hung.adapter, limiter, 5n, undefined, { signal: controller.signal });
    controller.abort(reason);
    expect(await pending.catch((e) => e)).toBe(reason);
  });

  it("goes through maxConcurrentRequests", async () => {
    let active = 0;
    let peak = 0;
    const { adapter } = byVersion(async () => {
      peak = Math.max(peak, ++active);
      await Bun.sleep(2);
      active--;
      return txn;
    });
    const one = new RequestLimiter(1);
    await Promise.all([eventsAt(adapter, one, 5n, undefined, {}), eventsAt(adapter, one, 5n, undefined, {})]);
    expect(peak).toBe(1);
  });
});
