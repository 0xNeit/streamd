import { describe, it, expect, spyOn } from "bun:test";
import { StreamdClient } from "../src/client.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdTimeoutError,
  StreamdTransactionError,
} from "../src/errors.js";
import type { PublicKey, SimpleTransaction } from "@aptos-labs/ts-sdk";
import type {
  EventAdapter,
  StreamdEvent,
  TransactionEvents,
  TransactionSimulation,
} from "../src/types.js";

const HASH = `0x${"cd".repeat(32)}`;
const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";
const SENDER = `0x${"0".repeat(60)}cafe`;
const UPPER_SENDER = `0x${"0".repeat(60)}CAFE`;
const STRANGER = `0x${"be".repeat(32)}`;

function ev(eventIndex: number, type: string, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return {
    type,
    data: { amount: String(10 * (eventIndex + 1)) },
    version: 9n,
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: HASH,
    sender: SENDER,
    ...extra,
  };
}

const EVENTS = [
  ev(0, PING),
  ev(1, PONG, { guid: { creationNumber: 5n, accountAddress: SENDER } }),
  ev(2, PING, { guid: { creationNumber: 6n, accountAddress: SENDER } }),
];
const FRAMEWORK = `0x${"0".repeat(63)}1`;

const committed: TransactionEvents = {
  version: 9n,
  success: true,
  vmStatus: "Executed successfully",
  events: EVENTS,
};

/** Answers `undefined` for the first `misses` lookups, then `result`. */
function lookup(result: TransactionEvents | Error = committed, misses = 0) {
  const calls: { hash: string; descriptor: unknown }[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    async getTransactionEvents(hash, descriptor) {
      calls.push({ hash, descriptor });
      if (calls.length <= misses) return undefined;
      if (result instanceof Error) throw result;
      return result;
    },
  };
  return { adapter, calls };
}

function client(adapter: EventAdapter, extra: object = {}) {
  return new StreamdClient({ adapter, defaults: { pollInterval: 5 }, ...extra });
}

describe("eventsOf", () => {
  it("waits through lookups that find nothing, then resolves with every event", async () => {
    const { adapter, calls } = lookup(committed, 2);
    const events = await client(adapter).eventsOf(HASH);
    expect(events).toEqual(EVENTS);
    expect(calls).toHaveLength(3);
    expect(calls[0]).toEqual({ hash: HASH, descriptor: undefined });
  });

  it("filters by module type, account sender and handle guid", async () => {
    const c = client(lookup().adapter);
    expect((await c.eventsOf(HASH, { kind: "module", eventType: PING })).map((e) => e.eventIndex))
      .toEqual([0, 2]);
    expect(
      (await c.eventsOf(HASH, { kind: "module", eventType: [PING, PONG] })).map((e) => e.eventIndex),
    ).toEqual([0, 1, 2]);
    expect(
      (await c.eventsOf(HASH, { kind: "account", accountAddress: UPPER_SENDER })).map((e) => e.eventIndex),
    ).toEqual([0, 1, 2]);
    expect(
      (await c.eventsOf(HASH, { kind: "account", accountAddress: SENDER, eventTypes: [PONG] })).map(
        (e) => e.eventIndex,
      ),
    ).toEqual([1]);
    expect(await c.eventsOf(HASH, { kind: "account", accountAddress: STRANGER })).toEqual([]);
    expect(
      (await c.eventsOf(HASH, { kind: "handle", accountAddress: UPPER_SENDER, creationNumber: 6n })).map(
        (e) => e.eventIndex,
      ),
    ).toEqual([2]);
    expect(
      await c.eventsOf(HASH, { kind: "handle", accountAddress: STRANGER, creationNumber: 6n }),
    ).toEqual([]);
  });

  it("passes the descriptor to the adapter for routing", async () => {
    const { adapter, calls } = lookup();
    const descriptor = { kind: "module", eventType: PING } as const;
    await client(adapter).eventsOf(HASH, descriptor);
    expect(calls[0]!.descriptor).toBe(descriptor);
  });

  it("decodes before where", async () => {
    const c = client(lookup().adapter);
    const events = await c.eventsOf<{ amount: bigint }>(HASH, undefined, {
      decode: ["amount"],
      where: (e) => {
        const { amount } = e.data as { amount: unknown };
        return typeof amount === "bigint" && amount > 10n;
      },
    });
    expect(events.map((e) => e.data.amount)).toEqual([20n, 30n]);
    expect(EVENTS[1]!.data).toEqual({ amount: "20" });
  });

  it("awaits an async where, rejecting with its reason or the signal's", async () => {
    const c = client(lookup().adapter);
    const events = await c.eventsOf(HASH, undefined, {
      where: async (e) => e.eventIndex !== 1,
    });
    expect(events.map((e) => e.eventIndex)).toEqual([0, 2]);
    const boom = new Error("lookup failed");
    await expect(c.eventsOf(HASH, undefined, { where: () => Promise.reject(boom) })).rejects.toBe(boom);
    const controller = new AbortController();
    const pending = c.eventsOf(HASH, undefined, {
      signal: controller.signal,
      where: () => new Promise<boolean>(() => {}),
    });
    await new Promise((r) => setTimeout(r, 5));
    controller.abort(boom);
    await expect(pending).rejects.toBe(boom);
  });

  it("rejects a failed transaction with its vm status", async () => {
    const failed = { ...committed, success: false, vmStatus: "Move abort in 0x1::coin: 0x10006", events: [] };
    const err = await client(lookup(failed).adapter).eventsOf(HASH).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: HASH, version: 9n, vmStatus: failed.vmStatus });
  });

  it("rejects with StreamdTimeoutError when the transaction never lands", async () => {
    const { adapter, calls } = lookup(committed, Infinity);
    const err = await client(adapter).eventsOf(HASH, undefined, { timeout: 150 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
    expect((err as StreamdTimeoutError).timeout).toBe(150);
    const seen = calls.length;
    expect(seen).toBeGreaterThan(1);
    await Bun.sleep(120);
    expect(calls).toHaveLength(seen);
  });

  it("times out while a lookup hangs", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      getTransactionEvents: () => new Promise(() => {}),
    };
    const err = await client(adapter).eventsOf(HASH, undefined, { timeout: 20 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
  });

  it("rejects with the signal's reason, before or during the wait", async () => {
    const { adapter, calls } = lookup(committed, Infinity);
    const c = client(adapter);
    const reason = new Error("stop");
    expect(await c.eventsOf(HASH, undefined, { signal: AbortSignal.abort(reason) }).catch((e) => e))
      .toBe(reason);
    expect(calls).toHaveLength(0);

    const controller = new AbortController();
    const pending = c.eventsOf(HASH, undefined, { signal: controller.signal });
    await Bun.sleep(12);
    controller.abort(reason);
    expect(await pending.catch((e: unknown) => e)).toBe(reason);
  });

  it("rejects at once with a lookup error", async () => {
    const boom = new StreamdNetworkError("down", 503);
    const { adapter, calls } = lookup(boom);
    expect(await client(adapter).eventsOf(HASH).catch((e: unknown) => e)).toBe(boom);
    expect(calls).toHaveLength(1);
  });

  it("refuses bad input before any lookup", async () => {
    const { adapter, calls } = lookup();
    const c = client(adapter);
    const refused = [
      c.eventsOf("0x12"),
      c.eventsOf(`0x${"cd".repeat(32)}?x=1`),
      c.eventsOf(`../0x${"cd".repeat(32)}`),
      c.eventsOf(HASH, undefined, { timeout: -1 }),
      c.eventsOf(HASH, undefined, { decode: [""] }),
      c.eventsOf(HASH, { kind: "module", eventType: [] }),
      c.eventsOf(HASH, { kind: "account", accountAddress: "nope" }),
      c.eventsOf(HASH, {
        kind: "handle",
        accountAddress: SENDER,
        eventHandleStruct: "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
        fieldName: "deposit_events",
      }),
      c.eventsOf(HASH, { kind: "nope" } as never),
    ];
    for (const call of refused) {
      const err = await call.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    expect(calls).toHaveLength(0);
  });

  it("refuses an adapter without getTransactionEvents", async () => {
    const c = client({ fetchEvents: async () => ({ events: [], hasMore: false }) });
    const err = await c.eventsOf(HASH).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain("getTransactionEvents");
  });

  it("goes through maxConcurrentRequests", async () => {
    let active = 0;
    let peak = 0;
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async getTransactionEvents() {
        peak = Math.max(peak, ++active);
        await Bun.sleep(5);
        active--;
        return committed;
      },
    };
    const c = client(adapter, { maxConcurrentRequests: 1 });
    await Promise.all([c.eventsOf(HASH), c.eventsOf(HASH), c.eventsOf(HASH)]);
    expect(peak).toBe(1);
  });

  it("works offline over a MemoryAdapter", async () => {
    const c = client(new MemoryAdapter(EVENTS));
    expect((await c.eventsOf(HASH, { kind: "module", eventType: PONG })).map((e) => e.eventIndex)).toEqual([1]);
  });

  it("matches event-side addresses in short form", async () => {
    const framework = {
      ...committed,
      events: [ev(0, PING, { sender: "0x1", guid: { creationNumber: 2n, accountAddress: "0x1" } })],
    };
    const c = client(lookup(framework).adapter);
    expect(await c.eventsOf(HASH, { kind: "account", accountAddress: FRAMEWORK })).toHaveLength(1);
    expect(
      await c.eventsOf(HASH, { kind: "handle", accountAddress: FRAMEWORK, creationNumber: 2n }),
    ).toHaveLength(1);
  });

  it("waits at least 100ms between lookups whatever pollInterval says", async () => {
    for (const pollInterval of [0, -5, Number.NaN, 3e9]) {
      const { adapter, calls } = lookup(committed, Infinity);
      const c = new StreamdClient({ adapter, defaults: { pollInterval } });
      await c.eventsOf(HASH, undefined, { timeout: 150 }).catch(() => {});
      expect(calls.length).toBeLessThanOrEqual(3);
    }
  });

  it("clears its timer and abort listener once settled", async () => {
    const controller = new AbortController();
    const removed: string[] = [];
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.removeEventListener = (type: string, listener: never) => {
      removed.push(type);
      remove(type, listener);
    };
    const cleared = spyOn(globalThis, "clearTimeout");
    await client(lookup().adapter).eventsOf(HASH, undefined, { signal: controller.signal });
    expect(cleared).toHaveBeenCalled();
    expect(removed).toContain("abort");
    cleared.mockRestore();
  });

  it("drops a lookup still queued when the wait times out", async () => {
    let release = () => {};
    let lookups = 0;
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async getTransactionEvents() {
        lookups++;
        await new Promise<void>((r) => (release = r));
        return committed;
      },
    };
    const c = client(adapter, { maxConcurrentRequests: 1 });
    const first = c.eventsOf(HASH);
    const second = await c.eventsOf(HASH, undefined, { timeout: 20 }).catch((e: unknown) => e);
    expect(second).toBeInstanceOf(StreamdTimeoutError);
    release();
    await first;
    await Bun.sleep(5);
    expect(lookups).toBe(1);
  });
});

const TRANSACTION = { rawTransaction: {} } as unknown as SimpleTransaction;
const KEY = {} as PublicKey;
const simulatedRun: TransactionEvents = {
  version: 12n,
  success: true,
  vmStatus: "Executed successfully",
  events: EVENTS.map((e) => ({ ...e, version: 12n, transactionHash: undefined })),
};

function simulator(result: TransactionEvents | Error = simulatedRun) {
  const calls: { simulation: TransactionSimulation; descriptor: unknown }[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    async simulateTransactionEvents(simulation, descriptor) {
      calls.push({ simulation, descriptor });
      if (result instanceof Error) throw result;
      return result;
    },
  };
  return { adapter, calls };
}

describe("previewEvents", () => {
  it("passes the transaction, key and descriptor to the adapter and resolves with every event", async () => {
    const { adapter, calls } = simulator();
    const descriptor = { kind: "account", accountAddress: SENDER } as const;
    const events = await client(adapter).previewEvents(TRANSACTION, descriptor, { signerPublicKey: KEY });
    expect(events).toEqual(simulatedRun.events);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.simulation.transaction).toBe(TRANSACTION);
    expect(calls[0]!.simulation.signerPublicKey).toBe(KEY);
    expect(calls[0]!.descriptor).toBe(descriptor);
  });

  it("filters by each descriptor kind, decoding before where", async () => {
    const c = client(simulator().adapter);
    expect((await c.previewEvents(TRANSACTION, { kind: "module", eventType: PONG })).map((e) => e.eventIndex))
      .toEqual([1]);
    expect(await c.previewEvents(TRANSACTION, { kind: "account", accountAddress: STRANGER })).toEqual([]);
    expect(
      (await c.previewEvents(TRANSACTION, { kind: "handle", accountAddress: SENDER, creationNumber: 5n })).map(
        (e) => e.eventIndex,
      ),
    ).toEqual([1]);
    const decoded = await c.previewEvents<{ amount: bigint }>(TRANSACTION, undefined, {
      decode: ["amount"],
      where: (e) => (e.data as { amount: unknown }).amount === 30n,
    });
    expect(decoded.map((e) => e.eventIndex)).toEqual([2]);
  });

  it("awaits an async where, rejecting with its reason", async () => {
    const c = client(simulator().adapter);
    const kept = await c.previewEvents(TRANSACTION, undefined, { where: async (e) => e.eventIndex > 0 });
    expect(kept.map((e) => e.eventIndex)).toEqual([1, 2]);
    const boom = new Error("lookup failed");
    await expect(
      c.previewEvents(TRANSACTION, undefined, { where: () => Promise.reject(boom) }),
    ).rejects.toBe(boom);
  });

  it("rejects a failed simulation with its vm status and no hash", async () => {
    const failed = { ...simulatedRun, success: false, vmStatus: "Move abort: EINSUFFICIENT_BALANCE", events: [] };
    const err = await client(simulator(failed).adapter).previewEvents(TRANSACTION).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: undefined, version: 12n, vmStatus: failed.vmStatus });
  });

  it("rejects with a simulation error as it is", async () => {
    const boom = new StreamdNetworkError("down", 503);
    const err = await client(simulator(boom).adapter).previewEvents(TRANSACTION).catch((e: unknown) => e);
    expect(err).toBe(boom);
  });

  it("refuses bad input and a missing adapter method before any call", async () => {
    const { adapter, calls } = simulator();
    const c = client(adapter);
    const refusals: [unknown, unknown, unknown][] = [
      [undefined, undefined, {}],
      [null, undefined, {}],
      [{}, undefined, {}],
      [{ rawTransaction: {}, secondarySignerAddresses: "0xb" }, undefined, {}],
      [TRANSACTION, undefined, { signerPublicKey: "0xkey" }],
      [TRANSACTION, undefined, { secondarySignersPublicKeys: [KEY] }],
      [{ rawTransaction: {}, secondarySignerAddresses: [{}, {}] }, undefined, { secondarySignersPublicKeys: [KEY] }],
      [{ rawTransaction: {}, secondarySignerAddresses: [{}] }, undefined, { secondarySignersPublicKeys: ["0xkey"] }],
      [TRANSACTION, undefined, { feePayerPublicKey: KEY }],
      [{ rawTransaction: {}, feePayerAddress: {} }, undefined, { feePayerPublicKey: "0xkey" }],
      [TRANSACTION, { kind: "handle", accountAddress: SENDER, eventHandleStruct: "0x1::a::B", fieldName: "f" }, {}],
      [TRANSACTION, { kind: "nope" }, {}],
      [TRANSACTION, undefined, { decode: "amount" }],
    ];
    for (const [transaction, descriptor, options] of refusals) {
      const err = await c
        .previewEvents(transaction as never, descriptor as never, options as never)
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    expect(calls).toEqual([]);
    const bare = client({ fetchEvents: async () => ({ events: [], hasMore: false }) });
    const err = await bare.previewEvents(TRANSACTION).catch((e: unknown) => e);
    expect((err as Error).message).toContain("simulateTransactionEvents");
  });

  it("goes through maxConcurrentRequests", async () => {
    let active = 0;
    let peak = 0;
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async simulateTransactionEvents() {
        peak = Math.max(peak, ++active);
        await Bun.sleep(5);
        active--;
        return simulatedRun;
      },
    };
    const c = client(adapter, { maxConcurrentRequests: 1 });
    await Promise.all([c.previewEvents(TRANSACTION), c.previewEvents(TRANSACTION)]);
    expect(peak).toBe(1);
  });
});

describe("previewEvents with several signers", () => {
  const MULTI = { rawTransaction: {}, secondarySignerAddresses: [{}, {}], feePayerAddress: {} } as unknown as SimpleTransaction;
  const SPONSORED = { rawTransaction: {}, feePayerAddress: {} } as unknown as SimpleTransaction;
  const OTHER_KEY = {} as PublicKey;

  it("passes secondary and fee-payer keys to the adapter, from previewEvents and expectPreview", async () => {
    const { adapter, calls } = simulator();
    const c = client(adapter);
    await c.previewEvents(MULTI, undefined, { secondarySignersPublicKeys: [KEY, undefined], feePayerPublicKey: OTHER_KEY });
    await c.expectPreview(SPONSORED, { feePayerPublicKey: OTHER_KEY }).toEmit({ kind: "module", eventType: PONG });
    await c.previewEvents(MULTI);
    expect(calls[0]!.simulation.transaction).toBe(MULTI);
    expect(calls[0]!.simulation.secondarySignersPublicKeys).toEqual([KEY, undefined]);
    expect(calls[0]!.simulation.feePayerPublicKey).toBe(OTHER_KEY);
    expect(calls[1]!.simulation.feePayerPublicKey).toBe(OTHER_KEY);
    expect(calls[1]!.simulation).not.toHaveProperty("secondarySignersPublicKeys");
    expect(calls[2]!.simulation).not.toHaveProperty("secondarySignersPublicKeys");
    expect(calls[2]!.simulation).not.toHaveProperty("feePayerPublicKey");
  });
});

describe("eventsOf with a resource descriptor", () => {
  it("rejects before any lookup", async () => {
    const { adapter, calls } = lookup();
    await expect(
      client(adapter).eventsOf(HASH, { kind: "resource", accountAddress: SENDER, resourceType: PING }),
    ).rejects.toThrow("not write sets");
    expect(calls).toHaveLength(0);
  });
});
