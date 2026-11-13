import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { recordingAdapter, replayAdapter, type Recording } from "../src/adapters/recording.js";
import { StreamdClient } from "../src/client.js";
import { RequestLimiter } from "../src/limiter.js";
import { eventsAt } from "../src/transaction.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent, TransactionSimulation } from "../src/types.js";

const PING = "0x1::test::Ping";
const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };

function ev(version: number, data: unknown = { amount: "1" }): StreamdEvent {
  return { type: PING, data, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

const block = { height: 7n, firstVersion: 10n, lastVersion: 12n, timestamp: 99n };

/** Every optional method, with fixed answers. */
function fullAdapter(): EventAdapter {
  return {
    async fetchEvents() {
      return { events: [ev(5)], nextCursor: "c1", hasMore: true, final: true, throughVersion: 5n };
    },
    async getHeadVersion() {
      return 100n;
    },
    async getEarliestVersion() {
      return 2n;
    },
    async getBlock() {
      return block;
    },
    async getBlockHeights() {
      return { oldest: 1n, newest: 9n };
    },
  };
}

describe("recordingAdapter", () => {
  it("records each call with a JSON-safe result", async () => {
    const recorder = recordingAdapter(fullAdapter());
    const page = await recorder.fetchEvents(moduleDesc, { fromVersion: 3n, limit: 2 });
    expect(page.events[0]!.version).toBe(5n);
    expect(await recorder.getHeadVersion!(moduleDesc)).toBe(100n);
    expect(await recorder.getEarliestVersion!(moduleDesc)).toBe(2n);
    expect(await recorder.getBlock!(moduleDesc, 7n)).toEqual(block);
    expect(await recorder.getBlockHeights!(moduleDesc)).toEqual({ oldest: 1n, newest: 9n });

    const recording = JSON.parse(JSON.stringify(recorder));
    expect(recording.format).toBe("streamd-recording");
    expect(recording.version).toBe(1);
    expect(recording.calls.map((c: { method: string }) => c.method)).toEqual([
      "fetchEvents",
      "getHeadVersion",
      "getEarliestVersion",
      "getBlock",
      "getBlockHeights",
    ]);
    expect(recording.calls[0].result).toEqual({
      events: [
        { type: PING, data: { amount: "1" }, version: "5", sequenceNumber: "0", eventIndex: 0 },
      ],
      nextCursor: "c1",
      hasMore: true,
      final: true,
      throughVersion: "5",
    });
    expect(recording.calls[1].result).toBe("100");
    expect(recording.calls[3].result).toEqual({
      height: "7",
      firstVersion: "10",
      lastVersion: "12",
      timestamp: "99",
    });
    expect(recording.calls[3].key).toEndWith("|7");
  });

  it("keys requests like CachingAdapter, ignoring option order and undefined fields", async () => {
    const recorder = recordingAdapter(new MemoryAdapter([ev(1)]));
    await recorder.fetchEvents(moduleDesc, { limit: 2, fromVersion: 1n });
    await recorder.fetchEvents(moduleDesc, { fromVersion: 1n, limit: 2, cursor: undefined });
    const [a, b] = recorder.toJSON().calls;
    expect(a!.key).toBe(b!.key);
  });

  it("exposes only the optional methods the source has", () => {
    const recorder = recordingAdapter({
      async fetchEvents() {
        return { events: [], hasMore: false };
      },
    });
    expect(recorder.getHeadVersion).toBeUndefined();
    expect(recorder.getEarliestVersion).toBeUndefined();
    expect(recorder.getBlock).toBeUndefined();
    expect(recorder.getBlockHeights).toBeUndefined();
  });

  it("passes failures through unrecorded", async () => {
    const failure = new StreamdNetworkError("down", 503);
    const recorder = recordingAdapter({
      async fetchEvents() {
        throw failure;
      },
      async getHeadVersion() {
        throw failure;
      },
    });
    await expect(recorder.fetchEvents(moduleDesc, {})).rejects.toBe(failure);
    await expect(recorder.getHeadVersion!(moduleDesc)).rejects.toBe(failure);
    expect(recorder.toJSON().calls).toEqual([]);
  });

  it("does not record a call whose descriptor has no key", async () => {
    const recorder = recordingAdapter({
      async fetchEvents() {
        return { events: [], hasMore: false };
      },
    });
    await recorder.fetchEvents({ kind: "nope" } as unknown as EventDescriptor, {});
    expect(recorder.toJSON().calls).toEqual([]);
  });

  it("is unaffected by mutating a returned page", async () => {
    const recorder = recordingAdapter(new MemoryAdapter([ev(1)]));
    const page = await recorder.fetchEvents(moduleDesc, {});
    page.events.length = 0;
    page.hasMore = true;
    const result = recorder.toJSON().calls[0]!.result as { events: unknown[]; hasMore: boolean };
    expect(result.events).toHaveLength(1);
    expect(result.hasMore).toBe(false);
  });

  it("rejects something that is not an adapter", () => {
    expect(() => recordingAdapter({} as EventAdapter)).toThrow(StreamdError);
  });
});

async function collectLive(adapter: EventAdapter, count: number): Promise<bigint[]> {
  const client = new StreamdClient({ adapter });
  const versions: bigint[] = [];
  await new Promise<void>((resolve) => {
    client.on(
      moduleDesc,
      (event) => {
        versions.push(event.version);
        if (versions.length === count) resolve();
      },
      { fromVersion: -3n, pollInterval: 5, batchSize: 2, onError: () => {} },
    );
  });
  client.destroy();
  return versions;
}

function valid(calls: unknown[]): unknown {
  return { format: "streamd-recording", version: 1, calls };
}

describe("replayAdapter", () => {
  const events = [1, 2, 3, 4, 5].map((v) => ev(v));

  it("replays a query offline from the JSON text", async () => {
    const recorder = recordingAdapter(new MemoryAdapter(events));
    const live = await new StreamdClient({ adapter: recorder }).getEvents(moduleDesc, {
      fromVersion: 2n,
      limit: 3,
    });
    const text = JSON.stringify(recorder);
    const replayed = await new StreamdClient({ adapter: replayAdapter(text) }).getEvents(
      moduleDesc,
      { fromVersion: 2n, limit: 3 },
    );
    expect(live.map((e) => e.version)).toEqual([2n, 3n, 4n]);
    expect(replayed).toEqual(live);
  });

  it("replays a live subscription, head version included", async () => {
    const recorder = recordingAdapter(new MemoryAdapter(events));
    const live = await collectLive(recorder, 4);
    const replay = replayAdapter(recorder.toJSON());
    expect(replay.getHeadVersion).toBeDefined();
    expect(replay.getBlock).toBeUndefined();
    expect(await collectLive(replay, 4)).toEqual(live);
    expect(live).toEqual([2n, 3n, 4n, 5n]);
  });

  it("replays the optional methods and keeps page flags", async () => {
    const recorder = recordingAdapter(fullAdapter());
    await recorder.fetchEvents(moduleDesc, { limit: 1 });
    await recorder.getHeadVersion!(moduleDesc);
    await recorder.getEarliestVersion!(moduleDesc);
    await recorder.getBlock!(moduleDesc, 7n);
    await recorder.getBlockHeights!(moduleDesc);
    const replay = replayAdapter(JSON.stringify(recorder));
    expect(await replay.fetchEvents(moduleDesc, { limit: 1 })).toEqual(
      await fullAdapter().fetchEvents(moduleDesc, {}),
    );
    expect(await replay.getHeadVersion!(moduleDesc)).toBe(100n);
    expect(await replay.getEarliestVersion!(moduleDesc)).toBe(2n);
    expect(await replay.getBlock!(moduleDesc, 7n)).toEqual(block);
    expect(await replay.getBlockHeights!(moduleDesc)).toEqual({ oldest: 1n, newest: 9n });
  });

  it("serves repeated identical requests in recorded order, then repeats the last", async () => {
    let head = 10n;
    const recorder = recordingAdapter({
      async fetchEvents() {
        return { events: [], hasMore: false };
      },
      async getHeadVersion() {
        return head++;
      },
    });
    for (let i = 0; i < 3; i++) await recorder.getHeadVersion!(moduleDesc);
    const replay = replayAdapter(recorder.toJSON());
    const heads = [];
    for (let i = 0; i < 5; i++) heads.push(await replay.getHeadVersion!(moduleDesc));
    expect(heads).toEqual([10n, 11n, 12n, 12n, 12n]);
  });

  it("records identical concurrent calls in the order they started", async () => {
    const resolvers: ((head: bigint) => void)[] = [];
    const recorder = recordingAdapter({
      async fetchEvents() {
        return { events: [], hasMore: false };
      },
      getHeadVersion: () => new Promise<bigint>((resolve) => resolvers.push(resolve)),
    });
    const first = recorder.getHeadVersion!(moduleDesc);
    const second = recorder.getHeadVersion!(moduleDesc);
    resolvers[1]!(200n);
    await second;
    resolvers[0]!(100n);
    await first;
    const replay = replayAdapter(recorder.toJSON());
    const [a, b] = await Promise.all([
      replay.getHeadVersion!(moduleDesc),
      replay.getHeadVersion!(moduleDesc),
    ]);
    expect([a, b]).toEqual([100n, 200n]);
  });

  it("drops a failed call and leaves a pending one out until it settles", async () => {
    const settle: [(head: bigint) => void, (err: unknown) => void][] = [];
    const recorder = recordingAdapter({
      async fetchEvents() {
        return { events: [], hasMore: false };
      },
      getHeadVersion: () => new Promise<bigint>((resolve, reject) => settle.push([resolve, reject])),
    });
    const failed = recorder.getHeadVersion!(moduleDesc).catch(() => "failed");
    const ok = recorder.getHeadVersion!(moduleDesc);
    settle[0]![1](new StreamdNetworkError("down"));
    expect(await failed).toBe("failed");
    expect(recorder.toJSON().calls).toEqual([]);
    settle[1]![0](7n);
    await ok;
    const { calls } = recorder.toJSON();
    expect(calls).toHaveLength(1);
    expect(calls[0]!.result).toBe("7");
  });

  it("rejects an unrecorded request with a StreamdError that is not a network error", async () => {
    const recorder = recordingAdapter(new MemoryAdapter(events));
    await recorder.fetchEvents(moduleDesc, { limit: 2 });
    const replay = replayAdapter(recorder.toJSON());
    for (const request of [
      replay.fetchEvents(moduleDesc, { limit: 3 }),
      replay.fetchEvents({ kind: "module", eventType: "0x1::test::Other" }, { limit: 2 }),
      replay.fetchEvents({ kind: "nope" } as unknown as EventDescriptor, { limit: 2 }),
    ]) {
      const err = await request.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
      expect((err as Error).message).toContain("No recorded fetchEvents call");
    }
  });

  it("returns a fresh events array each time", async () => {
    const recorder = recordingAdapter(new MemoryAdapter(events));
    await recorder.fetchEvents(moduleDesc, {});
    const replay = replayAdapter(recorder.toJSON());
    (await replay.fetchEvents(moduleDesc, {})).events.length = 0;
    expect((await replay.fetchEvents(moduleDesc, {})).events).toHaveLength(5);
  });

  it("accepts an empty recording", async () => {
    const replay = replayAdapter(valid([]) as Recording);
    expect(replay.getHeadVersion).toBeUndefined();
    await expect(replay.fetchEvents(moduleDesc, {})).rejects.toThrow(StreamdError);
  });

  const page = { events: [], hasMore: false };
  const boom = () => {
    throw new Error("boom");
  };
  const event = { type: PING, data: {}, version: "1", sequenceNumber: "0", eventIndex: 0 };
  const malformed: [string, unknown, string][] = [
    ["bad JSON", "{nope", "not valid JSON"],
    ["a non-object", 42, "format"],
    ["a wrong format tag", { format: "other", version: 1, calls: [] }, "format"],
    ["an unknown version", { format: "streamd-recording", version: 2, calls: [] }, "version 2"],
    ["calls that are not an array", { format: "streamd-recording", version: 1 }, '"calls"'],
    ["a bigint version", { format: "streamd-recording", version: 1n, calls: [] }, "version 1n"],
    ["a string version", { format: "streamd-recording", version: "1", calls: [] }, 'version "1"'],
    [
      "a null-prototype version",
      { format: "streamd-recording", version: Object.create(null), calls: [] },
      "version {}",
    ],
    ["a bigint method", valid([{ method: 1n, key: "k", result: page }]), "unknown method 1n"],
    [
      "a method with a throwing toString",
      valid([{ method: { toString: boom, toJSON: boom }, key: "k" }]),
      "unknown method object",
    ],
    ["a call without a key", valid([{ method: "fetchEvents", result: page }]), "call 0"],
    ["an unknown method", valid([{ method: "toString", key: "k", result: page }]), "unknown method"],
    ["a page without events", valid([{ method: "fetchEvents", key: "k", result: {} }]), '"events"'],
    [
      "a page without hasMore",
      valid([{ method: "fetchEvents", key: "k", result: { events: [] } }]),
      '"hasMore"',
    ],
    [
      "a non-string cursor",
      valid([{ method: "fetchEvents", key: "k", result: { ...page, nextCursor: 3 } }]),
      '"nextCursor"',
    ],
    [
      "a non-boolean final",
      valid([{ method: "fetchEvents", key: "k", result: { ...page, final: "yes" } }]),
      '"final"',
    ],
    [
      "a negative throughVersion",
      valid([{ method: "fetchEvents", key: "k", result: { ...page, throughVersion: "-1" } }]),
      '"throughVersion"',
    ],
    [
      "a bad event",
      valid([
        { method: "fetchEvents", key: "k", result: { ...page, events: [{ ...event, version: 1 }] } },
      ]),
      '"version"',
    ],
    [
      "a missing event",
      valid([{ method: "fetchEvents", key: "k", result: { ...page, events: [null] } }]),
      "JSON object",
    ],
    ["a numeric head", valid([{ method: "getHeadVersion", key: "k", result: 5 }]), "unsigned decimal"],
    [
      "a block missing a field",
      valid([
        { method: "getBlock", key: "k", result: { height: "1", firstVersion: "1", lastVersion: "2" } },
      ]),
      '"timestamp"',
    ],
    ["non-object heights", valid([{ method: "getBlockHeights", key: "k", result: "1" }]), "object"],
  ];
  for (const [name, input, message] of malformed) {
    it(`throws a StreamdError for ${name}`, () => {
      let caught: unknown;
      try {
        replayAdapter(input as Recording);
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(StreamdError);
      expect((caught as Error).message).toStartWith("Invalid recording:");
      expect((caught as Error).message).toContain(message);
    });
  }
});

describe("recording transaction lookups", () => {
  const hash = `0x${"ef".repeat(32)}`;
  const missing = `0x${"99".repeat(32)}`;
  const seeded: StreamdEvent = {
    type: PING,
    data: { amount: "5" },
    version: 2n ** 64n,
    sequenceNumber: 0n,
    eventIndex: 0,
    transactionHash: hash,
    timestamp: 1700000000000000n,
  };

  it("replays found and not-found lookups, bigints included, through eventsOf", async () => {
    const seen: unknown[] = [];
    const source = new MemoryAdapter([seeded]);
    const lookup = source.getTransactionEvents.bind(source);
    source.getTransactionEvents = (...args) => (seen.push(args), lookup(...args));
    const recorder = recordingAdapter(source);
    const live = await recorder.getTransactionEvents!(hash, moduleDesc);
    expect(seen[0]).toEqual([hash, moduleDesc]);
    expect(await recorder.getTransactionEvents!(missing)).toBeUndefined();

    const replay = replayAdapter(JSON.stringify(recorder));
    expect(await replay.getTransactionEvents!(hash)).toEqual(live);
    expect(await replay.getTransactionEvents!(missing)).toBeUndefined();
    expect((await replay.getTransactionEvents!(hash))?.events).toEqual([seeded]);
    await expect(replay.getTransactionEvents!(`0x${"aa".repeat(32)}`)).rejects.toBeInstanceOf(StreamdError);
  });

  it("has no getTransactionEvents when the source or the recording lacks it", () => {
    expect(recordingAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }).getTransactionEvents)
      .toBeUndefined();
    expect(replayAdapter({ format: "streamd-recording", version: 1, calls: [] }).getTransactionEvents).toBeUndefined();
  });

  it("refuses a malformed recorded lookup", () => {
    const bad = (result: unknown): Recording => ({
      format: "streamd-recording",
      version: 1,
      calls: [{ method: "getTransactionEvents", key: hash, result }],
    });
    for (const result of [
      { version: "1", success: "yes", vmStatus: "", events: [] },
      { version: "-1", success: true, vmStatus: "", events: [] },
      { version: "1", success: true, vmStatus: 3, events: [] },
      { version: "1", success: true, vmStatus: "", events: {} },
      undefined,
    ]) {
      expect(() => replayAdapter(bad(result))).toThrow("Invalid recording");
    }
  });
});

describe("recording lookups by version", () => {
  const seeded: StreamdEvent = {
    type: PING,
    data: { amount: "5" },
    version: 2n ** 63n,
    sequenceNumber: 0n,
    eventIndex: 0,
    transactionHash: `0x${"ef".repeat(32)}`,
    timestamp: 1700000000000000n,
  };
  const found = { version: 2n ** 63n, success: true, vmStatus: "Executed successfully", events: [seeded] };

  it("replays found and not-found lookups keyed by version, bigints included, through eventsAt", async () => {
    const seen: unknown[] = [];
    const recorder = recordingAdapter({
      fetchEvents: async () => ({ events: [], hasMore: false }),
      getTransactionEventsByVersion: async (...args) => (seen.push(args), args[0] === found.version ? found : undefined),
    });
    expect(await recorder.getTransactionEventsByVersion!(found.version, moduleDesc)).toBe(found);
    expect(seen[0]).toEqual([found.version, moduleDesc]);
    expect(await recorder.getTransactionEventsByVersion!(3n)).toBeUndefined();
    expect(recorder.toJSON().calls.map((c) => [c.method, c.key])).toEqual([
      ["getTransactionEventsByVersion", String(2n ** 63n)],
      ["getTransactionEventsByVersion", "3"],
    ]);

    const replay = replayAdapter(JSON.stringify(recorder));
    expect(await replay.getTransactionEventsByVersion!(found.version)).toEqual(found);
    expect(await replay.getTransactionEventsByVersion!(3n)).toBeUndefined();
    expect(await eventsAt(replay, new RequestLimiter(), found.version, moduleDesc, {})).toEqual([seeded]);
    await expect(replay.getTransactionEventsByVersion!(4n)).rejects.toBeInstanceOf(StreamdError);
  });

  it("has no getTransactionEventsByVersion when the source or the recording lacks it", () => {
    expect(recordingAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }).getTransactionEventsByVersion)
      .toBeUndefined();
    expect(replayAdapter({ format: "streamd-recording", version: 1, calls: [] }).getTransactionEventsByVersion).toBeUndefined();
  });

  it("refuses a malformed recorded lookup", () => {
    for (const result of [{ version: "1", success: "yes", vmStatus: "", events: [] }, undefined]) {
      const bad: Recording = {
        format: "streamd-recording",
        version: 1,
        calls: [{ method: "getTransactionEventsByVersion", key: "1", result }],
      };
      expect(() => replayAdapter(bad)).toThrow("Invalid recording");
    }
  });
});

describe("recordingAdapter simulation", () => {
  const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };

  it("passes simulateTransactionEvents through unrecorded, and replay never has it", async () => {
    const seen: unknown[] = [];
    const recorder = recordingAdapter({
      fetchEvents: async () => ({ events: [], hasMore: false }),
      simulateTransactionEvents: async (...args) => (seen.push(args), simulated),
    });
    expect(await recorder.simulateTransactionEvents!(simulation, moduleDesc)).toBe(simulated);
    expect(seen).toEqual([[simulation, moduleDesc]]);
    expect(recorder.toJSON().calls).toEqual([]);
    expect(replayAdapter(JSON.stringify(recorder)).simulateTransactionEvents).toBeUndefined();
    expect(recordingAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }).simulateTransactionEvents)
      .toBeUndefined();
  });
});

describe("recordingAdapter view", () => {
  const request = { function: "0x1::coin::balance" as const, functionArguments: ["0xa", 7n] };

  it("records views by request and version, and replays them offline", async () => {
    let calls = 0;
    const source = Object.assign(new MemoryAdapter(), {
      view: async (_r: unknown, version: bigint) => (calls++, [{ value: String(version) }]),
    });
    const recorder = recordingAdapter(source);
    expect(await recorder.view!(request, 3n)).toEqual([{ value: "3" }]);
    await recorder.view!(request, 4n);
    const replay = replayAdapter(JSON.stringify(recorder));
    expect(calls).toBe(2);
    const replayed = await replay.view!({ ...request, functionArguments: ["0xa", "7"] }, 3n);
    expect(replayed).toEqual([{ value: "3" }]);
    (replayed[0] as { value: string }).value = "changed";
    expect(await replay.view!(request, 3n)).toEqual([{ value: "3" }]);
    expect(await replay.view!(request, 4n)).toEqual([{ value: "4" }]);
    await expect(replay.view!(request, 5n)).rejects.toThrow("No recorded view call");
    await expect(replay.view!({ ...request, typeArguments: ["0x1::a::B"] }, 3n)).rejects.toThrow(
      "No recorded view call",
    );
    expect(replayAdapter(recordingAdapter(new MemoryAdapter()).toJSON()).view).toBeUndefined();
  });

  it("refuses a recorded view whose result is not an array", () => {
    const bad = { format: "streamd-recording", version: 1, calls: [{ method: "view", key: "k", result: {} }] };
    expect(() => replayAdapter(bad as never)).toThrow("expected an array");
  });

  it("serves withState offline from a recording", async () => {
    const event: StreamdEvent = { type: "0x1::a::B", data: {}, version: 9n, sequenceNumber: 0n, eventIndex: 0 };
    const source = Object.assign(new MemoryAdapter([event]), {
      view: async (_r: unknown, version: bigint) => [String(version)],
    });
    const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::a::B" };
    const options = { fromVersion: 0n, withState: () => request };
    const recorder = recordingAdapter(source);
    await new StreamdClient({ adapter: recorder }).getEvents(descriptor, options);
    const offline = new StreamdClient({ adapter: replayAdapter(JSON.stringify(recorder)) });
    expect((await offline.getEvents(descriptor, options))[0]!.state).toEqual(["9"]);
  });
});

describe("recordingAdapter readObjects", () => {
  it("records object reads by ids, version and hash, and replays them offline", async () => {
    let calls = 0;
    const source = Object.assign(new MemoryAdapter(), {
      readObjects: async (_r: unknown, at: { version: bigint }) => (calls++, [{ v: String(at.version) }, null]),
    });
    const recorder = recordingAdapter(source);
    const request = { objects: ["0xa", "0xb"] };
    expect(await recorder.readObjects!(request, { version: 3n, transactionHash: "t3" })).toEqual([{ v: "3" }, null]);
    const replay = replayAdapter(JSON.stringify(recorder));
    expect(calls).toBe(1);
    const replayed = await replay.readObjects!(request, { version: 3n, transactionHash: "t3" });
    expect(replayed).toEqual([{ v: "3" }, null]);
    (replayed[0] as { v: string }).v = "changed";
    expect(await replay.readObjects!(request, { version: 3n, transactionHash: "t3" })).toEqual([{ v: "3" }, null]);
    for (const [ids, at] of [
      [["0xa"], { version: 3n, transactionHash: "t3" }],
      [["0xa", "0xb"], { version: 4n, transactionHash: "t3" }],
      [["0xa", "0xb"], { version: 3n, transactionHash: "t4" }],
    ] as const) {
      await expect(replay.readObjects!({ objects: ids }, at)).rejects.toThrow("No recorded readObjects call");
    }
    expect(replayAdapter(recordingAdapter(new MemoryAdapter()).toJSON()).readObjects).toBeUndefined();
  });
});
