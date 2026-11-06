import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { recordingAdapter, replayAdapter, type Recording } from "../src/adapters/recording.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

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
