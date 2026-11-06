import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { recordingAdapter } from "../src/adapters/recording.js";
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
