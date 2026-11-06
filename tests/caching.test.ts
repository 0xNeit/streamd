import { describe, it, expect } from "bun:test";
import { CachingAdapter } from "../src/adapters/caching.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "../src/types.js";

const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";
const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };

function ev(version: number): StreamdEvent {
  return { type: PING, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

/** Counts fetches and answers each with a page built from the request. */
function counting(final: boolean | "unset" = true) {
  const calls: FetchOptions[] = [];
  const adapter: EventAdapter = {
    async fetchEvents(_descriptor, options) {
      calls.push(options);
      const page: EventPage = { events: [ev(calls.length)], nextCursor: "9", hasMore: true };
      if (final !== "unset") page.final = final;
      return page;
    },
  };
  return { adapter, calls };
}

describe("CachingAdapter", () => {
  it("serves a final page from the source once, then from the cache", async () => {
    const { adapter, calls } = counting();
    const cache = new CachingAdapter(adapter);
    const first = await cache.fetchEvents(moduleDesc, { fromVersion: 5n, limit: 2 });
    const second = await cache.fetchEvents(moduleDesc, { limit: 2, fromVersion: 5n });

    expect(calls).toHaveLength(1);
    expect(second).toEqual(first);
  });

  it("passes through pages that are not final, without storing them", async () => {
    for (const final of [false, "unset"] as const) {
      const { adapter, calls } = counting(final);
      const cache = new CachingAdapter(adapter);
      await cache.fetchEvents(moduleDesc, { cursor: "1" });
      await cache.fetchEvents(moduleDesc, { cursor: "1" });
      expect(calls).toHaveLength(2);
    }
  });

  it("stores nothing for a MemoryAdapter, whose pages are never final", async () => {
    const memory = new MemoryAdapter([ev(1), ev(2)]);
    const page = await new CachingAdapter(memory).fetchEvents(moduleDesc, { fromVersion: 0n });
    expect(page.final).toBeUndefined();
  });

  it("does not store a failure", async () => {
    let fail = true;
    const adapter: EventAdapter = {
      async fetchEvents() {
        if (fail) throw new StreamdNetworkError("down");
        return { events: [], hasMore: false, final: true };
      },
    };
    const cache = new CachingAdapter(adapter);
    await expect(cache.fetchEvents(moduleDesc, {})).rejects.toThrow("down");
    fail = false;
    expect((await cache.fetchEvents(moduleDesc, {})).final).toBe(true);
  });

  it("keys pages apart by every option and by what the descriptor selects", async () => {
    const { adapter, calls } = counting();
    const cache = new CachingAdapter(adapter);
    const requests: [EventDescriptor, FetchOptions][] = [
      [moduleDesc, { cursor: "1" }],
      [moduleDesc, { cursor: "2" }],
      [moduleDesc, { cursor: "1", limit: 5 }],
      [moduleDesc, { cursor: "1", orderBy: "desc" }],
      [moduleDesc, { fromVersion: 1n }],
      [moduleDesc, { fromVersion: 1n, toVersion: 9n }],
      [moduleDesc, { offset: 1 }],
      [{ kind: "module", eventType: PONG }, { cursor: "1" }],
    ];
    for (const [d, o] of requests) await cache.fetchEvents(d, o);
    expect(calls).toHaveLength(requests.length);

    for (const [d, o] of requests) await cache.fetchEvents(d, o);
    expect(calls).toHaveLength(requests.length);
  });

  it("shares an entry between descriptors that select the same events", async () => {
    const { adapter, calls } = counting();
    const cache = new CachingAdapter(adapter);
    await cache.fetchEvents({ kind: "module", eventType: [PING, PONG] }, { cursor: "1" });
    await cache.fetchEvents({ kind: "module", eventType: [PONG, PING] }, { cursor: "1" });
    await cache.fetchEvents({ kind: "account", accountAddress: "0x1" }, { cursor: "1" });
    await cache.fetchEvents(
      { kind: "account", accountAddress: "0x" + "0".repeat(63) + "1" },
      { cursor: "1", limit: undefined },
    );
    expect(calls).toHaveLength(2);
  });

  it("passes a descriptor of unknown kind through uncached", async () => {
    const { adapter, calls } = counting();
    const cache = new CachingAdapter(adapter);
    const odd = { kind: "resource" } as unknown as EventDescriptor;
    await cache.fetchEvents(odd, {});
    await cache.fetchEvents(odd, {});
    expect(calls).toHaveLength(2);
  });

  it("drops the least recently used page past maxEntries", async () => {
    const { adapter, calls } = counting();
    const cache = new CachingAdapter(adapter, { maxEntries: 2 });
    const get = (cursor: string) => cache.fetchEvents(moduleDesc, { cursor });
    await get("a");
    await get("b");
    await get("a"); // hit: "b" is now the oldest
    await get("c"); // evicts "b"
    expect(calls).toHaveLength(3);
    await get("a");
    await get("c");
    expect(calls).toHaveLength(3);
    await get("b");
    expect(calls).toHaveLength(4);
  });

  it("keeps what it serves safe from a caller that mutates a result", async () => {
    const { adapter } = counting();
    const cache = new CachingAdapter(adapter);
    const first = await cache.fetchEvents(moduleDesc, { cursor: "1" });
    first.events.length = 0;
    first.nextCursor = "x";
    const second = await cache.fetchEvents(moduleDesc, { cursor: "1" });
    second.events.push(ev(99));
    const third = await cache.fetchEvents(moduleDesc, { cursor: "1" });

    expect(third.events.map((e) => e.version)).toEqual([1n]);
    expect(third.nextCursor).toBe("9");
  });

  it("forwards the optional methods, uncached, only when the source has them", async () => {
    let heads = 0;
    const full: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      getHeadVersion: async () => BigInt(++heads),
      getEarliestVersion: async () => 3n,
      getBlock: async (_d, height) => ({
        height,
        firstVersion: 1n,
        lastVersion: 2n,
        timestamp: 0n,
      }),
      getBlockHeights: async () => ({ oldest: 4n, newest: 8n }),
    };
    const cache = new CachingAdapter(full);
    expect(await cache.getHeadVersion!(moduleDesc)).toBe(1n);
    expect(await cache.getHeadVersion!(moduleDesc)).toBe(2n);
    expect(await cache.getEarliestVersion!(moduleDesc)).toBe(3n);
    expect((await cache.getBlock!(moduleDesc, 7n)).height).toBe(7n);
    expect(await cache.getBlockHeights!(moduleDesc)).toEqual({ oldest: 4n, newest: 8n });

    const bare = new CachingAdapter({ fetchEvents: full.fetchEvents });
    expect(bare.getHeadVersion).toBeUndefined();
    expect(bare.getEarliestVersion).toBeUndefined();
    expect(bare.getBlock).toBeUndefined();
    expect(bare.getBlockHeights).toBeUndefined();
  });

  it("rejects an invalid adapter or maxEntries", () => {
    const { adapter } = counting();
    expect(() => new CachingAdapter({} as EventAdapter)).toThrow(StreamdError);
    for (const maxEntries of [0, -1, 1.5, Number.NaN]) {
      expect(() => new CachingAdapter(adapter, { maxEntries })).toThrow("Invalid maxEntries");
    }
  });
});

describe("CachingAdapter transaction lookup", () => {
  const hash = `0x${"ef".repeat(32)}`;
  const event: StreamdEvent = {
    type: "0x1::t::A",
    data: {},
    version: 2n,
    sequenceNumber: 0n,
    eventIndex: 0,
    transactionHash: hash,
  };

  it("passes getTransactionEvents through, only when the source has it", async () => {
    const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };
    const seen: unknown[] = [];
    const source = new MemoryAdapter([event]);
    const lookup = source.getTransactionEvents.bind(source);
    source.getTransactionEvents = (...args) => (seen.push(args), lookup(...args));
    expect((await new CachingAdapter(source).getTransactionEvents!(hash, descriptor))!.events).toEqual([
      event,
    ]);
    expect(seen).toEqual([[hash, descriptor]]);
    expect(
      new CachingAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }).getTransactionEvents,
    ).toBeUndefined();
  });
});
