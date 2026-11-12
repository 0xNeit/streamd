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
  TransactionSimulation,
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
    const odd = { kind: "table" } as unknown as EventDescriptor;
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

describe("CachingAdapter simulation", () => {
  const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };

  it("passes simulateTransactionEvents through uncached, only when the source has it", async () => {
    const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };
    const seen: unknown[] = [];
    const caching = new CachingAdapter({
      fetchEvents: async () => ({ events: [], hasMore: false }),
      simulateTransactionEvents: async (...args) => (seen.push(args), simulated),
    });
    expect(await caching.simulateTransactionEvents!(simulation, descriptor)).toBe(simulated);
    expect(await caching.simulateTransactionEvents!(simulation, descriptor)).toBe(simulated);
    expect(seen).toEqual([[simulation, descriptor], [simulation, descriptor]]);
    expect(
      new CachingAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }).simulateTransactionEvents,
    ).toBeUndefined();
  });
});

describe("CachingAdapter view", () => {
  it("passes view through uncached, only when the source has it", async () => {
    let calls = 0;
    const source = Object.assign(new MemoryAdapter(), {
      view: async (_r: unknown, version: bigint) => (calls++, [String(version)]),
    });
    const cached = new CachingAdapter(source);
    const request = { function: "0x1::coin::balance" as const };
    expect(await cached.view!(request, 4n)).toEqual(["4"]);
    expect(await cached.view!(request, 4n)).toEqual(["4"]);
    expect(calls).toBe(2);
    expect(new CachingAdapter(new MemoryAdapter()).view).toBeUndefined();
  });
});

describe("CachingAdapter with a page store", () => {
  const memoryStore = (entries = new Map<string, string>()) => ({
    entries,
    get: (key: string) => entries.get(key),
    set: (key: string, page: string) => void entries.set(key, page),
  });
  const full: StreamdEvent = {
    type: PING,
    data: { n: "1" },
    version: 2n ** 63n,
    sequenceNumber: 7n,
    eventIndex: 3,
    transactionHash: "0xab",
    timestamp: 9n,
    sender: "0x1",
    guid: { creationNumber: 4n, accountAddress: "0x2" },
    entryFunction: { function: "0x1::m::f", typeArguments: [], arguments: ["1"] },
  };
  const finalPage: EventPage = { events: [full], nextCursor: "9", hasMore: true, final: true, throughVersion: 2n ** 63n };
  const source = (page: EventPage = finalPage) => {
    let fetches = 0;
    const adapter: EventAdapter = { fetchEvents: async () => (fetches++, page) };
    return { adapter, fetches: () => fetches };
  };
  const options: FetchOptions = { fromVersion: 1n, limit: 10 };

  it("serves a restart from the store, with the page intact and no fetch", async () => {
    const store = memoryStore();
    const first = source();
    await new CachingAdapter(first.adapter, { store }).fetchEvents(moduleDesc, options);
    expect(store.entries.size).toBe(1);
    const second = source();
    const page = await new CachingAdapter(second.adapter, { store }).fetchEvents(moduleDesc, options);
    expect(second.fetches()).toBe(0);
    expect(page).toEqual(finalPage);
  });

  it("awaits an asynchronous store", async () => {
    const entries = new Map<string, string>();
    const store = {
      get: async (key: string) => entries.get(key) ?? null,
      set: async (key: string, page: string) => void entries.set(key, page),
    };
    await new CachingAdapter(source().adapter, { store }).fetchEvents(moduleDesc, options);
    const second = source();
    await new CachingAdapter(second.adapter, { store }).fetchEvents(moduleDesc, options);
    expect(second.fetches()).toBe(0);
  });

  it("never stores a page that is not final, and keys by request", async () => {
    const store = memoryStore();
    await new CachingAdapter(source({ events: [], hasMore: false }).adapter, { store }).fetchEvents(moduleDesc, options);
    expect(store.entries.size).toBe(0);
    const cache = new CachingAdapter(source().adapter, { store });
    await cache.fetchEvents(moduleDesc, options);
    await cache.fetchEvents(moduleDesc, { ...options, limit: 11 });
    expect(store.entries.size).toBe(2);
  });

  it("answers from memory before the store, and fills memory from it", async () => {
    let reads = 0;
    const inner = memoryStore();
    const store = { get: (key: string) => (reads++, inner.get(key)), set: inner.set };
    await new CachingAdapter(source().adapter, { store }).fetchEvents(moduleDesc, options);
    reads = 0;
    const cache = new CachingAdapter(source().adapter, { store });
    await cache.fetchEvents(moduleDesc, options);
    await cache.fetchEvents(moduleDesc, options);
    expect(reads).toBe(1);
  });

  it("treats a failing or corrupt store as a miss, reporting each", async () => {
    const errors: unknown[] = [];
    const onStoreError = (err: unknown) => errors.push(err);
    const broken = { get: () => { throw new Error("read"); }, set: () => { throw new Error("write"); } };
    const a = source();
    expect(await new CachingAdapter(a.adapter, { store: broken, onStoreError }).fetchEvents(moduleDesc, options)).toEqual(finalPage);
    expect(a.fetches()).toBe(1);
    expect(errors.map((e) => (e as Error).message)).toEqual(["read", "write"]);

    for (const bad of ["not json", '{"events":"x","hasMore":true}', JSON.stringify({ events: [], hasMore: false })]) {
      errors.length = 0;
      const store = { get: () => bad, set: () => {} };
      const b = source();
      await new CachingAdapter(b.adapter, { store, onStoreError }).fetchEvents(moduleDesc, options);
      expect(b.fetches()).toBe(1);
      expect(errors).toHaveLength(1);
    }
  });

  it("works without onStoreError, and refuses a store with no methods", async () => {
    const broken = { get: () => Promise.reject(new Error("read")), set: () => Promise.reject(new Error("write")) };
    expect(await new CachingAdapter(source().adapter, { store: broken }).fetchEvents(moduleDesc, options)).toEqual(finalPage);
    expect(() => new CachingAdapter(source().adapter, { store: {} as never })).toThrow(StreamdError);
  });
});

describe("CachingAdapter readObjects", () => {
  it("passes readObjects through uncached, only when the source has it", async () => {
    let calls = 0;
    const source = Object.assign(new MemoryAdapter(), {
      readObjects: async (_r: unknown, at: { version: bigint }) => (calls++, [String(at.version)]),
    });
    const cached = new CachingAdapter(source);
    expect(await cached.readObjects!({ objects: ["0x1"] }, { version: 4n })).toEqual(["4"]);
    expect(await cached.readObjects!({ objects: ["0x1"] }, { version: 4n })).toEqual(["4"]);
    expect(calls).toBe(2);
    expect(new CachingAdapter(new MemoryAdapter()).readObjects).toBeUndefined();
  });
});
