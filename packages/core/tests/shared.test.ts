import { describe, it, expect, vi, afterEach, beforeEach } from "bun:test";
import { SharedFetches } from "../src/shared.js";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../src/types.js";

const desc: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };
const page = (n: number): EventPage => ({ events: [], nextCursor: String(n), hasMore: false });

function spy(result: (call: number) => Promise<EventPage> = (n) => Promise.resolve(page(n))) {
  const calls: FetchOptions[] = [];
  const adapter: EventAdapter = {
    fetchEvents: (_d, options) => (calls.push(options), result(calls.length)),
  };
  return { adapter, calls };
}

describe("SharedFetches", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("serves identical concurrent and within-interval calls from one request", async () => {
    let release!: () => void;
    const s = spy((n) => new Promise((r) => (release = () => r(page(n)))));
    const shared = new SharedFetches(s.adapter);
    const a = shared.view(1000).fetchEvents(desc, { cursor: "5", limit: 25 });
    const b = shared.view(1000).fetchEvents(desc, { cursor: "5", limit: 25 });
    await Promise.resolve();
    release();
    expect(await a).toBe(await b);
    vi.advanceTimersByTime(999);
    expect(await shared.view(1000).fetchEvents(desc, { cursor: "5", limit: 25 })).toBe(await a);
    expect(s.calls).toHaveLength(1);
  });

  it("shares across descriptors with the same value", async () => {
    const s = spy();
    const shared = new SharedFetches(s.adapter);
    const view = shared.view(1000);
    await view.fetchEvents({ kind: "module", eventType: ["0x1::m::A", "0x1::m::B"] }, { cursor: "1" });
    await view.fetchEvents({ kind: "module", eventType: ["0x1::m::B", "0x1::m::A"] }, { cursor: "1" });
    expect(s.calls).toHaveLength(1);
  });

  it("requests again once the page is a poll interval old", async () => {
    const s = spy();
    const view = new SharedFetches(s.adapter).view(1000);
    await view.fetchEvents(desc, { cursor: "5" });
    vi.advanceTimersByTime(1000);
    await view.fetchEvents(desc, { cursor: "5" });
    expect(s.calls).toHaveLength(2);
  });

  it("never shares a start at the head", async () => {
    const s = spy();
    const view = new SharedFetches(s.adapter).view(1000);
    await Promise.all([view.fetchEvents(desc, { limit: 25 }), view.fetchEvents(desc, { limit: 25 })]);
    expect(s.calls).toHaveLength(2);
  });

  it("shares an absolute start, keyed by its version", async () => {
    const s = spy();
    const view = new SharedFetches(s.adapter).view(1000);
    await view.fetchEvents(desc, { fromVersion: 10n });
    await view.fetchEvents(desc, { fromVersion: 10n });
    await view.fetchEvents(desc, { fromVersion: 11n });
    expect(s.calls).toHaveLength(2);
  });

  it("keeps calls apart that differ in descriptor, any option, or interval", async () => {
    const s = spy();
    const shared = new SharedFetches(s.adapter);
    const base: FetchOptions = { cursor: "5", limit: 25, orderBy: "asc" };
    await shared.view(1000).fetchEvents(desc, base);
    await shared.view(1000).fetchEvents({ kind: "module", eventType: "0x1::m::F" }, base);
    await shared.view(1000).fetchEvents(desc, { ...base, cursor: "6" });
    await shared.view(1000).fetchEvents(desc, { ...base, limit: 10 });
    await shared.view(1000).fetchEvents(desc, { ...base, orderBy: "desc" });
    await shared.view(1000).fetchEvents(desc, { ...base, offset: 1 });
    await shared.view(1000).fetchEvents(desc, { ...base, toVersion: 9n });
    await shared.view(500).fetchEvents(desc, base);
    await shared.view(1000).fetchEvents(desc, { orderBy: "asc", limit: 25, cursor: "5" });
    expect(s.calls).toHaveLength(8);
  });

  it("hands a failure to the calls already waiting, then requests again", async () => {
    let reject!: (e: Error) => void;
    const s = spy((n) =>
      n === 1 ? new Promise((_, r) => (reject = r)) : Promise.resolve(page(n)),
    );
    const view = new SharedFetches(s.adapter).view(1000);
    const a = view.fetchEvents(desc, { cursor: "5" });
    const b = view.fetchEvents(desc, { cursor: "5" });
    await Promise.resolve();
    reject(new Error("down"));
    await expect(a).rejects.toThrow("down");
    await expect(b).rejects.toThrow("down");
    expect((await view.fetchEvents(desc, { cursor: "5" })).nextCursor).toBe("2");
    expect(s.calls).toHaveLength(2);
  });

  it("turns a synchronous throw into a rejection", async () => {
    const view = new SharedFetches({
      fetchEvents: () => {
        throw new Error("sync");
      },
    }).view(1000);
    await expect(view.fetchEvents(desc, { cursor: "5" })).rejects.toThrow("sync");
  });

  it("passes the optional methods through, and only those the adapter has", async () => {
    const adapter: EventAdapter = {
      fetchEvents: () => Promise.resolve(page(0)),
      getHeadVersion: async function (this: unknown) {
        return this === adapter ? 7n : -1n;
      },
    };
    const view = new SharedFetches(adapter).view(1000);
    expect(await view.getHeadVersion!(desc)).toBe(7n);
    expect(view.getEarliestVersion).toBeUndefined();
    expect(view.getBlock).toBeUndefined();
    expect(view.getBlockHeights).toBeUndefined();
  });

  it("joins a backfill page in flight but does not keep it", async () => {
    const s = spy((n) => Promise.resolve({ ...page(n), hasMore: true }));
    const view = new SharedFetches(s.adapter).view(1000);
    await Promise.all([view.fetchEvents(desc, { cursor: "5" }), view.fetchEvents(desc, { cursor: "5" })]);
    await view.fetchEvents(desc, { cursor: "5" });
    expect(s.calls).toHaveLength(2);
  });

  it("keeps a slow request joinable past its interval", async () => {
    let release!: () => void;
    const s = spy((n) => new Promise((r) => (release = () => r(page(n)))));
    const view = new SharedFetches(s.adapter).view(100);
    const a = view.fetchEvents(desc, { cursor: "5" });
    vi.advanceTimersByTime(500);
    const b = view.fetchEvents(desc, { cursor: "5" });
    await Promise.resolve();
    release();
    expect(await a).toBe(await b);
    expect(s.calls).toHaveLength(1);
  });

  it("never shares under an interval a timer cannot honour", async () => {
    for (const interval of [0, -1, NaN, Infinity, 3_000_000_000]) {
      const s = spy();
      const view = new SharedFetches(s.adapter).view(interval);
      await Promise.all([view.fetchEvents(desc, { cursor: "5" }), view.fetchEvents(desc, { cursor: "5" })]);
      expect(s.calls).toHaveLength(2);
    }
  });

  it("forgets every kept page on clear()", async () => {
    const s = spy();
    const shared = new SharedFetches(s.adapter);
    await shared.view(1000).fetchEvents(desc, { cursor: "5" });
    shared.clear();
    await shared.view(1000).fetchEvents(desc, { cursor: "5" });
    expect(s.calls).toHaveLength(2);
  });
});

describe("SharedFetches view", () => {
  it("passes view through to the adapter", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      view: async (_r, version) => [String(version)],
    };
    expect(await new SharedFetches(adapter).view(1000).view!({ function: "0x1::coin::balance" }, 5n)).toEqual([
      "5",
    ]);
  });
});
