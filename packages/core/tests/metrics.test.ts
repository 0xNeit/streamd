import { describe, expect, it } from "bun:test";
import { adapterMetrics } from "../src/adapters/metrics.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, ViewRequest } from "../src/types.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };

/** Each `view` takes the next duration from `delays` and fails with the next of `failures`. */
function source(delays: number[] = [], failures: (Error | undefined)[] = []) {
  const seen: unknown[][] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    getHeadVersion: async () => 7n,
    view: async (...args) => {
      seen.push(args);
      await new Promise((r) => setTimeout(r, delays.shift() ?? 0));
      const failure = failures.shift();
      if (failure !== undefined) throw failure;
      return ["ok"];
    },
  };
  return { adapter, seen };
}

const request: ViewRequest = { function: "0x1::m::f" };

describe("adapterMetrics", () => {
  it("counts each method apart, leaving out what was not called", async () => {
    const metrics = adapterMetrics(source().adapter);
    await metrics.adapter.fetchEvents(descriptor, {});
    await metrics.adapter.fetchEvents(descriptor, {});
    await metrics.adapter.getHeadVersion!(descriptor);
    const snapshot = metrics.snapshot();
    expect(Object.keys(snapshot).sort()).toEqual(["fetchEvents", "getHeadVersion"]);
    expect(snapshot.fetchEvents).toMatchObject({ requests: 2, errors: 0, rateLimited: 0 });
    expect(snapshot.getHeadVersion).toMatchObject({ requests: 1 });
  });

  it("passes arguments, results and errors through unchanged", async () => {
    const boom = new StreamdError("boom");
    const { adapter, seen } = source([], [undefined, boom]);
    const { adapter: wrapped } = adapterMetrics(adapter);
    expect(await wrapped.view!(request, 5n, descriptor)).toEqual(["ok"]);
    expect(seen[0]).toEqual([request, 5n, descriptor]);
    await expect(wrapped.view!(request, 5n)).rejects.toBe(boom);
  });

  it("keeps an adapter's methods bound to it", async () => {
    class Stateful extends MemoryAdapter {}
    const { adapter } = adapterMetrics(new Stateful([]));
    expect(await adapter.fetchEvents(descriptor, { fromVersion: 0n })).toMatchObject({ events: [] });
  });

  it("separates errors from rate limits, by status or by retryAfter", async () => {
    const failures = [
      new StreamdNetworkError("limited", 429),
      new StreamdNetworkError("hinted", 503, undefined, 100),
      new StreamdNetworkError("down", 503),
      new Error("other"),
      undefined,
    ];
    const metrics = adapterMetrics(source([], failures).adapter);
    for (let i = 0; i < 5; i++) await metrics.adapter.view!(request, 1n).catch(() => {});
    expect(metrics.snapshot().view).toMatchObject({ requests: 5, errors: 4, rateLimited: 2 });
  });

  it("gives nearest-rank percentiles over settled calls", async () => {
    const original = performance.now;
    let clock = 0;
    performance.now = () => clock;
    try {
      const durations = Array.from({ length: 100 }, (_, i) => i + 1);
      const adapter: EventAdapter = {
        fetchEvents: async () => {
          clock += durations.shift()!;
          return { events: [], hasMore: false };
        },
      };
      const metrics = adapterMetrics(adapter);
      for (let i = 0; i < 100; i++) await metrics.adapter.fetchEvents(descriptor, {});
      expect(metrics.snapshot().fetchEvents!.latency).toEqual({ p50: 50, p95: 95, p99: 99, max: 100 });
    } finally {
      performance.now = original;
    }
  });

  it("counts a call in flight, with no latency until one settles", async () => {
    let finish = () => {};
    const adapter: EventAdapter = {
      fetchEvents: () => new Promise((resolve) => (finish = () => resolve({ events: [], hasMore: false }))),
    };
    const metrics = adapterMetrics(adapter);
    const pending = metrics.adapter.fetchEvents(descriptor, {});
    expect(metrics.snapshot().fetchEvents).toEqual({ requests: 1, errors: 0, rateLimited: 0 });
    finish();
    await pending;
    expect(metrics.snapshot().fetchEvents!.latency).toBeDefined();
  });

  it("keeps only the most recent 1024 samples", async () => {
    const original = performance.now;
    let clock = 0;
    performance.now = () => clock;
    try {
      let duration = 1000;
      const adapter: EventAdapter = {
        fetchEvents: async () => {
          clock += duration;
          return { events: [], hasMore: false };
        },
      };
      const metrics = adapterMetrics(adapter);
      await metrics.adapter.fetchEvents(descriptor, {});
      duration = 1;
      for (let i = 0; i < 1024; i++) await metrics.adapter.fetchEvents(descriptor, {});
      const { requests, latency } = metrics.snapshot().fetchEvents!;
      expect(requests).toBe(1025);
      expect(latency!.max).toBe(1);
    } finally {
      performance.now = original;
    }
  });

  it("forgets on reset, and a call running across it does not bring old counts back", async () => {
    let finish = () => {};
    const adapter: EventAdapter = {
      fetchEvents: () => new Promise((resolve) => (finish = () => resolve({ events: [], hasMore: false }))),
    };
    const metrics = adapterMetrics(adapter);
    const pending = metrics.adapter.fetchEvents(descriptor, {});
    metrics.reset();
    finish();
    await pending;
    expect(metrics.snapshot()).toEqual({});
  });

  it("leaves absent optional methods absent, and refuses a non-adapter", () => {
    const { adapter } = adapterMetrics({ fetchEvents: async () => ({ events: [], hasMore: false }) });
    expect(adapter.view).toBeUndefined();
    expect(adapter.getHeadVersion).toBeUndefined();
    expect(() => adapterMetrics({} as never)).toThrow(StreamdError);
  });

  it("works under a client", async () => {
    const metrics = adapterMetrics(new MemoryAdapter([]));
    const client = new StreamdClient({ adapter: metrics.adapter });
    await client.getEvents(descriptor, { fromVersion: 0n });
    expect(metrics.snapshot().fetchEvents!.requests).toBeGreaterThan(0);
    client.destroy();
  });
});
