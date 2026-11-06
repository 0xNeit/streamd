import { describe, it, expect } from "bun:test";
import { RequestLimiter } from "../src/limiter.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventPage, TransactionSimulation } from "../src/types.js";

const flush = () => new Promise<void>((r) => setTimeout(r, 0));

/** Tasks that settle only when the test says so, recording start order and peak concurrency. */
function harness() {
  const started: number[] = [];
  const settle = new Map<number, { ok: () => void; fail: () => void }>();
  let inFlight = 0;
  let peak = 0;
  const task = (id: number) => () =>
    new Promise<number>((resolve, reject) => {
      started.push(id);
      peak = Math.max(peak, ++inFlight);
      settle.set(id, {
        ok: () => (inFlight--, resolve(id)),
        fail: () => (inFlight--, reject(new Error(`task ${id}`))),
      });
    });
  return { started, settle, task, peak: () => peak };
}

describe("RequestLimiter", () => {
  it("never runs more than max tasks at once, starting queued ones in FIFO order", async () => {
    const limiter = new RequestLimiter(2);
    const h = harness();
    const results = [1, 2, 3, 4, 5].map((id) => limiter.run(h.task(id)));
    await flush();
    expect(h.started).toEqual([1, 2]);

    h.settle.get(2)!.ok();
    await flush();
    expect(h.started).toEqual([1, 2, 3]);

    h.settle.get(1)!.ok();
    h.settle.get(3)!.ok();
    await flush();
    h.settle.get(4)!.ok();
    h.settle.get(5)!.ok();
    expect(await Promise.all(results)).toEqual([1, 2, 3, 4, 5]);
    expect(h.started).toEqual([1, 2, 3, 4, 5]);
    expect(h.peak()).toBe(2);
  });

  it("frees the slot of a task that rejects or throws synchronously", async () => {
    const limiter = new RequestLimiter(1);
    const h = harness();
    const first = limiter.run(h.task(1)).catch((e: Error) => e.message);
    const second = limiter
      .run(() => {
        throw new Error("sync");
      })
      .catch((e: Error) => e.message);
    const third = limiter.run(h.task(3));
    h.settle.get(1)!.fail();
    expect(await first).toBe("task 1");
    expect(await second).toBe("sync");
    await flush();
    expect(h.started).toEqual([1, 3]);
    h.settle.get(3)!.ok();
    expect(await third).toBe(3);
  });

  it("drops a queued task that is cancelled by the time a slot frees", async () => {
    const limiter = new RequestLimiter(1);
    const h = harness();
    let cancelled = false;
    void limiter.run(h.task(1));
    const dropped = limiter.run(h.task(2), () => cancelled).catch((e) => e);
    const after = limiter.run(h.task(3));
    cancelled = true;
    h.settle.get(1)!.ok();
    expect(await dropped).toBeInstanceOf(StreamdError);
    await flush();
    expect(h.started).toEqual([1, 3]);
    h.settle.get(3)!.ok();
    expect(await after).toBe(3);
  });

  it("starts every task at once when unlimited", async () => {
    const limiter = new RequestLimiter();
    const h = harness();
    for (const id of [1, 2, 3]) void limiter.run(h.task(id));
    await flush();
    expect(h.started).toEqual([1, 2, 3]);
  });

  it("wrap routes fetchEvents and getHeadVersion through the limit", async () => {
    const limiter = new RequestLimiter(1);
    const calls: string[] = [];
    let release = () => {};
    const page: EventPage = { events: [], hasMore: false };
    const inner: EventAdapter = {
      fetchEvents: () => {
        calls.push("fetch");
        return new Promise((r) => (release = () => r(page)));
      },
      async getHeadVersion() {
        calls.push(this === inner ? "head" : "head (unbound)");
        return 7n;
      },
    };
    const view = limiter.wrap(inner);
    const fetched = view.fetchEvents({ kind: "module", eventType: "0x1::a::B" }, {});
    const head = view.getHeadVersion!({ kind: "module", eventType: "0x1::a::B" });
    await flush();
    expect(calls).toEqual(["fetch"]);
    release();
    expect(await fetched).toBe(page);
    expect(await head).toBe(7n);
    expect(calls).toEqual(["fetch", "head"]);
  });

  it("wrap leaves getHeadVersion undefined when the inner adapter has none", () => {
    const view = new RequestLimiter(1).wrap({
      fetchEvents: async () => ({ events: [], hasMore: false }),
    });
    expect(view.getHeadVersion).toBeUndefined();
  });

  it("wrap queues getEarliestVersion behind the limit", async () => {
    const limiter = new RequestLimiter(1);
    const calls: string[] = [];
    let release = () => {};
    const page: EventPage = { events: [], hasMore: false };
    const inner: EventAdapter = {
      fetchEvents: () => {
        calls.push("fetch");
        return new Promise((r) => (release = () => r(page)));
      },
      async getEarliestVersion() {
        calls.push(this === inner ? "earliest" : "earliest (unbound)");
        return 3n;
      },
    };
    const view = limiter.wrap(inner);
    const fetched = view.fetchEvents({ kind: "module", eventType: "0x1::a::B" }, {});
    const earliest = view.getEarliestVersion!({ kind: "module", eventType: "0x1::a::B" });
    await flush();
    expect(calls).toEqual(["fetch"]);
    release();
    expect(await fetched).toBe(page);
    expect(await earliest).toBe(3n);
    expect(calls).toEqual(["fetch", "earliest"]);
  });

  it("wrap leaves getEarliestVersion undefined when the inner adapter has none", () => {
    const view = new RequestLimiter(1).wrap({
      fetchEvents: async () => ({ events: [], hasMore: false }),
    });
    expect(view.getEarliestVersion).toBeUndefined();
  });

  it("wrap queues getBlock behind the limit and passes its arguments", async () => {
    const limiter = new RequestLimiter(1);
    const calls: string[] = [];
    let release = () => {};
    const page: EventPage = { events: [], hasMore: false };
    const desc = { kind: "module", eventType: "0x1::a::B" } as const;
    const inner: EventAdapter = {
      fetchEvents: () => {
        calls.push("fetch");
        return new Promise((r) => (release = () => r(page)));
      },
      async getBlock(_descriptor, height) {
        calls.push(`block ${height}`);
        return { height, firstVersion: 1n, lastVersion: 2n, timestamp: 0n };
      },
    };
    const view = limiter.wrap(inner);
    const fetched = view.fetchEvents(desc, {});
    const block = view.getBlock!(desc, 7n);
    await flush();
    expect(calls).toEqual(["fetch"]);
    release();
    await fetched;
    expect((await block).height).toBe(7n);
    expect(calls).toEqual(["fetch", "block 7"]);
  });

  it("wrap leaves getBlock undefined when the inner adapter has none", () => {
    const view = new RequestLimiter(1).wrap({
      fetchEvents: async () => ({ events: [], hasMore: false }),
    });
    expect(view.getBlock).toBeUndefined();
    expect(view.getBlockHeights).toBeUndefined();
  });

  it("wrap queues getBlockHeights behind the limit", async () => {
    const limiter = new RequestLimiter(1);
    const calls: string[] = [];
    let release = () => {};
    const page: EventPage = { events: [], hasMore: false };
    const desc = { kind: "module", eventType: "0x1::a::B" } as const;
    const inner: EventAdapter = {
      fetchEvents: () => {
        calls.push("fetch");
        return new Promise((r) => (release = () => r(page)));
      },
      async getBlockHeights(descriptor) {
        calls.push(`heights ${descriptor.kind}`);
        return { oldest: 1n, newest: 2n };
      },
    };
    const view = limiter.wrap(inner);
    const fetched = view.fetchEvents(desc, {});
    const heights = view.getBlockHeights!(desc);
    await flush();
    expect(calls).toEqual(["fetch"]);
    release();
    await fetched;
    expect(await heights).toEqual({ oldest: 1n, newest: 2n });
    expect(calls).toEqual(["fetch", "heights module"]);
  });
});

describe("RequestLimiter.wrap transaction lookups", () => {
  it("runs getTransactionEvents through the cap, only when the adapter has it", async () => {
    const hash = `0x${"ef".repeat(32)}`;
    let active = 0;
    let peak = 0;
    const inner: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async getTransactionEvents() {
        peak = Math.max(peak, ++active);
        await Bun.sleep(2);
        active--;
        return undefined;
      },
    };
    const view = new RequestLimiter(1).wrap(inner);
    const seen: unknown[] = [];
    const lookup = inner.getTransactionEvents!;
    inner.getTransactionEvents = (...args) => (seen.push(args), lookup(...args));
    const descriptor = { kind: "module", eventType: "0x1::t::A" } as const;
    await Promise.all([view.getTransactionEvents!(hash, descriptor), view.getTransactionEvents!(hash)]);
    expect(peak).toBe(1);
    expect(seen[0]).toEqual([hash, descriptor]);
    expect(new RequestLimiter(1).wrap({ fetchEvents: inner.fetchEvents }).getTransactionEvents).toBeUndefined();
  });
});

describe("RequestLimiter.wrap simulations", () => {
  it("runs simulateTransactionEvents through the cap, only when the adapter has it", async () => {
    const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };
    let active = 0;
    let peak = 0;
    const seen: unknown[] = [];
    const inner: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async simulateTransactionEvents(...args) {
        seen.push(args);
        peak = Math.max(peak, ++active);
        await Bun.sleep(2);
        active--;
        return simulated;
      },
    };
    const view = new RequestLimiter(1).wrap(inner);
    const descriptor = { kind: "module", eventType: "0x1::t::A" } as const;
    await Promise.all([view.simulateTransactionEvents!(simulation, descriptor), view.simulateTransactionEvents!(simulation)]);
    expect(peak).toBe(1);
    expect(seen[0]).toEqual([simulation, descriptor]);
    expect(new RequestLimiter(1).wrap({ fetchEvents: inner.fetchEvents }).simulateTransactionEvents).toBeUndefined();
  });
});

describe("RequestLimiter.wrap views", () => {
  it("runs view through the cap, only when the adapter has it", async () => {
    let release!: () => void;
    const order: string[] = [];
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        order.push("fetch");
        await new Promise<void>((r) => (release = r));
        return { events: [], hasMore: false };
      },
      view: async (request, version) => (order.push("view"), [request.function, String(version)]),
    };
    const view = new RequestLimiter(1).wrap(adapter);
    const fetching = view.fetchEvents({ kind: "module", eventType: "0x1::a::B" }, {});
    const viewing = view.view!({ function: "0x1::coin::balance" }, 2n);
    await Promise.resolve();
    expect(order).toEqual(["fetch"]);
    release();
    await fetching;
    expect(await viewing).toEqual(["0x1::coin::balance", "2"]);
    expect(new RequestLimiter().wrap({ fetchEvents: adapter.fetchEvents }).view).toBeUndefined();
  });
});
