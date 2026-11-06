import { describe, it, expect } from "bun:test";
import { RequestLimiter } from "../src/limiter.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventPage } from "../src/types.js";

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
});
