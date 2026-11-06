import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError, StreamdPollingError } from "../src/errors.js";
import type { EventAdapter, EventPage, StreamdEvent } from "../src/types.js";

const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

function ev(version: number, eventIndex = 0, amount = "1"): StreamdEvent {
  return {
    type: "0x1::test::Event",
    data: { amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  };
}

/**
 * Serves a growing list in `limit`-sized pages; the cursor is the next unread
 * index. `withCursor: false` leaves `nextCursor` off every page.
 */
function growing(events: StreamdEvent[], withCursor = true) {
  const calls: { cursor?: string; fromVersion?: bigint }[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async (_d, options): Promise<EventPage> => {
      calls.push({ cursor: options.cursor, fromVersion: options.fromVersion });
      const from =
        options.cursor !== undefined
          ? Number(options.cursor)
          : events.findIndex((e) => e.version >= (options.fromVersion ?? 0n));
      const start = from < 0 ? events.length : from;
      const page = events.slice(start, start + (options.limit ?? 25));
      const next = start + page.length;
      return {
        events: page,
        nextCursor: withCursor ? String(next) : undefined,
        hasMore: next < events.length,
      };
    },
  };
  return { adapter, calls };
}

const ids = (list: StreamdEvent[]) => list.map((e) => `${e.version}:${e.eventIndex}`);
const collect = (seen: StreamdEvent[], e: StreamdEvent) => [...seen, e];

describe("reduce", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("folds the history, then each live event exactly once", async () => {
    const events = [1, 2, 3, 4, 5].map((v) => ev(v));
    const client = new StreamdClient({ adapter: growing(events).adapter });
    const changes: string[] = [];
    const r = client.reduce(descriptor, collect, [] as StreamdEvent[], {
      batchSize: 2,
      onChange: (state, e) => changes.push(`${state.length}@${e.version}:${e.eventIndex}`),
    });

    expect(ids(await r.ready)).toEqual(["1:0", "2:0", "3:0", "4:0", "5:0"]);
    events.push(ev(6), ev(6, 1), ev(7));
    await advanceTimersByTimeAsync(1000);
    await advanceTimersByTimeAsync(1000);
    r.unsubscribe();

    expect(ids(r.value)).toEqual(["1:0", "2:0", "3:0", "4:0", "5:0", "6:0", "6:1", "7:0"]);
    expect(changes).toEqual(["6@6:0", "7@6:1", "8@7:0"]);
  });

  it("reaches the same value however the events split across backfill and live", async () => {
    const all = [1, 2, 2, 3, 4, 5, 6].map((v, i) => ev(v, i));
    const sum = (n: bigint, e: StreamdEvent) => n + BigInt(e.version) * 10n + BigInt(e.eventIndex);
    const expected = all.reduce(sum, 0n);
    for (let split = 0; split <= all.length; split++) {
      const events = all.slice(0, split);
      const client = new StreamdClient({ adapter: growing(events).adapter });
      const r = client.reduce(descriptor, sum, 0n, { batchSize: 2 });
      await r.ready;
      events.push(...all.slice(split));
      for (let i = 0; i < 5; i++) await advanceTimersByTimeAsync(1000);
      r.unsubscribe();
      expect(r.value).toBe(expected);
    }
  });

  it("reports ready with the backfilled value even when no events arrive live", async () => {
    const client = new StreamdClient({ adapter: growing([ev(1, 0, "5"), ev(2, 0, "7")]).adapter });
    const r = client.reduce(
      descriptor,
      (total: bigint, e) => total + BigInt((e.data as { amount: string }).amount),
      0n,
    );
    expect(await r.ready).toBe(12n);
    expect(r.active).toBe(true);
    r.unsubscribe();
    expect(r.active).toBe(false);
  });

  it("applies where and decode to both halves", async () => {
    const events = [ev(1, 0, "10"), ev(2, 0, "20"), ev(3, 0, "30")];
    const client = new StreamdClient({ adapter: growing(events).adapter });
    const r = client.reduce(
      descriptor,
      (total: bigint, e) => total + (e.data as { amount: bigint }).amount,
      0n,
      { decode: ["amount"], where: (e) => e.version !== 2n && e.version !== 5n },
    );
    expect(await r.ready).toBe(40n);
    events.push(ev(4, 0, "40"), ev(5, 0, "50"));
    await advanceTimersByTimeAsync(1000);
    r.unsubscribe();
    expect(r.value).toBe(80n);
  });

  it("uses the client's decode default for the backfill as well as live", async () => {
    const events = [ev(1, 0, "10")];
    const client = new StreamdClient({
      adapter: growing(events).adapter,
      defaults: { decode: ["amount"] },
    });
    const r = client.reduce(
      descriptor,
      (total: bigint, e) => total + (e.data as { amount: bigint }).amount,
      0n,
    );
    expect(await r.ready).toBe(10n);
    events.push(ev(2, 0, "5"));
    await advanceTimersByTimeAsync(1000);
    r.unsubscribe();
    expect(r.value).toBe(15n);
  });

  it("follows from the query's end cursor", async () => {
    const events = [1, 2, 3].map((v) => ev(v));
    const { adapter, calls } = growing(events);
    const client = new StreamdClient({ adapter });
    const r = client.reduce(descriptor, collect, [] as StreamdEvent[], { fromVersion: 2n });
    expect(ids(await r.ready)).toEqual(["2:0", "3:0"]);
    await advanceTimersByTimeAsync(0);
    r.unsubscribe();
    expect(calls[0]).toEqual({ cursor: undefined, fromVersion: 2n });
    expect(calls.at(-1)).toEqual({ cursor: "3", fromVersion: undefined });
  });

  it("without a cursor, follows from the last folded version and folds nothing twice", async () => {
    const events = [ev(1), ev(2), ev(2, 1)];
    const { adapter, calls } = growing(events, false);
    const client = new StreamdClient({ adapter });
    const r = client.reduce(descriptor, collect, [] as StreamdEvent[], { dedupWindow: 0 });
    await r.ready;
    events.push(ev(2, 2), ev(3));
    await advanceTimersByTimeAsync(0);
    r.unsubscribe();
    expect(calls.at(-1)?.fromVersion).toBe(2n);
    expect(ids(r.value)).toEqual(["1:0", "2:0", "2:1", "2:2", "3:0"]);
  });

  it("stops when there is neither a cursor nor a folded event to follow from", async () => {
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter: growing([], false).adapter });
    const r = client.reduce(descriptor, collect, [] as StreamdEvent[], {
      onError: (e) => errors.push(e),
    });
    await expect(r.ready).rejects.toThrow(/Nothing to follow from/);
    expect(errors).toHaveLength(1);
    expect(r.active).toBe(false);
  });

  it("refuses options that leave no exact point to follow from", () => {
    const client = new StreamdClient({ adapter: growing([]).adapter });
    for (const key of ["limit", "offset", "toVersion", "toBlock", "orderBy", "onCursor", "cursor", "id"]) {
      const options = { [key]: 1 } as never;
      expect(() => client.reduce(descriptor, collect, [], options)).toThrow(StreamdError);
      expect(() => client.reduce(descriptor, collect, [], options)).toThrow(
        `Unsupported reduce option: ${key}`,
      );
    }
  });
});

describe("reduce failure and stop paths", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  /** An adapter whose fetches wait until released, to stop a reduction mid-backfill. */
  function stalled(events: StreamdEvent[]) {
    let release = () => {};
    let fetches = 0;
    const adapter: EventAdapter = {
      fetchEvents: async (d, options) => {
        fetches++;
        await new Promise<void>((r) => (release = r));
        return growing(events).adapter.fetchEvents(d, options);
      },
    };
    return { adapter, release: () => release(), fetches: () => fetches };
  }

  const failOn = (version: bigint) => (n: number, e: StreamdEvent) => {
    if (e.version === version) throw new Error("bad event");
    return n + 1;
  };

  it("stops when the reducer throws during the backfill", async () => {
    const errors: Error[] = [];
    const { adapter, calls } = growing([ev(1), ev(2), ev(3)]);
    const client = new StreamdClient({ adapter, onError: (e) => errors.push(e) });
    const r = client.reduce(descriptor, failOn(2n), 0);

    const err = await r.ready.catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as StreamdError).message).toBe("Reducer threw for 2:0");
    expect(((err as StreamdError).cause as Error).message).toBe("bad event");
    expect(errors).toEqual([err as Error]);
    expect(r.value).toBe(1);
    expect(r.active).toBe(false);
    await advanceTimersByTimeAsync(2000);
    expect(calls).toHaveLength(1);
  });

  it("stops when the reducer throws on a live event", async () => {
    const errors: Error[] = [];
    const changes: number[] = [];
    const events = [ev(1)];
    const client = new StreamdClient({ adapter: growing(events).adapter });
    const r = client.reduce(descriptor, failOn(3n), 0, {
      onError: (e) => errors.push(e),
      onChange: (n) => changes.push(n),
    });
    await r.ready;
    events.push(ev(2), ev(3), ev(4));
    await advanceTimersByTimeAsync(2000);

    expect(errors.map((e) => e.message)).toEqual(["Reducer threw for 3:0"]);
    expect(r.value).toBe(2);
    expect(changes).toEqual([2]);
    expect(r.active).toBe(false);
  });

  it("keeps folding when onChange throws, reporting it", async () => {
    const errors: Error[] = [];
    const events = [ev(1)];
    const client = new StreamdClient({ adapter: growing(events).adapter });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0, {
      onError: (e) => errors.push(e),
      onChange: () => {
        throw new Error("ui bug");
      },
    });
    await r.ready;
    events.push(ev(2), ev(3));
    await advanceTimersByTimeAsync(1000);
    r.unsubscribe();
    expect(r.value).toBe(3);
    expect(errors).toHaveLength(2);
  });

  it("stops and reports when the backfill query rejects", async () => {
    const errors: Error[] = [];
    const failure = new StreamdError("refused");
    const client = new StreamdClient({
      adapter: { fetchEvents: async () => Promise.reject(failure) },
      onError: (e) => errors.push(e),
    });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0);
    await expect(r.ready).rejects.toBe(failure);
    expect(errors).toEqual([failure]);
    expect(r.active).toBe(false);
  });

  it("goes inactive when live polling gives up after maxErrors", async () => {
    const errors: Error[] = [];
    let failing = false;
    const inner = growing([ev(1)]).adapter;
    const client = new StreamdClient({
      adapter: {
        fetchEvents: (d, o) =>
          failing ? Promise.reject(new StreamdNetworkError("down")) : inner.fetchEvents(d, o),
      },
    });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0, {
      maxErrors: 2,
      maxBackoff: 0,
      onError: (e) => errors.push(e),
    });
    await r.ready;
    failing = true;
    for (let i = 0; i < 5; i++) await advanceTimersByTimeAsync(1000);
    expect(r.active).toBe(false);
    expect(errors.at(-1)).toBeInstanceOf(StreamdPollingError);
    expect(r.value).toBe(1);
  });

  it("rejects ready with the signal's reason when aborted mid-backfill, without onError", async () => {
    const errors: Error[] = [];
    const { adapter, release } = stalled([ev(1)]);
    const client = new StreamdClient({ adapter, onError: (e) => errors.push(e) });
    const controller = new AbortController();
    const r = client.reduce(descriptor, (n: number) => n + 1, 0, { signal: controller.signal });
    await advanceTimersByTimeAsync(0);
    controller.abort("gone");
    release();
    await expect(r.ready).rejects.toBe("gone");
    expect(r.active).toBe(false);
    expect(r.value).toBe(0);
    expect(errors).toEqual([]);
  });

  it("never fetches with an already-aborted signal", async () => {
    const { adapter, calls } = growing([ev(1)]);
    const client = new StreamdClient({ adapter });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0, {
      signal: AbortSignal.abort("early"),
    });
    await expect(r.ready).rejects.toBe("early");
    expect(calls).toEqual([]);
  });

  it("rejects ready when unsubscribed mid-backfill, and never folds or follows", async () => {
    const errors: Error[] = [];
    const { adapter, release, fetches } = stalled([ev(1)]);
    const client = new StreamdClient({ adapter, onError: (e) => errors.push(e) });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0);
    await advanceTimersByTimeAsync(0);
    r.unsubscribe();
    release();
    await expect(r.ready).rejects.toThrow("Reduction stopped before its backfill finished");
    await advanceTimersByTimeAsync(2000);
    expect(r.value).toBe(0);
    expect(fetches()).toBe(1);
    expect(errors).toEqual([]);
  });

  it("stops following on abort, unsubscribe, dispose and destroy", async () => {
    const stops: ((r: { unsubscribe(): void; [Symbol.dispose](): void }) => void)[] = [
      (r) => r.unsubscribe(),
      (r) => r[Symbol.dispose](),
    ];
    for (const stop of stops) {
      const events = [ev(1)];
      const client = new StreamdClient({ adapter: growing(events).adapter });
      const r = client.reduce(descriptor, (n: number) => n + 1, 0);
      await r.ready;
      stop(r);
      events.push(ev(2));
      await advanceTimersByTimeAsync(2000);
      expect([r.value, r.active]).toEqual([1, false]);
    }

    const events = [ev(1)];
    const controller = new AbortController();
    const client = new StreamdClient({ adapter: growing(events).adapter });
    const aborted = client.reduce(descriptor, (n: number) => n + 1, 0, {
      signal: controller.signal,
    });
    const destroyed = client.reduce(descriptor, (n: number) => n + 1, 0);
    await Promise.all([aborted.ready, destroyed.ready]);
    controller.abort();
    client.destroy();
    events.push(ev(2));
    await advanceTimersByTimeAsync(2000);
    expect([aborted.value, aborted.active, destroyed.value, destroyed.active]).toEqual([
      1,
      false,
      1,
      false,
    ]);
  });

  it("ends a reduction still backfilling on destroy()", async () => {
    const { adapter, release } = stalled([ev(1)]);
    const client = new StreamdClient({ adapter });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0);
    await advanceTimersByTimeAsync(0);
    client.destroy();
    release();
    await expect(r.ready).rejects.toThrow(StreamdError);
    expect(r.active).toBe(false);
  });

  it("leaves no unhandled rejection when ready is never awaited", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      const client = new StreamdClient({ adapter: growing([ev(1)]).adapter });
      client.reduce(descriptor, (n: number) => n + 1, 0).unsubscribe();
      client.reduce(descriptor, failOn(1n), 0);
      await advanceTimersByTimeAsync(10);
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
    }
  });

  it("is not a listener, and its live half shares polling with an identical on()", async () => {
    const events = [ev(1)];
    const { adapter, calls } = growing(events);
    const client = new StreamdClient({ adapter });
    const r = client.reduce(descriptor, (n: number) => n + 1, 0);
    await r.ready;
    expect(client.listenerCount()).toBe(0);
    client.removeAllListeners();
    const seen: string[] = [];
    const sub = client.on(descriptor, (e) => seen.push(`${e.version}`), { cursor: "1" });
    const before = calls.length;
    events.push(ev(2));
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();
    r.unsubscribe();
    expect(calls.length - before).toBe(1);
    expect([r.value, seen]).toEqual([2, ["2"]]);
  });
});
