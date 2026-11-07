import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { StreamdClient } from "../src/client.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, ReplayOptions, StreamdEvent } from "../src/types.js";

const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

function ev(version: number, eventIndex = 0, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return {
    type: "0x1::test::Event",
    data: { amount: String(version * 10) },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
    ...extra,
  };
}

const ids = (list: StreamdEvent[]) => list.map((e) => `${e.version}:${e.eventIndex}`);
const backlog = () => [ev(1), ev(2), ev(2, 1), ev(3), ev(4), ev(5)];
const tick = () => new Promise<void>((r) => setImmediate(r));

function client(events = backlog(), onError?: (e: Error) => void) {
  return new StreamdClient({ adapter: new MemoryAdapter(events), onError });
}

describe("replay", () => {
  it("delivers the range in order, awaiting each callback, and counts it in done", async () => {
    const seen: string[] = [];
    let running = 0;
    const r = client().replay(
      descriptor,
      async (e) => {
        expect(running++).toBe(0);
        await tick();
        seen.push(`${e.version}:${e.eventIndex}`);
        running--;
      },
      { batchSize: 2 },
    );
    expect(await r.done).toBe(6);
    expect(seen).toEqual(["1:0", "2:0", "2:1", "3:0", "4:0", "5:0"]);
    expect(r.active).toBe(false);
  });

  it("narrows as getEvents does with the same options", async () => {
    const c = client();
    const options = {
      fromVersion: 2n,
      toVersion: 4n,
      decode: ["amount"],
      where: (e) => (e.data as { amount: bigint }).amount >= 30n,
      limit: 1,
    } satisfies ReplayOptions;
    const seen: StreamdEvent[] = [];
    await c.replay(descriptor, (e) => void seen.push(e), options).done;
    expect(seen).toEqual(await c.getEvents(descriptor, options));
    expect(ids(seen)).toEqual(["3:0"]);
    expect(seen[0]!.data).toEqual({ amount: 30n });
  });

  it("takes decode and batchSize from the client defaults", async () => {
    const sizes: (number | undefined)[] = [];
    const memory = new MemoryAdapter(backlog());
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => (sizes.push(o.limit), memory.fetchEvents(d, o)),
    };
    const c = new StreamdClient({ adapter, defaults: { decode: ["amount"], batchSize: 4 } });
    const seen: StreamdEvent[] = [];
    await c.replay(descriptor, (e) => void seen.push(e)).done;
    expect(seen[0]!.data).toEqual({ amount: 10n });
    expect(sizes[0]).toBe(4);
  });

  it("reports a throwing or rejecting callback and carries on", async () => {
    const errors: Error[] = [];
    const seen: string[] = [];
    const r = client(backlog(), (e) => errors.push(e)).replay(descriptor, async (e) => {
      seen.push(`${e.version}:${e.eventIndex}`);
      if (e.version === 2n) throw new Error("boom");
    });
    expect(await r.done).toBe(6);
    expect(seen).toHaveLength(6);
    expect(errors.map((e) => e.message)).toEqual([
      "Event callback threw for 2:0",
      "Event callback threw for 2:1",
    ]);
    expect(errors[0]).toBeInstanceOf(StreamdError);
    expect((errors[0]!.cause as Error).message).toBe("boom");
  });

  it("ends on a failed query: onError once, done rejects, nothing more fetched", async () => {
    let calls = 0;
    const failure = new StreamdNetworkError("down");
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        calls++;
        throw failure;
      },
    };
    const errors: Error[] = [];
    const own: Error[] = [];
    const c = new StreamdClient({ adapter, onError: (e) => errors.push(e) });
    const r = c.replay(descriptor, () => {}, { onError: (e) => own.push(e) });
    await expect(r.done).rejects.toBe(failure);
    expect(own).toEqual([failure]);
    expect(errors).toEqual([]);
    expect(calls).toBe(1);
    expect(r.active).toBe(false);
  });

  it("calls onCaughtUp once after the last callback settles, a throw reported", async () => {
    const order: string[] = [];
    const errors: Error[] = [];
    const r = client([ev(1)]).replay(descriptor, async () => {
      await tick();
      order.push("event");
    }, {
      onCaughtUp: () => {
        order.push("caught up");
        throw new Error("x");
      },
      onError: (e) => errors.push(e),
    });
    expect(await r.done).toBe(1);
    expect(order).toEqual(["event", "caught up"]);
    expect(errors.map((e) => e.message)).toEqual(["onCaughtUp threw"]);
  });

  it("stops on unsubscribe mid-replay, from inside the callback too", async () => {
    const seen: string[] = [];
    const r = client().replay(descriptor, (e) => {
      seen.push(`${e.version}:${e.eventIndex}`);
      if (e.version === 2n) r.unsubscribe();
    });
    await expect(r.done).rejects.toThrow("Replay stopped before it finished");
    expect(seen).toEqual(["1:0", "2:0"]);
    expect(r.active).toBe(false);
  });

  it("stops on abort with the signal's reason, and never starts when already aborted", async () => {
    const reason = new Error("aborted");
    const before = new AbortController();
    before.abort(reason);
    let calls = 0;
    const early = client().replay(descriptor, () => void calls++, { signal: before.signal });
    await expect(early.done).rejects.toBe(reason);

    const during = new AbortController();
    const seen: bigint[] = [];
    const r = client().replay(
      descriptor,
      (e) => {
        seen.push(e.version);
        if (e.version === 3n) during.abort(reason);
      },
      { signal: during.signal },
    );
    await expect(r.done).rejects.toBe(reason);
    expect(calls).toBe(0);
    expect(seen).toEqual([1n, 2n, 2n, 3n]);
  });

  it("is ended by destroy() and not counted as a listener", async () => {
    const c = client();
    let release!: () => void;
    const r = c.replay(descriptor, () => new Promise<void>((resolve) => (release = resolve)));
    await tick();
    expect(c.listenerCount()).toBe(0);
    c.removeAllListeners();
    expect(r.active).toBe(true);
    c.destroy();
    release();
    await expect(r.done).rejects.toThrow("Replay stopped before it finished");
  });

  it("holds delivery while paused and resumes with the next event", async () => {
    const seen: bigint[] = [];
    const r = client().replay(descriptor, (e) => {
      seen.push(e.version);
      if (e.version === 2n && e.eventIndex === 0) r.pause();
    });
    for (let i = 0; i < 5; i++) await tick();
    expect(seen).toEqual([1n, 2n]);
    expect(r.paused).toBe(true);
    expect(r.active).toBe(false);
    r.resume();
    expect(await r.done).toBe(6);
    expect(seen).toEqual([1n, 2n, 2n, 3n, 4n, 5n]);
    expect(r.paused).toBe(false);
  });

  it("unsubscribing while paused ends it", async () => {
    const r = client().replay(descriptor, () => r.pause());
    await tick();
    r.unsubscribe();
    await expect(r.done).rejects.toThrow(StreamdError);
    expect(r.paused).toBe(false);
  });

  it("disposes through Symbol.dispose", async () => {
    let r!: ReturnType<StreamdClient["replay"]>;
    {
      using handle = client().replay(descriptor, () => {});
      r = handle;
    }
    await expect(r.done).rejects.toThrow(StreamdError);
  });

  it("merges a list of descriptors", async () => {
    const other = { kind: "module", eventType: "0x1::test::Other" } as const;
    const events = [ev(1), { ...ev(2), type: "0x1::test::Other" }, ev(3)];
    const seen: bigint[] = [];
    await client(events).replay([descriptor, other], (e) => void seen.push(e.version)).done;
    expect(seen).toEqual([1n, 2n, 3n]);
  });

  it("refuses orderBy, unknown options and a bad speed synchronously", () => {
    const c = client();
    const bad = (options: unknown) => () =>
      c.replay(descriptor, () => {}, options as ReplayOptions);
    expect(bad({ orderBy: "desc" })).toThrow("Unsupported replay option: orderBy");
    expect(bad({ pollInterval: 5 })).toThrow("Unsupported replay option: pollInterval");
    for (const speed of [0, -1, Number.NaN, "2", null]) {
      expect(bad({ speed })).toThrow("Invalid speed");
    }
    expect(bad({ speed: Infinity })).not.toThrow();
  });
});

describe("replay speed", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const SECOND = 1_000_000n;
  const at = (version: number, seconds?: number) =>
    ev(version, 0, seconds === undefined ? {} : { timestamp: BigInt(seconds) * SECOND });

  /** Replays `events` and records the fake clock at each delivery, relative to the start. */
  function timed(events: StreamdEvent[], options: ReplayOptions, onEvent?: (e: StreamdEvent) => unknown) {
    const start = Date.now();
    const times: number[] = [];
    const r = client(events).replay(
      descriptor,
      (e) => {
        times.push(Date.now() - start);
        return onEvent?.(e) as void;
      },
      options,
    );
    return { r, times };
  }

  it("scales the gaps between timestamps by speed", async () => {
    const { r, times } = timed([at(1, 0), at(2, 10), at(3, 30)], { speed: 2 });
    await advanceTimersByTimeAsync(4990);
    expect(times).toEqual([0]);
    await advanceTimersByTimeAsync(20);
    expect(times).toEqual([0, 5000]);
    await advanceTimersByTimeAsync(10000);
    expect(times).toEqual([0, 5000, 15000]);
    expect(await r.done).toBe(3);
  });

  it("delivers an event without a timestamp at once, measuring on from the last stamp", async () => {
    const { r, times } = timed([at(1, 0), at(2), at(3, 4)], { speed: 1 });
    await advanceTimersByTimeAsync(3990);
    expect(times).toEqual([0, 0]);
    await advanceTimersByTimeAsync(20);
    expect(times).toEqual([0, 0, 4000]);
    expect(await r.done).toBe(3);
  });

  it("subtracts the time the callback took from the next wait", async () => {
    const slow = () => new Promise<void>((resolve) => setTimeout(resolve, 3000));
    const { r, times } = timed([at(1, 0), at(2, 5), at(3, 6)], { speed: 1 }, (e) =>
      e.version === 1n ? slow() : undefined,
    );
    await advanceTimersByTimeAsync(5000);
    expect(times).toEqual([0, 5000]);
    // A gap shorter than the callback before it is not waited at all.
    const late = timed([at(1, 0), at(2, 1)], { speed: 1 }, (e) => (e.version === 1n ? slow() : undefined));
    await advanceTimersByTimeAsync(3000);
    expect(late.times).toEqual([0, 3000]);
    await advanceTimersByTimeAsync(1000);
    expect(times).toEqual([0, 5000, 6000]);
    expect(await r.done).toBe(3);
  });

  it("holds an event whose wait ends while paused until resume", async () => {
    const { r, times } = timed([at(1, 0), at(2, 2)], { speed: 1 });
    await advanceTimersByTimeAsync(1000);
    r.pause();
    await advanceTimersByTimeAsync(5000);
    expect(times).toEqual([0]);
    const pausedFor = times.length;
    r.resume();
    await advanceTimersByTimeAsync(0);
    expect(pausedFor).toBe(1);
    // Delivered on resume, not 2000ms in when its wait ended.
    expect(times[1]).toBeGreaterThanOrEqual(6000);
    expect(times[1]).toBeLessThan(6010);
    expect(await r.done).toBe(2);
  });

  it("delivers nothing more when unsubscribed during a wait", async () => {
    const { r, times } = timed([at(1, 0), at(2, 100)], { speed: 1 });
    await advanceTimersByTimeAsync(10);
    r.unsubscribe();
    await expect(r.done).rejects.toThrow("Replay stopped before it finished");
    await advanceTimersByTimeAsync(100);
    expect(times).toEqual([0]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
