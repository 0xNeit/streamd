import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import {
  advanceTimersByTimeAsync,
  advanceTimersToNextTimerAsync,
  captureMicrotaskThrows,
  mocked,
} from "./timers.js";
import { EventPoller } from "../src/poller.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
} from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  ListenerOptions,
  StreamdEvent,
} from "../src/types.js";

function makeEvent(version: number, eventIndex = 0): StreamdEvent {
  return {
    type: "0x1::test::TestEvent",
    data: { value: version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  };
}

function page(events: StreamdEvent[], cursor: number, hasMore = false): EventPage {
  return { events, nextCursor: String(cursor), hasMore };
}

/** Replays a fixed list of pages, then empty pages that keep advancing. */
function createMockAdapter(pages: EventPage[]): EventAdapter {
  let callIndex = 0;
  return {
    fetchEvents: vi.fn(async () => {
      const p = pages[callIndex] ?? page([], 9000 + callIndex);
      callIndex++;
      return p;
    }),
  };
}

const descriptor: EventDescriptor = {
  kind: "module",
  eventType: "0x1::test::TestEvent",
};

describe("EventPoller", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("calls callback for each new event", async () => {
    const events = [makeEvent(1), makeEvent(2)];
    const adapter = createMockAdapter([page(events, 3)]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 1000,
    });
    poller.start();

    // First poll fires immediately (delay=0)
    await advanceTimersByTimeAsync(0);

    expect(callback).toHaveBeenCalledTimes(2);
    expect(callback).toHaveBeenCalledWith(events[0]);
    expect(callback).toHaveBeenCalledWith(events[1]);

    poller.stop();
  });

  it("deduplicates events repeated across pages", async () => {
    const event = makeEvent(1);
    const adapter = createMockAdapter([page([event], 2), page([event], 3)]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
    });
    poller.start();

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(100);

    expect(callback).toHaveBeenCalledTimes(1);

    poller.stop();
  });

  it("evicts older events once dedupWindow is exceeded and disables dedup when dedupWindow is 0", async () => {
    const e1 = makeEvent(1);
    const e2 = makeEvent(2);
    const e3 = makeEvent(3);
    const windowedAdapter = createMockAdapter([
      page([e1, e2, e3], 4),
      page([e2, e3, e1], 5),
    ]);
    const seenWindowed: bigint[] = [];
    const windowedPoller = new EventPoller(
      windowedAdapter,
      descriptor,
      (e) => seenWindowed.push(e.version),
      { pollInterval: 100, dedupWindow: 2 },
    );
    windowedPoller.start();

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(100);
    windowedPoller.stop();

    // e1 was evicted when e3 was marked (window=2 held {e2, e3}), so on the
    // second page e2 and e3 are skipped and e1 is delivered again.
    expect(seenWindowed).toEqual([1n, 2n, 3n, 1n]);

    const disabledAdapter = createMockAdapter([page([e1], 2), page([e1], 3)]);
    const seenDisabled: bigint[] = [];
    const disabledPoller = new EventPoller(
      disabledAdapter,
      descriptor,
      (e) => seenDisabled.push(e.version),
      { pollInterval: 100, dedupWindow: 0 },
    );
    disabledPoller.start();

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(100);
    disabledPoller.stop();

    expect(seenDisabled).toEqual([1n, 1n]);
    expect(disabledPoller.status.lastVersion).toBe(1n);
  });

  it("throws StreamdError when constructed with an invalid dedupWindow", () => {
    const adapter = createMockAdapter([]);
    for (const bad of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(
        () => new EventPoller(adapter, descriptor, vi.fn(), { dedupWindow: bad }),
      ).toThrow(StreamdError);
    }
  });

  it("distinguishes events sharing a version by eventIndex", async () => {
    // Module events routinely report sequenceNumber 0, so several events in
    // one transaction are only distinguishable by their index.
    const events = [makeEvent(7, 0), makeEvent(7, 1), makeEvent(7, 2)];
    const adapter = createMockAdapter([page(events, 8)]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 1000,
    });
    poller.start();

    await advanceTimersByTimeAsync(0);

    expect(callback).toHaveBeenCalledTimes(3);

    poller.stop();
  });

  it("advances past pages containing no matching events", async () => {
    // Regression: the cursor used to advance only from matched events, so a
    // sparse event type re-read the same window forever and never reached
    // the events waiting beyond it.
    const adapter = createMockAdapter([
      page([], 100, true),
      page([], 200, true),
      page([makeEvent(250)], 300),
    ]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 1000,
    });
    poller.start();

    await advanceTimersToNextTimerAsync();
    await advanceTimersToNextTimerAsync();
    await advanceTimersToNextTimerAsync();

    poller.stop();

    const calls = mocked(adapter.fetchEvents).mock.calls;
    expect(calls[0][1].cursor).toBeUndefined();
    expect(calls[1][1].cursor).toBe("100");
    expect(calls[2][1].cursor).toBe("200");
    expect(callback).toHaveBeenCalledTimes(1);
  });

  it("seeds the first poll from fromVersion, then follows the cursor", async () => {
    const adapter = createMockAdapter([page([], 500, true), page([], 600)]);

    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 1000,
      fromVersion: 42n,
    });
    poller.start();

    await advanceTimersToNextTimerAsync();
    await advanceTimersToNextTimerAsync();

    poller.stop();

    const calls = mocked(adapter.fetchEvents).mock.calls;
    expect(calls[0][1].fromVersion).toBe(42n);
    expect(calls[1][1].fromVersion).toBeUndefined();
    expect(calls[1][1].cursor).toBe("500");
  });

  it("resumes from a given cursor instead of fromVersion", async () => {
    const adapter = createMockAdapter([page([], 51)]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      cursor: "50",
      fromVersion: 7n,
    });
    expect(poller.cursor).toBe("50");
    poller.start();
    await advanceTimersByTimeAsync(0);

    const first = mocked(adapter.fetchEvents).mock.calls[0]![1];
    expect(first.cursor).toBe("50");
    expect(first.fromVersion).toBeUndefined();

    poller.stop();
  });

  it("reports the cursor of the last page it finished", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2), page([], 5)]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { pollInterval: 100 });
    expect(poller.cursor).toBeUndefined();
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.cursor).toBe("2");
    await advanceTimersByTimeAsync(100);
    expect(poller.cursor).toBe("5");

    poller.stop();
    expect(poller.cursor).toBe("5");
  });

  it("enters catch-up mode when the adapter reports more data", async () => {
    const adapter = createMockAdapter([
      page([makeEvent(1)], 2, true),
      page([makeEvent(2)], 3, false),
    ]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 1000,
      batchSize: 25,
    });
    poller.start();

    // First poll at delay=0; hasMore reschedules the second at delay=0 too.
    await advanceTimersToNextTimerAsync();
    await advanceTimersToNextTimerAsync();

    poller.stop();

    expect(callback).toHaveBeenCalledTimes(2);
  });

  it("stops after max consecutive errors", async () => {
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new Error("network error");
      }),
    };
    const callback = vi.fn();
    const onError = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      maxErrors: 3,
      onError,
    });
    poller.start();

    // Let it fail 3 times with exponential backoff
    await advanceTimersByTimeAsync(0); // fail 1
    await advanceTimersByTimeAsync(200); // fail 2 (backoff doubled)
    await advanceTimersByTimeAsync(400); // fail 3 (backoff doubled again)

    expect(onError).toHaveBeenCalledTimes(3);
    expect(poller.active).toBe(false);
    expect(callback).not.toHaveBeenCalled();

    poller.stop();
  });

  describe("backoff", () => {
    /** Fails unless `succeeds(n)` for the nth call, recording the fake clock per call. */
    function timedAdapter(succeeds: (n: number) => boolean = () => false) {
      const at: number[] = [];
      const fetchEvents = vi.fn(async (): Promise<EventPage> => {
        at.push(Date.now());
        if (!succeeds(at.length)) throw new Error("network error");
        return page([], at.length);
      });
      const gaps = () => at.slice(1).map((t, i) => t - at[i]!);
      return { fetchEvents, gaps };
    }

    it("doubles the wait after each consecutive failure", async () => {
      const { fetchEvents, gaps } = timedAdapter();
      const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
        pollInterval: 100,
        maxErrors: 10,
      });
      poller.start();
      await advanceTimersByTimeAsync(1500);

      expect(gaps()).toEqual([200, 400, 800]);
      poller.stop();
    });

    it("starts a restart after an auto-stop with a fresh error budget and backoff", async () => {
      const { fetchEvents, gaps } = timedAdapter();
      const onError = vi.fn();
      const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
        pollInterval: 100,
        maxErrors: 2,
        onError,
      });
      poller.start();
      await advanceTimersByTimeAsync(200);
      expect(poller.active).toBe(false);

      poller.start();
      await advanceTimersByTimeAsync(0);
      expect(poller.active).toBe(true);
      expect(onError.mock.calls.at(-1)![0].message).toBe("Polling failed (1/2)");
      await advanceTimersByTimeAsync(200);
      expect(gaps().at(-1)).toBe(200);
      expect(poller.active).toBe(false);
    });

    it("never waits longer than a minute", async () => {
      const { fetchEvents, gaps } = timedAdapter();
      const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
        pollInterval: 40_000,
        maxErrors: 10,
      });
      poller.start();
      // Step to each pending timer rather than tick through two minutes.
      for (let i = 0; i < 3; i++) await advanceTimersToNextTimerAsync();

      // Doubling 40s would give 80s; the cap holds every wait at 60s.
      expect(gaps()).toEqual([60_000, 60_000]);
      poller.stop();
    });

    it("starts backing off from the poll interval again after a success", async () => {
      const { fetchEvents, gaps } = timedAdapter((n) => n === 3);
      const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
        pollInterval: 100,
        maxErrors: 10,
      });
      poller.start();
      await advanceTimersByTimeAsync(1000);

      // Fail, fail, succeed, fail: 200 then 400, 100 after the success, and the
      // next failure backs off 200 again rather than doubling from 400.
      expect(gaps()).toEqual([200, 400, 100, 200]);
      poller.stop();
    });

    it("caps backoff at a custom maxBackoff smaller or larger than the 60s default", async () => {
      const small = timedAdapter();
      const smallPoller = new EventPoller(
        { fetchEvents: small.fetchEvents },
        descriptor,
        vi.fn(),
        { pollInterval: 100, maxBackoff: 300, maxErrors: 10 },
      );
      smallPoller.start();
      await advanceTimersByTimeAsync(850);
      smallPoller.stop();
      expect(small.gaps()).toEqual([200, 300, 300]);

      const large = timedAdapter();
      const largePoller = new EventPoller(
        { fetchEvents: large.fetchEvents },
        descriptor,
        vi.fn(),
        { pollInterval: 40_000, maxBackoff: 100_000, maxErrors: 10 },
      );
      largePoller.start();
      for (let i = 0; i < 3; i++) await advanceTimersToNextTimerAsync();
      largePoller.stop();
      expect(large.gaps()).toEqual([80_000, 100_000]);
    });

    it("honours maxBackoff below pollInterval (including 0) and still resets to pollInterval on success", async () => {
      const low = timedAdapter((n) => n === 3);
      const lowPoller = new EventPoller(
        { fetchEvents: low.fetchEvents },
        descriptor,
        vi.fn(),
        { pollInterval: 200, maxBackoff: 50, maxErrors: 10 },
      );
      lowPoller.start();
      await advanceTimersByTimeAsync(360);
      lowPoller.stop();
      // Fail (50), fail (50), succeed (200), fail (50).
      expect(low.gaps()).toEqual([50, 50, 200, 50]);

      const zero = timedAdapter();
      const zeroPoller = new EventPoller(
        { fetchEvents: zero.fetchEvents },
        descriptor,
        vi.fn(),
        { pollInterval: 100, maxBackoff: 0, maxErrors: 3 },
      );
      zeroPoller.start();
      for (let i = 0; i < 3; i++) await advanceTimersByTimeAsync(0);
      expect(zero.fetchEvents).toHaveBeenCalledTimes(3);
      expect(zeroPoller.active).toBe(false);
    });

    describe("Retry-After hints", () => {
      /** Fails every call, with the nth call's hint from `hints` (none past the end). */
      function hintedAdapter(hints: (number | undefined)[]) {
        const at: number[] = [];
        const fetchEvents = vi.fn(async (): Promise<EventPage> => {
          at.push(Date.now());
          throw new StreamdNetworkError("rate limited", 429, undefined, hints[at.length - 1]);
        });
        const gaps = () => at.slice(1).map((t, i) => t - at[i]!);
        return { fetchEvents, gaps };
      }

      it("waits the hint instead of the doubled interval, then resumes the sequence", async () => {
        const { fetchEvents, gaps } = hintedAdapter([350, undefined]);
        const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
          pollInterval: 100,
          maxErrors: 10,
        });
        poller.start();
        await advanceTimersByTimeAsync(1000);

        // Hinted 350 in place of 200; the next, unhinted failure waits 400.
        expect(gaps()).toEqual([350, 400]);
        poller.stop();
      });

      it("caps a hint at maxBackoff and retries at once on a hint of 0", async () => {
        const capped = hintedAdapter([5000, 5000]);
        const cappedPoller = new EventPoller({ fetchEvents: capped.fetchEvents }, descriptor, vi.fn(), {
          pollInterval: 100,
          maxBackoff: 300,
          maxErrors: 10,
        });
        cappedPoller.start();
        await advanceTimersByTimeAsync(650);
        cappedPoller.stop();
        expect(capped.gaps()).toEqual([300, 300]);

        const zero = hintedAdapter([0, 0]);
        const zeroPoller = new EventPoller({ fetchEvents: zero.fetchEvents }, descriptor, vi.fn(), {
          pollInterval: 100,
          maxErrors: 10,
        });
        zeroPoller.start();
        for (let i = 0; i < 3; i++) await advanceTimersByTimeAsync(0);
        zeroPoller.stop();
        // Three polls with no time advanced: none waited the 200ms backoff.
        expect(zero.fetchEvents).toHaveBeenCalledTimes(3);
      });

      it("ignores an invalid hint", async () => {
        const { fetchEvents, gaps } = hintedAdapter([-5, NaN]);
        const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
          pollInterval: 100,
          maxErrors: 10,
        });
        poller.start();
        await advanceTimersByTimeAsync(700);
        expect(gaps()).toEqual([200, 400]);
        poller.stop();
      });

      it("still counts a hinted failure toward maxErrors", async () => {
        const { fetchEvents } = hintedAdapter([10, 10, 10]);
        const onError = vi.fn();
        const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
          pollInterval: 100,
          maxErrors: 3,
          onError,
        });
        poller.start();
        await advanceTimersByTimeAsync(100);

        expect(fetchEvents).toHaveBeenCalledTimes(3);
        expect(poller.active).toBe(false);
        expect(onError).toHaveBeenCalledTimes(3);
        expect(onError.mock.calls[2]![0]).toBeInstanceOf(StreamdPollingError);
      });
    });

    it("throws StreamdError when constructed with an invalid maxBackoff", () => {
      const adapter = createMockAdapter([]);
      for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
        expect(
          () => new EventPoller(adapter, descriptor, vi.fn(), { maxBackoff: bad }),
        ).toThrow(StreamdError);
        expect(
          () => new EventPoller(adapter, descriptor, vi.fn(), { maxBackoff: bad }),
        ).toThrow(/Invalid maxBackoff/);
      }
    });
  });

  describe("invalid input", () => {
    const run = async (error: Error) => {
      const fetchEvents = vi.fn(async (): Promise<EventPage> => {
        throw error;
      });
      const onError = vi.fn();
      const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
        pollInterval: 100,
        maxErrors: 5,
        onError,
      });
      poller.start();
      await advanceTimersByTimeAsync(1000);
      poller.stop();
      return { fetchEvents, onError, poller };
    };

    it("stops on the first plain StreamdError from the adapter", async () => {
      const bad = new StreamdError("Not a valid Aptos address: nope");
      const { fetchEvents, onError, poller } = await run(bad);

      expect(fetchEvents).toHaveBeenCalledTimes(1);
      expect(onError).toHaveBeenCalledTimes(1);
      const reported = onError.mock.calls[0]![0];
      expect(reported).toBeInstanceOf(StreamdPollingError);
      expect(reported.cause).toBe(bad);
      expect(reported.message).toBe(
        "Polling stopped on invalid input: Not a valid Aptos address: nope",
      );
      expect(poller.active).toBe(false);
    });

    it("still retries a StreamdNetworkError", async () => {
      const { fetchEvents } = await run(new StreamdNetworkError("503", 503));
      expect(fetchEvents.mock.calls.length).toBeGreaterThan(1);
    });

    it("still retries an error that is not a StreamdError", async () => {
      const { fetchEvents } = await run(new Error("socket hang up"));
      expect(fetchEvents.mock.calls.length).toBeGreaterThan(1);
    });
  });

  it("is already inactive when the final error is reported", async () => {
    // once() rejects by checking `active` inside onError, so the poller must
    // stop before it notifies on the fatal error.
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new Error("network error");
      }),
    };
    const activeAtError: boolean[] = [];

    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 100,
      maxErrors: 2,
      onError: () => activeAtError.push(poller.active),
    });
    poller.start();

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);

    expect(activeAtError).toEqual([true, false]);
  });

  it("keeps polling until maxErrors and surfaces each onError throw", async () => {
    const fetchEvents = vi.fn(async () => {
      throw new Error("network down");
    });
    const onError = vi.fn(() => {
      throw new Error("handler bug");
    });
    const surfaced = captureMicrotaskThrows();
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
      maxErrors: 3,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(700);
    surfaced.restore();

    expect(fetchEvents).toHaveBeenCalledTimes(3);
    expect(onError).toHaveBeenCalledTimes(3);
    expect(poller.active).toBe(false);
    expect(surfaced.thrown).toHaveLength(3);
  });

  it("does not count a throwing callback and onError as a polling failure", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    const onError = vi.fn((_: Error) => {
      throw new Error("handler bug");
    });
    const callback = vi.fn(() => {
      throw new Error("callback bug");
    });
    const surfaced = captureMicrotaskThrows();
    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    surfaced.restore();

    expect(onError).toHaveBeenCalledTimes(1);
    expect(surfaced.thrown.map((e) => (e as Error).message)).toEqual(["handler bug"]);
    expect(onError.mock.calls[0]![0]).not.toBeInstanceOf(StreamdPollingError);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBeGreaterThanOrEqual(3);

    poller.stop();
  });

  it("counts only consecutive failures toward maxErrors", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValueOnce(new Error("down"))
      .mockRejectedValueOnce(new Error("down"))
      .mockResolvedValueOnce(page([], 1))
      .mockRejectedValue(new Error("down"));
    const counts: number[] = [];
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
      maxErrors: 3,
      onError: (e) => counts.push((e as StreamdPollingError).consecutiveFailures),
    });
    poller.start();
    // Fail, fail, succeed, fail, fail: five polls inside the first second.
    await advanceTimersByTimeAsync(1000);

    expect(fetchEvents).toHaveBeenCalledTimes(5);
    expect(counts).toEqual([1, 2, 1, 2]);
    expect(poller.active).toBe(true);

    poller.stop();
  });

  it("resets error count on successful poll", async () => {
    let callCount = 0;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        callCount++;
        if (callCount === 1) throw new Error("transient");
        return { events: [], nextCursor: String(callCount), hasMore: false };
      }),
    };
    const callback = vi.fn();
    const onError = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      maxErrors: 3,
      onError,
    });
    poller.start();

    await advanceTimersByTimeAsync(0); // fail
    await advanceTimersByTimeAsync(200); // success (backoff)
    await advanceTimersByTimeAsync(100); // success again

    expect(onError).toHaveBeenCalledTimes(1);
    expect(poller.active).toBe(true);

    poller.stop();
  });

  it("ignores start() while already running", async () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { pollInterval: 100 });
    poller.start();
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    poller.stop();
  });

  it("stop prevents further polling", async () => {
    const adapter = createMockAdapter([]);
    const callback = vi.fn();

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
    });
    poller.start();
    poller.stop();

    await advanceTimersByTimeAsync(1000);

    // Only the initial poll (if it fired) should have happened
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(0);
  });

  it("delivers nothing from a fetch that resolves after stop", async () => {
    let release!: (p: EventPage) => void;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(() => new Promise<EventPage>((r) => (release = r))),
    };
    const callback = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback);
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    release(page([makeEvent(1)], 2));
    await advanceTimersByTimeAsync(0);

    expect(callback).not.toHaveBeenCalled();
  });

  it("resumes from the old cursor after a fetch that resolved post-stop", async () => {
    let release!: (p: EventPage) => void;
    const fetchEvents = vi
      .fn()
      .mockImplementationOnce(() => new Promise<EventPage>((r) => (release = r)))
      .mockImplementation(async () => page([], 1));
    const adapter: EventAdapter = { fetchEvents };
    const poller = new EventPoller(adapter, descriptor, vi.fn());
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    release(page([], 99));
    await advanceTimersByTimeAsync(0);
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(fetchEvents).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[1]![1].cursor).toBeUndefined();

    poller.stop();
  });

  it("runs one poll chain after stop() and start() during an in-flight fetch", async () => {
    let release!: (p: EventPage) => void;
    const fetchEvents = vi
      .fn()
      .mockImplementationOnce(() => new Promise<EventPage>((r) => (release = r)))
      .mockImplementation(async () => page([], 1));
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    poller.start();
    await advanceTimersByTimeAsync(0);
    release(page([], 2));
    await advanceTimersByTimeAsync(0);
    const afterRestart = fetchEvents.mock.calls.length;

    await advanceTimersByTimeAsync(300);
    expect(fetchEvents.mock.calls.length - afterRestart).toBe(3);

    poller.stop();
  });

  it("ignores a fetch that fails after stop() and start()", async () => {
    let fail!: (e: Error) => void;
    const fetchEvents = vi
      .fn()
      .mockImplementationOnce(() => new Promise<EventPage>((_, r) => (fail = r)))
      .mockImplementation(async () => page([], 1));
    const onError = vi.fn();
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    poller.start();
    await advanceTimersByTimeAsync(0);
    fail(new Error("stale failure"));
    await advanceTimersByTimeAsync(0);
    const afterRestart = fetchEvents.mock.calls.length;

    await advanceTimersByTimeAsync(300);
    expect(onError).not.toHaveBeenCalled();
    expect(fetchEvents.mock.calls.length - afterRestart).toBe(3);

    poller.stop();
  });

  it("runs one poll chain when the callback restarts the poller", async () => {
    const fetchEvents = vi
      .fn()
      .mockImplementationOnce(async () => page([makeEvent(1), makeEvent(2)], 3))
      .mockImplementation(async () => page([], 3));
    const callback = vi.fn(() => {
      if (callback.mock.calls.length === 1) {
        poller.stop();
        poller.start();
      }
    });
    const poller = new EventPoller({ fetchEvents }, descriptor, callback, {
      pollInterval: 100,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    const afterRestart = fetchEvents.mock.calls.length;

    await advanceTimersByTimeAsync(300);
    expect(callback).toHaveBeenCalledTimes(1);
    // The restarted run's immediate poll, then one per interval.
    expect(fetchEvents.mock.calls.length - afterRestart).toBe(4);

    poller.stop();
  });

  it("runs one poll chain when onError restarts the poller", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValueOnce(new Error("down"))
      .mockImplementation(async () => page([], 1));
    let restarted = false;
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
      onError: () => {
        if (restarted) return;
        restarted = true;
        poller.stop();
        poller.start();
      },
    });
    poller.start();
    await advanceTimersByTimeAsync(1000);

    // The failed poll, the restart's immediate poll, then one per interval.
    expect(fetchEvents).toHaveBeenCalledTimes(11);
    poller.stop();
  });

  it("reports nothing from a fetch that fails after stop", async () => {
    let fail!: (e: Error) => void;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(() => new Promise<EventPage>((_, r) => (fail = r))),
    };
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    fail(new Error("boom"));
    await advanceTimersByTimeAsync(0);

    expect(onError).not.toHaveBeenCalled();
  });

  it("isolates a throwing callback from the rest of the page", async () => {
    const events = [makeEvent(1), makeEvent(2), makeEvent(3)];
    const adapter = createMockAdapter([page(events, 4)]);
    const delivered: bigint[] = [];
    const callback = vi.fn((e: StreamdEvent) => {
      delivered.push(e.version);
      if (e.version === 2n) throw new Error("handler bug");
    });
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      maxErrors: 1,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(250);

    expect(delivered).toEqual([1n, 2n, 3n]);
    expect(onError).toHaveBeenCalledTimes(1);
    const err = mocked(onError).mock.calls[0]![0];
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdPollingError);
    expect((err as StreamdError).cause).toBeInstanceOf(Error);
    expect(poller.active).toBe(true);

    poller.stop();
  });

  it("reports nothing when the callback unsubscribes and then throws", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    const onError = vi.fn();
    let poller!: EventPoller;
    const callback = vi.fn(() => {
      poller.stop();
      throw new Error("after stop");
    });
    poller = new EventPoller(adapter, descriptor, callback, { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });

  describe("async handlers", () => {
    function deferred() {
      let resolve!: () => void;
      let reject!: (e: Error) => void;
      const promise = new Promise<void>((res, rej) => {
        resolve = res;
        reject = rej;
      });
      return { promise, resolve, reject };
    }

    it("waits for a handler before the next event and the next poll", async () => {
      const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 3)]);
      const first = deferred();
      const callback = vi
        .fn<(e: StreamdEvent) => Promise<void> | void>()
        .mockReturnValueOnce(first.promise);
      const poller = new EventPoller(adapter, descriptor, callback, { pollInterval: 100 });
      poller.start();
      await advanceTimersByTimeAsync(300);

      expect(callback).toHaveBeenCalledTimes(1);
      expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

      first.resolve();
      await advanceTimersByTimeAsync(0);
      expect(callback).toHaveBeenCalledTimes(2);
      await advanceTimersByTimeAsync(100);
      expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);

      poller.stop();
    });

    it("holds the cursor until the page's handlers settle", async () => {
      const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
      const pending = deferred();
      const callback = vi
        .fn<(e: StreamdEvent) => Promise<void> | void>()
        .mockReturnValueOnce(pending.promise);
      const poller = new EventPoller(adapter, descriptor, callback, {
        pollInterval: 100,
        cursor: "1",
      });
      poller.start();
      await advanceTimersByTimeAsync(0);
      expect(poller.cursor).toBe("1");

      pending.resolve();
      await advanceTimersByTimeAsync(0);
      expect(poller.cursor).toBe("2");

      poller.stop();
    });

    it("leaves the cursor alone when stopped during a handler", async () => {
      const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
      const pending = deferred();
      const callback = vi
        .fn<(e: StreamdEvent) => Promise<void> | void>()
        .mockReturnValueOnce(pending.promise);
      const poller = new EventPoller(adapter, descriptor, callback, { cursor: "1" });
      poller.start();
      await advanceTimersByTimeAsync(0);

      poller.stop();
      pending.resolve();
      await advanceTimersByTimeAsync(0);
      expect(poller.cursor).toBe("1");
    });

    it("does not deliver alongside a pending handler after stop() and start()", async () => {
      const adapter = createMockAdapter([
        page([makeEvent(1), makeEvent(2)], 3),
        page([makeEvent(1), makeEvent(2)], 3),
      ]);
      const pending = deferred();
      const order: string[] = [];
      const callback = vi.fn((e: StreamdEvent) => {
        order.push(`start ${e.version}`);
        if (e.version === 1n) return pending.promise.then(() => void order.push("end 1"));
      });
      const poller = new EventPoller(adapter, descriptor, callback, {
        pollInterval: 100,
        cursor: "1",
      });
      poller.start();
      await advanceTimersByTimeAsync(0);

      // Restart twice while the handler is pending: the middle run is left
      // waiting on the handler and must not fetch once it settles.
      poller.stop();
      poller.start();
      await advanceTimersByTimeAsync(0);
      poller.stop();
      poller.start();
      await advanceTimersByTimeAsync(300);
      expect(order).toEqual(["start 1"]);
      expect(poller.cursor).toBe("1");

      pending.resolve();
      await advanceTimersByTimeAsync(0);
      expect(order).toEqual(["start 1", "end 1", "start 2"]);
      // The live run refetches the page the first never finished; the run
      // stopped while waiting makes no fetch of its own.
      expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
      expect(mocked(adapter.fetchEvents).mock.calls[1]![1].cursor).toBe("1");
      expect(poller.cursor).toBe("3");

      poller.stop();
    });

    it("keeps the held cursor when a restarted page's last handler settles late", async () => {
      const adapter = createMockAdapter([page([makeEvent(1)], 2), page([], 2)]);
      const pending = deferred();
      const callback = vi
        .fn<(e: StreamdEvent) => Promise<void> | void>()
        .mockReturnValueOnce(pending.promise);
      const poller = new EventPoller(adapter, descriptor, callback, { cursor: "1" });
      poller.start();
      await advanceTimersByTimeAsync(0);

      poller.stop();
      poller.start();
      pending.resolve();
      await advanceTimersByTimeAsync(0);

      // The stale run must not write its page's cursor over the live run's.
      expect(mocked(adapter.fetchEvents).mock.calls[1]![1].cursor).toBe("1");

      poller.stop();
    });

    it("recovers on the next poll after a malformed page", async () => {
      const bad = { events: null, nextCursor: "1", hasMore: false } as unknown as EventPage;
      const adapter = createMockAdapter([bad, page([makeEvent(2)], 3)]);
      const callback = vi.fn();
      const onError = vi.fn();
      const poller = new EventPoller(adapter, descriptor, callback, {
        pollInterval: 100,
        onError,
      });
      poller.start();
      await advanceTimersByTimeAsync(300);

      expect(onError).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledTimes(1);
      expect(poller.active).toBe(true);

      poller.stop();
    });

    it("reports a rejected handler and carries on with the page", async () => {
      const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 3)]);
      const first = deferred();
      const callback = vi
        .fn<(e: StreamdEvent) => Promise<void> | void>()
        .mockReturnValueOnce(first.promise);
      const onError = vi.fn();
      const poller = new EventPoller(adapter, descriptor, callback, { onError });
      poller.start();
      await advanceTimersByTimeAsync(0);

      first.reject(new Error("handler failed"));
      await advanceTimersByTimeAsync(0);
      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError.mock.calls[0]![0]).not.toBeInstanceOf(StreamdPollingError);
      expect(callback).toHaveBeenCalledTimes(2);

      poller.stop();
    });
  });

  it("reports a rejecting async callback through onError", async () => {
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 3)]);
    const boom = new Error("async handler bug");
    const callback = vi.fn(async () => {
      throw boom;
    });
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(callback).toHaveBeenCalledTimes(2);
    expect(onError).toHaveBeenCalledTimes(2);
    const err = onError.mock.calls[0]![0];
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdPollingError);
    expect(err.cause).toBe(boom);
    expect(poller.active).toBe(true);

    poller.stop();
  });

  it("reports nothing from an async callback that rejects after stop", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    let reject!: (e: Error) => void;
    const callback = vi.fn(() => new Promise<void>((_, r) => (reject = r)));
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    reject(new Error("late"));
    await advanceTimersByTimeAsync(0);

    expect(onError).not.toHaveBeenCalled();
  });

  it("reports a late async rejection from before a restart", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    let reject!: (e: Error) => void;
    const callback = vi.fn(() => new Promise<void>((_, r) => (reject = r)));
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    poller.start();
    reject(new Error("late"));
    await advanceTimersByTimeAsync(0);

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![0].cause.message).toBe("late");

    poller.stop();
  });

  it("reports a callback that restarts the poller and then throws", async () => {
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 3)]);
    const onError = vi.fn();
    const callback = vi.fn(() => {
      poller.stop();
      poller.start();
      throw new Error("after restart");
    });
    const poller = new EventPoller(adapter, descriptor, callback, { onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(1);

    poller.stop();
  });

  it("isolates a throwing where predicate from the rest of the page", async () => {
    const events = [makeEvent(1), makeEvent(2), makeEvent(3)];
    const adapter = createMockAdapter([page(events, 4)]);
    const callback = vi.fn();
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      maxErrors: 1,
      onError,
      where: (e) => {
        if (e.version === 2n) throw new Error("predicate bug");
        return true;
      },
    });
    poller.start();
    await advanceTimersByTimeAsync(250);

    expect(mocked(callback).mock.calls.map(([e]) => e.version)).toEqual([1n, 3n]);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(mocked(onError).mock.calls[0]![0]).not.toBeInstanceOf(StreamdPollingError);
    expect(poller.active).toBe(true);

    poller.stop();
  });

  it("stops delivering the page when stopped from the callback", async () => {
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 3)]);
    let poller!: EventPoller;
    const callback = vi.fn(() => poller.stop());
    poller = new EventPoller(adapter, descriptor, callback, { pollInterval: 100 });
    poller.start();
    await advanceTimersByTimeAsync(300);

    expect(callback).toHaveBeenCalledTimes(1);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });
});

describe("EventPoller signal", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("stops polling when the signal aborts, without reporting an error", async () => {
    const adapter = createMockAdapter([]);
    const onError = vi.fn();
    const controller = new AbortController();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 100,
      onError,
      signal: controller.signal,
    });
    poller.start();
    await advanceTimersByTimeAsync(150);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);

    controller.abort();
    expect(poller.active).toBe(false);
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    expect(onError).not.toHaveBeenCalled();
  });

  it("never starts on an already-aborted signal", async () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      signal: AbortSignal.abort(),
    });
    poller.start();
    await advanceTimersByTimeAsync(100);

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
  });

  it("detaches from the signal when stopped", () => {
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const poller = new EventPoller(createMockAdapter([]), descriptor, vi.fn(), {
      signal: controller.signal,
    });
    poller.start();
    poller.stop();

    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
    // Restarting after the stop still honours the signal.
    poller.start();
    controller.abort();
    expect(poller.active).toBe(false);
  });
});

describe("EventPoller status", () => {
  let now = 10_000;

  beforeEach(() => {
    vi.useFakeTimers();
    now = 10_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("reports initial status before start, after start with a seeded cursor, and on a pre-aborted signal", () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      cursor: "42",
    });
    expect(poller.status).toEqual({
      active: false,
      cursor: "42",
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });

    poller.start();
    expect(poller.status).toEqual({
      active: true,
      cursor: "42",
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });
    poller.stop();

    const aborted = new EventPoller(adapter, descriptor, vi.fn(), {
      signal: AbortSignal.abort(),
    });
    aborted.start();
    expect(aborted.status).toEqual({
      active: false,
      cursor: undefined,
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });
  });

  it("tracks lastVersion, cursor, and lastSuccessAt across empty, filtered, multi-event, and out-of-order pages", async () => {
    const adapter = createMockAdapter([
      page([], 10),
      page([makeEvent(5), makeEvent(9), makeEvent(12)], 20),
      page([makeEvent(7)], 30),
    ]);
    const versionsInCallback: Array<bigint | undefined> = [];
    const lastSuccessInCallback: Array<number | undefined> = [];
    const poller = new EventPoller(
      adapter,
      descriptor,
      () => {
        versionsInCallback.push(poller.status.lastVersion);
        lastSuccessInCallback.push(poller.status.lastSuccessAt);
      },
      {
        pollInterval: 100,
        where: (e) => e.version !== 12n,
      },
    );
    poller.start();

    // First poll: empty page at 10_000.
    await advanceTimersByTimeAsync(0);
    expect(poller.status).toEqual({
      active: true,
      cursor: "10",
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: 10_000,
    });

    // Second poll at 10_100: events 5 and 9 match where, 12 is filtered out
    // after being marked seen, so lastVersion reaches 12n.
    now = 10_100;
    await advanceTimersByTimeAsync(100);
    expect(versionsInCallback).toEqual([5n, 9n]);
    expect(lastSuccessInCallback).toEqual([10_000, 10_000]);
    expect(poller.status).toEqual({
      active: true,
      cursor: "20",
      lastVersion: 12n,
      consecutiveErrors: 0,
      lastSuccessAt: 10_100,
    });

    // Third poll at 10_200: out-of-order lower version 7n does not move lastVersion backwards.
    now = 10_200;
    await advanceTimersByTimeAsync(100);
    expect(poller.status).toEqual({
      active: true,
      cursor: "30",
      lastVersion: 12n,
      consecutiveErrors: 0,
      lastSuccessAt: 10_200,
    });

    poller.stop();
  });

  it("tracks consecutiveErrors across failures and resets on a successful poll while ignoring callback errors", async () => {
    now = 50_000;
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce(page([makeEvent(3)], 4))
      .mockRejectedValueOnce(new Error("down 1"))
      .mockRejectedValueOnce(new Error("down 2"))
      .mockResolvedValueOnce(page([makeEvent(8)], 9))
      .mockRejectedValue(new Error("down final"));
    const statusAtError: Array<{
      active: boolean;
      consecutiveErrors: number;
      lastVersion: bigint | undefined;
      lastSuccessAt: number | undefined;
    }> = [];
    const poller = new EventPoller(
      { fetchEvents },
      descriptor,
      (e) => {
        if (e.version === 8n) throw new Error("callback bug");
      },
      {
        pollInterval: 100,
        maxErrors: 2,
        onError: (err) => {
          if (err instanceof StreamdPollingError) {
            const s = poller.status;
            statusAtError.push({
              active: s.active,
              consecutiveErrors: s.consecutiveErrors,
              lastVersion: s.lastVersion,
              lastSuccessAt: s.lastSuccessAt,
            });
          }
        },
      },
    );
    poller.start();

    // Poll 1 at 50_000 succeeds.
    await advanceTimersByTimeAsync(0);
    expect(poller.status).toEqual({
      active: true,
      cursor: "4",
      lastVersion: 3n,
      consecutiveErrors: 0,
      lastSuccessAt: 50_000,
    });

    // Poll 2 at 50_100 fails (1/2).
    now = 50_100;
    await advanceTimersByTimeAsync(100);
    expect(poller.status).toEqual({
      active: true,
      cursor: "4",
      lastVersion: 3n,
      consecutiveErrors: 1,
      lastSuccessAt: 50_000,
    });

    // Poll 3 at 50_300 fails (2/2) and auto-stops.
    now = 50_300;
    await advanceTimersByTimeAsync(200);
    expect(poller.status).toEqual({
      active: false,
      cursor: "4",
      lastVersion: 3n,
      consecutiveErrors: 2,
      lastSuccessAt: 50_000,
    });
    expect(statusAtError).toEqual([
      { active: true, consecutiveErrors: 1, lastVersion: 3n, lastSuccessAt: 50_000 },
      { active: false, consecutiveErrors: 2, lastVersion: 3n, lastSuccessAt: 50_000 },
    ]);

    // Restart and let poll 4 succeed despite a throwing callback: consecutiveErrors resets to 0.
    now = 50_400;
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.status).toEqual({
      active: true,
      cursor: "9",
      lastVersion: 8n,
      consecutiveErrors: 0,
      lastSuccessAt: 50_400,
    });

    poller.stop();
  });

  it("holds cursor and lastSuccessAt while an async handler is pending and when stopped mid-page", async () => {
    now = 20_000;
    let releaseFirst!: () => void;
    let releaseSecond!: () => void;
    const adapter = createMockAdapter([
      page([makeEvent(1)], 2),
      page([makeEvent(10), makeEvent(20)], 3),
    ]);
    const callback = vi
      .fn<(e: StreamdEvent) => Promise<void> | void>()
      .mockImplementationOnce(() => new Promise<void>((r) => (releaseFirst = r)))
      .mockImplementationOnce(() => new Promise<void>((r) => (releaseSecond = r)));

    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      cursor: "0",
    });
    poller.start();

    await advanceTimersByTimeAsync(0);
    // While event 1's handler is pending, lastVersion is 1n, but cursor and lastSuccessAt have not advanced.
    expect(poller.status).toEqual({
      active: true,
      cursor: "0",
      lastVersion: 1n,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });

    now = 20_050;
    releaseFirst();
    await advanceTimersByTimeAsync(0);
    expect(poller.status).toEqual({
      active: true,
      cursor: "2",
      lastVersion: 1n,
      consecutiveErrors: 0,
      lastSuccessAt: 20_050,
    });

    // Next poll starts at 20_150 and pauses on event 10; stop before it resolves.
    now = 20_150;
    await advanceTimersByTimeAsync(100);
    expect(poller.status.lastVersion).toBe(10n);
    expect(poller.status.cursor).toBe("2");
    expect(poller.status.lastSuccessAt).toBe(20_050);

    poller.stop();
    releaseSecond();
    await advanceTimersByTimeAsync(0);

    // Stopped mid-page: event 20 was never seen, cursor stayed "2", and lastSuccessAt stayed 20_050.
    expect(poller.status).toEqual({
      active: false,
      cursor: "2",
      lastVersion: 10n,
      consecutiveErrors: 0,
      lastSuccessAt: 20_050,
    });
  });

  it("leaves status untouched when a fetch resolves or rejects after stop, and returns an isolated snapshot", async () => {
    let resolveStale!: (p: EventPage) => void;
    let rejectStale!: (e: Error) => void;
    const adapter: EventAdapter = {
      fetchEvents: vi
        .fn<EventAdapter["fetchEvents"]>()
        .mockImplementationOnce(() => new Promise<EventPage>((r) => (resolveStale = r)))
        .mockImplementationOnce(() => new Promise<EventPage>((_, r) => (rejectStale = r))),
    };
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { cursor: "5" });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.stop();
    resolveStale(page([makeEvent(99)], 100));
    await advanceTimersByTimeAsync(0);

    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();
    rejectStale(new Error("stale error"));
    await advanceTimersByTimeAsync(0);

    const snap = poller.status as {
      active: boolean;
      cursor: string | undefined;
      lastVersion: bigint | undefined;
      consecutiveErrors: number;
      lastSuccessAt: number | undefined;
    };
    expect(snap).toEqual({
      active: false,
      cursor: "5",
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });

    snap.active = true;
    snap.cursor = "mutated";
    snap.lastVersion = 999n;
    snap.consecutiveErrors = 77;
    snap.lastSuccessAt = 12345;

    expect(poller.status).toEqual({
      active: false,
      cursor: "5",
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });
  });
});

describe("EventPoller pause and resume", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("stops fetching while paused and resumes from the held cursor", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2), page([makeEvent(2)], 3)]);
    const callback = vi.fn();
    const poller = new EventPoller(adapter, descriptor, callback, { pollInterval: 100 });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.cursor).toBe("2");

    poller.pause();
    expect(poller.active).toBe(false);
    expect(poller.paused).toBe(true);
    expect(poller.status.active).toBe(false);
    await advanceTimersByTimeAsync(1000);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    poller.resume();
    expect(poller.active).toBe(true);
    expect(poller.paused).toBe(false);
    await advanceTimersByTimeAsync(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    expect(mocked(adapter.fetchEvents).mock.calls[1]![1].cursor).toBe("2");
    expect(callback.mock.calls.map(([e]) => e.version)).toEqual([1n, 2n]);

    poller.stop();
  });

  it("discards a fetch that resolves while paused", async () => {
    let release!: (p: EventPage) => void;
    const fetchEvents = vi
      .fn()
      .mockImplementationOnce(() => new Promise<EventPage>((r) => (release = r)))
      .mockImplementation(async () => page([], 5));
    const callback = vi.fn();
    const poller = new EventPoller({ fetchEvents }, descriptor, callback, { cursor: "1" });
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.pause();
    release(page([makeEvent(1)], 2));
    await advanceTimersByTimeAsync(0);
    expect(callback).not.toHaveBeenCalled();
    expect(poller.cursor).toBe("1");
    expect(poller.status.lastVersion).toBeUndefined();

    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(fetchEvents.mock.calls[1]![1].cursor).toBe("1");

    poller.stop();
  });

  it("stops the page when paused from the callback and skips delivered events on resume", async () => {
    const twoEvents = page([makeEvent(1), makeEvent(2)], 3);
    const adapter = createMockAdapter([twoEvents, twoEvents]);
    const callback = vi.fn((_e: StreamdEvent) => {
      if (callback.mock.calls.length === 1) poller.pause();
    });
    const poller = new EventPoller(adapter, descriptor, callback, { cursor: "1" });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(poller.cursor).toBe("1");

    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(mocked(adapter.fetchEvents).mock.calls[1]![1].cursor).toBe("1");
    expect(callback.mock.calls.map(([e]) => e.version)).toEqual([1n, 2n]);
    expect(poller.cursor).toBe("3");

    poller.stop();
  });

  it("seeds from fromVersion when resumed before any page finished", async () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { fromVersion: 42n });
    poller.start();
    poller.pause();
    await advanceTimersByTimeAsync(100);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();

    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(mocked(adapter.fetchEvents).mock.calls[0]![1]).toMatchObject({
      cursor: undefined,
      fromVersion: 42n,
    });

    poller.stop();
  });

  it("ignores pause() when not active and resume() unless paused", async () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { pollInterval: 100 });

    poller.pause();
    expect(poller.paused).toBe(false);
    poller.resume();
    expect(poller.active).toBe(false);

    poller.start();
    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    poller.pause();
    poller.pause();
    poller.stop();
    expect(poller.paused).toBe(false);
    poller.resume();
    expect(poller.active).toBe(false);
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("does not revive a poller that gave up after maxErrors", async () => {
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new Error("down");
      }),
    };
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 10,
      maxErrors: 1,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.active).toBe(false);

    poller.resume();
    await advanceTimersByTimeAsync(100);
    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("ends on abort while paused", async () => {
    const controller = new AbortController();
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      signal: controller.signal,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.pause();

    controller.abort();
    expect(poller.paused).toBe(false);
    poller.resume();
    await advanceTimersByTimeAsync(100);
    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("reports a pending handler that rejects while paused", async () => {
    let reject!: (e: Error) => void;
    const pending = new Promise<void>((_, r) => (reject = r));
    const onError = vi.fn();
    const callback = vi
      .fn<(e: StreamdEvent) => Promise<void> | void>()
      .mockReturnValueOnce(pending);
    const poller = new EventPoller(
      createMockAdapter([page([makeEvent(1)], 2)]),
      descriptor,
      callback,
      { onError },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);

    poller.pause();
    reject(new Error("handler failed"));
    await advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![0]).toBeInstanceOf(StreamdError);
    expect(poller.paused).toBe(true);
    expect(poller.status.consecutiveErrors).toBe(0);
  });

  it("cancels the pending poll timer, so an immediate resume runs one polling loop", async () => {
    const adapter = createMockAdapter([]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { pollInterval: 100 });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    poller.pause();
    poller.resume();
    await advanceTimersByTimeAsync(1000);
    // The first poll, the resume's immediate poll, then one per interval.
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(12);

    poller.stop();
  });

  it("never has two fetches in flight when paused and resumed during one", async () => {
    let inFlight = 0;
    let peak = 0;
    const fetchEvents = vi.fn(async () => {
      peak = Math.max(peak, ++inFlight);
      await new Promise((r) => setTimeout(r, 200));
      inFlight--;
      return page([], 1);
    });
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), { pollInterval: 1000 });
    poller.start();
    await advanceTimersByTimeAsync(10);
    for (let i = 0; i < 5; i++) {
      poller.pause();
      poller.resume();
      await advanceTimersByTimeAsync(10);
    }
    await advanceTimersByTimeAsync(500);
    expect(peak).toBe(1);
    expect(fetchEvents).toHaveBeenCalledTimes(2);

    poller.stop();
  });

  it("carries consecutiveErrors across a pause and retries at once on resume", async () => {
    const fetchEvents = vi
      .fn()
      .mockRejectedValueOnce(new Error("down"))
      .mockImplementation(async () => page([], 1));
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 1000,
      maxErrors: 5,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.status.consecutiveErrors).toBe(1);

    poller.pause();
    expect(poller.status.consecutiveErrors).toBe(1);
    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
    expect(poller.status.consecutiveErrors).toBe(0);

    poller.stop();
  });
});

describe("EventPoller onCaughtUp", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("fires once after the backlog is delivered, not before", async () => {
    const order: string[] = [];
    const adapter = createMockAdapter([
      page([makeEvent(1)], 1, true),
      page([makeEvent(2)], 2, false),
    ]);
    const poller = new EventPoller(
      adapter,
      descriptor,
      (e) => void order.push(`event ${e.version}`),
      { pollInterval: 1000, onCaughtUp: () => void order.push("caught up") },
    );
    poller.start();

    await advanceTimersByTimeAsync(0);
    expect(order).toEqual(["event 1"]);
    await advanceTimersByTimeAsync(1);
    expect(order).toEqual(["event 1", "event 2", "caught up"]);
    expect(poller.cursor).toBe("2");

    await advanceTimersByTimeAsync(3000);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBeGreaterThanOrEqual(4);
    expect(order.filter((o) => o === "caught up")).toHaveLength(1);
    poller.stop();
  });

  it("is not called for a failed poll", async () => {
    const onCaughtUp = vi.fn();
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new StreamdNetworkError("down");
      }),
    };
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      onCaughtUp,
      onError: vi.fn(),
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(onCaughtUp).not.toHaveBeenCalled();
    poller.stop();
  });

  it("reports a throw through onError and keeps polling", async () => {
    const onError = vi.fn();
    const adapter = createMockAdapter([page([], 1)]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 1000,
      onError,
      onCaughtUp: () => {
        throw new Error("boom");
      },
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(onError).toHaveBeenCalledTimes(1);
    const err = mocked(onError).mock.calls[0]![0] as StreamdError;
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdPollingError);
    expect((err.cause as Error).message).toBe("boom");
    expect(poller.status.consecutiveErrors).toBe(0);

    await advanceTimersByTimeAsync(1000);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    expect(poller.active).toBe(true);
    poller.stop();
  });

  it("schedules no further poll when it stops the poller", async () => {
    const adapter = createMockAdapter([page([], 1)]);
    const poller: EventPoller = new EventPoller(adapter, descriptor, vi.fn(), {
      pollInterval: 1000,
      onCaughtUp: () => poller.stop(),
    });
    poller.start();
    await advanceTimersByTimeAsync(2000);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(poller.active).toBe(false);
  });

  it("fires after resume when a pause came before it", async () => {
    const onCaughtUp = vi.fn();
    const adapter = createMockAdapter([
      page([makeEvent(1)], 1, true),
      page([makeEvent(2)], 2, false),
    ]);
    const poller: EventPoller = new EventPoller(
      adapter,
      descriptor,
      (e) => {
        if (e.version === 1n) poller.pause();
      },
      { pollInterval: 1000, onCaughtUp },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.paused).toBe(true);
    expect(onCaughtUp).not.toHaveBeenCalled();

    poller.resume();
    await advanceTimersByTimeAsync(0);
    expect(onCaughtUp).toHaveBeenCalledTimes(1);

    poller.pause();
    poller.resume();
    await advanceTimersByTimeAsync(2000);
    expect(onCaughtUp).toHaveBeenCalledTimes(1);
    poller.stop();
  });
});

describe("EventPoller batch delivery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>((res) => (resolve = res));
    return { promise, resolve };
  }

  const versions = (events: StreamdEvent[]) => events.map((e) => Number(e.version));

  it("delivers an event a page repeats once, unless dedupWindow is 0", async () => {
    for (const [dedupWindow, expected] of [[undefined, [1, 2]], [0, [1, 1, 2]]] as const) {
      const adapter = createMockAdapter([page([makeEvent(1), makeEvent(1), makeEvent(2)], 2)]);
      const batches: number[][] = [];
      const poller = new EventPoller(adapter, descriptor, { batch: (es) => void batches.push(versions(es)) }, {
        dedupWindow,
      });
      poller.start();
      await advanceTimersByTimeAsync(0);
      poller.stop();
      expect(batches).toEqual([[...expected]]);
    }
  });

  it("delivers each page's new, filtered events in one call, in order", async () => {
    const adapter = createMockAdapter([
      page([makeEvent(1), makeEvent(2), makeEvent(3)], 3, true),
      page([makeEvent(3), makeEvent(4), makeEvent(5)], 5),
    ]);
    const batch = vi.fn((_: StreamdEvent[]) => {});
    const poller = new EventPoller(adapter, descriptor, { batch }, {
      pollInterval: 1000,
      where: (e) => e.version !== 2n,
    });
    poller.start();
    await advanceTimersByTimeAsync(1);

    expect(batch.mock.calls.map(([events]) => versions(events))).toEqual([
      [1, 3],
      [4, 5],
    ]);
    expect(poller.cursor).toBe("5");
    expect(poller.status.lastVersion).toBe(5n);
    poller.stop();
  });

  it("skips the call for a page with nothing new but still advances", async () => {
    const adapter = createMockAdapter([
      page([makeEvent(1)], 1, true),
      page([makeEvent(1)], 2, true),
      page([], 3),
    ]);
    const batch = vi.fn((_: StreamdEvent[]) => {});
    const poller = new EventPoller(adapter, descriptor, { batch }, { pollInterval: 1000 });
    poller.start();
    await advanceTimersByTimeAsync(2);

    expect(adapter.fetchEvents).toHaveBeenCalledTimes(3);
    expect(batch).toHaveBeenCalledTimes(1);
    expect(poller.cursor).toBeDefined();
    poller.stop();
  });

  it("holds the cursor and the next poll until the batch settles", async () => {
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 2)]);
    const pending = deferred();
    const poller = new EventPoller(
      adapter,
      descriptor,
      { batch: () => pending.promise },
      { pollInterval: 100, cursor: "0" },
    );
    poller.start();
    await advanceTimersByTimeAsync(300);
    expect(poller.cursor).toBe("0");
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    pending.resolve();
    await advanceTimersByTimeAsync(0);
    expect(poller.cursor).toBe("2");
    await advanceTimersByTimeAsync(100);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    poller.stop();
  });

  it("reports a rejected batch without spending the error budget", async () => {
    const onError = vi.fn();
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 2)]);
    const poller = new EventPoller(
      adapter,
      descriptor,
      { batch: async () => Promise.reject(new Error("boom")) },
      { pollInterval: 1000, onError },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(onError).toHaveBeenCalledTimes(1);
    const err = mocked(onError).mock.calls[0]![0] as StreamdError;
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdPollingError);
    expect(err.message).toBe("Batch callback threw for 2 events from 1:0 to 2:0");
    expect((err.cause as Error).message).toBe("boom");
    expect(poller.status.consecutiveErrors).toBe(0);
    expect(poller.cursor).toBe("2");

    await advanceTimersByTimeAsync(1000);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    poller.stop();
  });

  it("drops only the event whose where predicate throws", async () => {
    const onError = vi.fn();
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2), makeEvent(3)], 3)]);
    const batch = vi.fn((_: StreamdEvent[]) => {});
    const poller = new EventPoller(adapter, descriptor, { batch }, {
      onError,
      where: (e) => {
        if (e.version === 2n) throw new Error("bad");
        return true;
      },
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(onError).toHaveBeenCalledTimes(1);
    expect(versions(batch.mock.calls[0]![0])).toEqual([1, 3]);
    poller.stop();
  });

  it("leaves the cursor alone when stopped during a pending batch", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    const pending = deferred();
    const poller = new EventPoller(
      adapter,
      descriptor,
      { batch: () => pending.promise },
      { pollInterval: 100, cursor: "1" },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();
    pending.resolve();
    await advanceTimersByTimeAsync(0);

    expect(poller.cursor).toBe("1");
  });
});

describe("EventPoller relative start", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function headAdapter(head: bigint, pages: EventPage[] = []) {
    const adapter = createMockAdapter(pages);
    adapter.getHeadVersion = vi.fn(async () => head);
    return adapter;
  }

  const firstFrom = (adapter: EventAdapter) =>
    mocked(adapter.fetchEvents).mock.calls[0]?.[1]?.fromVersion;

  it("starts a negative fromVersion that many versions before the head", async () => {
    const adapter = headAdapter(10n);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { fromVersion: -2n });
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();

    expect(firstFrom(adapter)).toBe(8n);
    expect(adapter.getHeadVersion).toHaveBeenCalledWith(descriptor);
  });

  it("clamps at version 0", async () => {
    const adapter = headAdapter(3n);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { fromVersion: -50n });
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();

    expect(firstFrom(adapter)).toBe(0n);
  });

  it("reads no head for a non-negative fromVersion or a cursor", async () => {
    for (const options of [{ fromVersion: 4n }, { fromVersion: -2n, cursor: "7" }, {}]) {
      const adapter = headAdapter(10n);
      const poller = new EventPoller(adapter, descriptor, vi.fn(), options);
      poller.start();
      await advanceTimersByTimeAsync(0);
      poller.stop();

      expect(adapter.getHeadVersion).not.toHaveBeenCalled();
      expect(firstFrom(adapter)).toBe(options.cursor ? undefined : options.fromVersion);
    }
  });

  it("stops as invalid input when the adapter cannot report a head", async () => {
    const adapter = createMockAdapter([]);
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      fromVersion: -2n,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledTimes(1);
    const err = onError.mock.calls[0]![0] as StreamdPollingError;
    expect(err).toBeInstanceOf(StreamdPollingError);
    expect(err.message).toContain("getHeadVersion");
  });

  it("retries a head read that failed on the network", async () => {
    const adapter = headAdapter(10n);
    mocked(adapter.getHeadVersion!).mockRejectedValueOnce(
      new StreamdNetworkError("503", 503),
    );
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      fromVersion: -2n,
      pollInterval: 100,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(poller.active).toBe(true);

    await advanceTimersByTimeAsync(200);
    poller.stop();
    expect(adapter.getHeadVersion).toHaveBeenCalledTimes(2);
    expect(firstFrom(adapter)).toBe(8n);
  });

  it("reuses the resolved start when the fetch after it fails", async () => {
    const adapter = headAdapter(10n);
    mocked(adapter.fetchEvents).mockRejectedValueOnce(
      new StreamdNetworkError("503", 503),
    );
    mocked(adapter.getHeadVersion!).mockResolvedValueOnce(10n).mockResolvedValue(99n);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      fromVersion: -2n,
      pollInterval: 100,
      onError: () => {},
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    poller.stop();

    const froms = mocked(adapter.fetchEvents).mock.calls.map((c) => c[1].fromVersion);
    expect(froms.slice(0, 2)).toEqual([8n, 8n]);
    expect(adapter.getHeadVersion).toHaveBeenCalledTimes(1);
  });
});

describe("EventPoller earliest start", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function earliestAdapter(earliest: bigint, pages: EventPage[] = []) {
    const adapter = createMockAdapter(pages);
    adapter.getEarliestVersion = vi.fn(async () => earliest);
    return adapter;
  }

  const firstFrom = (adapter: EventAdapter) =>
    mocked(adapter.fetchEvents).mock.calls[0]?.[1]?.fromVersion;

  async function firstPoll(adapter: EventAdapter, options: ListenerOptions) {
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { onError, ...options });
    poller.start();
    await advanceTimersByTimeAsync(0);
    return { poller, onError };
  }

  it("starts at the adapter's earliest version", async () => {
    const adapter = earliestAdapter(5n);
    const { poller } = await firstPoll(adapter, { fromVersion: "earliest" });
    poller.stop();

    expect(firstFrom(adapter)).toBe(5n);
    expect(adapter.getEarliestVersion).toHaveBeenCalledWith(descriptor);
  });

  it("reads no earliest version when a cursor is given or fromVersion is a bigint", async () => {
    for (const options of [
      { fromVersion: "earliest", cursor: "7" },
      { fromVersion: 4n },
    ] as ListenerOptions[]) {
      const adapter = earliestAdapter(5n);
      const { poller } = await firstPoll(adapter, options);
      poller.stop();

      expect(adapter.getEarliestVersion).not.toHaveBeenCalled();
      expect(firstFrom(adapter)).toBe(options.cursor ? undefined : 4n);
    }
  });

  it("stops as invalid input when the adapter cannot report an earliest version", async () => {
    const adapter = createMockAdapter([]);
    const { poller, onError } = await firstPoll(adapter, { fromVersion: "earliest" });

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    const err = onError.mock.calls[0]![0] as StreamdPollingError;
    expect(err).toBeInstanceOf(StreamdPollingError);
    expect(err.message).toContain("getEarliestVersion");
  });

  it("stops as invalid input when the earliest version is malformed", async () => {
    const adapter = earliestAdapter(-500n);
    const { poller, onError } = await firstPoll(adapter, { fromVersion: "earliest" });

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect((onError.mock.calls[0]![0] as Error).message).toContain(
      "getEarliestVersion resolved -500",
    );
  });

  it("stops as invalid input on a fromVersion that is neither a bigint nor earliest", async () => {
    const adapter = earliestAdapter(5n);
    const { poller, onError } = await firstPoll(adapter, {
      fromVersion: "latest" as unknown as bigint,
    });

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect(adapter.getEarliestVersion).not.toHaveBeenCalled();
    expect((onError.mock.calls[0]![0] as Error).message).toContain("Invalid fromVersion");
  });

  it("retries an earliest read that failed on the network", async () => {
    const adapter = earliestAdapter(5n);
    mocked(adapter.getEarliestVersion!).mockRejectedValueOnce(
      new StreamdNetworkError("503", 503),
    );
    const { poller, onError } = await firstPoll(adapter, {
      fromVersion: "earliest",
      pollInterval: 100,
    });
    expect(onError).toHaveBeenCalledTimes(1);
    expect(poller.active).toBe(true);

    await advanceTimersByTimeAsync(200);
    poller.stop();
    expect(adapter.getEarliestVersion).toHaveBeenCalledTimes(2);
    expect(firstFrom(adapter)).toBe(5n);
  });

  it("reuses the resolved start when the fetch after it fails", async () => {
    const adapter = earliestAdapter(5n);
    mocked(adapter.fetchEvents).mockRejectedValueOnce(
      new StreamdNetworkError("503", 503),
    );
    mocked(adapter.getEarliestVersion!).mockResolvedValueOnce(5n).mockResolvedValue(99n);
    const { poller } = await firstPoll(adapter, {
      fromVersion: "earliest",
      pollInterval: 100,
    });
    await advanceTimersByTimeAsync(200);
    poller.stop();

    const froms = mocked(adapter.fetchEvents).mock.calls.map((c) => c[1].fromVersion);
    expect(froms.slice(0, 2)).toEqual([5n, 5n]);
    expect(adapter.getEarliestVersion).toHaveBeenCalledTimes(1);
  });
});

describe("EventPoller decode", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function amountEvent(version: number, amount: unknown): StreamdEvent {
    return { ...makeEvent(version), data: { amount } };
  }

  it("delivers decoded data, and where sees the same decoded event", async () => {
    const seen: unknown[] = [];
    const tested: unknown[] = [];
    const source = amountEvent(1, "18446744073709551615");
    const adapter = createMockAdapter([page([source, amountEvent(2, "3")], 2)]);
    const poller = new EventPoller(adapter, descriptor, (e) => void seen.push(e.data), {
      decode: ["amount"],
      where: (e) => {
        tested.push((e.data as { amount: unknown }).amount);
        return (e.data as { amount: bigint }).amount > 5n;
      },
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(tested).toEqual([18446744073709551615n, 3n]);
    expect(seen).toEqual([{ amount: 18446744073709551615n }]);
    expect(source.data).toEqual({ amount: "18446744073709551615" });
    poller.stop();
  });

  it("reports an undecodable event through onError, skips it, and keeps polling", async () => {
    const seen: bigint[] = [];
    const onError = vi.fn();
    const adapter = createMockAdapter([
      page([amountEvent(1, "not a number"), amountEvent(2, "4")], 2),
    ]);
    const poller = new EventPoller(adapter, descriptor, (e) => void seen.push(e.version), {
      decode: ["amount"],
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(seen).toEqual([2n]);
    expect(onError).toHaveBeenCalledTimes(1);
    const reported = onError.mock.calls[0]![0] as StreamdError;
    expect(reported).toBeInstanceOf(StreamdError);
    expect(reported).not.toBeInstanceOf(StreamdPollingError);
    expect((reported.cause as Error).message).toMatch(/"amount" of 1:0/);
    expect(poller.status.consecutiveErrors).toBe(0);
    expect(poller.cursor).toBe("2");
    expect(poller.active).toBe(true);
    poller.stop();
  });

  it("rejects an invalid decode option at construction", () => {
    expect(
      () => new EventPoller(createMockAdapter([]), descriptor, vi.fn(), { decode: [""] }),
    ).toThrow(StreamdError);
  });
});

describe("EventPoller time start", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // Block n holds versions 10n..10n+9 and is stamped n * 1000.
  function timedAdapter(pages: EventPage[] = []) {
    const adapter = createMockAdapter(pages);
    adapter.getBlockHeights = vi.fn(async () => ({ oldest: 0n, newest: 99n }));
    adapter.getBlock = vi.fn(async (_d: EventDescriptor, height: bigint) => ({
      height,
      firstVersion: height * 10n,
      lastVersion: height * 10n + 9n,
      timestamp: height * 1000n,
    }));
    return adapter;
  }

  const froms = (adapter: EventAdapter) =>
    mocked(adapter.fetchEvents).mock.calls.map((c) => c[1].fromVersion);

  it("starts at the first block stamped at or after fromTime, searching once", async () => {
    const adapter = timedAdapter([page([makeEvent(420)], 421, true)]);
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      fromTime: 41_500n,
      pollInterval: 100,
    });
    poller.start();
    await advanceTimersByTimeAsync(250);
    poller.stop();

    expect(froms(adapter)[0]).toBe(420n);
    expect(froms(adapter).slice(1).every((v) => v === undefined)).toBe(true);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBeGreaterThan(1);
    expect(adapter.getBlockHeights).toHaveBeenCalledTimes(1);
  });

  it("does not search when a cursor is given", async () => {
    const adapter = timedAdapter();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { fromTime: 1n, cursor: "7" });
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();

    expect(adapter.getBlockHeights).not.toHaveBeenCalled();
    expect(mocked(adapter.fetchEvents).mock.calls[0]![1].cursor).toBe("7");
  });

  it("throws from the constructor on a conflicting or malformed fromTime", () => {
    const adapter = timedAdapter();
    for (const options of [
      { fromTime: 1n, fromVersion: 5n },
      { fromTime: 1n, fromVersion: "earliest" },
      { fromTime: -1n },
    ] as ListenerOptions[]) {
      expect(() => new EventPoller(adapter, descriptor, vi.fn(), options)).toThrow(StreamdError);
    }
  });

  it("stops as invalid input when the adapter cannot search blocks", async () => {
    const adapter = createMockAdapter([]);
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), { fromTime: 1n, onError });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(poller.active).toBe(false);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    const err = onError.mock.calls[0]![0] as StreamdPollingError;
    expect(err).toBeInstanceOf(StreamdPollingError);
    expect(err.message).toContain("getBlockHeights");
  });

  it("restarts the search on a later poll after a network failure partway through", async () => {
    const adapter = timedAdapter();
    const lookup = adapter.getBlock!;
    let failed = false;
    adapter.getBlock = vi.fn(async (d: EventDescriptor, height: bigint) => {
      if (height !== 0n && height !== 99n && !failed) {
        failed = true;
        throw new StreamdNetworkError("429 Too Many Requests", 429);
      }
      return lookup(d, height);
    });
    const onError = vi.fn();
    const poller = new EventPoller(adapter, descriptor, vi.fn(), {
      fromTime: 41_500n,
      pollInterval: 100,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(poller.active).toBe(true);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();

    await advanceTimersByTimeAsync(200);
    poller.stop();
    expect(adapter.getBlockHeights).toHaveBeenCalledTimes(2);
    expect(froms(adapter)[0]).toBe(420n);
  });

  it("abandons a search once the subscription stops", async () => {
    const adapter = timedAdapter();
    const lookup = adapter.getBlock!;
    let poller: EventPoller | undefined;
    adapter.getBlock = vi.fn(async (d: EventDescriptor, height: bigint) => {
      if (height === 99n) poller!.stop();
      return lookup(d, height);
    });
    poller = new EventPoller(adapter, descriptor, vi.fn(), { fromTime: 41_500n });
    poller.start();
    await advanceTimersByTimeAsync(0);

    expect(mocked(adapter.getBlock!).mock.calls.map((c) => c[1])).toEqual([0n, 99n]);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
  });
});

describe("EventPoller page delivery", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const versions = (events: StreamdEvent[]) => events.map((e) => Number(e.version));

  it("hands over every finished page, empty ones included, with the page itself", async () => {
    const pages: EventPage[] = [
      { ...page([makeEvent(1), makeEvent(2)], 2, true), throughVersion: 1n },
      { ...page([], 3, false), throughVersion: 5n },
    ];
    const adapter = createMockAdapter(pages);
    const seen: [number[], bigint | undefined, string | undefined][] = [];
    const poller = new EventPoller(
      adapter,
      descriptor,
      { page: (es, p) => void seen.push([versions(es), p.throughVersion, p.nextCursor]) },
      { pollInterval: 1000, where: (e) => e.version !== 2n },
    );
    poller.start();
    await advanceTimersByTimeAsync(10);
    poller.stop();

    expect(seen).toEqual([
      [[1], 1n, "2"],
      [[], 5n, "3"],
    ]);
    expect(poller.cursor).toBe("3");
  });

  it("hands over nothing from a failed page, and the retry delivers its events", async () => {
    const events = [makeEvent(1), makeEvent(2)];
    let calls = 0;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => page(events, 2)),
      view: vi.fn(async () => {
        calls++;
        if (calls === 2) throw new StreamdNetworkError("view down");
        return [];
      }),
    };
    const handed: number[][] = [];
    const errors: Error[] = [];
    const poller = new EventPoller(
      adapter,
      descriptor,
      { page: (es) => void handed.push(versions(es)) },
      {
        pollInterval: 1000,
        withState: () => ({ function: "0x1::m::f", typeArguments: [], functionArguments: [] }),
        onError: (e) => void errors.push(e),
      },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(handed).toEqual([]);
    expect(errors).toHaveLength(1);
    expect(poller.cursor).toBeUndefined();

    await advanceTimersByTimeAsync(2000);
    poller.stop();
    expect(handed[0]).toEqual([1, 2]);
  });

  it("reports a throwing page callback without failing the poll", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 1)]);
    const errors: Error[] = [];
    const poller = new EventPoller(
      adapter,
      descriptor,
      {
        page: () => {
          throw new Error("boom");
        },
      },
      { pollInterval: 1000, onError: (e) => void errors.push(e) },
    );
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();

    expect(errors.map((e) => e.message)).toEqual(["Page callback threw"]);
    expect(poller.cursor).toBe("1");
    expect(poller.status.consecutiveErrors).toBe(0);
  });
});

describe("EventPoller start version", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("delivers nothing below fromVersion on any page", async () => {
    const seen: bigint[] = [];
    const adapter = createMockAdapter([
      page([makeEvent(1), makeEvent(2)], 1, true),
      page([makeEvent(3), makeEvent(4)], 2, true),
      page([makeEvent(5)], 3, false),
    ]);
    const poller = new EventPoller(adapter, descriptor, (e: StreamdEvent) => void seen.push(e.version), {
      fromVersion: 4n,
      pollInterval: 100,
    });
    poller.start();
    for (let i = 0; i < 6; i++) await advanceTimersByTimeAsync(100);
    poller.stop();
    expect(seen).toEqual([4n, 5n]);
  });

  it("lets a cursor win over a later start", async () => {
    const seen: bigint[] = [];
    const adapter = createMockAdapter([page([makeEvent(1), makeEvent(2)], 1, false)]);
    const poller = new EventPoller(adapter, descriptor, (e: StreamdEvent) => void seen.push(e.version), {
      cursor: "0",
      fromVersion: 100n,
      pollInterval: 100,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    poller.stop();
    expect(seen).toEqual([1n, 2n]);
  });

  it("bounds every page by a start it had to resolve", async () => {
    const seen: bigint[] = [];
    const adapter: EventAdapter = {
      ...createMockAdapter([page([makeEvent(1)], 1, true), page([makeEvent(90), makeEvent(95)], 2, false)]),
      getHeadVersion: async () => 100n,
    };
    const poller = new EventPoller(adapter, descriptor, (e: StreamdEvent) => void seen.push(e.version), {
      fromVersion: -10n,
      pollInterval: 100,
    });
    poller.start();
    for (let i = 0; i < 4; i++) await advanceTimersByTimeAsync(100);
    poller.stop();
    expect(seen).toEqual([90n, 95n]);
  });

  it("exposes no cursor until the walk reaches its start", async () => {
    const adapter = createMockAdapter([
      page([makeEvent(1)], 1, true),
      page([makeEvent(2)], 2, true),
      page([makeEvent(9)], 3, false),
    ]);
    const poller = new EventPoller(adapter, descriptor, () => {}, { fromVersion: 5n, pollInterval: 100 });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.cursor).toBeUndefined();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(100);
    expect(poller.cursor).toBeDefined();
    poller.stop();
  });

  it("counts throughVersion as reaching the start", async () => {
    const empty: EventPage = { events: [], nextCursor: "7", hasMore: false, throughVersion: 6n };
    const poller = new EventPoller(createMockAdapter([empty]), descriptor, () => {}, { fromVersion: 5n, pollInterval: 100 });
    poller.start();
    await advanceTimersByTimeAsync(0);
    expect(poller.cursor).toBe("7");
    poller.stop();
  });
});

describe("EventPoller maxPollInterval", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  /** `script(n)` answers the nth fetch; gaps are the waits between fetches. */
  function scripted(script: (n: number) => EventPage | Error) {
    const at: number[] = [];
    const fetchEvents = vi.fn(async (): Promise<EventPage> => {
      at.push(Date.now());
      const answer = script(at.length);
      if (answer instanceof Error) throw answer;
      return answer;
    });
    return { fetchEvents, gaps: () => at.slice(1).map((t, i) => t - at[i]!) };
  }
  const empty = (n: number) => page([], n);
  const start = (a: { fetchEvents: EventAdapter["fetchEvents"] }, options: ListenerOptions) => {
    const poller = new EventPoller(a, descriptor, vi.fn(), { maxErrors: 100, ...options });
    poller.start();
    return poller;
  };

  it("doubles the wait over empty pages up to the cap", async () => {
    const a = scripted(empty);
    const poller = start(a, { pollInterval: 100, maxPollInterval: 500 });
    await advanceTimersByTimeAsync(2500);
    poller.stop();
    expect(a.gaps()).toEqual([100, 200, 400, 500, 500, 500]);
  });

  it("returns to pollInterval when an event arrives", async () => {
    const a = scripted((n) => (n === 4 ? page([makeEvent(n)], n) : empty(n)));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800 });
    await advanceTimersByTimeAsync(1500);
    poller.stop();
    expect(a.gaps()).toEqual([100, 200, 400, 100, 100, 200, 400]);
  });

  it("counts an event where filters out as activity", async () => {
    const a = scripted((n) => page([makeEvent(n)], n));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800, where: () => false });
    await advanceTimersByTimeAsync(350);
    poller.stop();
    expect(a.gaps()).toEqual([100, 100, 100]);
  });

  it("counts events below the start as activity", async () => {
    const a = scripted((n) => page([makeEvent(n)], n));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800, fromVersion: 1000n });
    await advanceTimersByTimeAsync(350);
    poller.stop();
    expect(a.gaps()).toEqual([100, 100, 100]);
  });

  it("polls at once while the source has more, without stretching", async () => {
    const a = scripted((n) => page([], n, n <= 3));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800 });
    await advanceTimersByTimeAsync(350);
    poller.stop();
    // The fake clock steps 1ms at a time, so a zero wait reads as 1.
    expect(a.gaps()).toEqual([1, 1, 1, 100, 200]);
  });

  it("resets on a page with events that has more", async () => {
    const a = scripted((n) => (n === 3 ? page([makeEvent(n)], n, true) : empty(n)));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800 });
    await advanceTimersByTimeAsync(700);
    poller.stop();
    expect(a.gaps()).toEqual([100, 200, 1, 100, 200]);
  });

  it("keeps errors on their own backoff and the quiet delay where it was", async () => {
    const a = scripted((n) => (n === 3 ? new Error("network") : empty(n)));
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800 });
    await advanceTimersByTimeAsync(900);
    poller.stop();
    expect(a.gaps()).toEqual([100, 200, 200, 400]);
  });

  it("starts over after a restart, and carries on after a resume", async () => {
    const a = scripted(empty);
    const poller = start(a, { pollInterval: 100, maxPollInterval: 800 });
    await advanceTimersByTimeAsync(350);
    poller.pause();
    poller.resume();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(400);
    poller.stop();
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(150);
    poller.stop();
    const gaps = a.gaps();
    expect(gaps.slice(0, 2)).toEqual([100, 200]);
    expect(gaps[3]).toBeGreaterThanOrEqual(400);
    expect(gaps[3]).toBeLessThan(410);
    expect(gaps.at(-1)).toBe(100);
  });

  it("changes nothing when unset", async () => {
    const a = scripted(empty);
    const poller = start(a, { pollInterval: 100 });
    await advanceTimersByTimeAsync(350);
    poller.stop();
    expect(a.gaps()).toEqual([100, 100, 100]);
  });

  it("never stretches at or below pollInterval", async () => {
    for (const maxPollInterval of [100, 10, 0]) {
      const a = scripted(empty);
      const poller = start(a, { pollInterval: 100, maxPollInterval });
      await advanceTimersByTimeAsync(350);
      poller.stop();
      expect(a.gaps()).toEqual([100, 100, 100]);
    }
  });

  it("stretches from a pollInterval of zero", async () => {
    const a = scripted(empty);
    const poller = start(a, { pollInterval: 0, maxPollInterval: 4 });
    await advanceTimersByTimeAsync(12);
    poller.stop();
    expect(a.gaps().slice(1, 5)).toEqual([1, 2, 4, 4]);
  });

  it("rejects an invalid value", () => {
    for (const bad of [-1, Number.NaN, Infinity, 2_147_483_648, "5"]) {
      expect(() =>
        new EventPoller({ fetchEvents: vi.fn() }, descriptor, vi.fn(), { maxPollInterval: bad as number }),
      ).toThrow(/maxPollInterval/);
    }
  });
});
