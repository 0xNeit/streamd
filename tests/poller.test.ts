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
