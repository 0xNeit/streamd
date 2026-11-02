import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import {
  advanceTimersByTimeAsync,
  advanceTimersToNextTimerAsync,
  mocked,
} from "./timers.js";
import { EventPoller } from "../src/poller.js";
import { StreamdError, StreamdPollingError } from "../src/errors.js";
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

  it("keeps polling until maxErrors when onError always throws", async () => {
    const fetchEvents = vi.fn(async () => {
      throw new Error("network down");
    });
    const onError = vi.fn(() => {
      throw new Error("handler bug");
    });
    const poller = new EventPoller({ fetchEvents }, descriptor, vi.fn(), {
      pollInterval: 100,
      maxErrors: 3,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(700);

    expect(fetchEvents).toHaveBeenCalledTimes(3);
    expect(onError).toHaveBeenCalledTimes(3);
    expect(poller.active).toBe(false);
  });

  it("does not count a throwing callback and onError as a polling failure", async () => {
    const adapter = createMockAdapter([page([makeEvent(1)], 2)]);
    const onError = vi.fn((_: Error) => {
      throw new Error("handler bug");
    });
    const callback = vi.fn(() => {
      throw new Error("callback bug");
    });
    const poller = new EventPoller(adapter, descriptor, callback, {
      pollInterval: 100,
      onError,
    });
    poller.start();
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![0]).not.toBeInstanceOf(StreamdPollingError);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBeGreaterThanOrEqual(3);

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
