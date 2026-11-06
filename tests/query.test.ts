import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { EventQuery } from "../src/query.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { advanceTimersByTimeAsync, advanceTimersToNextTimerAsync } from "./timers.js";
import type { EventAdapter, EventDescriptor, EventFilter, FetchOptions } from "../src/types.js";
import { MemoryAdapter } from "../src/adapters/memory.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::test::Event" };

function event(version: number) {
  return { type: "0x1::test::Event", data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

describe("EventQuery paging", () => {
  it("stops when the adapter returns the cursor it was given", async () => {
    const fetchEvents = vi.fn(async () => {
      // Fail fast rather than hang if the guard is missing.
      if (fetchEvents.mock.calls.length > 10) throw new Error("walk did not stop");
      return { events: [], nextCursor: "x", hasMore: true };
    });
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor);

    expect(events).toEqual([]);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
  });

  it("seeds only the first request from fromVersion and offset", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce({ events: [event(5)], nextCursor: "6", hasMore: true })
      .mockResolvedValueOnce({ events: [event(7)], nextCursor: "8", hasMore: false });
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor, {
      fromVersion: 5n,
      offset: 2,
    });

    expect(events.map((e) => e.version)).toEqual([5n, 7n]);
    const [first, second] = fetchEvents.mock.calls.map((c) => c[1] as FetchOptions);
    expect(first).toMatchObject({ fromVersion: 5n, offset: 2, cursor: undefined });
    expect(second).toMatchObject({ cursor: "6" });
    expect(second?.fromVersion).toBeUndefined();
    expect(second?.offset).toBeUndefined();
  });

  it("rejects a pre-aborted getEvents with the signal reason without fetching", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    const reason = new Error("already aborted");
    const promise = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      signal: AbortSignal.abort(reason),
    });

    await expect(promise).rejects.toBe(reason);
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects during a pending fetch before that fetch resolves, and never passes signal to the adapter", async () => {
    const controller = new AbortController();
    const reason = new Error("abort in flight");
    let resolvePage!: () => void;
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(
      () =>
        new Promise((resolve) => {
          resolvePage = () => resolve({ events: [event(1)], nextCursor: "2", hasMore: false });
        }),
    );

    const promise = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      fromVersion: 1n,
      signal: controller.signal,
    });
    expect(fetchEvents).toHaveBeenCalledTimes(1);
    const passedOptions = fetchEvents.mock.calls[0]![1] as Record<string, unknown>;
    expect("signal" in passedOptions).toBe(false);

    controller.abort(reason);
    await expect(promise).rejects.toBe(reason);

    // Resolving the abandoned fetch afterwards is a no-op.
    resolvePage();
  });

  it("stops fetching further pages when aborted between getEventPages pages, and detaches per page", async () => {
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const reason = new Error("stop paging");
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce({ events: [event(1)], nextCursor: "2", hasMore: true })
      .mockResolvedValueOnce({ events: [event(2)], nextCursor: "3", hasMore: false });

    const it = new EventQuery({ fetchEvents }).getEventPages(descriptor, {
      signal: controller.signal,
    });

    const first = await it.next();
    expect(first.value?.map((e) => e.version)).toEqual([1n]);
    expect(remove).toHaveBeenCalledTimes(1);

    controller.abort(reason);
    await expect(it.next()).rejects.toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });
});

describe("EventQuery relative start", () => {
  function adapter(head: bigint) {
    return {
      fetchEvents: vi.fn<EventAdapter["fetchEvents"]>(async () => ({
        events: [],
        nextCursor: "1",
        hasMore: false,
      })),
      getHeadVersion: vi.fn(async () => head),
    };
  }

  it("seeds the first request from the head, clamped at 0", async () => {
    for (const [head, from, expected] of [[10n, -2n, 8n], [3n, -50n, 0n]] as const) {
      const source = adapter(head);
      await new EventQuery(source).getEvents(descriptor, { fromVersion: from });

      expect(source.getHeadVersion).toHaveBeenCalledWith(descriptor);
      expect(source.fetchEvents.mock.calls[0]![1].fromVersion).toBe(expected);
    }
  });

  it("reads no head for a non-negative fromVersion", async () => {
    const source = adapter(10n);
    await new EventQuery(source).getEvents(descriptor, { fromVersion: 4n });

    expect(source.getHeadVersion).not.toHaveBeenCalled();
    expect(source.fetchEvents.mock.calls[0]![1].fromVersion).toBe(4n);
  });

  it("rejects when the adapter cannot report a head", async () => {
    const { fetchEvents } = adapter(10n);
    await expect(
      new EventQuery({ fetchEvents }).getEvents(descriptor, { fromVersion: -2n }),
    ).rejects.toThrow("getHeadVersion");
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects with the signal's reason when aborted during the head read", async () => {
    const source = adapter(10n);
    const controller = new AbortController();
    source.getHeadVersion.mockImplementation(() => new Promise(() => {}));
    const pending = new EventQuery(source).getEvents(descriptor, {
      fromVersion: -2n,
      signal: controller.signal,
    });
    const reason = new Error("stop");
    controller.abort(reason);

    await expect(pending).rejects.toBe(reason);
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });
});

describe("EventQuery earliest start", () => {
  function adapter(earliest: bigint) {
    return {
      fetchEvents: vi.fn<EventAdapter["fetchEvents"]>(async () => ({
        events: [],
        nextCursor: "1",
        hasMore: false,
      })),
      getEarliestVersion: vi.fn(async () => earliest),
    };
  }

  it("seeds the first request from the adapter's earliest version", async () => {
    const source = adapter(5n);
    await new EventQuery(source).getEvents(descriptor, { fromVersion: "earliest" });

    expect(source.getEarliestVersion).toHaveBeenCalledWith(descriptor);
    expect(source.fetchEvents.mock.calls[0]![1].fromVersion).toBe(5n);
  });

  it("rejects when the adapter cannot report an earliest version", async () => {
    const { fetchEvents } = adapter(5n);
    await expect(
      new EventQuery({ fetchEvents }).getEvents(descriptor, { fromVersion: "earliest" }),
    ).rejects.toThrow("getEarliestVersion");
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects a fromVersion that is neither a bigint nor earliest", async () => {
    const source = adapter(5n);
    const err = await new EventQuery(source)
      .getEvents(descriptor, { fromVersion: "latest" as unknown as bigint })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain("Invalid fromVersion");
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects with the signal's reason when aborted during the read", async () => {
    const source = adapter(5n);
    const controller = new AbortController();
    source.getEarliestVersion.mockImplementation(() => new Promise(() => {}));
    const pending = new EventQuery(source).getEvents(descriptor, {
      fromVersion: "earliest",
      signal: controller.signal,
    });
    const reason = new Error("stop");
    controller.abort(reason);

    await expect(pending).rejects.toBe(reason);
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });
});

describe("EventQuery decode", () => {
  function amountEvent(version: number, amount: unknown) {
    return { ...event(version), data: { amount } };
  }

  it("returns decoded data, filters on it, and never passes decode to the adapter", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValue({
        events: [amountEvent(1, "2"), amountEvent(2, "340282366920938463463374607431768211455")],
        hasMore: false,
      });
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor, {
      decode: ["amount"],
      where: (e) => (e.data as { amount: bigint }).amount > 2n,
    });

    expect(events.map((e) => e.data)).toEqual([
      { amount: 340282366920938463463374607431768211455n },
    ]);
    expect(fetchEvents.mock.calls[0]![1]).not.toHaveProperty("decode");
  });

  it("rejects when a named field cannot be decoded", async () => {
    const fetchEvents = vi.fn(async () => ({ events: [amountEvent(1, 5)], hasMore: false }));
    await expect(
      new EventQuery({ fetchEvents }).getEvents(descriptor, { decode: ["amount"] }),
    ).rejects.toThrow(/"amount" of 1:0/);
  });
});

describe("EventQuery orderBy desc", () => {
  const at = (version: number, eventIndex = 0) => ({ ...event(version), eventIndex });

  // Three ascending pages: versions 1-2, 3-4 (two events in 4), 5-6.
  function threePages() {
    return vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce({ events: [at(1), at(2)], nextCursor: "3", hasMore: true })
      .mockResolvedValueOnce({ events: [at(3), at(4, 0), at(4, 1)], nextCursor: "5", hasMore: true })
      .mockResolvedValueOnce({ events: [at(5), at(6)], nextCursor: "7", hasMore: false });
  }
  const ids = (events: { version: bigint; eventIndex: number }[]) =>
    events.map((e) => `${e.version}:${e.eventIndex}`);

  it("returns the last N across pages, newest first, asking the adapter for asc", async () => {
    const fetchEvents = threePages();
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor, {
      orderBy: "desc",
      limit: 4,
    });

    expect(ids(events)).toEqual(["6:0", "5:0", "4:1", "4:0"]);
    expect(fetchEvents).toHaveBeenCalledTimes(3);
    for (const [, options] of fetchEvents.mock.calls) {
      expect(options.orderBy).toBe("asc");
      expect(options.limit).toBe(25);
    }
  });

  it("counts limit after where", async () => {
    const events = await new EventQuery({ fetchEvents: threePages() }).getEvents(descriptor, {
      orderBy: "desc",
      limit: 2,
      where: (e) => e.version % 2n === 1n,
    });
    expect(ids(events)).toEqual(["5:0", "3:0"]);
  });

  it("returns the whole range reversed without a limit", async () => {
    const events = await new EventQuery({ fetchEvents: threePages() }).getEvents(descriptor, {
      orderBy: "desc",
    });
    expect(ids(events)).toEqual(["6:0", "5:0", "4:1", "4:0", "3:0", "2:0", "1:0"]);
  });

  it("makes no fetch for limit 0", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor, {
      orderBy: "desc",
      limit: 0,
    });
    expect(events).toEqual([]);
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("caps the walk with maxPages and returns the latest of what was walked", async () => {
    const fetchEvents = threePages();
    const events = await new EventQuery({ fetchEvents }).getEvents(descriptor, {
      orderBy: "desc",
      limit: 2,
      maxPages: 2,
    });
    expect(ids(events)).toEqual(["4:1", "4:0"]);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
  });

  it("yields getEventPages in pageSize chunks, newest first", async () => {
    const query = new EventQuery({ fetchEvents: threePages() });
    const pages: string[][] = [];
    for await (const page of query.getEventPages(descriptor, { orderBy: "desc", limit: 5 }, 2)) {
      pages.push(ids(page));
    }
    expect(pages).toEqual([["6:0", "5:0"], ["4:1", "4:0"], ["3:0"]]);
  });

  it("rejects an unknown orderBy before fetching", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    const promise = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      orderBy: "DESC" as "desc",
    });
    await expect(promise).rejects.toBeInstanceOf(StreamdError);
    await expect(promise).rejects.toThrow('orderBy must be "asc" or "desc"');
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("leaves ascending unchanged", async () => {
    const events = await new EventQuery({ fetchEvents: threePages() }).getEvents(descriptor, {
      orderBy: "asc",
      limit: 3,
    });
    expect(ids(events)).toEqual(["1:0", "2:0", "3:0"]);
  });
});

describe("EventQuery retries", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const down = () => new StreamdNetworkError("node down", 503);
  const page = (versions: number[], next: string, hasMore: boolean) => ({
    events: versions.map(event),
    nextCursor: next,
    hasMore,
  });

  // Settles `promise` while stepping the fake clock through each backoff wait.
  async function drain<T>(promise: Promise<T>): Promise<T> {
    let done = false;
    promise.then(
      () => (done = true),
      () => (done = true),
    );
    for (let i = 0; i < 20 && !done; i++) await advanceTimersToNextTimerAsync();
    return promise;
  }

  it("retries a network failure from the same cursor, waiting 1s then 2s", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce(page([1], "2", true))
      .mockRejectedValueOnce(down())
      .mockRejectedValueOnce(new Error("socket hang up"))
      .mockResolvedValueOnce(page([2, 3], "4", false));
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, { retries: 2 });
    // The helper adds a tick per call, so these windows straddle 1000ms and 2000ms.
    await advanceTimersByTimeAsync(990);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
    await advanceTimersByTimeAsync(20);
    expect(fetchEvents).toHaveBeenCalledTimes(3);
    await advanceTimersByTimeAsync(1970);
    expect(fetchEvents).toHaveBeenCalledTimes(3);
    await advanceTimersByTimeAsync(20);

    expect((await result).map((e) => e.version)).toEqual([1n, 2n, 3n]);
    expect(fetchEvents.mock.calls.slice(1).map((c) => c[1].cursor)).toEqual(["2", "2", "2"]);
    expect("retries" in fetchEvents.mock.calls[0]![1]).toBe(false);
  });

  it("rejects with the last error once retries are spent", async () => {
    const last = down();
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValueOnce(down())
      .mockRejectedValueOnce(last);
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, { retries: 1 });

    await expect(drain(result)).rejects.toBe(last);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
  });

  it("gives each page its own budget", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(page([1], "2", true))
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(page([2], "3", false));
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, { retries: 1 });

    expect((await drain(result)).map((e) => e.version)).toEqual([1n, 2n]);
  });

  it("does not retry a StreamdError that is not a network failure", async () => {
    const invalid = new StreamdError("bad cursor");
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockRejectedValue(invalid);
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, { retries: 3 });

    await expect(drain(result)).rejects.toBe(invalid);
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("does not retry when retries is omitted", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockRejectedValue(down());

    await expect(drain(new EventQuery({ fetchEvents }).getEvents(descriptor))).rejects.toThrow(
      "node down",
    );
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("retries a failed head read for a relative start", async () => {
    const getHeadVersion = vi.fn<() => Promise<bigint>>()
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(10n);
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockResolvedValue(page([], "11", false));
    const result = new EventQuery({ fetchEvents, getHeadVersion }).getEvents(descriptor, {
      fromVersion: -2n,
      retries: 1,
    });

    await drain(result);
    expect(getHeadVersion).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[0]![1].fromVersion).toBe(8n);
  });

  it("retries a failed earliest read", async () => {
    const getEarliestVersion = vi.fn<() => Promise<bigint>>()
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(5n);
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockResolvedValue(page([], "6", false));
    const result = new EventQuery({ fetchEvents, getEarliestVersion }).getEvents(descriptor, {
      fromVersion: "earliest",
      retries: 1,
    });

    await drain(result);
    expect(getEarliestVersion).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[0]![1].fromVersion).toBe(5n);
  });

  it("retries a failed middle page of a descending walk", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce(page([1, 2], "3", true))
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(page([3, 4], "5", false));
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      orderBy: "desc",
      limit: 3,
      retries: 1,
    });

    expect((await drain(result)).map((e) => e.version)).toEqual([4n, 3n, 2n]);
    expect(fetchEvents.mock.calls[2]![1].cursor).toBe("3");
  });

  it("rejects with the signal reason when aborted during the wait, with no further call", async () => {
    const controller = new AbortController();
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockRejectedValue(down());
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      retries: 5,
      signal: controller.signal,
    });
    await advanceTimersByTimeAsync(500);
    const reason = new Error("stop");
    controller.abort(reason);

    await expect(result).rejects.toBe(reason);
    expect(vi.getTimerCount()).toBe(0);
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("rejects an invalid retries value without fetching", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    for (const retries of [-1, 1.5, NaN, Infinity]) {
      await expect(
        new EventQuery({ fetchEvents }).getEvents(descriptor, { retries }),
      ).rejects.toBeInstanceOf(StreamdError);
    }
    expect(fetchEvents).not.toHaveBeenCalled();
  });
});

describe("EventQuery iterateEvents", () => {
  const typed = (version: number, amount: number) => ({
    ...event(version),
    data: { amount: String(amount) },
  });
  const memory = () =>
    new MemoryAdapter(Array.from({ length: 12 }, (_, i) => typed(i + 1, (i + 1) * 10)));

  async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
    const out: T[] = [];
    for await (const item of iterable) out.push(item);
    return out;
  }

  it("yields the events of getEventPages one at a time, across options", async () => {
    const query = new EventQuery(memory());
    const filters: EventFilter[] = [
      {},
      { where: (e) => e.version % 2n === 0n, limit: 4 },
      { orderBy: "desc", limit: 5 },
      { fromVersion: -6n, decode: ["amount"], where: (e) => (e.data as { amount: bigint }).amount > 80n },
      { maxPages: 2 },
    ];
    for (const filter of filters) {
      const single = await collect(query.iterateEvents(descriptor, filter, 3));
      const pages = await collect(query.getEventPages(descriptor, filter, 3));
      expect(single.length).toBeGreaterThan(0);
      expect(single).toEqual(pages.flat());
      if (filter.maxPages === undefined) {
        expect(single).toEqual(await query.getEvents(descriptor, filter));
      }
    }
    const first = await query.iterateEvents(descriptor, {}, 3).next();
    expect(first.value?.version).toBe(1n);
  });

  it("fetches nothing further after the loop breaks", async () => {
    const source = memory();
    const fetchEvents = vi.spyOn(source, "fetchEvents");
    for await (const e of new EventQuery(source).iterateEvents(descriptor, {}, 3)) {
      expect(e.version).toBe(1n);
      break;
    }
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("rejects the next event with the reason when aborted mid-page", async () => {
    const source = memory();
    const fetchEvents = vi.spyOn(source, "fetchEvents");
    const controller = new AbortController();
    const reason = new Error("stop");
    const it = new EventQuery(source).iterateEvents(descriptor, { signal: controller.signal }, 3);

    expect((await it.next()).value?.version).toBe(1n);
    controller.abort(reason);
    await expect(it.next()).rejects.toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("rejects invalid input as getEventPages does", async () => {
    const it = new EventQuery(memory()).iterateEvents(descriptor, {
      orderBy: "sideways" as "asc",
    });
    await expect(it.next()).rejects.toBeInstanceOf(StreamdError);
  });

  it("retries a network failure, then keeps yielding", async () => {
    vi.useFakeTimers();
    try {
      const fetchEvents = vi
        .fn<EventAdapter["fetchEvents"]>()
        .mockRejectedValueOnce(new StreamdNetworkError("node down", 503))
        .mockResolvedValueOnce({ events: [event(1), event(2)], nextCursor: "3", hasMore: false });
      const it = new EventQuery({ fetchEvents }).iterateEvents(descriptor, { retries: 1 });
      const next = it.next();
      await advanceTimersByTimeAsync(1010);

      expect((await next).value?.version).toBe(1n);
      expect((await it.next()).value?.version).toBe(2n);
      expect((await it.next()).done).toBe(true);
      expect(fetchEvents).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});
