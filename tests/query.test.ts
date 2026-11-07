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

describe("EventQuery head snapshot", () => {
  // A chain whose head moves one version past every fetch, with one event per
  // version, so an unbounded walk would never run out of pages.
  function busy(head = 3n) {
    const source = {
      head,
      fetchEvents: vi.fn<EventAdapter["fetchEvents"]>(async (_d, options = {}) => {
        source.head++;
        const at = options.cursor !== undefined ? BigInt(options.cursor) : (options.fromVersion ?? 0n);
        if (options.toVersion !== undefined && at > options.toVersion) {
          return { events: [], nextCursor: String(at), hasMore: false };
        }
        return { events: [event(Number(at))], nextCursor: String(at + 1n), hasMore: true };
      }),
      getHeadVersion: vi.fn(async () => source.head),
    };
    return source;
  }

  it("ends at the head read once before the first page", async () => {
    const source = busy(3n);
    const events = await new EventQuery(source).getEvents(descriptor, { toVersion: "head" });

    expect(events.map((e) => e.version)).toEqual([0n, 1n, 2n, 3n]);
    expect(source.getHeadVersion).toHaveBeenCalledTimes(1);
    expect(source.getHeadVersion).toHaveBeenCalledWith(descriptor);
    for (const [, options] of source.fetchEvents.mock.calls) expect(options?.toVersion).toBe(3n);
  });

  it("reuses the one head read for a negative fromVersion", async () => {
    const source = busy(10n);
    const events = await new EventQuery(source).getEvents(descriptor, {
      fromVersion: -2n,
      toVersion: "head",
    });

    expect(events.map((e) => e.version)).toEqual([8n, 9n, 10n]);
    expect(source.getHeadVersion).toHaveBeenCalledTimes(1);
  });

  it("keeps a negative fromVersion relative to the head under a fixed toVersion", async () => {
    const source = busy(10n);
    await new EventQuery(source).getEvents(descriptor, { fromVersion: -2n, toVersion: 9n });

    expect(source.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 8n, toVersion: 9n });
  });

  it("bounds a descending walk to the snapshot", async () => {
    const source = busy(5n);
    const events = await new EventQuery(source).getEvents(descriptor, {
      toVersion: "head",
      orderBy: "desc",
      limit: 2,
    });

    expect(events.map((e) => e.version)).toEqual([5n, 4n]);
  });

  it("rejects without fetching when the adapter cannot report a head", async () => {
    const { fetchEvents } = busy();
    await expect(
      new EventQuery({ fetchEvents }).getEvents(descriptor, { toVersion: "head" }),
    ).rejects.toThrow('toVersion "head" needs an adapter that implements getHeadVersion');
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("reads no head when no page is fetched", async () => {
    for (const filter of [{ limit: 0 }, { maxPages: 0 }, { limit: 0, orderBy: "desc" as const }]) {
      const source = busy();
      await new EventQuery(source).getEvents(descriptor, { ...filter, toVersion: "head" });
      expect(source.getHeadVersion).not.toHaveBeenCalled();
    }
  });

  it.each([
    [{ toVersion: "head" as const, toBlock: 1n }, "toBlock or toVersion"],
    [{ toVersion: "latest" as unknown as bigint }, "Invalid toVersion"],
  ])("rejects %p as invalid input", async (filter, message) => {
    const source = busy();
    await expect(new EventQuery(source).getEvents(descriptor, filter)).rejects.toThrow(message);
    expect(source.getHeadVersion).not.toHaveBeenCalled();
  });

  it("reports no resume cursor, like any version bound", async () => {
    const reported: (string | undefined)[] = [];
    await new EventQuery(busy()).getEvents(descriptor, {
      toVersion: "head",
      onCursor: (c) => reported.push(c),
    });
    expect(reported).toEqual([undefined]);
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

describe("EventQuery block range", () => {
  // Block n holds versions 10n..10n+9.
  function adapter() {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => ({
      events: [event(25)],
      nextCursor: "x",
      hasMore: false,
    }));
    const getBlock = vi.fn(async (_d: EventDescriptor, height: bigint) => ({
      height,
      firstVersion: height * 10n,
      lastVersion: height * 10n + 9n,
      timestamp: 0n,
    }));
    return { fetchEvents, getBlock };
  }

  it("turns fromBlock and toBlock into an inclusive version range", async () => {
    const source = adapter();
    await new EventQuery(source).getEvents(descriptor, { fromBlock: 2n, toBlock: 4n });

    expect(source.getBlock.mock.calls.map((c) => c[1])).toEqual([2n, 4n]);
    expect(source.getBlock.mock.calls[0]![0]).toBe(descriptor);
    const options = source.fetchEvents.mock.calls[0]![1];
    expect(options).toMatchObject({ fromVersion: 20n, toVersion: 49n });
    expect("fromBlock" in options || "toBlock" in options).toBe(false);
  });

  it("keeps the other bound when only one block bound is set", async () => {
    const from = adapter();
    await new EventQuery(from).getEvents(descriptor, { fromBlock: 3n, toVersion: 100n });
    expect(from.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 30n, toVersion: 100n });
    expect(from.getBlock).toHaveBeenCalledTimes(1);

    const to = adapter();
    await new EventQuery(to).getEvents(descriptor, { fromVersion: 5n, toBlock: 1n });
    expect(to.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 5n, toVersion: 19n });
  });

  it("looks blocks up once per query, not per page", async () => {
    const source = adapter();
    source.fetchEvents
      .mockResolvedValueOnce({ events: [event(20)], nextCursor: "21", hasMore: true })
      .mockResolvedValueOnce({ events: [event(22)], nextCursor: "23", hasMore: false });
    const events = await new EventQuery(source).getEvents(descriptor, { fromBlock: 2n, toBlock: 2n });

    expect(events.map((e) => e.version)).toEqual([20n, 22n]);
    expect(source.getBlock).toHaveBeenCalledTimes(2);
    expect(source.fetchEvents.mock.calls[1]![1]).toMatchObject({ cursor: "21", toVersion: 29n });
  });

  it("looks nothing up when no fetch happens", async () => {
    const source = adapter();
    await new EventQuery(source).getEvents(descriptor, { fromBlock: 1n, limit: 0 });
    await new EventQuery(source).getEvents(descriptor, { fromBlock: 1n, maxPages: 0 });
    expect(source.getBlock).not.toHaveBeenCalled();
  });

  it.each([
    [{ fromBlock: 1n, fromVersion: 5n }, "fromBlock or fromVersion"],
    [{ fromBlock: 1n, fromVersion: "earliest" as const }, "fromBlock or fromVersion"],
    [{ toBlock: 1n, toVersion: 5n }, "toBlock or toVersion"],
    [{ fromBlock: 3n, toBlock: 2n }, "after toBlock"],
    [{ fromBlock: -1n }, "Invalid fromBlock"],
    [{ toBlock: 2 as unknown as bigint }, "Invalid toBlock"],
  ])("rejects %p before any adapter call", async (filter, message) => {
    const source = adapter();
    const err = await new EventQuery(source)
      .getEvents(descriptor, filter as EventFilter)
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain(message);
    expect(source.getBlock).not.toHaveBeenCalled();
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects when the adapter cannot look blocks up", async () => {
    const { fetchEvents } = adapter();
    const err = await new EventQuery({ fetchEvents })
      .getEvents(descriptor, { toBlock: 1n })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("getBlock");
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("applies the range to a descending query and to iterateEvents", async () => {
    const desc = adapter();
    await new EventQuery(desc).getEvents(descriptor, { fromBlock: 2n, toBlock: 3n, orderBy: "desc" });
    expect(desc.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 20n, toVersion: 39n, orderBy: "asc" });

    const iter = adapter();
    const seen: bigint[] = [];
    for await (const e of new EventQuery(iter).iterateEvents(descriptor, { fromBlock: 2n })) {
      seen.push(e.version);
    }
    expect(seen).toEqual([25n]);
    expect(iter.fetchEvents.mock.calls[0]![1].fromVersion).toBe(20n);
  });

  it("propagates the adapter's error for a block it cannot serve", async () => {
    const source = adapter();
    source.getBlock.mockRejectedValueOnce(new StreamdNetworkError("pruned", 410));
    const err = await new EventQuery(source)
      .getEvents(descriptor, { fromBlock: 1n })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(410);
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

describe("EventQuery fromTime", () => {
  // Block n holds versions 10n..10n+9 and is stamped n * 1000.
  function adapter() {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => ({
      events: [event(25)],
      nextCursor: "x",
      hasMore: false,
    }));
    const getBlockHeights = vi.fn(async (_d: EventDescriptor) => ({ oldest: 0n, newest: 99n }));
    const getBlock = vi.fn(async (_d: EventDescriptor, height: bigint) => ({
      height,
      firstVersion: height * 10n,
      lastVersion: height * 10n + 9n,
      timestamp: height * 1000n,
    }));
    return { fetchEvents, getBlockHeights, getBlock };
  }

  it("starts at the first block stamped at or after the time, keeping toVersion", async () => {
    const source = adapter();
    await new EventQuery(source).getEvents(descriptor, { fromTime: 41_500n, toVersion: 600n });

    const options = source.fetchEvents.mock.calls[0]![1];
    expect(options).toMatchObject({ fromVersion: 420n, toVersion: 600n });
    expect("fromTime" in options).toBe(false);
    expect(source.getBlockHeights.mock.calls[0]![0]).toBe(descriptor);
  });

  it("combines with toBlock", async () => {
    const source = adapter();
    await new EventQuery(source).getEvents(descriptor, { fromTime: 3000n, toBlock: 5n });
    expect(source.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 30n, toVersion: 59n });
  });

  it("searches once per query, not per page, and not when no fetch happens", async () => {
    const source = adapter();
    source.fetchEvents
      .mockResolvedValueOnce({ events: [event(420)], nextCursor: "421", hasMore: true })
      .mockResolvedValueOnce({ events: [event(422)], nextCursor: "423", hasMore: false });
    const events = await new EventQuery(source).getEvents(descriptor, { fromTime: 42_000n });

    expect(events.map((e) => e.version)).toEqual([420n, 422n]);
    expect(source.getBlockHeights).toHaveBeenCalledTimes(1);
    expect(source.fetchEvents.mock.calls[1]![1]).toMatchObject({ cursor: "421", fromVersion: undefined });

    const idle = adapter();
    await new EventQuery(idle).getEvents(descriptor, { fromTime: 1n, limit: 0 });
    await new EventQuery(idle).getEvents(descriptor, { fromTime: 1n, maxPages: 0 });
    expect(idle.getBlockHeights).not.toHaveBeenCalled();
  });

  it.each([
    [{ fromTime: 1n, fromVersion: 5n }, "fromTime or fromVersion"],
    [{ fromTime: 1n, fromBlock: 2n }, "fromTime or fromBlock"],
    [{ fromTime: -1n }, "Invalid fromTime"],
  ])("rejects %p before any adapter call", async (filter, message) => {
    const source = adapter();
    const err = await new EventQuery(source)
      .getEvents(descriptor, filter as EventFilter)
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain(message);
    expect(source.getBlockHeights).not.toHaveBeenCalled();
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects without retrying when the adapter cannot search blocks", async () => {
    const { fetchEvents, getBlock } = adapter();
    const err = await new EventQuery({ fetchEvents, getBlock })
      .getEvents(descriptor, { fromTime: 1n, retries: 3 })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("getBlockHeights");
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("applies to a descending query", async () => {
    const source = adapter();
    await new EventQuery(source).getEvents(descriptor, { fromTime: 2000n, orderBy: "desc" });
    expect(source.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 20n, orderBy: "asc" });
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

  it("retries a failed head read for a head snapshot", async () => {
    const getHeadVersion = vi.fn<() => Promise<bigint>>()
      .mockRejectedValueOnce(down())
      .mockResolvedValueOnce(10n);
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockResolvedValue(page([], "11", false));
    const result = new EventQuery({ fetchEvents, getHeadVersion }).getEvents(descriptor, {
      toVersion: "head",
      retries: 1,
    });

    await drain(result);
    expect(getHeadVersion).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[0]![1].toVersion).toBe(10n);
  });

  it("retries a block lookup that fails partway through a fromTime search", async () => {
    let failed = false;
    const getBlock = vi.fn(async (_d: EventDescriptor, height: bigint) => {
      if (height !== 0n && height !== 99n && !failed) {
        failed = true;
        throw new StreamdNetworkError("429 Too Many Requests", 429);
      }
      return { height, firstVersion: height * 10n, lastVersion: height * 10n + 9n, timestamp: height * 1000n };
    });
    const getBlockHeights = vi.fn(async (_d: EventDescriptor) => ({ oldest: 0n, newest: 99n }));
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockResolvedValue(page([], "x", false));
    const result = new EventQuery({ fetchEvents, getBlock, getBlockHeights }).getEvents(descriptor, {
      fromTime: 41_500n,
      retries: 1,
    });

    await drain(result);
    expect(getBlockHeights).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[0]![1].fromVersion).toBe(420n);
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

  it("retries a failed block lookup", async () => {
    const getBlock = vi.fn<NonNullable<EventAdapter["getBlock"]>>()
      .mockRejectedValueOnce(down())
      .mockResolvedValue({ height: 4n, firstVersion: 40n, lastVersion: 49n, timestamp: 0n });
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>().mockResolvedValue(page([], "50", false));
    const result = new EventQuery({ fetchEvents, getBlock }).getEvents(descriptor, {
      fromBlock: 4n,
      retries: 1,
    });

    await drain(result);
    expect(getBlock).toHaveBeenCalledTimes(2);
    expect(fetchEvents.mock.calls[0]![1].fromVersion).toBe(40n);
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

  it("waits a Retry-After hint instead of the backoff, capped at 60s", async () => {
    const limited = (ms: number) => new StreamdNetworkError("rate limited", 429, undefined, ms);
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValueOnce(limited(300))
      .mockRejectedValueOnce(limited(600_000))
      .mockResolvedValueOnce(page([1], "2", false));
    const start = Date.now();
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, { retries: 2 });
    await advanceTimersByTimeAsync(310);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(1);
    await advanceTimersToNextTimerAsync();

    expect((await result).map((e) => e.version)).toEqual([1n]);
    // 300ms hinted, then the 600s hint held to 60s, against 1s and 2s unhinted.
    expect(Date.now() - start).toBeGreaterThanOrEqual(60_300);
    expect(Date.now() - start).toBeLessThan(61_000);
  });

  it("rejects with the signal reason when aborted during a hinted wait", async () => {
    const controller = new AbortController();
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockRejectedValue(new StreamdNetworkError("rate limited", 429, undefined, 30_000));
    const result = new EventQuery({ fetchEvents }).getEvents(descriptor, {
      retries: 5,
      signal: controller.signal,
    });
    await advanceTimersByTimeAsync(10);
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

describe("EventQuery onCursor", () => {
  // Ten events at versions 1..10; MemoryAdapter cursors are indexes into that list.
  const ten = () => new MemoryAdapter(Array.from({ length: 10 }, (_, i) => event(i + 1)));

  async function endOf(adapter: EventAdapter, filter: EventFilter, pageSize?: number) {
    const reported: (string | undefined)[] = [];
    const pages = new EventQuery(adapter).getEventPages(
      descriptor,
      { ...filter, onCursor: (c) => reported.push(c) },
      pageSize,
    );
    for await (const _ of pages);
    expect(reported).toHaveLength(1);
    return reported[0];
  }

  it("reports the cursor after a full walk", async () => {
    expect(await endOf(ten(), {}, 3)).toBe("10");
  });

  it("reports the cursor after the last page maxPages read", async () => {
    expect(await endOf(ten(), { maxPages: 2 }, 3)).toBe("6");
  });

  it("reports the cursor when limit is met exactly at a page end", async () => {
    expect(await endOf(ten(), { limit: 4 }, 2)).toBe("4");
  });

  it("reports the cursor past events where filtered out", async () => {
    const where = (e: { version: bigint }) => e.version % 2n === 0n;
    expect(await endOf(ten(), { where }, 3)).toBe("10");
    expect(await endOf(ten(), { where, limit: 2 }, 3)).toBe("4");
  });

  it("reports the end of the walked range for desc", async () => {
    expect(await endOf(ten(), { orderBy: "desc", limit: 3 }, 4)).toBe("10");
    expect(await endOf(ten(), { orderBy: "desc", maxPages: 1 }, 4)).toBe("4");
  });

  it("reports undefined when limit drops events of the last page", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => ({
      // One transaction can overshoot the requested limit.
      events: [event(1), { ...event(1), eventIndex: 1 }],
      nextCursor: "2",
      hasMore: true,
    }));
    expect(await endOf({ fetchEvents }, { limit: 1 })).toBeUndefined();
  });

  it("reports undefined when no page is read", async () => {
    expect(await endOf(ten(), { limit: 0 })).toBeUndefined();
    expect(await endOf(ten(), { maxPages: 0 })).toBeUndefined();
    expect(await endOf(ten(), { orderBy: "desc", limit: 0 })).toBeUndefined();
  });

  it("reports undefined when a version bound ended the walk", async () => {
    expect(await endOf(ten(), { toVersion: 5n })).toBeUndefined();
    expect(await endOf(ten(), { orderBy: "desc", toVersion: 5n })).toBeUndefined();
    const source = {
      fetchEvents: ten().fetchEvents.bind(ten()),
      getBlock: async (_d: EventDescriptor, height: bigint) => ({
        height,
        firstVersion: height,
        lastVersion: height,
        timestamp: 0n,
      }),
    };
    expect(await endOf(source, { toBlock: 5n })).toBeUndefined();
  });

  it("reports undefined when the last page has no cursor", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => ({
      events: [event(1)],
      hasMore: false,
    }));
    expect(await endOf({ fetchEvents }, {})).toBeUndefined();
  });

  it("is called once from getEvents and iterateEvents", async () => {
    const onCursor = vi.fn();
    const query = new EventQuery(ten());
    expect(await query.getEvents(descriptor, { onCursor })).toHaveLength(10);
    for await (const _ of query.iterateEvents(descriptor, { onCursor }, 4));
    expect(onCursor.mock.calls).toEqual([["10"], ["10"]]);
  });

  it("is not called when the loop is left early or the query rejects", async () => {
    const onCursor = vi.fn();
    const query = new EventQuery(ten());
    for await (const _ of query.getEventPages(descriptor, { onCursor }, 3)) break;
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => {
      throw new StreamdNetworkError("node down", 503);
    });
    await expect(
      new EventQuery({ fetchEvents }).getEvents(descriptor, { onCursor }),
    ).rejects.toBeInstanceOf(StreamdNetworkError);
    expect(onCursor).not.toHaveBeenCalled();
  });

  it("never passes onCursor to the adapter", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(async () => ({
      events: [],
      nextCursor: "1",
      hasMore: false,
    }));
    await new EventQuery({ fetchEvents }).getEvents(descriptor, { onCursor: () => {} });
    expect("onCursor" in fetchEvents.mock.calls[0]![1]).toBe(false);
  });
});

describe("EventQuery over several descriptors", () => {
  const A: EventDescriptor = { kind: "module", eventType: "0x1::a::A" };
  const B: EventDescriptor = { kind: "module", eventType: "0x1::b::B" };
  const all: EventDescriptor = { kind: "account", accountAddress: "0x1" };
  const at = (type: string, version: number, eventIndex = 0, n = version) => ({
    type,
    data: { n },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  });
  const seeded = () =>
    new MemoryAdapter([
      at("0x1::a::A", 1),
      at("0x1::b::B", 2),
      at("0x1::a::A", 3, 1),
      at("0x1::b::B", 3, 0),
      at("0x1::a::A", 5),
      at("0x1::b::B", 6),
    ]);
  const ids = (events: { version: bigint; eventIndex: number }[]) =>
    events.map((e) => `${e.version}:${e.eventIndex}`);

  it("merges every descriptor's events in (version, eventIndex) order", async () => {
    const events = await new EventQuery(seeded()).getEvents([A, B]);
    expect(ids(events)).toEqual(["1:0", "2:0", "3:0", "3:1", "5:0", "6:0"]);
  });

  it("returns an event two descriptors select once", async () => {
    const events = await new EventQuery(seeded()).getEvents([A, all, B]);
    expect(ids(events)).toEqual(["1:0", "2:0", "3:0", "3:1", "5:0", "6:0"]);
  });

  it("pages the merged history by pageSize", async () => {
    const sizes: number[] = [];
    for await (const page of new EventQuery(seeded()).getEventPages([A, B], {}, 4)) {
      sizes.push(page.length);
    }
    expect(sizes).toEqual([4, 2]);
  });

  it("counts limit over the merge, after where", async () => {
    const events = await new EventQuery(seeded()).getEvents([A, B], {
      where: (e) => (e.data as { n: number }).n > 1,
      limit: 3,
    });
    expect(ids(events)).toEqual(["2:0", "3:0", "3:1"]);
  });

  it("applies maxPages to each descriptor's walk", async () => {
    const pages: string[][] = [];
    for await (const page of new EventQuery(seeded()).getEventPages([A, B], { maxPages: 1 }, 1)) {
      pages.push(ids(page));
    }
    expect(pages).toEqual([["1:0"], ["2:0"]]);
  });

  it("returns the last matches newest first for orderBy desc", async () => {
    const events = await new EventQuery(seeded()).getEvents([A, B], { orderBy: "desc", limit: 3 });
    expect(ids(events)).toEqual(["6:0", "5:0", "3:1"]);
  });

  it("reads the head once for every descriptor", async () => {
    const adapter = seeded();
    const getHeadVersion = vi.spyOn(adapter, "getHeadVersion").mockResolvedValue(3n);
    const events = await new EventQuery(adapter).getEvents([A, B], {
      fromVersion: -1n,
      toVersion: "head",
    });
    expect(ids(events)).toEqual(["2:0", "3:0", "3:1"]);
    expect(getHeadVersion).toHaveBeenCalledTimes(1);
  });

  it("resolves fromVersion earliest for each descriptor", async () => {
    const adapter = seeded();
    const getEarliestVersion = vi.spyOn(adapter, "getEarliestVersion");
    const events = await new EventQuery(adapter).getEvents([A, B], { fromVersion: "earliest" });
    expect(events).toHaveLength(6);
    expect(getEarliestVersion).toHaveBeenCalledTimes(2);
  });

  it("rejects an empty list, onCursor and offset without fetching", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    const query = new EventQuery({ fetchEvents });
    await expect(query.getEvents([])).rejects.toThrow("at least one descriptor");
    await expect(query.getEvents([A], { onCursor: () => {} })).rejects.toThrow("onCursor");
    await expect(query.getEvents([A, B], { offset: 1 })).rejects.toThrow("offset");
    expect(fetchEvents).not.toHaveBeenCalled();
  });
});

describe("EventQuery bound lookups", () => {
  const fetchEvents = () =>
    vi.fn<EventAdapter["fetchEvents"]>(async () => ({ events: [], hasMore: false }));
  const good = { firstVersion: 10n, lastVersion: 19n, timestamp: 0n };

  const cases: [string, object, object, string][] = [
    ["a snake_case block", { fromBlock: 1n }, { getBlock: async () => ({ first_version: 10n }) }, "getBlock height is not a bigint"],
    ["an inverted block", { toBlock: 1n }, { getBlock: async () => ({ ...good, height: 1n, firstVersion: 50n }) }, "getBlock firstVersion is above lastVersion"],
    ["a negative firstVersion", { fromBlock: 1n }, { getBlock: async () => ({ ...good, height: 1n, firstVersion: -1n }) }, "getBlock firstVersion is negative"],
    ["another block's height", { fromBlock: 1n }, { getBlock: async () => ({ ...good, height: 2n }) }, "getBlock(1) returned height 2"],
    ["a negative earliest", { fromVersion: "earliest" }, { getEarliestVersion: async () => -500n }, "getEarliestVersion resolved -500"],
    ["a string earliest", { fromVersion: "earliest" }, { getEarliestVersion: async () => "900" }, "getEarliestVersion resolved 900"],
    ["a string head for toVersion", { toVersion: "head" }, { getHeadVersion: async () => "900" }, "getHeadVersion resolved 900"],
    ["a string head for a negative start", { fromVersion: -5n }, { getHeadVersion: async () => "900" }, "getHeadVersion resolved 900"],
  ];

  it.each(cases)("rejects %s without fetching or retrying", async (_label, filter, lookups, message) => {
    const source = { fetchEvents: fetchEvents(), ...lookups } as unknown as EventAdapter;
    const err = await new EventQuery(source)
      .getEvents(descriptor, { ...(filter as EventFilter), retries: 3 })
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain(message);
    expect(source.fetchEvents).not.toHaveBeenCalled();
  });

  it("checks a block's fields as read once, so a getter cannot change them after", async () => {
    let reads = 0;
    const block = {
      height: 1n,
      lastVersion: 19n,
      timestamp: 0n,
      get firstVersion() {
        return ++reads > 1 ? -999n : 10n;
      },
    };
    const source = { fetchEvents: fetchEvents(), getBlock: async () => block };
    const events = await new EventQuery(source).getEvents(descriptor, { fromBlock: 1n });

    expect(events).toEqual([]);
    expect(source.fetchEvents.mock.calls[0]![1]).toMatchObject({ fromVersion: 10n });
    expect(reads).toBe(1);
  });
});
