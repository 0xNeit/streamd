import { describe, it, expect, vi } from "bun:test";
import { EventQuery } from "../src/query.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, FetchOptions } from "../src/types.js";

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
