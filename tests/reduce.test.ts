import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError } from "../src/errors.js";
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
