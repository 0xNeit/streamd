import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  StreamdEvent,
  TransactionGroup,
} from "../src/types.js";

const T = "0x1::test::Event";
const descriptor: EventDescriptor = { kind: "module", eventType: T };

function ev(version: number, eventIndex: number, type = T): StreamdEvent {
  return {
    type,
    data: { value: `${version}:${eventIndex}` },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: `0x${version.toString(16).padStart(2, "0")}`,
    sender: "0xa",
  };
}

/** Serves `pages` in order, then empty caught-up pages. */
function scripted(pages: EventPage[]): EventAdapter {
  let i = 0;
  return {
    fetchEvents: vi.fn(async () => pages[i++] ?? { events: [], nextCursor: "end", hasMore: false }),
  };
}

const shape = (groups: TransactionGroup[]) =>
  groups.map((g) => `${g.version}:${g.events.map((e) => e.eventIndex).join(",")}`);

describe("onTransaction, one descriptor", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const backlog = [ev(10, 0), ev(10, 1), ev(10, 2), ev(11, 0), ev(11, 1)];

  it("delivers each transaction whole across pages that split it", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const groups: TransactionGroup[] = [];
    client.onTransaction(descriptor, (g) => void groups.push(g), { batchSize: 2 });
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(shape(groups)).toEqual(["10:0,1,2", "11:0,1"]);
    expect(groups[0]!.transactionHash).toBe("0x0a");
    expect(groups[0]!.sender).toBe("0xa");
    expect(groups[0]!.timestamp).toBeUndefined();
  });

  it("without throughVersion, holds the highest version until a later one or the end", async () => {
    const adapter = scripted([
      { events: [ev(10, 0), ev(10, 1)], nextCursor: "a", hasMore: true },
      { events: [ev(10, 2), ev(11, 0)], nextCursor: "b", hasMore: true },
      { events: [ev(11, 1)], nextCursor: "c", hasMore: false },
    ]);
    const client = new StreamdClient({ adapter });
    const log: string[] = [];
    client.onTransaction(descriptor, (g) => void log.push(shape([g])[0]!));
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log).toEqual(["10:0,1,2", "11:0,1"]);
  });

  it("moves cursor only past a page that left nothing held", async () => {
    const adapter = scripted([
      { events: [ev(10, 0), ev(10, 1)], nextCursor: "a", hasMore: false, throughVersion: 9n },
      { events: [ev(10, 2), ev(11, 0)], nextCursor: "b", hasMore: false, throughVersion: 10n },
      { events: [ev(11, 1)], nextCursor: "c", hasMore: false, throughVersion: 11n },
    ]);
    const client = new StreamdClient({ adapter });
    const log: string[] = [];
    const sub = client.onTransaction(descriptor, (g) => void log.push(shape([g])[0]!), {
      pollInterval: 1000,
    });

    await advanceTimersByTimeAsync(0);
    expect(log).toEqual([]);
    expect(sub.cursor).toBeUndefined();
    await advanceTimersByTimeAsync(1000);
    expect(log).toEqual(["10:0,1,2"]);
    expect(sub.cursor).toBeUndefined();
    await advanceTimersByTimeAsync(1000);
    expect(log).toEqual(["10:0,1,2", "11:0,1"]);
    expect(sub.cursor).toBe("c");
    expect(sub.status.cursor).toBe("c");
    client.destroy();
  });

  it("starts from the cursor option, reported until a page moves it", async () => {
    const adapter = scripted([
      { events: [ev(12, 0), ev(12, 1)], nextCursor: "d", hasMore: false, throughVersion: 11n },
    ]);
    const client = new StreamdClient({ adapter });
    const sub = client.onTransaction(descriptor, () => {}, { cursor: "c" });
    await advanceTimersByTimeAsync(0);
    expect(mockedCursor(adapter)).toBe("c");
    expect(sub.cursor).toBe("c");
    client.destroy();
  });

  it("filters with where before grouping, skipping a transaction with no match", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const log: string[] = [];
    client.onTransaction(descriptor, (g) => void log.push(shape([g])[0]!), {
      where: async (e) => e.version === 11n && e.eventIndex === 1,
    });
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log).toEqual(["11:1"]);
  });

  it("reports a throwing callback and carries on with the next transaction", async () => {
    const errors: Error[] = [];
    const client = new StreamdClient({
      adapter: new MemoryAdapter(backlog),
      onError: (e) => void errors.push(e),
    });
    const log: bigint[] = [];
    client.onTransaction(descriptor, async (g) => {
      log.push(g.version);
      if (g.version === 10n) throw new Error("boom");
    });
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log).toEqual([10n, 11n]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(StreamdError);
    expect(errors[0]!.message).toBe("Transaction callback threw for 10");
  });

  it("holds the rest while paused and delivers it once on resume", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const log: bigint[] = [];
    const sub = client.onTransaction(descriptor, (g) => {
      log.push(g.version);
      if (g.version === 10n) sub.pause();
    });
    await advanceTimersByTimeAsync(2000);
    expect(log).toEqual([10n]);
    expect(sub.paused).toBe(true);
    expect(sub.active).toBe(false);

    sub.resume();
    await advanceTimersByTimeAsync(10);
    client.destroy();
    expect(log).toEqual([10n, 11n]);
  });

  it("fires onCaughtUp once, after the backlog's groups", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const log: string[] = [];
    client.onTransaction(descriptor, (g) => void log.push(String(g.version)), {
      batchSize: 2,
      onCaughtUp: () => void log.push("caught up"),
    });
    await advanceTimersByTimeAsync(3000);
    client.destroy();

    expect(log).toEqual(["10", "11", "caught up"]);
  });

  it("is not a listener, claims its id, and ends on destroy()", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const cb = vi.fn();
    const sub = client.onTransaction(descriptor, cb, { id: "tx" });
    expect(sub.id).toBe("tx");
    expect(() => client.on(descriptor, () => {}, { id: "tx" })).toThrow(StreamdError);
    expect(client.listenerCount()).toBe(0);
    client.off(descriptor);
    expect(sub.active).toBe(true);

    client.destroy();
    expect(sub.active).toBe(false);
    await advanceTimersByTimeAsync(10);
    expect(cb).not.toHaveBeenCalled();
  });

  it("ends on an aborted signal and on dispose", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(backlog) });
    const cb = vi.fn();
    const controller = new AbortController();
    const aborted = client.onTransaction(descriptor, cb, { signal: controller.signal });
    controller.abort();
    const disposed = client.onTransaction(descriptor, cb);
    disposed[Symbol.dispose]();
    await advanceTimersByTimeAsync(10);

    expect(aborted.active).toBe(false);
    expect(disposed.active).toBe(false);
    expect(cb).not.toHaveBeenCalled();
  });

  it("stops after maxErrors failed polls", async () => {
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new StreamdNetworkError("down");
      }),
    };
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter });
    const sub = client.onTransaction(descriptor, () => {}, {
      maxErrors: 2,
      maxBackoff: 10,
      onError: (e) => void errors.push(e),
    });
    await advanceTimersByTimeAsync(100);

    expect(sub.active).toBe(false);
    expect(errors).toHaveLength(2);
  });

  it("refuses an empty descriptor list", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    expect(() => client.onTransaction([], () => {})).toThrow(StreamdError);
  });
});

function mockedCursor(adapter: EventAdapter): string | undefined {
  const calls = (adapter.fetchEvents as unknown as { mock: { calls: [unknown, { cursor?: string }][] } })
    .mock.calls;
  return calls[0]?.[1].cursor;
}
