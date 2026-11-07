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

describe("onTransaction, several descriptors", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const A = "0x1::test::A";
  const B = "0x1::test::B";
  const onA: EventDescriptor = { kind: "module", eventType: A };
  const onB: EventDescriptor = { kind: "module", eventType: B };
  const typeOf = (d: EventDescriptor) => (d.kind === "module" ? d.eventType : undefined);

  /** Serves each descriptor its own script of pages, then empty caught-up pages. */
  function perDescriptor(scripts: Record<string, EventPage[]>): EventAdapter {
    const at: Record<string, number> = {};
    return {
      fetchEvents: vi.fn(async (d: EventDescriptor) => {
        const key = String(typeOf(d));
        const i = (at[key] = (at[key] ?? -1) + 1);
        return scripts[key]![i] ?? { events: [], nextCursor: "end", hasMore: false };
      }),
    };
  }

  it("merges the walks, one call per transaction in eventIndex order", async () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([ev(10, 0, A), ev(10, 1, B), ev(10, 2, A), ev(11, 0, B)]),
    });
    const groups: TransactionGroup[] = [];
    client.onTransaction([onA, onB], (g) => void groups.push(g), { batchSize: 1 });
    await advanceTimersByTimeAsync(20);
    client.destroy();

    expect(shape(groups)).toEqual(["10:0,1,2", "11:0"]);
    expect(groups[0]!.events.map((e) => e.type)).toEqual([A, B, A]);
  });

  it("delivers an event matched by two descriptors once", async () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([ev(10, 0, A), ev(10, 1, B)]),
    });
    const groups: TransactionGroup[] = [];
    client.onTransaction([onA, { kind: "module", eventType: [A, B] }], (g) => void groups.push(g));
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(shape(groups)).toEqual(["10:0,1"]);
  });

  it("releases only up to the walk that has settled least", async () => {
    const adapter = perDescriptor({
      [A]: [
        { events: [ev(10, 0, A)], nextCursor: "a1", hasMore: false, throughVersion: 10n },
        { events: [ev(20, 0, A)], nextCursor: "a2", hasMore: false, throughVersion: 20n },
      ],
      [B]: [{ events: [ev(10, 1, B), ev(20, 1, B)], nextCursor: "b1", hasMore: false, throughVersion: 20n }],
    });
    const client = new StreamdClient({ adapter });
    const log: string[] = [];
    const sub = client.onTransaction([onA, onB], (g) => void log.push(shape([g])[0]!), {
      pollInterval: 1000,
    });
    await advanceTimersByTimeAsync(0);
    expect(log).toEqual(["10:0,1"]);
    await advanceTimersByTimeAsync(1000);
    expect(log).toEqual(["10:0,1", "20:0,1"]);
    expect(sub.cursor).toBeUndefined();
    expect(sub.status.cursor).toBeUndefined();
    expect(sub.status.active).toBe(true);
    client.destroy();
  });

  describe("a walk idle at the head without throughVersion", () => {
    const empty: EventPage = { events: [], nextCursor: "c", hasMore: false };
    const run = async (scripts: Record<string, EventPage[]>, ms: number) => {
      const client = new StreamdClient({ adapter: perDescriptor(scripts) });
      const log: string[] = [];
      client.onTransaction([onA, onB], (g) => void log.push(shape([g])[0]!), { pollInterval: 1000 });
      await advanceTimersByTimeAsync(ms);
      client.destroy();
      return log;
    };

    it("holds a transaction the other walk read until its own next fetch", async () => {
      const log = await run(
        {
          [A]: [empty, empty, { events: [ev(5, 0, A)], nextCursor: "a", hasMore: false, throughVersion: 5n }],
          [B]: [empty, { events: [ev(5, 1, B)], nextCursor: "b", hasMore: false, throughVersion: 5n }],
        },
        3000,
      );
      expect(log).toEqual(["5:0,1"]);
    });

    it("does not release a later version ahead of its own earlier one", async () => {
      const log = await run(
        {
          [A]: [empty, empty, { events: [ev(5, 0, A)], nextCursor: "a", hasMore: false, throughVersion: 5n }],
          [B]: [empty, { events: [ev(6, 0, B)], nextCursor: "b", hasMore: false, throughVersion: 6n }],
        },
        3000,
      );
      expect(log).toEqual(["5:0", "6:0"]);
    });

    it("lets the other walk's transactions through once it polls again", async () => {
      const log = await run(
        { [A]: [], [B]: [empty, { events: [ev(5, 1, B)], nextCursor: "b", hasMore: false }] },
        2000,
      );
      expect(log).toEqual(["5:1"]);
    });
  });

  it("reads its own pages beside a subscription that shares a slower walk's requests", async () => {
    // A growing ledger; A's replies take 400ms and answer from the ledger as it stood when asked.
    const ledger: StreamdEvent[] = [];
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => {
        const after = o.cursor === undefined ? -1 : Number(o.cursor);
        const page: EventPage = {
          events: ledger.filter((e, i) => i > after && e.type === typeOf(d)),
          nextCursor: String(ledger.length - 1),
          hasMore: false,
        };
        return new Promise((r) => setTimeout(() => r(page), typeOf(d) === A ? 400 : 0));
      },
    };
    const client = new StreamdClient({ adapter });
    client.on(onA, () => {}, { pollInterval: 1000, fromVersion: 0n });
    await advanceTimersByTimeAsync(700);
    const log: string[] = [];
    client.onTransaction([onA, onB], (g) => void log.push(shape([g])[0]!), {
      pollInterval: 1000,
      fromVersion: 1n,
    });
    await advanceTimersByTimeAsync(800);
    ledger.push(ev(5, 0, A), ev(5, 1, B));
    await advanceTimersByTimeAsync(2500);
    client.destroy();

    expect(log).toEqual(["5:0,1"]);
  });

  it("resolves an earliest start through each walk's adapter", async () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([ev(1, 0, A), ev(2, 0, A), ev(2, 1, B), ev(3, 0, B)]),
    });
    const log: string[] = [];
    const errors: Error[] = [];
    client.onTransaction([onA, onB], (g) => void log.push(shape([g])[0]!), {
      fromVersion: "earliest",
      onError: (e) => void errors.push(e),
    });
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(errors).toEqual([]);
    expect(log).toEqual(["1:0", "2:0,1", "3:0"]);
  });

  it("drops a late copy of an event already delivered through another walk", async () => {
    const shared = ev(10, 0, B);
    const adapter = perDescriptor({
      [`${A},${B}`]: [
        { events: [], nextCursor: "a1", hasMore: false },
        { events: [shared], nextCursor: "a2", hasMore: false },
      ],
      [B]: [{ events: [shared], nextCursor: "b1", hasMore: false }],
    });
    const client = new StreamdClient({ adapter });
    const log: string[] = [];
    client.onTransaction([{ kind: "module", eventType: [A, B] }, onB], (g) => void log.push(shape([g])[0]!), {
      pollInterval: 1000,
    });
    await advanceTimersByTimeAsync(2000);
    client.destroy();

    expect(log).toEqual(["10:0"]);
  });

  it("releases nothing until every walk has finished a page", async () => {
    let releaseB: (() => void) | undefined;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async (d: EventDescriptor) => {
        if (typeOf(d) === B) await new Promise<void>((r) => (releaseB = r));
        return typeOf(d) === A
          ? { events: [ev(10, 0, A)], nextCursor: "a", hasMore: false, throughVersion: 10n }
          : { events: [], nextCursor: "b", hasMore: false, throughVersion: 10n };
      }),
    };
    const client = new StreamdClient({ adapter });
    const log: bigint[] = [];
    client.onTransaction([onA, onB], (g) => void log.push(g.version), { pollInterval: 1000 });
    await advanceTimersByTimeAsync(0);
    expect(log).toEqual([]);
    releaseB!();
    await advanceTimersByTimeAsync(0);
    expect(log).toEqual([10n]);
    client.destroy();
  });

  it("ends every walk when one gives up", async () => {
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async (d: EventDescriptor) => {
        if (typeOf(d) === B) throw new StreamdNetworkError("down");
        return { events: [], nextCursor: "a", hasMore: false };
      }),
    };
    const client = new StreamdClient({ adapter });
    const errors: Error[] = [];
    const sub = client.onTransaction([onA, onB], () => {}, {
      maxErrors: 2,
      maxBackoff: 10,
      pollInterval: 10,
      onError: (e) => void errors.push(e),
    });
    await advanceTimersByTimeAsync(100);
    const calls = (adapter.fetchEvents as unknown as { mock: { calls: unknown[] } }).mock.calls.length;
    await advanceTimersByTimeAsync(100);

    expect(sub.active).toBe(false);
    expect(errors).toHaveLength(2);
    expect((adapter.fetchEvents as unknown as { mock: { calls: unknown[] } }).mock.calls.length).toBe(calls);
  });

  it("fires onCaughtUp once every walk has caught up", async () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([ev(10, 0, A), ev(11, 0, B)]),
    });
    const caught = vi.fn();
    client.onTransaction([onA, onB], () => {}, { onCaughtUp: caught });
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(caught).toHaveBeenCalledTimes(1);
  });

  it("refuses the cursor option", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    expect(() => client.onTransaction([onA, onB], () => {}, { cursor: "1" })).toThrow(StreamdError);
  });
});
