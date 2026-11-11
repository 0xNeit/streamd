import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { eventId } from "../src/cursor.js";
import { defineEvent } from "../src/define.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const A = "0x1::test::A";
const B = "0x1::test::B";
const onA: EventDescriptor = { kind: "module", eventType: A };
const onB: EventDescriptor = { kind: "module", eventType: B };
const alsoA: EventDescriptor = { kind: "module", eventType: [A] };
const LIST = [onA, onB, alsoA];

function ev(version: number, eventIndex: number, type: string): StreamdEvent {
  return { type, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

const HISTORY = [ev(1, 0, A), ev(2, 0, B), ev(3, 0, A), ev(3, 1, B), ev(4, 0, A)];
const append = (ids: string[], e: StreamdEvent) => [...ids, eventId(e)];

describe("reduce over several descriptors", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("folds the merged history once each, then the live tail in order, with nothing twice", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const changes: string[] = [];
    const r = client.reduce(LIST, append, [] as string[], {
      fromVersion: 0n,
      onChange: (_s, e) => void changes.push(eventId(e)),
    });
    await advanceTimersByTimeAsync(10);
    expect(await r.ready).toEqual(["1:0", "2:0", "3:0", "3:1", "4:0"]);
    memory.push(ev(5, 0, B), ev(5, 1, A), ev(6, 0, A));
    await advanceTimersByTimeAsync(3000);
    expect(r.value).toEqual(["1:0", "2:0", "3:0", "3:1", "4:0", "5:0", "5:1", "6:0"]);
    expect(changes).toEqual(["5:0", "5:1", "6:0"]);
    r.unsubscribe();
    client.destroy();
  });

  it("ends the backfill at the head read first, so nothing committed after it is folded twice", async () => {
    const memory = new MemoryAdapter(HISTORY);
    let reads = 0;
    // A transaction lands right after the head is read and before the history walk.
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => memory.fetchEvents(d, o),
      getHeadVersion: async () => {
        const head = await memory.getHeadVersion();
        if (reads++ === 0) memory.push(ev(9, 0, A));
        return head;
      },
    };
    const client = new StreamdClient({ adapter });
    const r = client.reduce(LIST, append, [] as string[], { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    expect(await r.ready).toEqual(["1:0", "2:0", "3:0", "3:1", "4:0"]);
    await advanceTimersByTimeAsync(3000);
    expect(r.value).toEqual(["1:0", "2:0", "3:0", "3:1", "4:0", "9:0"]);
    r.unsubscribe();
    client.destroy();
  });

  it("follows from a start above the head, and from an empty history", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const ahead = client.reduce(LIST, append, [] as string[], { fromVersion: 7n });
    const fresh = client.reduce([onB, { kind: "module", eventType: "0x1::test::C" }], append, [] as string[], {
      fromVersion: 3n,
    });
    await advanceTimersByTimeAsync(10);
    expect(await ahead.ready).toEqual([]);
    expect(await fresh.ready).toEqual(["3:1"]);
    memory.push(ev(6, 0, B), ev(7, 0, A), ev(8, 0, B));
    await advanceTimersByTimeAsync(3000);
    expect(ahead.value).toEqual(["7:0", "8:0"]);
    expect(fresh.value).toEqual(["3:1", "6:0", "8:0"]);
    ahead.unsubscribe();
    fresh.unsubscribe();
    client.destroy();
  });

  it("resumes from its own cursor without folding anything twice", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const first = new StreamdClient({ adapter: memory });
    const r = first.reduce(LIST, append, [] as string[], { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    await r.ready;
    memory.push(ev(5, 0, A), ev(5, 1, B));
    await advanceTimersByTimeAsync(3000);
    const saved = r.cursor!;
    const value = r.value;
    r.unsubscribe();
    first.destroy();
    expect(value).toEqual(["1:0", "2:0", "3:0", "3:1", "4:0", "5:0", "5:1"]);

    memory.push(ev(6, 0, B), ev(6, 1, A));
    const second = new StreamdClient({ adapter: memory });
    const resumed = second.reduce(LIST, append, value, { cursor: saved });
    await advanceTimersByTimeAsync(3000);
    expect(resumed.value).toEqual([...value, "6:0", "6:1"]);
    resumed.unsubscribe();
    second.destroy();
  });

  it("gives a cursor before any event is folded, and resumes after the head it marks", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const r = client.reduce(LIST, append, [] as string[], { fromVersion: 10n });
    await advanceTimersByTimeAsync(10);
    await r.ready;
    const saved = r.cursor!;
    expect(saved).toBeDefined();
    r.unsubscribe();
    memory.push(ev(10, 0, A), ev(11, 0, B));
    const resumed = client.reduce(LIST, append, [] as string[], { cursor: saved });
    await advanceTimersByTimeAsync(3000);
    expect(resumed.value).toEqual(["10:0", "11:0"]);
    resumed.unsubscribe();
    client.destroy();
  });

  it("refuses an adapter cursor and an empty list, needs a head, and treats one descriptor as itself", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    expect(() => client.reduce(LIST, append, [], { cursor: "3" })).toThrow("cannot resume several descriptors");
    expect(() => client.reduce([], append, [])).toThrow("At least one descriptor");
    const single = client.reduce([onA], append, [] as string[], { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    expect(await single.ready).toEqual(["1:0", "3:0", "4:0"]);
    single.unsubscribe();
    client.destroy();

    const memory = new MemoryAdapter(HISTORY);
    const headless = new StreamdClient({ adapter: { fetchEvents: (d, o) => memory.fetchEvents(d, o) } });
    const errors: Error[] = [];
    const r = headless.reduce(LIST, append, [], { fromVersion: 0n, onError: (e) => errors.push(e) });
    await expect(r.ready).rejects.toBeInstanceOf(StreamdError);
    expect(errors[0]?.message).toContain("getHeadVersion");
    headless.destroy();
  });
});

// Compiled by typecheck, never run.
export function typesOfMergedReduce(client: StreamdClient) {
  const X = defineEvent<{ x: string }>("0x1::m::X");
  const Y = defineEvent<{ y: number }>("0x1::m::Y");
  client.reduce([X, Y], (n: number, e) => n + ("x" in e.data ? e.data.x.length : e.data.y), 0);
  // @ts-expect-error the payload is the union, not one member
  client.reduce([X, Y], (n: number, e) => n + e.data.y, 0);
}
