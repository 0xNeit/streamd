import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { eventId } from "../src/cursor.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const A = "0x1::test::A";
const B = "0x1::test::B";
const onA: EventDescriptor = { kind: "module", eventType: A };
const onB: EventDescriptor = { kind: "module", eventType: B };
// A separate walk over A, so every A event is matched twice.
const alsoA: EventDescriptor = { kind: "module", eventType: [A] };
const LIST = [onB, onA, alsoA];

function ev(version: number, eventIndex: number, type: string): StreamdEvent {
  return { type, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

const HISTORY = [ev(1, 0, A), ev(2, 0, B), ev(3, 0, A), ev(3, 1, B), ev(4, 0, A), ev(5, 0, B)];
const MERGED = ["1:0", "2:0", "3:0", "3:1", "4:0", "5:0"];

describe("merged live subscriptions", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("on() delivers several descriptors in version order, a shared event once, live ones too", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const seen: string[] = [];
    const sub = client.on(LIST, (e) => void seen.push(eventId(e)), { fromVersion: 0n, batchSize: 1 });
    await advanceTimersByTimeAsync(50);
    expect(seen).toEqual(MERGED);
    memory.push(ev(6, 0, B), ev(6, 1, A), ev(7, 0, A));
    await advanceTimersByTimeAsync(3000);
    expect(seen).toEqual([...MERGED, "6:0", "6:1", "7:0"]);
    expect(sub.cursor).toBeUndefined();
    expect(client.listenerCount()).toBe(0);
    client.destroy();
  });

  it("on() reports a throwing callback and still delivers the rest", async () => {
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    const seen: string[] = [];
    client.on(
      LIST,
      (e) => {
        seen.push(eventId(e));
        if (eventId(e) === "2:0") throw new Error("boom");
      },
      { fromVersion: 0n, onError: (err) => errors.push(err) },
    );
    await advanceTimersByTimeAsync(50);
    client.destroy();
    expect(seen).toEqual(MERGED);
    expect(errors.map((e) => e.message)).toEqual(["Event callback threw for 2:0"]);
    expect((errors[0]!.cause as Error).message).toBe("boom");
  });

  it("onBatch() delivers whole transactions, merged, each event once", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    const batches: string[][] = [];
    client.onBatch(LIST, (events) => void batches.push(events.map(eventId)), { fromVersion: 0n, batchSize: 1 });
    await advanceTimersByTimeAsync(50);
    client.destroy();
    expect(batches.flat()).toEqual(MERGED);
    // A batch never splits a transaction.
    for (const batch of batches) {
      const versions = new Set(batch.map((id) => id.split(":")[0]));
      for (const v of versions) expect(batches.filter((b) => b.some((id) => id.startsWith(`${v}:`)))).toHaveLength(1);
    }
  });

  it("onBatch() hands over everything one release frees as one batch", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    const batches: string[][] = [];
    client.onBatch(LIST, (events) => void batches.push(events.map(eventId)), { fromVersion: 0n, batchSize: 100 });
    await advanceTimersByTimeAsync(50);
    client.destroy();
    // Each walk read everything in its first page, so the last of them frees it all at once.
    expect(batches).toEqual([MERGED]);
  });

  it("once() resolves with the earliest merged event, and rejects when a walk gives up", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    const first = client.once([onB, onA], { fromVersion: 0n });
    await advanceTimersByTimeAsync(50);
    expect(eventId(await first)).toBe("1:0");

    const failing: EventAdapter = {
      fetchEvents: async (d, o) => {
        if (d.kind === "module" && d.eventType === B) throw new StreamdNetworkError("down");
        return new MemoryAdapter(HISTORY).fetchEvents(d, o);
      },
    };
    const broken = new StreamdClient({ adapter: failing });
    const waiting = broken.once(LIST, { fromVersion: 0n, maxErrors: 1, onError: () => {} });
    const outcome = waiting.then(() => "resolved", (err: Error) => err.message);
    await advanceTimersByTimeAsync(50);
    expect(await outcome).toContain("Polling failed");
    client.destroy();
    broken.destroy();
  });

  it("stream() yields merged events one at a time", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    const stream = client.stream(LIST, { fromVersion: 0n });
    const seen: string[] = [];
    const reading = (async () => {
      for await (const e of stream) {
        seen.push(eventId(e));
        if (seen.length === 2) {
          await new Promise((r) => setTimeout(r, 1000));
          // Held back: nothing more arrived while the loop body ran.
          expect(seen).toHaveLength(2);
        }
        if (seen.length === MERGED.length) break;
      }
    })();
    await advanceTimersByTimeAsync(2000);
    await reading;
    expect(seen).toEqual(MERGED);
    client.destroy();
  });

  it("refuses a cursor and an empty list, and treats a one-element list as the descriptor", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(HISTORY) });
    expect(() => client.on(LIST, () => {}, { cursor: "1" })).toThrow(StreamdError);
    expect(() => client.on([], () => {})).toThrow("At least one descriptor");
    expect(() => client.onBatch([], () => {})).toThrow("At least one descriptor");
    const callback = () => {};
    const sub = client.on([onA], callback, { fromVersion: 0n });
    expect(client.listenerCount(onA)).toBe(1);
    await advanceTimersByTimeAsync(50);
    expect(sub.cursor).toBeDefined();
    client.off(onA, callback);
    expect(client.listenerCount()).toBe(0);
    client.destroy();
  });
});
