import { describe, it, expect } from "bun:test";
import { PoolAdapter } from "../src/adapters/pool.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { eventId } from "../src/cursor.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const T = "0x1::test::E";
const d: EventDescriptor = { kind: "module", eventType: T };
const ev = (v: number): StreamdEvent => ({ type: T, data: {}, version: BigInt(v), sequenceNumber: 0n, eventIndex: 0 });
const HISTORY = Array.from({ length: 9 }, (_, i) => ev(i + 1));

/** A node over the same history that logs its calls and fails while `fail` returns an error. */
function node(name: string, log: string[], fail: () => Error | undefined = () => undefined): EventAdapter {
  const memory = new MemoryAdapter(HISTORY);
  const guard = async () => {
    const err = fail();
    if (err !== undefined) throw err;
  };
  return {
    fetchEvents: async (desc, o) => (log.push(`${name} ${o.cursor ?? "start"}`), await guard(), memory.fetchEvents(desc, o)),
    getHeadVersion: async () => (log.push(`${name} head`), await guard(), 9n),
    view: async () => (log.push(`${name} view`), await guard(), [name]),
  };
}

const limit = (retryAfter?: number) => new StreamdNetworkError("rate limited", 429, undefined, retryAfter);

describe("PoolAdapter", () => {
  it("rotates new walks across nodes and keeps each walk on its own", async () => {
    const log: string[] = [];
    const pool = new PoolAdapter([node("a", log), node("b", log)]);
    const first = await pool.fetchEvents(d, { limit: 2 });
    const second = await pool.fetchEvents(d, { fromVersion: 5n, limit: 2 });
    await pool.fetchEvents(d, { cursor: first.nextCursor, limit: 2 });
    await pool.fetchEvents(d, { cursor: second.nextCursor, limit: 2 });
    expect(log).toEqual(["a start", "b start", `a ${first.nextCursor}`, `b ${second.nextCursor}`]);
  });

  it("tells apart walks on different descriptors that reach the same cursor", async () => {
    const log: string[] = [];
    const pool = new PoolAdapter([node("a", log), node("b", log)]);
    const onA = await pool.fetchEvents(d, { limit: 2 });
    const onB = await pool.fetchEvents({ kind: "module", eventType: [T] }, { limit: 2 });
    expect(onB.nextCursor).toBe(onA.nextCursor!);
    await pool.fetchEvents(d, { cursor: onA.nextCursor, limit: 1 });
    expect(log).toEqual(["a start", "b start", `a ${onA.nextCursor}`]);
  });

  it("moves a rate-limited call to the next node, cools the first, and the walk stays where it moved", async () => {
    const log: string[] = [];
    let aLimited = false;
    const pool = new PoolAdapter([node("a", log, () => (aLimited ? limit() : undefined)), node("b", log)], { cooldown: 60_000 });
    const first = await pool.fetchEvents(d, { limit: 2 });
    aLimited = true;
    const moved = await pool.fetchEvents(d, { cursor: first.nextCursor, limit: 2 });
    aLimited = false;
    await pool.fetchEvents(d, { cursor: moved.nextCursor, limit: 2 });
    // A new walk skips the cooling node too.
    await pool.fetchEvents(d, { limit: 1 });
    expect(log).toEqual(["a start", `a ${first.nextCursor}`, `b ${first.nextCursor}`, `b ${moved.nextCursor}`, "b start"]);
  });

  it("honours retryAfter over the cooldown, and takes a node back once it has cooled", async () => {
    const log: string[] = [];
    let aLimited = true;
    const pool = new PoolAdapter([node("a", log, () => (aLimited ? limit(20) : undefined)), node("b", log)], { cooldown: 60_000 });
    await pool.fetchEvents(d, {});
    aLimited = false;
    await Bun.sleep(30);
    await pool.fetchEvents(d, {});
    await pool.fetchEvents(d, {});
    expect(log).toEqual(["a start", "b start", "b start", "a start"]);
  });

  it("rethrows the last 429 when every node is limited, and asks the walk's node again once all are cooling", async () => {
    const log: string[] = [];
    const pool = new PoolAdapter([node("a", log, () => limit()), node("b", log, () => limit())]);
    await expect(pool.fetchEvents(d, {})).rejects.toBeInstanceOf(StreamdNetworkError);
    expect(log).toEqual(["a start", "b start"]);
    await expect(pool.fetchEvents(d, {})).rejects.toMatchObject({ statusCode: 429 });
    expect(log).toHaveLength(3);
  });

  it("rethrows any other failure without moving", async () => {
    const log: string[] = [];
    const down = new StreamdNetworkError("down", 500);
    const pool = new PoolAdapter([node("a", log, () => down), node("b", log)]);
    await expect(pool.fetchEvents(d, {})).rejects.toBe(down);
    expect(log).toEqual(["a start"]);
  });

  it("rotates and steps away on other methods, defining only those every node has", async () => {
    const log: string[] = [];
    const pool = new PoolAdapter([node("a", log, () => limit()), node("b", log)]);
    expect(await pool.view!({ function: "0x1::m::f" }, 1n, d)).toEqual(["b"]);
    expect(await pool.getHeadVersion!(d)).toBe(9n);
    expect(log).toEqual(["a view", "b view", "b head"]);
    expect(pool.getBlock).toBeUndefined();
    const mixed = new PoolAdapter([node("a", []), { fetchEvents: async () => ({ events: [], hasMore: false }) }]);
    expect(mixed.getHeadVersion).toBeUndefined();
  });

  it("forgets old cursors past its bound, treating them as new walks", async () => {
    const log: string[] = [];
    const pool = new PoolAdapter([node("a", log), node("b", log)]);
    const first = await pool.fetchEvents(d, { limit: 1 });
    for (let i = 0; i < 1100; i++) await pool.fetchEvents(d, { fromVersion: BigInt((i % 9) + 1), limit: 1 + (i % 50) });
    log.length = 0;
    // The first walk started on a; 1101 calls later rotation is on b, which a forgotten cursor gets.
    await pool.fetchEvents(d, { cursor: first.nextCursor, limit: 1 });
    expect(log).toEqual([`b ${first.nextCursor}`]);
  });

  it("refuses an empty pool and a bad cooldown", () => {
    expect(() => new PoolAdapter([])).toThrow(StreamdError);
    expect(() => new PoolAdapter([node("a", [])], { cooldown: -1 })).toThrow("Invalid cooldown");
  });

  it("serves a client walk with one node rate-limited, every event once", async () => {
    const log: string[] = [];
    let calls = 0;
    const pool = new PoolAdapter([node("a", log, () => (calls++ % 2 === 0 ? limit(0) : undefined)), node("b", log)]);
    const client = new StreamdClient({ adapter: pool });
    const pages = [];
    for await (const page of client.getEventPages(d, { fromVersion: 1n }, 2)) pages.push(...page);
    expect(pages.map(eventId)).toEqual(HISTORY.map(eventId));
    client.destroy();
  });
});
