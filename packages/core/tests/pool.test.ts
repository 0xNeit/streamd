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
    const onB = await pool.fetchEvents({ kind: "module", eventType: [T, "0x1::test::Other"] }, { limit: 2 });
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
    const seen: string[] = [];
    let n = 0;
    const counting = (name: string): EventAdapter => ({
      fetchEvents: async (_d, o) => (seen.push(`${name} ${o.cursor ?? "start"}`), { events: [], nextCursor: `c${n++}`, hasMore: false }),
    });
    const pool = new PoolAdapter([counting("a"), counting("b")]);
    await pool.fetchEvents(d, {}); // a, cursor c0
    await pool.fetchEvents(d, { cursor: "c0" }); // still a, cursor c1
    for (let i = 0; i < 1024; i++) await pool.fetchEvents(d, {});
    seen.length = 0;
    // c1 belonged to a but is older than the 1024 most recent, so rotation picks b;
    // c1025 is recent, from a walk that started on a.
    await pool.fetchEvents(d, { cursor: "c1" });
    await pool.fetchEvents(d, { cursor: "c1025" });
    expect(seen).toEqual(["b c1", "a c1025"]);
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

describe("PoolAdapter readObjects", () => {
  it("rotates readObjects past a rate-limited node, only when every node has it", async () => {
    let first = true;
    const reader = (name: string): EventAdapter => ({
      ...node(name, []),
      readObjects: async () => {
        if (name === "a" && first) {
          first = false;
          throw limit(1000);
        }
        return [name];
      },
    });
    const pool = new PoolAdapter([reader("a"), reader("b")]);
    expect(await pool.readObjects!({ objects: ["0x1"] }, { version: 1n }, d)).toEqual(["b"]);
    expect(new PoolAdapter([reader("a"), node("b", [])]).readObjects).toBeUndefined();
  });
});

describe("PoolAdapter lookup by version", () => {
  it("rotates past a rate-limited node, only when every node has it", async () => {
    const asked: unknown[] = [];
    const looker = (name: string, limited: boolean): EventAdapter => ({
      ...node(name, []),
      getTransactionEventsByVersion: async (...args) => {
        asked.push([name, ...args]);
        if (limited) throw limit(1000);
        return { version: 4n, success: true, vmStatus: name, events: [] };
      },
    });
    const pool = new PoolAdapter([looker("a", true), looker("b", false)]);
    expect((await pool.getTransactionEventsByVersion!(4n, d))!.vmStatus).toBe("b");
    expect(asked).toEqual([["a", 4n, d], ["b", 4n, d]]);
    expect(new PoolAdapter([looker("a", false), node("b", [])]).getTransactionEventsByVersion).toBeUndefined();
  });
});

describe("PoolAdapter conservative bounds", () => {
  const PRUNED = new StreamdNetworkError("Version pruned", 410);

  function boundedNode(edge: bigint, maxBlock = 9n, fail?: () => Error | undefined): EventAdapter {
    const memory = new MemoryAdapter(HISTORY);
    const guard = async () => {
      const err = fail?.();
      if (err !== undefined) throw err;
    };
    return {
      fetchEvents: async (desc, o) => {
        await guard();
        if (o.fromVersion !== undefined && o.fromVersion < edge) throw PRUNED;
        return memory.fetchEvents(desc, o);
      },
      getEarliestVersion: async () => (await guard(), edge),
      getBlockHeights: async () => (await guard(), { oldest: edge, newest: maxBlock }),
      getBlock: async (_d, h) => {
        await guard();
        if (h < edge) throw PRUNED;
        if (h > maxBlock) throw new StreamdNetworkError("Block not found", 404);
        return { height: h, firstVersion: h, lastVersion: h, timestamp: h };
      },
    };
  }

  it("uses the newest earliest version across nodes so fromVersion 'earliest' never hits a 410", async () => {
    const pool = new PoolAdapter([boundedNode(1n), boundedNode(5n)]);
    expect(await pool.getEarliestVersion!(d)).toBe(5n);
    const client = new StreamdClient({ adapter: pool });
    for (let i = 0; i < 4; i++) {
      const events = await client.getEvents(d, { fromVersion: "earliest" });
      expect(events.map(eventId)).toEqual(["5:0", "6:0", "7:0", "8:0", "9:0"]);
    }
    client.destroy();
  });

  it("intersects block heights across nodes for fromTime and clamps disjoint ranges", async () => {
    const pool = new PoolAdapter([boundedNode(1n, 9n), boundedNode(5n, 8n)]);
    expect(await pool.getBlockHeights!(d)).toEqual({ oldest: 5n, newest: 8n });
    const client = new StreamdClient({ adapter: pool });
    for (let i = 0; i < 4; i++) {
      expect((await client.getEvents(d, { fromTime: 6n })).map(eventId)).toEqual(["6:0", "7:0", "8:0", "9:0"]);
    }
    expect((await client.getEvents(d, { fromTime: 2n })).map(eventId)).toEqual(["5:0", "6:0", "7:0", "8:0", "9:0"]);
    client.destroy();

    const disjoint = new PoolAdapter([boundedNode(1n, 3n), boundedNode(6n, 9n)]);
    expect(await disjoint.getBlockHeights!(d)).toEqual({ oldest: 6n, newest: 6n });
  });

  it("cools down a 429 node during bound reads and answers from remaining open nodes", async () => {
    let firstLimited = true;
    const n0: EventAdapter = {
      ...boundedNode(9n, 6n, () => (firstLimited ? limit(60_000) : undefined)),
      getHeadVersion: async () => 1n,
    };
    const n1: EventAdapter = {
      ...boundedNode(3n, 8n),
      getHeadVersion: async () => 2n,
    };
    const pool = new PoolAdapter([n0, n1], { cooldown: 60_000 });
    expect(await pool.getEarliestVersion!(d)).toBe(3n);
    firstLimited = false;
    expect(await pool.getBlockHeights!(d)).toEqual({ oldest: 3n, newest: 8n });
    expect(await pool.getHeadVersion!(d)).toBe(2n);
  });
});
