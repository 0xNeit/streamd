import { describe, it, expect } from "bun:test";
import { ArchiveFallbackAdapter } from "../src/adapters/archive.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { checkAdapter } from "../src/conformance.js";
import { eventId } from "../src/cursor.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const T = "0x1::test::E";
const d: EventDescriptor = { kind: "module", eventType: T };
const ev = (v: number): StreamdEvent => ({ type: T, data: {}, version: BigInt(v), sequenceNumber: 0n, eventIndex: 0 });
const HISTORY = Array.from({ length: 10 }, (_, i) => ev(i + 1));
const PRUNED = new StreamdNetworkError("Version pruned", 410);

/** A fullnode that has pruned everything below `edge`; the start of each fetch decides. */
function fullnode(edge: number, calls: string[]): EventAdapter {
  const memory = new MemoryAdapter(HISTORY);
  const start = (o: { cursor?: string; fromVersion?: bigint }) =>
    o.cursor !== undefined ? Number(o.cursor) + 1 : Number(o.fromVersion ?? 0n);
  return {
    fetchEvents: async (desc, o) => {
      calls.push(`fullnode ${start(o)}`);
      if (start(o) < edge) throw PRUNED;
      return memory.fetchEvents(desc, o);
    },
    getHeadVersion: async () => (calls.push("fullnode head"), 10n),
    getEarliestVersion: async () => BigInt(edge),
    getBlock: async (_d, h) => {
      if (h < BigInt(edge)) throw PRUNED;
      return { height: h, firstVersion: h, lastVersion: h, timestamp: h };
    },
    getBlockHeights: async () => ({ oldest: BigInt(edge), newest: 10n }),
    getTransactionEvents: async (hash) => {
      if (hash === "0xold") throw PRUNED;
      return { status: "success", events: [ev(9)], version: 9n } as never;
    },
    view: async (_r, version) => {
      if (version < BigInt(edge)) throw PRUNED;
      return ["fullnode"];
    },
    simulateTransactionEvents: async () => ({ success: true, vmStatus: "ok", events: [], version: 1n }) as never,
  };
}

function archive(calls: string[]): EventAdapter {
  const memory = new MemoryAdapter(HISTORY);
  return {
    fetchEvents: async (desc, o) => (calls.push("archive"), memory.fetchEvents(desc, o)),
    getHeadVersion: async () => (calls.push("archive head"), 9n),
    getEarliestVersion: async () => 0n,
    getBlock: async (_d, h) => (calls.push("archive block"), { height: h, firstVersion: h, lastVersion: h, timestamp: h }),
    getBlockHeights: async () => ({ oldest: 0n, newest: 9n }),
    getTransactionEvents: async () => (calls.push("archive tx"), { status: "success", events: [ev(2)], version: 2n }) as never,
    view: async () => (calls.push("archive view"), ["archive"]),
  };
}

describe("ArchiveFallbackAdapter", () => {
  it("sends only what the fullnode refuses as pruned to the archive", async () => {
    const calls: string[] = [];
    const adapter = new ArchiveFallbackAdapter(fullnode(5, calls), archive(calls));
    expect((await adapter.fetchEvents(d, { fromVersion: 2n, limit: 1 })).events.map(eventId)).toEqual(["2:0"]);
    expect(calls).toEqual(["fullnode 2", "archive"]);
    calls.length = 0;
    expect((await adapter.fetchEvents(d, { fromVersion: 6n, limit: 1 })).events.map(eventId)).toEqual(["6:0"]);
    expect(calls).toEqual(["fullnode 6"]);
    calls.length = 0;
    expect((await adapter.getBlock!(d, 1n)).height).toBe(1n);
    expect(await adapter.view!({ function: "0x1::m::f", typeArguments: [], functionArguments: [] }, 2n, d)).toEqual(["archive"]);
    expect(await adapter.view!({ function: "0x1::m::f", typeArguments: [], functionArguments: [] }, 8n, d)).toEqual(["fullnode"]);
    expect((await adapter.getTransactionEvents!("0xold", d))?.version).toBe(2n);
    expect((await adapter.getTransactionEvents!("0xnew", d))?.version).toBe(9n);
    expect(calls).toEqual(["archive block", "archive view", "archive tx"]);
  });

  it("rethrows a rate limit, an outage and bad input without touching the archive", async () => {
    for (const failure of [
      new StreamdNetworkError("rate limited", 429),
      new StreamdNetworkError("down", 500),
      new StreamdNetworkError("no status"),
      new StreamdError("bad input"),
    ]) {
      const calls: string[] = [];
      const primary: EventAdapter = { fetchEvents: async () => Promise.reject(failure) };
      const adapter = new ArchiveFallbackAdapter(primary, archive(calls));
      await expect(adapter.fetchEvents(d, {})).rejects.toBe(failure);
      expect(calls).toEqual([]);
    }
  });

  it("takes the head and simulations from the fullnode, the earliest and oldest block from the archive", async () => {
    const calls: string[] = [];
    const adapter = new ArchiveFallbackAdapter(fullnode(5, calls), archive(calls));
    expect(await adapter.getHeadVersion!(d)).toBe(10n);
    expect(await adapter.getEarliestVersion!(d)).toBe(0n);
    expect(await adapter.getBlockHeights!(d)).toEqual({ oldest: 0n, newest: 10n });
    expect(adapter.simulateTransactionEvents).toBeDefined();
    expect(calls).toEqual(["fullnode head"]);
  });

  it("defines a method only when the fullnode has it, and refuses a missing adapter", () => {
    const bare = new ArchiveFallbackAdapter({ fetchEvents: async () => ({ events: [], hasMore: false }) }, archive([]));
    for (const m of ["getHeadVersion", "getBlock", "getBlockHeights", "getTransactionEvents", "view", "simulateTransactionEvents"] as const) {
      expect(bare[m]).toBeUndefined();
    }
    expect(bare.getEarliestVersion).toBeDefined();
    expect(() => new ArchiveFallbackAdapter(undefined as never, archive([]))).toThrow(StreamdError);
  });

  it("walks from below the edge to the head with no gap or repeat, back on the fullnode past the edge", async () => {
    const calls: string[] = [];
    const client = new StreamdClient({ adapter: new ArchiveFallbackAdapter(fullnode(5, calls), archive(calls)) });
    const events = await client.getEvents(d, { fromVersion: 1n });
    expect(events.map(eventId)).toEqual(HISTORY.map(eventId));
    const paged = [];
    for await (const page of client.getEventPages(d, { fromVersion: 1n }, 3)) paged.push(...page);
    expect(paged.map(eventId)).toEqual(HISTORY.map(eventId));
    expect(calls.filter((c) => c === "archive").length).toBeGreaterThan(0);
    expect(calls.at(-1)).toMatch(/^fullnode/);
    client.destroy();
  });

  it("passes checkAdapter across the edge", async () => {
    const adapter = new ArchiveFallbackAdapter(fullnode(5, []), archive([]));
    const report = await checkAdapter(adapter, d, { limit: 3, fromVersion: 1n });
    expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
  });

});
