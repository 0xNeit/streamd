import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdError, checkAdapter, eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent } from "@streamd/core";
import { SqliteEventStore, StoreAdapter } from "../src/index.js";

const A = "0x" + "0".repeat(63) + "a";
const B = "0x" + "b".repeat(64);
const SWAP = "0x1::dex::Swap";
const MINT = "0x1::coin::Mint";

function ev(version: bigint, eventIndex: number, type: string, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type, data: { v: String(version) }, version, sequenceNumber: 0n, eventIndex, ...extra };
}

// Versions 1..12, two events each, alternating senders and types, with a guid on the mints.
const EVENTS: StreamdEvent[] = Array.from({ length: 12 }, (_, i) => {
  const version = BigInt(i + 1);
  const sender = i % 2 === 0 ? "0xa" : B;
  return [
    ev(version, 0, SWAP, { sender }),
    ev(version, 1, MINT, { sender, guid: { creationNumber: 2n, accountAddress: "0xa" } }),
  ];
}).flat();

function seeded(events = EVENTS): StoreAdapter {
  const db = new Database(":memory:");
  new SqliteEventStore(db).write(events);
  return new StoreAdapter(db);
}

const ids = (events: StreamdEvent[]) => events.map(eventId);

const swaps: EventDescriptor = { kind: "module", eventType: SWAP };
const fromA: EventDescriptor = { kind: "account", accountAddress: "0xa" };
const mintHandle: EventDescriptor = { kind: "handle", accountAddress: A, creationNumber: 2n };

describe("StoreAdapter", () => {
  it("returns each descriptor's events in ascending order", async () => {
    const adapter = seeded([...EVENTS].reverse());
    const all = { limit: 100 };
    expect(ids((await adapter.fetchEvents(swaps, all)).events)).toEqual(
      EVENTS.filter((e) => e.type === SWAP).map(eventId),
    );
    expect(ids((await adapter.fetchEvents({ kind: "module", eventType: [SWAP, MINT] }, all)).events)).toEqual(
      EVENTS.map(eventId),
    );
    const sentByA = (await adapter.fetchEvents(fromA, all)).events;
    expect(sentByA).toHaveLength(12);
    expect(sentByA.every((e) => e.version % 2n === 1n)).toBe(true);
    const mintsByA = await adapter.fetchEvents({ ...fromA, eventTypes: [MINT] }, all);
    expect(mintsByA.events.every((e) => e.type === MINT)).toBe(true);
    expect(mintsByA.events).toHaveLength(6);
    expect((await adapter.fetchEvents({ ...fromA, eventTypes: [] }, all)).events).toEqual([]);
    expect(ids((await adapter.fetchEvents(mintHandle, all)).events)).toEqual(
      EVENTS.filter((e) => e.type === MINT).map(eventId),
    );
  });

  it("pages with fromVersion, toVersion, limit, offset and cursor", async () => {
    const adapter = seeded();
    const first = await adapter.fetchEvents(swaps, { fromVersion: 3n, toVersion: 8n, limit: 2 });
    expect(ids(first.events)).toEqual(["3:0", "4:0"]);
    expect(first.hasMore).toBe(true);
    expect(first.throughVersion).toBe(3n);
    const rest = await adapter.fetchEvents(swaps, { cursor: first.nextCursor, fromVersion: 1n, toVersion: 8n, limit: 10 });
    expect(ids(rest.events)).toEqual(["5:0", "6:0", "7:0", "8:0"]);
    expect(rest.hasMore).toBe(false);
    const skipped = await adapter.fetchEvents(swaps, { fromVersion: 3n, offset: 2, limit: 1 });
    expect(ids(skipped.events)).toEqual(["5:0"]);
    const desc = await adapter.fetchEvents(swaps, { toVersion: 3n, orderBy: "desc" });
    expect(ids(desc.events)).toEqual(["3:0", "2:0", "1:0"]);
  });

  it("keeps an empty page's cursor where it was, and finds events written after it", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const adapter = new StoreAdapter(db);
    const empty = await adapter.fetchEvents(swaps, { fromVersion: 5n });
    expect(empty).toEqual({ events: [], nextCursor: "5:-1", hasMore: false });
    store.write([ev(4n, 0, SWAP), ev(5n, 0, SWAP)]);
    expect(ids((await adapter.fetchEvents(swaps, { cursor: empty.nextCursor })).events)).toEqual(["5:0"]);
    expect((await adapter.fetchEvents(swaps, { limit: 0 })).hasMore).toBe(false);
  });

  it("reports head and earliest over the whole store, 0n when empty", async () => {
    expect(await seeded([]).getHeadVersion()).toBe(0n);
    expect(await seeded([]).getEarliestVersion()).toBe(0n);
    const adapter = seeded([ev(7n, 0, SWAP), ev(2n ** 64n - 1n, 0, MINT)]);
    expect(await adapter.getHeadVersion()).toBe(2n ** 64n - 1n);
    expect(await adapter.getEarliestVersion()).toBe(7n);
  });

  it("refuses foreign cursors, struct-and-field handles and bad options as StreamdError", async () => {
    const adapter = seeded();
    const refused = [
      adapter.fetchEvents(swaps, { cursor: "12" }),
      adapter.fetchEvents(swaps, { cursor: "1:x" }),
      adapter.fetchEvents({ kind: "handle", accountAddress: A, eventHandleStruct: "0x1::m::S", fieldName: "f" }, {}),
      adapter.fetchEvents({ kind: "module", eventType: [] }, {}),
      adapter.fetchEvents({ kind: "module", eventType: "not a type" as never }, {}),
      adapter.fetchEvents({ kind: "account", accountAddress: "nope" }, {}),
      adapter.fetchEvents(swaps, { limit: -1 }),
      adapter.fetchEvents(swaps, { fromVersion: 2n ** 64n }),
      adapter.fetchEvents({ kind: "bogus" } as never, {}),
    ];
    for (const attempt of refused) await expect(attempt).rejects.toBeInstanceOf(StreamdError);
  });

  it("passes checkAdapter for every descriptor kind", async () => {
    const adapter = seeded();
    for (const descriptor of [swaps, fromA, mintHandle]) {
      for (const options of [{ limit: 2 }, { limit: 3, fromVersion: 4n }]) {
        const report = await checkAdapter(adapter, descriptor, options);
        expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
        expect(report.pagesFetched).toBe(3);
      }
    }
  });

  it("answers client queries, including relative starts and toVersion head", async () => {
    const client = new StreamdClient({ adapter: seeded() });
    const recent = await client.getEvents(swaps, { fromVersion: -2n, toVersion: "head" });
    expect(ids(recent)).toEqual(["10:0", "11:0", "12:0"]);
    const same = await new StreamdClient({ adapter: new MemoryAdapter(EVENTS) }).getEvents(swaps, {
      fromVersion: -2n,
      toVersion: "head",
    });
    expect(ids(same)).toEqual(ids(recent));
  });
});
