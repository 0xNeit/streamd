import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "bun:test";
import {
  MemoryAdapter,
  StreamdClient,
  StreamdError,
  StreamdNetworkError,
  checkAdapter,
  eventId,
} from "@streamd/core";
import type { EventDescriptor, StreamdEvent } from "@streamd/core";
import { PostgresEventStore, PostgresStoreAdapter } from "../src/index.js";

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

async function seeded(events = EVENTS): Promise<PostgresStoreAdapter> {
  const db = new PGlite();
  await new PostgresEventStore(db).write(events);
  return new PostgresStoreAdapter(db);
}

const ids = (events: StreamdEvent[]) => events.map(eventId);

const swaps: EventDescriptor = { kind: "module", eventType: SWAP };
const fromA: EventDescriptor = { kind: "account", accountAddress: "0xa" };
const mintHandle: EventDescriptor = { kind: "handle", accountAddress: A, creationNumber: 2n };

describe("PostgresStoreAdapter", () => {
  it("returns each descriptor's events in ascending order", async () => {
    const adapter = await seeded([...EVENTS].reverse());
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
    const adapter = await seeded();
    const first = await adapter.fetchEvents(swaps, { fromVersion: 3n, toVersion: 8n, limit: 2 });
    expect(ids(first.events)).toEqual(["3:0", "4:0"]);
    expect(first.hasMore).toBe(true);
    expect(first.throughVersion).toBeUndefined();
    const rest = await adapter.fetchEvents(swaps, { cursor: first.nextCursor, fromVersion: 1n, toVersion: 8n, limit: 10 });
    expect(ids(rest.events)).toEqual(["5:0", "6:0", "7:0", "8:0"]);
    expect(rest.hasMore).toBe(false);
    const skipped = await adapter.fetchEvents(swaps, { fromVersion: 3n, offset: 2, limit: 1 });
    expect(ids(skipped.events)).toEqual(["5:0"]);
    const resumed = await adapter.fetchEvents(swaps, { cursor: first.nextCursor, offset: 2, limit: 1 });
    expect(ids(resumed.events)).toEqual(["5:0"]);
    const desc = await adapter.fetchEvents(swaps, { toVersion: 3n, orderBy: "desc" });
    expect(ids(desc.events)).toEqual(["3:0", "2:0", "1:0"]);
  });

  it("keeps an empty page's cursor where it was, and finds events written after it", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const adapter = new PostgresStoreAdapter(db);
    const empty = await adapter.fetchEvents(swaps, { fromVersion: 5n });
    expect(empty).toEqual({ events: [], nextCursor: "5:-1", hasMore: false });
    await store.write([ev(4n, 0, SWAP), ev(5n, 0, SWAP)]);
    expect(ids((await adapter.fetchEvents(swaps, { cursor: empty.nextCursor })).events)).toEqual(["5:0"]);
    expect((await adapter.fetchEvents(swaps, { limit: 0 })).hasMore).toBe(false);
  });

  it("reports head and earliest over the whole store, 0n when empty", async () => {
    expect(await (await seeded([])).getHeadVersion()).toBe(0n);
    expect(await (await seeded([])).getEarliestVersion()).toBe(0n);
    const adapter = await seeded([ev(7n, 0, SWAP), ev(2n ** 64n - 1n, 0, MINT)]);
    expect(await adapter.getHeadVersion()).toBe(2n ** 64n - 1n);
    expect(await adapter.getEarliestVersion()).toBe(7n);
  });

  it("refuses foreign cursors, struct-and-field handles and bad options as StreamdError", async () => {
    const adapter = await seeded();
    const refused = [
      adapter.fetchEvents(swaps, { cursor: "12" }),
      adapter.fetchEvents(swaps, { cursor: "1:x" }),
      adapter.fetchEvents(swaps, { cursor: "1:99999999999999999999" }),
      adapter.fetchEvents({ kind: "handle", accountAddress: A, eventHandleStruct: "0x1::m::S", fieldName: "f" }, {}),
      adapter.fetchEvents({ kind: "module", eventType: [] }, {}),
      adapter.fetchEvents({ kind: "module", eventType: "not a type" as never }, {}),
      adapter.fetchEvents({ kind: "account", accountAddress: "nope" }, {}),
      adapter.fetchEvents({ kind: "account", accountAddress: "0xa", received: true }, {}),
      adapter.fetchEvents(swaps, { limit: -1 }),
      adapter.fetchEvents(swaps, { fromVersion: 2n ** 64n }),
      adapter.fetchEvents({ kind: "bogus" } as never, {}),
    ];
    for (const attempt of refused) await expect(attempt).rejects.toBeInstanceOf(StreamdError);
  });

  it("passes checkAdapter for every descriptor kind", async () => {
    const adapter = await seeded();
    for (const descriptor of [swaps, fromA, mintHandle]) {
      for (const options of [{ limit: 2 }, { limit: 3, fromVersion: 4n }]) {
        const report = await checkAdapter(adapter, descriptor, options);
        expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
        expect(report.pagesFetched).toBe(3);
      }
    }
  });

  it("answers client queries, including relative starts and toVersion head", async () => {
    const client = new StreamdClient({ adapter: await seeded() });
    const recent = await client.getEvents(swaps, { fromVersion: -2n, toVersion: "head" });
    expect(ids(recent)).toEqual(["10:0", "11:0", "12:0"]);
    const same = await new StreamdClient({ adapter: new MemoryAdapter(EVENTS) }).getEvents(swaps, {
      fromVersion: -2n,
      toVersion: "head",
    });
    expect(ids(same)).toEqual(ids(recent));
  });

  it("orders versions numerically across digit counts", async () => {
    const adapter = await seeded([ev(10n, 0, SWAP), ev(9n, 0, SWAP), ev(100n, 0, SWAP), ev(2n ** 64n - 1n, 0, SWAP)]);
    expect(ids((await adapter.fetchEvents(swaps, { fromVersion: 9n })).events)).toEqual([
      "9:0", "10:0", "100:0", `${2n ** 64n - 1n}:0`,
    ]);
  });
});

describe("PostgresStoreAdapter after prune", () => {
  async function pruned(beforeVersion: bigint, events = EVENTS) {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write(events);
    await store.prune({ beforeVersion });
    return { db, store, adapter: new PostgresStoreAdapter(db) };
  }

  async function failure(run: () => Promise<unknown>): Promise<unknown> {
    return run().then(
      () => undefined,
      (err) => err,
    );
  }

  function expectPruned(err: unknown) {
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(410);
    expect((err as Error).message).toContain("oldest kept is 5");
  }

  it("fails a fromVersion below the edge as a pruned fullnode does", async () => {
    const { adapter } = await pruned(5n);
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { fromVersion: 4n })));
  });

  it("fails even a zero limit below the edge", async () => {
    const { adapter } = await pruned(5n);
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { fromVersion: 4n, limit: 0 })));
  });

  it("fails a cursor below the edge, however close", async () => {
    const before = await (await seeded()).fetchEvents(swaps, { fromVersion: 4n, limit: 1 });
    expect(before.nextCursor).toBe("4:0");
    const { adapter } = await pruned(5n);
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { cursor: before.nextCursor })));
  });

  it("fails a range that ends below the edge", async () => {
    const { adapter } = await pruned(5n);
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { toVersion: 4n })));
  });

  it("starts at the oldest kept when given no start, past a late row below the edge", async () => {
    const { store, adapter } = await pruned(5n);
    await store.write([ev(2n, 0, SWAP)]);
    const page = await adapter.fetchEvents(swaps, { limit: 1 });
    expect(page.events.map((e) => e.version)).toEqual([5n]);
    expect(page.hasMore).toBe(true);
    const empty = await (await pruned(100n)).adapter.fetchEvents(swaps);
    expect(empty).toEqual({ events: [], nextCursor: "100:-1", hasMore: false });
  });

  it("serves a read exactly at the edge", async () => {
    const { adapter } = await pruned(5n);
    const page = await adapter.fetchEvents(swaps, { fromVersion: 5n, toVersion: 5n });
    expect(page.events.map((e) => e.version)).toEqual([5n]);
    const zero = await adapter.fetchEvents(swaps, { fromVersion: 5n, limit: 0 });
    expect(zero).toEqual({ events: [], nextCursor: "5:-1", hasMore: false });
  });

  it("keeps the edge when a later prune asks for less", async () => {
    const { store, adapter } = await pruned(5n);
    await store.prune({ beforeVersion: 2n });
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { fromVersion: 3n })));
  });

  it("raises the edge when a later prune asks for more", async () => {
    const { store, adapter } = await pruned(3n);
    await store.prune({ beforeVersion: 5n });
    expectPruned(await failure(() => adapter.fetchEvents(swaps, { fromVersion: 3n })));
  });

  it("compares versions across digit counts", async () => {
    const { adapter, store } = await pruned(9n);
    await store.prune({ beforeVersion: 10n });
    expect(await adapter.getEarliestVersion()).toBe(10n);
    expect((await adapter.fetchEvents(swaps, { fromVersion: 10n })).events.map((e) => e.version)).toEqual([
      10n,
      11n,
      12n,
    ]);
  });

  it("keeps the edge across new objects on the same database", async () => {
    const { db } = await pruned(5n);
    const again = new PostgresStoreAdapter(db);
    new PostgresEventStore(db);
    expectPruned(await failure(() => again.fetchEvents(swaps, { fromVersion: 1n })));
  });

  it("reports the earliest as the edge, even above a late row or with nothing left", async () => {
    const { store, adapter } = await pruned(5n);
    expect(await adapter.getEarliestVersion()).toBe(5n);
    await store.write([ev(2n, 0, SWAP)]);
    expect(await adapter.getEarliestVersion()).toBe(5n);
    expect(await (await pruned(100n)).adapter.getEarliestVersion()).toBe(100n);
  });

  it("reports the lowest stored when that is above the edge", async () => {
    const { adapter } = await pruned(5n, EVENTS.filter((e) => e.version >= 8n));
    expect(await adapter.getEarliestVersion()).toBe(8n);
  });

  it("reports the head as stored, never below the edge", async () => {
    expect(await (await pruned(5n)).adapter.getHeadVersion()).toBe(12n);
    const emptied = await pruned(100n);
    expect(await emptied.adapter.getHeadVersion()).toBe(100n);
    await emptied.store.write([ev(2n, 0, SWAP)]);
    expect(await emptied.adapter.getHeadVersion()).toBe(100n);
    expect(await (await seeded([])).getHeadVersion()).toBe(0n);
  });

  it("answers an emptied store's whole range with nothing rather than failing", async () => {
    const client = new StreamdClient({ adapter: (await pruned(100n)).adapter });
    const events = await client.getEvents(swaps, { fromVersion: "earliest", toVersion: "head" });
    expect(events).toEqual([]);
  });

  it("fails a relative start that reaches below the edge", async () => {
    const client = new StreamdClient({ adapter: (await pruned(5n)).adapter });
    expectPruned(await failure(() => client.getEvents(swaps, { fromVersion: -100n })));
    const recent = await client.getEvents(swaps, { fromVersion: -1n });
    expect(recent.map((e) => e.version)).toEqual([11n, 12n]);
  });

  it("lets a client start from earliest after pruning", async () => {
    const client = new StreamdClient({ adapter: (await pruned(11n)).adapter });
    const events = await client.getEvents(swaps, { fromVersion: "earliest" });
    expect(events.map((e) => e.version)).toEqual([11n, 12n]);
  });

  it("reads the edge and the page in one statement", async () => {
    const db = new PGlite();
    await new PostgresEventStore(db).write(EVENTS);
    const seen: string[] = [];
    const adapter = new PostgresStoreAdapter({ query: (text, params) => (seen.push(text), db.query(text, params)) });
    await adapter.fetchEvents(swaps, { fromVersion: 3n });
    await adapter.getEarliestVersion();
    const reads = seen.filter((t) => !t.includes("CREATE"));
    expect(reads).toHaveLength(2);
    expect(reads.every((t) => t.includes("streamd:pruned-before"))).toBe(true);
  });

  it("still passes checkAdapter after pruning", async () => {
    const { adapter } = await pruned(5n);
    const report = await checkAdapter(adapter, swaps);
    expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
  });
});
