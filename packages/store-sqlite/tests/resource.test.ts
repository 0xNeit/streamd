import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, checkAdapter, eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent } from "@streamd/core";
import { SqliteEventStore, StoreAdapter } from "../src/index.js";
import { versionKey } from "../src/schema.js";
import type { SqliteDatabase } from "../src/schema.js";

const A = "0x" + "0".repeat(63) + "a";
const B = "0x" + "b".repeat(64);
const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
const ACCOUNT = "0x1::account::Account";

// The previous release's event schema, exactly, with no account_address.
const OLD_SCHEMA = `
CREATE TABLE streamd_events (
  version TEXT NOT NULL,
  event_index INTEGER NOT NULL,
  type TEXT NOT NULL,
  sender TEXT,
  guid_address TEXT,
  guid_creation TEXT,
  event TEXT NOT NULL,
  PRIMARY KEY (version, event_index)
) WITHOUT ROWID;
CREATE INDEX streamd_events_type ON streamd_events (type, version, event_index);
CREATE INDEX streamd_events_sender ON streamd_events (sender, version, event_index) WHERE sender IS NOT NULL;
CREATE INDEX streamd_events_guid ON streamd_events (guid_address, guid_creation, version, event_index)
  WHERE guid_address IS NOT NULL;
CREATE TABLE streamd_cursors (name TEXT PRIMARY KEY, cursor TEXT NOT NULL);
`;

function change(version: bigint, eventIndex: number, type = COIN, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type, data: { coin: { value: String(version) } }, version, sequenceNumber: 0n, eventIndex, ...extra };
}

// What an older release left: a resource change stored with no address, and a plain event.
function oldDatabase(db: Database): void {
  db.exec(OLD_SCHEMA);
  const insert = db.prepare("INSERT INTO streamd_events VALUES (?, ?, ?, NULL, NULL, NULL, ?)");
  insert.run(versionKey(1n), 2, COIN, JSON.stringify({ type: COIN, data: null, version: "1", sequenceNumber: "0", eventIndex: 2 }));
  insert.run(versionKey(1n), 0, "0x1::m::E", JSON.stringify({ type: "0x1::m::E", data: {}, version: "1", sequenceNumber: "0", eventIndex: 0 }));
  db.prepare("INSERT INTO streamd_cursors VALUES ('kept', '7')").run();
}

function columns(db: Database): string[] {
  return db.query("SELECT name FROM pragma_table_info('streamd_events')").all().map((r) => (r as { name: string }).name);
}

const coinAtA: EventDescriptor = { kind: "resource", accountAddress: "0xa", resourceType: COIN };
const ids = (events: StreamdEvent[]) => events.map(eventId);

// Versions 2..9: A's coin store at index 3, B's at index 4, A's account at index 5.
const CHANGES = Array.from({ length: 8 }, (_, i) => BigInt(i + 2));

function seeded(): { db: Database; store: SqliteEventStore; adapter: StoreAdapter } {
  const db = new Database(":memory:");
  const store = new SqliteEventStore(db);
  store.write(CHANGES.map((v) => change(v, 3)), undefined, { accountAddress: "0xA" });
  store.write(CHANGES.map((v) => change(v, 4)), undefined, { accountAddress: B });
  store.write(CHANGES.map((v) => change(v, 5, ACCOUNT)), undefined, { accountAddress: A });
  return { db, store, adapter: new StoreAdapter(db) };
}

describe("SqliteEventStore resource addresses", () => {
  it("migrates a database made by the previous schema on first write, keeping its rows", async () => {
    const db = new Database(":memory:");
    oldDatabase(db);
    const store = new SqliteEventStore(db);
    expect(columns(db)).not.toContain("account_address");
    expect(store.readCursor("kept")).toBe("7");

    store.write([change(3n, 1)], undefined, { accountAddress: A });
    expect(columns(db)).toContain("account_address");
    const index = db.query("SELECT sql FROM sqlite_master WHERE name = 'streamd_events_resource'").get() as { sql: string };
    expect(index.sql).toContain("account_address, type, version, event_index");

    const adapter = new StoreAdapter(db);
    const all = await adapter.fetchEvents({ kind: "module", eventType: [COIN, "0x1::m::E"] }, { limit: 10 });
    expect(ids(all.events)).toEqual(["1:0", "1:2", "3:1"]);
    // The old row has no address, so only the new one is served.
    expect(ids((await adapter.fetchEvents(coinAtA, {})).events)).toEqual(["3:1"]);

    // A second store over a migrated database finds nothing to do.
    new SqliteEventStore(db).write([change(4n, 1)], undefined, { accountAddress: A });
    expect(ids((await adapter.fetchEvents(coinAtA, {})).events)).toEqual(["3:1", "4:1"]);
  });

  it("carries on when another connection migrated first, and rethrows any other failure", () => {
    const db = new Database(":memory:");
    new SqliteEventStore(db).write([]);
    // Reports the column missing once, as a connection that checked before another migrated would.
    let stale = true;
    const racing: SqliteDatabase = {
      exec: (sql) => db.exec(sql),
      prepare: (sql) => (sql.includes("pragma_table_info") && stale ? ((stale = false), db.prepare("SELECT 1 WHERE 0")) : db.prepare(sql)),
    };
    expect(() => new SqliteEventStore(racing).write([change(1n, 0)], undefined, { accountAddress: A })).not.toThrow();

    const old = new Database(":memory:");
    oldDatabase(old);
    const failing: SqliteDatabase = {
      exec: (sql) => {
        if (sql.includes("ALTER TABLE")) throw new Error("disk full");
        return old.exec(sql);
      },
      prepare: (sql) => old.prepare(sql),
    };
    expect(() => new SqliteEventStore(failing).write([change(1n, 0)])).toThrow("disk full");
    expect(columns(old)).not.toContain("account_address");
  });

  it("migrates again after a caller's transaction rolls back the first write", async () => {
    const db = new Database(":memory:");
    oldDatabase(db);
    const store = new SqliteEventStore(db);
    expect(() =>
      db.transaction(() => {
        store.write([change(2n, 1)], undefined, { accountAddress: A });
        throw new Error("caller rolls back");
      })(),
    ).toThrow("caller rolls back");
    expect(columns(db)).not.toContain("account_address");
    store.write([change(3n, 1)], undefined, { accountAddress: A });
    expect(ids((await new StoreAdapter(db).fetchEvents(coinAtA, {})).events)).toEqual(["3:1"]);
  });

  it("lets an adapter built before the migration serve what is written after it", async () => {
    const db = new Database(":memory:");
    oldDatabase(db);
    const adapter = new StoreAdapter(db);
    expect((await adapter.fetchEvents(coinAtA, {})).events).toEqual([]);
    new SqliteEventStore(db).write([change(2n, 1)], undefined, { accountAddress: A });
    expect(ids((await adapter.fetchEvents(coinAtA, {})).events)).toEqual(["2:1"]);
  });

  it("stores the address canonically and keeps it through a rewrite without one", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([change(1n, 0)], undefined, { accountAddress: "0xA" });
    store.write([change(1n, 0, COIN, { state: ["later"] }), change(2n, 0)]);
    const stored = db.query("SELECT version, account_address, event FROM streamd_events ORDER BY version").all() as {
      account_address: string | null;
      event: string;
    }[];
    expect(stored.map((r) => r.account_address)).toEqual([A, null]);
    expect(stored[0]!.event).toContain("later");
    store.write([change(1n, 0)], undefined, { accountAddress: B });
    expect(db.query("SELECT account_address FROM streamd_events WHERE version = ?").get(versionKey(1n))).toEqual({
      account_address: B,
    });
  });

  it("refuses a malformed address before writing anything", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    expect(() => store.write([change(1n, 0)], { name: "c", cursor: "9" }, { accountAddress: "nope" })).toThrow(StreamdError);
    expect(db.query("SELECT count(*) AS n FROM streamd_events").get()).toEqual({ n: 0 });
    expect(store.readCursor("c")).toBeUndefined();
  });

  it("still constructs a store and an adapter over an unmigrated read-only database", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-"));
    const path = join(dir, "events.db");
    try {
      const writable = new Database(path);
      oldDatabase(writable);
      writable.close();
      const readonly = new Database(path, { readonly: true });
      expect(new SqliteEventStore(readonly).cursors()).toEqual([{ name: "kept", cursor: "7" }]);
      const adapter = new StoreAdapter(readonly);
      const page = await adapter.fetchEvents(coinAtA, { fromVersion: 1n });
      expect(page).toEqual({ events: [], nextCursor: "1:-1", hasMore: false });
      expect((await adapter.fetchEvents({ kind: "module", eventType: COIN }, {})).events).toHaveLength(1);
      readonly.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("StoreAdapter resource descriptors", () => {
  it("serves one address's changes of one type, in order", async () => {
    const { adapter } = seeded();
    const page = await adapter.fetchEvents(coinAtA, { limit: 100 });
    expect(ids(page.events)).toEqual(CHANGES.map((v) => `${v}:3`));
    expect(page.events[0]).toEqual(change(2n, 3));
    expect(ids((await adapter.fetchEvents({ ...coinAtA, accountAddress: B }, { limit: 100 })).events)).toEqual(
      CHANGES.map((v) => `${v}:4`),
    );
    expect(ids((await adapter.fetchEvents({ ...coinAtA, resourceType: ACCOUNT }, { limit: 100 })).events)).toEqual(
      CHANGES.map((v) => `${v}:5`),
    );
    // The type matches exactly as written, so the struct alone names nothing.
    expect((await adapter.fetchEvents({ ...coinAtA, resourceType: "0x1::coin::CoinStore" }, {})).events).toEqual([]);
  });

  it("pages with cursors, ranges, offset and desc as every kind does", async () => {
    const { adapter, store } = seeded();
    const first = await adapter.fetchEvents(coinAtA, { limit: 3, fromVersion: 3n });
    expect(ids(first.events)).toEqual(["3:3", "4:3", "5:3"]);
    expect(first).toMatchObject({ nextCursor: "5:3", hasMore: true });
    const second = await adapter.fetchEvents(coinAtA, { limit: 3, cursor: first.nextCursor, toVersion: 7n });
    expect(ids(second.events)).toEqual(["6:3", "7:3"]);
    expect(second.hasMore).toBe(false);
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2, offset: 1 })).events)).toEqual(["3:3", "4:3"]);
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2, orderBy: "desc" })).events)).toEqual(["3:3", "2:3"]);

    store.write([change(10n, 3)], undefined, { accountAddress: A });
    const later = await adapter.fetchEvents(coinAtA, { cursor: `${CHANGES.at(-1)}:3` });
    expect(ids(later.events)).toEqual(["10:3"]);
  });

  it("passes checkAdapter", async () => {
    const { adapter } = seeded();
    for (const options of [{ limit: 2 }, { limit: 3, fromVersion: 4n }]) {
      const report = await checkAdapter(adapter, coinAtA, options);
      expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
    }
  });

  it("refuses a malformed address or resource type", async () => {
    const { adapter } = seeded();
    await expect(adapter.fetchEvents({ ...coinAtA, accountAddress: "nope" }, {})).rejects.toBeInstanceOf(StreamdError);
    await expect(adapter.fetchEvents({ ...coinAtA, resourceType: "coin" as never }, {})).rejects.toBeInstanceOf(StreamdError);
  });

  it("keeps the pruned edge", async () => {
    const { adapter, store } = seeded();
    store.prune({ beforeVersion: 5n });
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2 })).events)).toEqual(["5:3", "6:3"]);
    await expect(adapter.fetchEvents(coinAtA, { fromVersion: 4n })).rejects.toMatchObject({ statusCode: 410 });
  });

  it("answers client queries as MemoryAdapter does over the same changes", async () => {
    const { adapter } = seeded();
    const local = await new StreamdClient({ adapter }).getEvents(coinAtA, { fromVersion: -3n, toVersion: "head" });
    expect(ids(local)).toEqual(["6:3", "7:3", "8:3", "9:3"]);
    const memory = new MemoryAdapter(CHANGES.map((v) => change(v, 3)));
    const same = await new StreamdClient({ adapter: memory }).getEvents(coinAtA, { fromVersion: -3n, toVersion: "head" });
    expect(local).toEqual(same);
  });
});
