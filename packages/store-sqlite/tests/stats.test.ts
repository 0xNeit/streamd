import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";
import { SqliteEventStore } from "../src/index.js";
import type { StoreStats } from "../src/index.js";
import { versionKey } from "../src/schema.js";

const A = "0x1::a::A";
const B = "0x1::b::B";
const C = "0x1::c::C";
const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
const HOLDER = "0x" + "0".repeat(63) + "a";

function ev(version: bigint, eventIndex: number, type: string): StreamdEvent {
  return { type, data: {}, version, sequenceNumber: 0n, eventIndex };
}

function tr(version: bigint, eventIndex: number): StreamdEvent<Transfer> {
  return { type: "0x1::fungible_asset::Deposit", data: { asset: "0xa", amount: 5n }, version, sequenceNumber: 0n, eventIndex };
}

// The same sequence and expected answers as the Postgres suite, so the two stores are held to one result.
const WRITTEN: StoreStats = {
  events: 5n,
  resourceChanges: 1n,
  transfers: 2n,
  lowestVersion: 3n,
  highestVersion: 9n,
  prunedBefore: 0n,
  byType: [
    { type: A, count: 2n },
    { type: B, count: 2n },
    { type: C, count: 1n },
  ],
};
const PRUNED: StoreStats = {
  events: 3n,
  resourceChanges: 1n,
  transfers: 2n,
  lowestVersion: 5n,
  highestVersion: 9n,
  prunedBefore: 5n,
  byType: [
    { type: B, count: 2n },
    { type: A, count: 1n },
  ],
};
const LATE: StoreStats = { ...PRUNED, events: 4n, lowestVersion: 2n, byType: [...PRUNED.byType!, { type: C, count: 1n }] };

function write(store: SqliteEventStore): void {
  store.write([ev(9n, 0, B), ev(3n, 0, A), ev(5n, 1, A), ev(7n, 0, B), ev(4n, 0, C)]);
  store.write([ev(6n, 2, COIN)], undefined, { accountAddress: HOLDER });
  store.writeTransfers([tr(5n, 1), tr(8n, 0)]);
}

function tableNames(db: Database): string[] {
  return db.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all().map((r) => (r as { name: string }).name);
}

describe("SqliteEventStore.stats", () => {
  it("reports an empty store with no bounds, without creating the transfer table", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    expect(store.stats()).toEqual({ events: 0n, resourceChanges: 0n, transfers: 0n, prunedBefore: 0n });
    expect(store.stats({ byType: true })).toEqual({ events: 0n, resourceChanges: 0n, transfers: 0n, prunedBefore: 0n, byType: [] });
    expect(tableNames(db)).not.toContain("streamd_transfers");
  });

  it("counts events, resource changes and transfers apart, with bounds and per-type counts", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    write(store);
    expect(store.stats({ byType: true })).toEqual(WRITTEN);
    const { byType, ...totals } = WRITTEN;
    expect(store.stats()).toEqual(totals);
    expect(store.stats({ byType: false })).toEqual(totals);
    // A redelivery replaces rows, so nothing is counted twice.
    write(store);
    expect(store.stats({ byType: true })).toEqual(WRITTEN);
  });

  it("reports the pruned edge, and a row written below it afterwards", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    write(store);
    store.prune({ beforeVersion: 5n });
    expect(store.stats({ byType: true })).toEqual(PRUNED);
    // A lower prune leaves the edge where it is.
    store.prune({ beforeVersion: 1n });
    expect(store.stats({ byType: true })).toEqual(PRUNED);
    store.write([ev(2n, 0, C)]);
    expect(store.stats({ byType: true })).toEqual(LATE);
  });

  it("counts transfers stored without any event", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    store.writeTransfers([tr(1n, 0)]);
    expect(store.stats()).toEqual({ events: 0n, resourceChanges: 0n, transfers: 1n, prunedBefore: 0n });
  });

  it("answers inside a caller's transaction and leaves it open", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec("BEGIN");
    store.write([ev(1n, 0, A)]);
    expect(store.stats().events).toBe(1n);
    db.exec("ROLLBACK");
    expect(store.stats().events).toBe(0n);
  });

  it("answers from a read-only database made by the original schema without changing it", () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-"));
    const path = join(dir, "events.db");
    try {
      const writable = new Database(path);
      writable.exec(`
        CREATE TABLE streamd_events (
          version TEXT NOT NULL, event_index INTEGER NOT NULL, type TEXT NOT NULL, sender TEXT,
          guid_address TEXT, guid_creation TEXT, event TEXT NOT NULL, PRIMARY KEY (version, event_index)
        ) WITHOUT ROWID;
        CREATE INDEX streamd_events_type ON streamd_events (type, version, event_index);
        CREATE INDEX streamd_events_sender ON streamd_events (sender, version, event_index) WHERE sender IS NOT NULL;
        CREATE INDEX streamd_events_guid ON streamd_events (guid_address, guid_creation, version, event_index)
          WHERE guid_address IS NOT NULL;
        CREATE TABLE streamd_cursors (name TEXT PRIMARY KEY, cursor TEXT NOT NULL);`);
      const insert = writable.prepare("INSERT INTO streamd_events (version, event_index, type, event) VALUES (?, 0, ?, '{}')");
      insert.run(versionKey(12n), A);
      insert.run(versionKey(10n), B);
      insert.run(versionKey(11n), A);
      writable.prepare("INSERT INTO streamd_cursors VALUES ('streamd:pruned-before', ?)").run(versionKey(10n));
      writable.close();

      const readonly = new Database(path, { readonly: true });
      const store = new SqliteEventStore(readonly);
      expect(store.stats({ byType: true })).toEqual({
        events: 3n,
        resourceChanges: 0n,
        transfers: 0n,
        lowestVersion: 10n,
        highestVersion: 12n,
        prunedBefore: 10n,
        byType: [
          { type: A, count: 2n },
          { type: B, count: 1n },
        ],
      });
      const columns = readonly.query("SELECT name FROM pragma_table_info('streamd_events')").all();
      expect(columns.map((r) => (r as { name: string }).name)).not.toContain("account_address");
      expect(tableNames(readonly)).toEqual(["streamd_cursors", "streamd_events"]);
      readonly.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses options that are not an object with a boolean byType", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    expect(() => store.stats({ byType: "yes" as unknown as boolean })).toThrow(StreamdError);
    expect(() => store.stats(null as unknown as undefined)).toThrow(StreamdError);
    expect(() => store.stats(true as unknown as undefined)).toThrow(StreamdError);
  });
});
