import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  RequestLimiter,
  StreamdAssertionError,
  StreamdError,
  StreamdNetworkError,
  StreamdTimeoutError,
  eventId,
  eventsOf,
  expectEvents,
} from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { SqliteEventStore, StoreAdapter } from "../src/index.js";
import { versionKey } from "../src/schema.js";
import type { SqliteDatabase } from "../src/schema.js";

const A = "0x" + "0".repeat(63) + "a";
const HASH = "0x" + "ab".repeat(32);
const UPPER = "0x" + "AB".repeat(32);
const OTHER = "0x" + "cd".repeat(32);
// Base58 is case-sensitive: these two are different digests.
const DIGEST = "5QCqxf87tpC2WqD2hvZu1dPnrDa6ukeSztVDtjH3UteV";
const SWAPPED = "5qcQXF87TPc2wQd2HVzU1DpNRdA6UKEsZTvdTJh3uTEv";
const SWAP = "0x1::dex::Swap";
const FEE = "0x1::transaction_fee::FeeStatement";
const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";

const ORIGINAL = `
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

// The schema the release before this one left: the original plus the resource address.
const WITH_ADDRESS = `${ORIGINAL}
ALTER TABLE streamd_events ADD COLUMN account_address TEXT;
CREATE INDEX streamd_events_resource ON streamd_events (account_address, type, version, event_index)
  WHERE account_address IS NOT NULL;
`;

// The backfill the README gives, exactly.
const BACKFILL = `UPDATE streamd_events
SET transaction_hash = CASE
  WHEN json_extract(event, '$.transactionHash') LIKE '0x%' THEN lower(json_extract(event, '$.transactionHash'))
  ELSE json_extract(event, '$.transactionHash') END
WHERE transaction_hash IS NULL AND account_address IS NULL AND event LIKE '%"transactionHash"%'`;

function ev(version: bigint, eventIndex: number, transactionHash?: string, type = SWAP): StreamdEvent {
  return {
    type,
    data: { amount: String(eventIndex) },
    version,
    sequenceNumber: 0n,
    eventIndex,
    ...(transactionHash !== undefined && { transactionHash }),
  };
}

// An older database holding one event of HASH, its JSON carrying the hash, and a cursor.
function older(db: Database, schema: string): void {
  db.exec(schema);
  const row = JSON.stringify({ type: SWAP, data: {}, version: "5", sequenceNumber: "0", eventIndex: 1, transactionHash: UPPER });
  db.prepare("INSERT INTO streamd_events (version, event_index, type, event) VALUES (?, 1, ?, ?)").run(versionKey(5n), SWAP, row);
  db.prepare("INSERT INTO streamd_cursors VALUES ('kept', '7')").run();
}

function columns(db: Database): string[] {
  return db.query("SELECT name FROM pragma_table_info('streamd_events')").all().map((r) => (r as { name: string }).name);
}

function indexSql(db: Database, name: string): string | undefined {
  return (db.query("SELECT sql FROM sqlite_master WHERE name = ?").get(name) as { sql: string } | null)?.sql;
}

const ids = (events: StreamdEvent[] | undefined) => (events ?? []).map(eventId);

describe("SqliteEventStore transaction hashes", () => {
  for (const [label, schema] of [
    ["the original schema", ORIGINAL],
    ["the schema with account_address", WITH_ADDRESS],
  ] as const) {
    it(`migrates a database made by ${label} on first write, keeping its rows`, async () => {
      const db = new Database(":memory:");
      older(db, schema);
      const store = new SqliteEventStore(db);
      const adapter = new StoreAdapter(db);
      expect(columns(db)).not.toContain("transaction_hash");
      expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();

      store.write([ev(6n, 0, OTHER)]);
      expect(columns(db)).toEqual(expect.arrayContaining(["account_address", "transaction_hash"]));
      expect(indexSql(db, "streamd_events_transaction")).toContain("transaction_hash, version, event_index");
      expect(indexSql(db, "streamd_events_resource")).toContain("account_address, type, version, event_index");
      expect(store.readCursor("kept")).toBe("7");
      expect(ids((await adapter.fetchEvents({ kind: "module", eventType: SWAP }, {})).events)).toEqual(["5:1", "6:0"]);
      // The old row is not backfilled, so its hash is not found until the owner fills it.
      expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();
      expect(ids((await adapter.getTransactionEvents(OTHER))?.events)).toEqual(["6:0"]);

      db.exec(BACKFILL);
      expect(ids((await adapter.getTransactionEvents(HASH))?.events)).toEqual(["5:1"]);
      // A second store finds nothing left to migrate.
      expect(() => new SqliteEventStore(db).write([ev(7n, 0, HASH)])).not.toThrow();
    });
  }

  it("carries on when another connection added the column first, and rethrows any other failure", () => {
    const db = new Database(":memory:");
    new SqliteEventStore(db).write([]);
    let stale = true;
    const racing: SqliteDatabase = {
      exec: (sql) => db.exec(sql),
      prepare: (sql) => (sql.includes("pragma_table_info") && stale ? ((stale = false), db.prepare("SELECT 1 WHERE 0")) : db.prepare(sql)),
    };
    expect(() => new SqliteEventStore(racing).write([ev(1n, 0, HASH)])).not.toThrow();

    const old = new Database(":memory:");
    older(old, WITH_ADDRESS);
    const failing: SqliteDatabase = {
      exec: (sql) => {
        if (sql.includes("ADD COLUMN transaction_hash")) throw new Error("disk full");
        return old.exec(sql);
      },
      prepare: (sql) => old.prepare(sql),
    };
    expect(() => new SqliteEventStore(failing).write([ev(1n, 0, HASH)])).toThrow("disk full");
    expect(columns(old)).not.toContain("transaction_hash");
  });

  it("migrates again after a caller's transaction rolls back the first write", async () => {
    const db = new Database(":memory:");
    older(db, ORIGINAL);
    const store = new SqliteEventStore(db);
    expect(() =>
      db.transaction(() => {
        store.write([ev(6n, 0, HASH)]);
        throw new Error("caller rolls back");
      })(),
    ).toThrow("caller rolls back");
    expect(columns(db)).not.toContain("transaction_hash");
    store.write([ev(6n, 0, HASH)]);
    expect(ids((await new StoreAdapter(db).getTransactionEvents(HASH))?.events)).toEqual(["6:0"]);
  });

  it("stores an Aptos hash lowercased and a base58 digest verbatim, and a rewrite takes the new hash", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n, 0, UPPER), ev(2n, 0, DIGEST), ev(3n, 0)]);
    const hashes = () => db.query("SELECT transaction_hash AS h FROM streamd_events ORDER BY version").all();
    expect(hashes()).toEqual([{ h: HASH }, { h: DIGEST }, { h: null }]);
    store.write([ev(3n, 0, OTHER)]);
    expect(hashes()).toEqual([{ h: HASH }, { h: DIGEST }, { h: OTHER }]);
  });
});

describe("StoreAdapter transaction lookups", () => {
  function seeded() {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    // Written out of order on purpose; a resource change of HASH, and one of another transaction.
    store.write([ev(10n, 3, HASH, FEE), ev(10n, 0, HASH), ev(11n, 0, OTHER), ev(12n, 0, DIGEST)]);
    store.write([ev(10n, 5, HASH, COIN)], undefined, { accountAddress: A });
    return { db, store, adapter: new StoreAdapter(db) };
  }

  it("finds a stored transaction by hash, in event order, without its resource changes", async () => {
    const { adapter } = seeded();
    const found = await adapter.getTransactionEvents(UPPER);
    expect(found?.version).toBe(10n);
    expect(found?.success).toBe(true);
    expect(found?.vmStatus).toBe("");
    expect(ids(found?.events)).toEqual(["10:0", "10:3"]);
    expect(found?.events[0]).toEqual(ev(10n, 0, HASH));
  });

  it("keeps a base58 digest case-sensitive", async () => {
    const { adapter } = seeded();
    expect(ids((await adapter.getTransactionEvents(DIGEST))?.events)).toEqual(["12:0"]);
    expect(await adapter.getTransactionEvents(SWAPPED)).toBeUndefined();
  });

  it("answers undefined for a hash it does not hold, and refuses a malformed one", async () => {
    const { adapter } = seeded();
    expect(await adapter.getTransactionEvents("0x" + "ef".repeat(32))).toBeUndefined();
    await expect(adapter.getTransactionEvents("")).rejects.toThrow(StreamdError);
    await expect(adapter.getTransactionEvents(7 as unknown as string)).rejects.toThrow(StreamdError);
  });

  it("finds a stored transaction by version, and keeps the pruned edge", async () => {
    const { store, adapter } = seeded();
    expect(ids((await adapter.getTransactionEventsByVersion(10n))?.events)).toEqual(["10:0", "10:3"]);
    expect(await adapter.getTransactionEventsByVersion(13n)).toBeUndefined();
    await expect(adapter.getTransactionEventsByVersion(-1n)).rejects.toThrow(StreamdError);
    store.prune({ beforeVersion: 11n });
    expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();
    const pruned = adapter.getTransactionEventsByVersion(10n);
    await expect(pruned).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(pruned).rejects.toMatchObject({ statusCode: 410 });
    expect(ids((await adapter.getTransactionEventsByVersion(11n))?.events)).toEqual(["11:0"]);
  });

  it("answers from an unmigrated read-only database without altering it", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-"));
    const path = join(dir, "events.db");
    try {
      const writable = new Database(path);
      older(writable, ORIGINAL);
      writable.close();
      const readonly = new Database(path, { readonly: true });
      const adapter = new StoreAdapter(readonly);
      expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();
      expect(ids((await adapter.getTransactionEventsByVersion(5n))?.events)).toEqual(["5:1"]);
      expect(columns(readonly)).not.toContain("transaction_hash");
      readonly.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("serves eventsOf and expectEvents from the store", async () => {
    const { adapter } = seeded();
    const limiter = new RequestLimiter();
    const swaps = { kind: "module", eventType: SWAP } as const;
    const events = await eventsOf(adapter, limiter, 100, HASH, swaps, {});
    expect(ids(events)).toEqual(["10:0"]);
    const expectation = expectEvents(adapter, limiter, 100, HASH, {});
    expect(ids(await expectation.toEmit({ kind: "module", eventType: FEE }))).toEqual(["10:3"]);
    await expectation.notToEmit({ kind: "module", eventType: "0x1::dex::Other" });
    await expect(expectation.toEmit({ kind: "module", eventType: "0x1::dex::Other" })).rejects.toBeInstanceOf(StreamdAssertionError);
    // A hash the store never caught waits out its timeout.
    await expect(eventsOf(adapter, limiter, 100, OTHER.replace("cd", "ee"), undefined, { timeout: 250 })).rejects.toBeInstanceOf(
      StreamdTimeoutError,
    );
  });
});
