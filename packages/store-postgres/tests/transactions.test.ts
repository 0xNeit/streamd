import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "bun:test";
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
import { PostgresEventStore, PostgresStoreAdapter } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

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
const DEPOSIT = "0x1::coin::DepositEvent";

const ORIGINAL = [
  `CREATE TABLE streamd_events (
    version numeric(20, 0) NOT NULL,
    event_index integer NOT NULL,
    type text NOT NULL,
    sender text,
    guid_address text,
    guid_creation numeric(39, 0),
    event text NOT NULL,
    PRIMARY KEY (version, event_index)
  )`,
  "CREATE INDEX streamd_events_type ON streamd_events (type, version, event_index)",
  "CREATE INDEX streamd_events_sender ON streamd_events (sender, version, event_index) WHERE sender IS NOT NULL",
  `CREATE INDEX streamd_events_guid ON streamd_events (guid_address, guid_creation, version, event_index)
    WHERE guid_address IS NOT NULL`,
  "CREATE TABLE streamd_cursors (name text PRIMARY KEY, cursor text NOT NULL)",
];

// The schema the release before this one left: the original plus the resource address.
const WITH_ADDRESS = [
  ...ORIGINAL,
  "ALTER TABLE streamd_events ADD COLUMN account_address text",
  `CREATE INDEX streamd_events_resource ON streamd_events (account_address, type, version, event_index)
    WHERE account_address IS NOT NULL`,
];

// The backfill the README gives, exactly.
const BACKFILL = `UPDATE streamd_events
SET transaction_hash = CASE
  WHEN event::json->>'transactionHash' ~ '^0x[0-9a-fA-F]+$' THEN lower(event::json->>'transactionHash')
  ELSE event::json->>'transactionHash' END
WHERE transaction_hash IS NULL AND account_address IS NULL AND event LIKE '%"transactionHash"%'
  AND strpos(event, '\\u0000') = 0`;

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
async function older(schema: readonly string[]): Promise<PGlite> {
  const db = new PGlite();
  for (const statement of schema) await db.query(statement);
  const row = JSON.stringify({ type: SWAP, data: {}, version: "5", sequenceNumber: "0", eventIndex: 1, transactionHash: UPPER });
  await db.query("INSERT INTO streamd_events (version, event_index, type, event) VALUES (5, 1, $1, $2)", [SWAP, row]);
  await db.query("INSERT INTO streamd_cursors VALUES ('kept', '7')");
  return db;
}

async function columns(db: PostgresClient): Promise<string[]> {
  const { rows } = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'streamd_events'");
  return rows.map((r) => String(r.column_name));
}

async function indexDef(db: PostgresClient, name: string): Promise<string | undefined> {
  const { rows } = await db.query("SELECT indexdef FROM pg_indexes WHERE indexname = $1", [name]);
  return rows[0] === undefined ? undefined : String(rows[0].indexdef);
}

const ids = (events: StreamdEvent[] | undefined) => (events ?? []).map(eventId);

describe("PostgresEventStore transaction hashes", () => {
  for (const [label, schema] of [
    ["the original schema", ORIGINAL],
    ["the schema with account_address", WITH_ADDRESS],
  ] as const) {
    it(`migrates a database made by ${label}, keeping its rows`, async () => {
      const db = await older(schema);
      const store = new PostgresEventStore(db);
      await store.write([ev(6n, 0, OTHER)]);
      expect(await columns(db)).toEqual(expect.arrayContaining(["account_address", "transaction_hash"]));
      expect(await indexDef(db, "streamd_events_transaction")).toContain("(transaction_hash, version, event_index)");
      expect(await indexDef(db, "streamd_events_resource")).toContain("(account_address, type, version, event_index)");
      expect(await store.readCursor("kept")).toBe("7");

      const adapter = new PostgresStoreAdapter(db);
      expect(ids((await adapter.fetchEvents({ kind: "module", eventType: SWAP }, {})).events)).toEqual(["5:1", "6:0"]);
      // The old row is not backfilled, so its hash is not found until the owner fills it.
      expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();
      expect(ids((await adapter.getTransactionEvents(OTHER))?.events)).toEqual(["6:0"]);
      // A row holding an escaped NUL is left out rather than failing the statement.
      const nul = JSON.stringify({ type: SWAP, data: { s: "a\u0000b" }, version: "4", sequenceNumber: "0", eventIndex: 0, transactionHash: OTHER });
      await db.query("INSERT INTO streamd_events (version, event_index, type, event) VALUES (4, 0, $1, $2)", [SWAP, nul]);
      await db.query(BACKFILL);
      expect(ids((await adapter.getTransactionEvents(HASH))?.events)).toEqual(["5:1"]);
      expect(ids((await adapter.getTransactionEvents(OTHER))?.events)).toEqual(["6:0"]);
    });

    it(`lets the adapter migrate a database made by ${label}, and a new client rerun it as a no-op`, async () => {
      const db = await older(schema);
      expect(await new PostgresStoreAdapter(db).getTransactionEvents(HASH)).toBeUndefined();
      expect(await columns(db)).toContain("transaction_hash");
      // A wrapper is a new client to the schema memo, so it runs every statement again.
      const again: PostgresClient = { query: (text, params) => db.query(text, params) };
      await new PostgresEventStore(again).write([ev(6n, 0, HASH)]);
      expect(ids((await new PostgresStoreAdapter(again).getTransactionEvents(HASH))?.events)).toEqual(["6:0"]);
    });
  }

  it("stores an Aptos hash lowercased and a base58 digest verbatim, and a rewrite takes the new hash", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n, 0, UPPER), ev(2n, 0, DIGEST), ev(3n, 0)]);
    const hashes = async () =>
      (await db.query("SELECT transaction_hash AS h FROM streamd_events ORDER BY version")).rows;
    expect(await hashes()).toEqual([{ h: HASH }, { h: DIGEST }, { h: null }]);
    await store.write([ev(3n, 0, OTHER)]);
    expect(await hashes()).toEqual([{ h: HASH }, { h: DIGEST }, { h: OTHER }]);
    // The column follows the stored JSON, so a rewrite without a hash clears it.
    await store.write([ev(3n, 0)]);
    expect(await hashes()).toEqual([{ h: HASH }, { h: DIGEST }, { h: null }]);
  });
});

describe("PostgresStoreAdapter transaction lookups", () => {
  async function seeded() {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    // Written out of order on purpose; a resource change of HASH, and one of another transaction.
    // 10:1 is a handle event of HASH's transaction, which carries no hash.
    await store.write([ev(10n, 3, HASH, FEE), ev(10n, 1, undefined, DEPOSIT), ev(10n, 0, HASH), ev(11n, 0, OTHER), ev(12n, 0, DIGEST)]);
    await store.write([ev(10n, 5, HASH, COIN)], undefined, { accountAddress: A });
    return { db, store, adapter: new PostgresStoreAdapter(db) };
  }

  it("finds a stored transaction by hash, in event order, with its hashless events and without its resource changes", async () => {
    const { adapter } = await seeded();
    const found = await adapter.getTransactionEvents(UPPER);
    expect(found?.version).toBe(10n);
    expect(found?.success).toBe(true);
    expect(found?.vmStatus).toBe("");
    expect(ids(found?.events)).toEqual(["10:0", "10:1", "10:3"]);
    expect(found?.events[0]).toEqual(ev(10n, 0, HASH));
  });

  it("keeps a base58 digest case-sensitive", async () => {
    const { adapter } = await seeded();
    expect(ids((await adapter.getTransactionEvents(DIGEST))?.events)).toEqual(["12:0"]);
    expect(await adapter.getTransactionEvents(SWAPPED)).toBeUndefined();
  });

  it("answers undefined for a hash it does not hold, and refuses a malformed one", async () => {
    const { adapter } = await seeded();
    expect(await adapter.getTransactionEvents("0x" + "ef".repeat(32))).toBeUndefined();
    await expect(adapter.getTransactionEvents("")).rejects.toThrow(StreamdError);
    await expect(adapter.getTransactionEvents(7 as unknown as string)).rejects.toThrow(StreamdError);
  });

  it("finds a stored transaction by version, and keeps the pruned edge", async () => {
    const { store, adapter } = await seeded();
    expect(ids((await adapter.getTransactionEventsByVersion(10n))?.events)).toEqual(["10:0", "10:1", "10:3"]);
    expect(await adapter.getTransactionEventsByVersion(10n)).toEqual(await adapter.getTransactionEvents(HASH));
    expect(await adapter.getTransactionEventsByVersion(13n)).toBeUndefined();
    await expect(adapter.getTransactionEventsByVersion(-1n)).rejects.toThrow(StreamdError);
    await store.prune({ beforeVersion: 11n });
    expect(await adapter.getTransactionEvents(HASH)).toBeUndefined();
    const pruned = adapter.getTransactionEventsByVersion(10n);
    await expect(pruned).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(pruned).rejects.toMatchObject({ statusCode: 410 });
    expect(ids((await adapter.getTransactionEventsByVersion(11n))?.events)).toEqual(["11:0"]);
  });

  it("serves eventsOf and expectEvents from the store", async () => {
    const { adapter } = await seeded();
    const limiter = new RequestLimiter();
    const events = await eventsOf(adapter, limiter, 100, HASH, { kind: "module", eventType: SWAP }, {});
    expect(ids(events)).toEqual(["10:0"]);
    const expectation = expectEvents(adapter, limiter, 100, HASH, {});
    expect(ids(await expectation.toEmit({ kind: "module", eventType: FEE }))).toEqual(["10:3"]);
    await expectation.notToEmit({ kind: "module", eventType: "0x1::dex::Other" });
    await expect(expectation.toEmit({ kind: "module", eventType: "0x1::dex::Other" })).rejects.toBeInstanceOf(StreamdAssertionError);
    await expect(eventsOf(adapter, limiter, 100, "0x" + "ee".repeat(32), undefined, { timeout: 250 })).rejects.toBeInstanceOf(
      StreamdTimeoutError,
    );
  });
});
