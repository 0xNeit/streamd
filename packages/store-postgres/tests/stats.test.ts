import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";
import { PostgresEventStore } from "../src/index.js";
import type { StoreStats } from "../src/index.js";

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

// The same sequence and expected answers as the SQLite suite, so the two stores are held to one result.
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

async function write(store: PostgresEventStore): Promise<void> {
  await store.write([ev(9n, 0, B), ev(3n, 0, A), ev(5n, 1, A), ev(7n, 0, B), ev(4n, 0, C)]);
  await store.write([ev(6n, 2, COIN)], undefined, { accountAddress: HOLDER });
  await store.writeTransfers([tr(5n, 1), tr(8n, 0)]);
}

async function tableNames(db: PGlite): Promise<string[]> {
  const { rows } = await db.query<{ tablename: string }>(
    "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() ORDER BY tablename",
  );
  return rows.map((r) => r.tablename);
}

describe("PostgresEventStore.stats", () => {
  it("reports an empty store with no bounds", async () => {
    const store = new PostgresEventStore(new PGlite());
    expect(await store.stats()).toEqual({ events: 0n, resourceChanges: 0n, transfers: 0n, prunedBefore: 0n });
    expect(await store.stats({ byType: true })).toEqual({
      events: 0n,
      resourceChanges: 0n,
      transfers: 0n,
      prunedBefore: 0n,
      byType: [],
    });
  });

  it("counts events, resource changes and transfers apart, with bounds and per-type counts", async () => {
    const store = new PostgresEventStore(new PGlite());
    await write(store);
    expect(await store.stats({ byType: true })).toEqual(WRITTEN);
    const { byType, ...totals } = WRITTEN;
    expect(await store.stats()).toEqual(totals);
    expect(await store.stats({ byType: false })).toEqual(totals);
    // A redelivery replaces rows, so nothing is counted twice.
    await write(store);
    expect(await store.stats({ byType: true })).toEqual(WRITTEN);
  });

  it("reports the pruned edge, and a row written below it afterwards", async () => {
    const store = new PostgresEventStore(new PGlite());
    await write(store);
    await store.prune({ beforeVersion: 5n });
    expect(await store.stats({ byType: true })).toEqual(PRUNED);
    // A lower prune leaves the edge where it is.
    await store.prune({ beforeVersion: 1n });
    expect(await store.stats({ byType: true })).toEqual(PRUNED);
    await store.write([ev(2n, 0, C)]);
    expect(await store.stats({ byType: true })).toEqual(LATE);
  });

  it("counts transfers stored without any event", async () => {
    const store = new PostgresEventStore(new PGlite());
    await store.writeTransfers([tr(1n, 0)]);
    expect(await store.stats()).toEqual({ events: 0n, resourceChanges: 0n, transfers: 1n, prunedBefore: 0n });
  });

  it("keeps exact counts and versions at the top of the u64 range", async () => {
    const store = new PostgresEventStore(new PGlite());
    const top = 2n ** 64n - 1n;
    await store.write([ev(top, 0, A), ev(top - 1n, 0, A)]);
    await store.prune({ beforeVersion: top - 1n });
    expect(await store.stats()).toMatchObject({ events: 2n, lowestVersion: top - 1n, highestVersion: top, prunedBefore: top - 1n });
  });

  it("answers a database made by the original schema, migrating it as every read does", async () => {
    const db = new PGlite();
    await db.exec(`
      CREATE TABLE streamd_events (
        version numeric(20, 0) NOT NULL, event_index integer NOT NULL, type text NOT NULL, sender text,
        guid_address text, guid_creation numeric(39, 0), event text NOT NULL, PRIMARY KEY (version, event_index)
      );
      CREATE TABLE streamd_cursors (name text PRIMARY KEY, cursor text NOT NULL);
      INSERT INTO streamd_events (version, event_index, type, event) VALUES (12, 0, '${A}', '{}'), (10, 0, '${B}', '{}'), (11, 0, '${A}', '{}');
      INSERT INTO streamd_cursors VALUES ('streamd:pruned-before', '10');`);
    const store = new PostgresEventStore(db);
    expect(await store.stats({ byType: true })).toEqual({
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
    expect(await tableNames(db)).toEqual(["streamd_cursors", "streamd_events", "streamd_transfers"]);
  });

  it("refuses options that are not an object with a boolean byType, before any query", async () => {
    let queries = 0;
    const client = { query: async () => (queries++, { rows: [] }) };
    const store = new PostgresEventStore(client);
    await expect(store.stats({ byType: "yes" as unknown as boolean })).rejects.toBeInstanceOf(StreamdError);
    await expect(store.stats(null as unknown as undefined)).rejects.toBeInstanceOf(StreamdError);
    await expect(store.stats(true as unknown as undefined)).rejects.toBeInstanceOf(StreamdError);
    expect(queries).toBe(0);
  });
});
