import { PGlite } from "./helpers.js";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdError, checkAdapter, eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent } from "@streamd/core";
import { PostgresEventStore, PostgresStoreAdapter } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

const A = "0x" + "0".repeat(63) + "a";
const B = "0x" + "b".repeat(64);
const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
const ACCOUNT = "0x1::account::Account";

// The previous release's event schema, exactly, with no account_address.
const OLD_SCHEMA = [
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

function change(version: bigint, eventIndex: number, type = COIN, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type, data: { coin: { value: String(version) } }, version, sequenceNumber: 0n, eventIndex, ...extra };
}

// What an older release left: a resource change stored with no address, and a plain event.
async function oldDatabase(): Promise<PGlite> {
  const db = new PGlite();
  for (const statement of OLD_SCHEMA) await db.query(statement);
  const insert = "INSERT INTO streamd_events VALUES ($1, $2, $3, NULL, NULL, NULL, $4)";
  await db.query(insert, ["1", 2, COIN, JSON.stringify({ type: COIN, data: null, version: "1", sequenceNumber: "0", eventIndex: 2 })]);
  await db.query(insert, ["1", 0, "0x1::m::E", JSON.stringify({ type: "0x1::m::E", data: {}, version: "1", sequenceNumber: "0", eventIndex: 0 })]);
  await db.query("INSERT INTO streamd_cursors VALUES ('kept', '7')");
  return db;
}

async function columns(db: PostgresClient): Promise<string[]> {
  const { rows } = await db.query(
    "SELECT column_name FROM information_schema.columns WHERE table_name = 'streamd_events' ORDER BY ordinal_position",
  );
  return rows.map((r) => String(r.column_name));
}

const coinAtA: EventDescriptor = { kind: "resource", accountAddress: "0xa", resourceType: COIN };
const ids = (events: StreamdEvent[]) => events.map(eventId);

// Versions 2..9: A's coin store at index 3, B's at index 4, A's account at index 5.
const CHANGES = Array.from({ length: 8 }, (_, i) => BigInt(i + 2));

async function seeded(): Promise<{ db: PGlite; store: PostgresEventStore; adapter: PostgresStoreAdapter }> {
  const db = new PGlite();
  const store = new PostgresEventStore(db);
  await store.write(CHANGES.map((v) => change(v, 3)), undefined, { accountAddress: "0xA" });
  await store.write(CHANGES.map((v) => change(v, 4)), undefined, { accountAddress: B });
  await store.write(CHANGES.map((v) => change(v, 5, ACCOUNT)), undefined, { accountAddress: A });
  return { db, store, adapter: new PostgresStoreAdapter(db) };
}

describe("PostgresEventStore resource addresses", () => {
  it("migrates a database made by the previous schema, keeping its rows", async () => {
    const db = await oldDatabase();
    const store = new PostgresEventStore(db);
    expect(await store.readCursor("kept")).toBe("7");
    expect(await columns(db)).toContain("account_address");
    const { rows } = await db.query<{ indexdef: string }>("SELECT indexdef FROM pg_indexes WHERE indexname = 'streamd_events_resource'");
    expect(String(rows[0]!.indexdef)).toContain("(account_address, type, version, event_index)");

    await store.write([change(3n, 1)], undefined, { accountAddress: A });
    const adapter = new PostgresStoreAdapter(db);
    const all = await adapter.fetchEvents({ kind: "module", eventType: [COIN, "0x1::m::E"] }, { limit: 10 });
    // The old change has no address, so it still reads as an event and only the new one as a change.
    expect(ids(all.events)).toEqual(["1:0", "1:2"]);
    expect(ids((await adapter.fetchEvents(coinAtA, {})).events)).toEqual(["3:1"]);

    // Another client over the same database runs the schema again and finds nothing to do.
    const again: PostgresClient = { query: (text, params) => db.query(text, params) };
    await new PostgresEventStore(again).write([change(4n, 1)], undefined, { accountAddress: A });
    expect(ids((await new PostgresStoreAdapter(again).fetchEvents(coinAtA, {})).events)).toEqual(["3:1", "4:1"]);
  });

  it("is migrated by the adapter too, serving the old rows' resource as empty", async () => {
    const db = await oldDatabase();
    const adapter = new PostgresStoreAdapter(db);
    const page = await adapter.fetchEvents(coinAtA, { fromVersion: 1n });
    expect(page).toEqual({ events: [], nextCursor: "1:-1", hasMore: false });
    expect((await adapter.fetchEvents({ kind: "module", eventType: COIN }, {})).events).toHaveLength(1);
    expect(await columns(db)).toContain("account_address");
  });

  it("stores the address canonically and keeps it through a rewrite without one", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([change(1n, 0)], undefined, { accountAddress: "0xA" });
    await store.write([change(1n, 0, COIN, { state: ["later"] }), change(2n, 0)]);
    const { rows } = await db.query<{ account_address: string | null; event: string }>("SELECT account_address, event FROM streamd_events ORDER BY version");
    expect(rows.map((r) => r.account_address)).toEqual([A, null]);
    expect(String(rows[0]!.event)).toContain("later");
    await store.write([change(1n, 0)], undefined, { accountAddress: B });
    const replaced = await db.query("SELECT account_address FROM streamd_events WHERE version = 1");
    expect(replaced.rows).toEqual([{ account_address: B }]);
  });

  it("refuses a malformed address before sending anything", async () => {
    let sent = 0;
    const store = new PostgresEventStore({ query: async () => (sent++, { rows: [] }) });
    const write = store.write([change(1n, 0)], { name: "c", cursor: "9" }, { accountAddress: "nope" });
    await expect(write).rejects.toBeInstanceOf(StreamdError);
    expect(sent).toBe(0);
  });
});

describe("PostgresStoreAdapter resource descriptors", () => {
  it("serves one address's changes of one type, in order", async () => {
    const { adapter } = await seeded();
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
    const { adapter, store } = await seeded();
    const first = await adapter.fetchEvents(coinAtA, { limit: 3, fromVersion: 3n });
    expect(ids(first.events)).toEqual(["3:3", "4:3", "5:3"]);
    expect(first).toMatchObject({ nextCursor: "5:3", hasMore: true });
    const second = await adapter.fetchEvents(coinAtA, { limit: 3, cursor: first.nextCursor, toVersion: 7n });
    expect(ids(second.events)).toEqual(["6:3", "7:3"]);
    expect(second.hasMore).toBe(false);
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2, offset: 1 })).events)).toEqual(["3:3", "4:3"]);
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2, orderBy: "desc" })).events)).toEqual(["3:3", "2:3"]);

    await store.write([change(10n, 3)], undefined, { accountAddress: A });
    const later = await adapter.fetchEvents(coinAtA, { cursor: `${CHANGES.at(-1)}:3` });
    expect(ids(later.events)).toEqual(["10:3"]);
  });

  it("keeps stored changes out of module, account and handle reads", async () => {
    const { store, adapter } = await seeded();
    // A change carrying a sender, hash and guid, as the fullnode delivers it, beside one real event.
    const guid = { creationNumber: 9n, accountAddress: A };
    await store.write([change(20n, 1, COIN, { sender: A, transactionHash: "0x" + "ab".repeat(32), guid })], undefined, {
      accountAddress: A,
    });
    await store.write([change(20n, 0, "0x1::coin::Deposit", { sender: A, guid })]);
    const read = async (descriptor: EventDescriptor) => ids((await adapter.fetchEvents(descriptor, { limit: 100 })).events);
    expect(await read({ kind: "account", accountAddress: A })).toEqual(["20:0"]);
    expect(await read({ kind: "module", eventType: "0x1::coin::CoinStore" })).toEqual([]);
    expect(await read({ kind: "module", eventType: [COIN, ACCOUNT, "0x1::coin::Deposit"] })).toEqual(["20:0"]);
    expect(await read({ kind: "handle", accountAddress: A, creationNumber: 9n })).toEqual(["20:0"]);
    expect((await store.stats()).events).toBe(1n);
  });

  it("passes checkAdapter, refuses malformed input and keeps the pruned edge", async () => {
    const { adapter, store } = await seeded();
    for (const options of [{ limit: 2 }, { limit: 3, fromVersion: 4n }]) {
      const report = await checkAdapter(adapter, coinAtA, options);
      expect(report.results.filter((r) => r.status === "fail")).toEqual([]);
    }
    await expect(adapter.fetchEvents({ ...coinAtA, accountAddress: "nope" }, {})).rejects.toBeInstanceOf(StreamdError);
    await expect(adapter.fetchEvents({ ...coinAtA, resourceType: "coin" as never }, {})).rejects.toBeInstanceOf(
      StreamdError,
    );
    await store.prune({ beforeVersion: 5n });
    expect(ids((await adapter.fetchEvents(coinAtA, { limit: 2 })).events)).toEqual(["5:3", "6:3"]);
    await expect(adapter.fetchEvents(coinAtA, { fromVersion: 4n })).rejects.toMatchObject({ statusCode: 410 });
  });

  it("answers client queries as MemoryAdapter does over the same changes", async () => {
    const { adapter } = await seeded();
    const local = await new StreamdClient({ adapter }).getEvents(coinAtA, { fromVersion: -3n, toVersion: "head" });
    expect(ids(local)).toEqual(["6:3", "7:3", "8:3", "9:3"]);
    const memory = new MemoryAdapter(CHANGES.map((v) => change(v, 3)));
    const same = await new StreamdClient({ adapter: memory }).getEvents(coinAtA, { fromVersion: -3n, toVersion: "head" });
    expect(local).toEqual(same);
  });
});
