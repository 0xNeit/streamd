import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient } from "@streamd/core";
import type { EventTable, StreamdEvent, Subscription } from "@streamd/core";
import { SqliteEventStore, eventTableSchema, storeTables } from "../src/index.js";

const SWAP: EventTable = {
  eventType: "0x2::dex::Swap",
  table: "dex_swap",
  generic: true,
  columns: [
    { name: "amount", type: "u256", path: ["amount"], nullable: false },
    { name: "delta", type: "i64", path: ["delta"], nullable: false },
    { name: "fee", type: "u16", path: ["fee"], nullable: false },
    { name: "buy", type: "bool", path: ["buy"], nullable: false },
    { name: "pool", type: "address", path: ["pool", "inner"], nullable: false },
    { name: "memo", type: "text", path: ["memo", "vec", 0], nullable: true },
    { name: "legs", type: "json", path: ["legs"], nullable: false },
  ],
};

const DEPOSIT: EventTable = {
  eventType: "0x1::fungible_asset::Deposit",
  table: "fungible_asset_deposit",
  generic: false,
  columns: [
    { name: "store", type: "address", path: ["store"], nullable: false },
    { name: "amount", type: "u64", path: ["amount"], nullable: false },
  ],
};

const TABLES = [SWAP, DEPOSIT];
const MAX = (2n ** 256n - 1n).toString();

const swap = (version: bigint, over: Record<string, unknown> = {}): StreamdEvent => ({
  type: "0x2::dex::Swap<0x1::aptos_coin::AptosCoin, u8>",
  data: { amount: MAX, delta: "-9223372036854775808", fee: 30, buy: true, pool: { inner: "0xAB" }, memo: { vec: [] }, legs: [1, "2"], ...over },
  version,
  sequenceNumber: 0n,
  eventIndex: 0,
  transactionHash: "0xFF",
  timestamp: 1_700_000_000_000_000n,
  sender: "0x5",
});

const deposit = (version: bigint, amount: string): StreamdEvent => ({
  type: "0x1::fungible_asset::Deposit",
  data: { store: "0x7", amount },
  version,
  sequenceNumber: 0n,
  eventIndex: 1,
});

const rows = (db: Database, table: string) => db.prepare(`SELECT * FROM "${table}" ORDER BY version, event_index`).all();

describe("eventTableSchema", () => {
  it("quotes every name and keys each table on version and event index", () => {
    const [swapTable, depositTable] = eventTableSchema(TABLES);
    expect(swapTable).toBe(
      [
        'CREATE TABLE IF NOT EXISTS "dex_swap" (',
        '  "version" INTEGER NOT NULL,',
        '  "event_index" INTEGER NOT NULL,',
        '  "transaction_hash" TEXT,',
        '  "timestamp" INTEGER,',
        '  "sender" TEXT,',
        '  "type_arguments" TEXT,',
        '  "amount" TEXT NOT NULL,',
        '  "delta" TEXT NOT NULL,',
        '  "fee" INTEGER NOT NULL,',
        '  "buy" INTEGER NOT NULL,',
        '  "pool" TEXT NOT NULL,',
        '  "memo" TEXT,',
        '  "legs" TEXT NOT NULL,',
        '  PRIMARY KEY ("version", "event_index")',
        ") WITHOUT ROWID",
      ].join("\n"),
    );
    expect(depositTable).not.toContain("type_arguments");
  });

  it("refuses a spec with an unsafe name before writing any SQL", () => {
    expect(() => eventTableSchema([{ ...DEPOSIT, table: 'x"; DROP TABLE y; --' }])).toThrow(/Invalid event tables/);
  });
});

describe("SqliteEventStore.writeTables", () => {
  it("writes rows of several tables and the cursor together, exact and idempotent", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTables(TABLES, [swap(1n), deposit(1n, "18446744073709551615"), swap(2n, { memo: { vec: ["hi"] }, buy: false })], {
      name: "a",
      cursor: "c1",
    });
    store.writeTables(TABLES, [swap(1n)], { name: "a", cursor: "c2" });
    expect(rows(db, "dex_swap")).toEqual([
      {
        version: 1, event_index: 0, transaction_hash: "0xff", timestamp: 1_700_000_000_000_000, sender: `0x${"0".repeat(63)}5`,
        type_arguments: "0x1::aptos_coin::AptosCoin, u8", amount: MAX, delta: "-9223372036854775808", fee: 30, buy: 1,
        pool: `0x${"0".repeat(62)}ab`, memo: null, legs: '[1,"2"]',
      },
      expect.objectContaining({ version: 2, memo: "hi", buy: 0 }),
    ]);
    expect(rows(db, "fungible_asset_deposit")).toEqual([
      expect.objectContaining({ version: 1, event_index: 1, amount: "18446744073709551615", store: `0x${"0".repeat(63)}7` }),
    ]);
    expect(store.readCursor("a")).toBe("c2");
  });

  it("replaces every column of a redelivered event, and saves a cursor with no events", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTables(TABLES, [swap(1n)]);
    store.writeTables(TABLES, [swap(1n, { fee: 31, memo: { vec: ["x"] } })], { name: "a", cursor: "c1" });
    expect(rows(db, "dex_swap")).toEqual([expect.objectContaining({ fee: 31, memo: "x" })]);
    store.writeTables(TABLES, [], { name: "a", cursor: "c2" });
    expect(store.readCursor("a")).toBe("c2");
  });

  it("writes nothing and keeps the cursor when one event is refused", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTables(TABLES, [swap(1n)], { name: "a", cursor: "c1" });
    expect(() => store.writeTables(TABLES, [swap(2n), swap(3n, { fee: 70_000 })], { name: "a", cursor: "c2" })).toThrow(
      "Column dex_swap.fee of event 3:0 expects a u16",
    );
    expect(() => store.writeTables(TABLES, [swap(4n), { ...deposit(4n, "1"), type: "0x9::x::Y" }])).toThrow(/No event table/);
    expect(rows(db, "dex_swap")).toHaveLength(1);
    expect(store.readCursor("a")).toBe("c1");
  });
});

function caughtUp(start: (done: () => void) => Subscription): Promise<Subscription> {
  return new Promise((resolve) => {
    const sub = start(() => queueMicrotask(() => resolve(sub)));
  });
}

describe("storeTables", () => {
  it("creates the tables, fills them and resumes from its saved cursor without repeats", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const adapter = new MemoryAdapter([swap(1n), deposit(2n, "5"), { ...deposit(3n, "6"), type: "0x1::fungible_asset::Withdraw" }]);
    const options = { name: "dex", batchSize: 1, pollInterval: 5 };
    const first = await caughtUp((onCaughtUp) => storeTables(new StreamdClient({ adapter }), store, TABLES, { ...options, onCaughtUp }));
    first.unsubscribe();
    expect(store.readCursor("tables:dex")).toBeDefined();
    expect(store.readCursor("dex")).toBeUndefined();

    adapter.push(swap(4n), deposit(5n, "7"));
    const seen: bigint[] = [];
    const second = await caughtUp((onCaughtUp) =>
      storeTables(new StreamdClient({ adapter }), store, TABLES, { ...options, onCaughtUp, where: (e) => (seen.push(e.version), true) }),
    );
    second.unsubscribe();
    expect(seen).toEqual([4n, 5n]);
    expect(rows(db, "dex_swap").map((r) => (r as { version: number }).version)).toEqual([1, 4]);
    expect(rows(db, "fungible_asset_deposit").map((r) => (r as { amount: string }).amount)).toEqual(["5", "7"]);
  });

  it("creates the tables before any event arrives", () => {
    const db = new Database(":memory:");
    const sub = storeTables(new StreamdClient({ adapter: new MemoryAdapter([]) }), new SqliteEventStore(db), TABLES, { name: "x" });
    sub.unsubscribe();
    const names = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all();
    expect(names).toContainEqual({ name: "dex_swap" });
    expect(names).toContainEqual({ name: "fungible_asset_deposit" });
  });

  it("needs a name and valid tables before subscribing", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    expect(() => storeTables(client, store, TABLES, { name: "" })).toThrow(/needs a name/);
    expect(() => storeTables(client, store, [], { name: "x" })).toThrow(/Invalid event tables/);
  });
});
