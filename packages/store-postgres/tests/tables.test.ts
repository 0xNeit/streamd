import { PGlite } from "./helpers.js";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient } from "@streamd/core";
import type { EventTable, StreamdEvent, Subscription } from "@streamd/core";
import { PostgresEventStore, eventTableSchema, storeTables } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

const SWAP: EventTable = {
  eventType: "0x2::dex::Swap",
  table: "dex_swap",
  generic: true,
  columns: [
    { name: "amount", type: "u256", path: ["amount"], nullable: false },
    { name: "delta", type: "i64", path: ["delta"], nullable: false },
    { name: "fee", type: "u16", path: ["fee"], nullable: false },
    { name: "wide", type: "u32", path: ["wide"], nullable: false },
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
  data: {
    amount: MAX, delta: "-9223372036854775808", fee: 30, wide: 4294967295, buy: true, pool: { inner: "0xAB" },
    memo: { vec: [] }, legs: [1, "2"], ...over,
  },
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

// Every value as text, so numerics compare exactly whatever the driver parses them into.
async function rows(db: PGlite, table: string) {
  const { rows } = await db.query<{ row: Record<string, string | null> }>(
    `SELECT to_jsonb(t) #>> '{}' AS row FROM (SELECT * FROM "${table}" ORDER BY version, event_index) t`,
  );
  return rows.map((r) => JSON.parse(r.row as unknown as string) as Record<string, unknown>);
}

describe("eventTableSchema", () => {
  it("quotes every name, keys each table on version and event index, and returns one array per list", () => {
    const schema = eventTableSchema(TABLES);
    expect(schema[0]).toBe(
      [
        'CREATE TABLE IF NOT EXISTS "dex_swap" (',
        '  "version" bigint NOT NULL,',
        '  "event_index" integer NOT NULL,',
        '  "transaction_hash" text,',
        '  "timestamp" bigint,',
        '  "sender" text,',
        '  "type_arguments" text,',
        '  "amount" numeric(78, 0) NOT NULL,',
        '  "delta" bigint NOT NULL,',
        '  "fee" integer NOT NULL,',
        '  "wide" bigint NOT NULL,',
        '  "buy" boolean NOT NULL,',
        '  "pool" text NOT NULL,',
        '  "memo" text,',
        '  "legs" jsonb NOT NULL,',
        '  PRIMARY KEY ("version", "event_index")',
        ")",
      ].join("\n"),
    );
    expect(eventTableSchema(TABLES)).toBe(schema);
  });
});

describe("PostgresEventStore.writeTables", () => {
  it("writes rows of several tables and the cursor in one statement, exact and idempotent", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeTables(TABLES, []);
    const calls: string[] = [];
    const counted = new PostgresEventStore({
      query: (text, params) => (calls.push(text), db.query(text, params)),
    } as PostgresClient);
    await counted.writeTables(TABLES, [swap(1n), deposit(1n, "18446744073709551615"), swap(2n, { memo: { vec: ["a\u0000b"] }, buy: false })], {
      name: "a",
      cursor: "c1",
    });
    expect(calls.filter((c) => c.includes("INSERT"))).toHaveLength(1);
    await store.writeTables(TABLES, [swap(1n), swap(1n)], { name: "a", cursor: "c2" });
    expect(await rows(db, "dex_swap")).toEqual([
      {
        version: 1, event_index: 0, transaction_hash: "0xff", timestamp: 1_700_000_000_000_000, sender: `0x${"0".repeat(63)}5`,
        type_arguments: "0x1::aptos_coin::AptosCoin, u8", amount: Number(MAX), delta: -9223372036854775808, fee: 30, wide: 4294967295,
        buy: true, pool: `0x${"0".repeat(62)}ab`, memo: null, legs: [1, "2"],
      },
      expect.objectContaining({ version: 2, memo: "a�b", buy: false }),
    ]);
    const exact = await db.query<{ amount: string; delta: string }>(`SELECT amount::text AS amount, delta::text AS delta FROM dex_swap`);
    expect(exact.rows[0]).toEqual({ amount: MAX, delta: "-9223372036854775808" });
    const deposits = await db.query<{ amount: string }>(`SELECT amount::text AS amount FROM fungible_asset_deposit`);
    expect(deposits.rows).toEqual([{ amount: "18446744073709551615" }]);
    expect(await store.readCursor("a")).toBe("c2");
  });

  it("replaces every column of a redelivered event, keeps a batch's last copy, and saves a cursor with no events", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeTables(TABLES, [swap(1n)]);
    await store.writeTables(TABLES, [swap(1n, { fee: 31, memo: { vec: ["x"] } })]);
    expect(await rows(db, "dex_swap")).toEqual([expect.objectContaining({ fee: 31, memo: "x" })]);
    await store.writeTables(TABLES, [swap(1n, { fee: 1 }), swap(1n, { fee: 2 })]);
    expect(await rows(db, "dex_swap")).toEqual([expect.objectContaining({ fee: 2 })]);
    await store.writeTables(TABLES, [], { name: "a", cursor: "c" });
    expect(await store.readCursor("a")).toBe("c");
  });

  it("stores a lone surrogate as U+FFFD rather than failing every retry", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeTables(TABLES, [swap(1n, { memo: { vec: ["a\uD800"] }, legs: ["\uDC00b"] })]);
    expect(await rows(db, "dex_swap")).toEqual([expect.objectContaining({ memo: "a\uFFFD", legs: ["\uFFFDb"] })]);
  });

  it("writes nothing and keeps the cursor when one event is refused", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeTables(TABLES, [swap(1n)], { name: "a", cursor: "c1" });
    await expect(store.writeTables(TABLES, [swap(2n), swap(3n, { fee: 70_000 })], { name: "a", cursor: "c2" })).rejects.toThrow(
      "Column dex_swap.fee of event 3:0 expects a u16",
    );
    expect(await rows(db, "dex_swap")).toHaveLength(1);
    expect(await store.readCursor("a")).toBe("c1");
  });
});

async function caughtUp(start: (done: () => void) => Promise<Subscription>): Promise<Subscription> {
  let fire!: () => void;
  const done = new Promise<void>((resolve) => (fire = resolve));
  const sub = await start(() => fire());
  await done;
  return sub;
}

describe("storeTables", () => {
  it("creates the tables, fills them and resumes from its saved cursor without repeats", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const adapter = new MemoryAdapter([swap(1n), deposit(2n, "5")]);
    const options = { name: "dex", batchSize: 1, pollInterval: 5 };
    const first = await caughtUp((onCaughtUp) => storeTables(new StreamdClient({ adapter }), store, TABLES, { ...options, onCaughtUp }));
    first.unsubscribe();
    expect(await store.readCursor("tables:dex")).toBeDefined();

    adapter.push(swap(4n), deposit(5n, "7"));
    const seen: bigint[] = [];
    const second = await caughtUp((onCaughtUp) =>
      storeTables(new StreamdClient({ adapter }), store, TABLES, { ...options, onCaughtUp, where: (e) => (seen.push(e.version), true) }),
    );
    second.unsubscribe();
    expect(seen).toEqual([4n, 5n]);
    expect((await rows(db, "dex_swap")).map((r) => r.version)).toEqual([1, 4]);
    expect((await rows(db, "fungible_asset_deposit")).map((r) => r.amount)).toEqual([5, 7]);
  });

  it("creates the tables before any event arrives", async () => {
    const db = new PGlite();
    const sub = await storeTables(new StreamdClient({ adapter: new MemoryAdapter([]) }), new PostgresEventStore(db), TABLES, { name: "x" });
    sub.unsubscribe();
    const { rows } = await db.query<{ t: string | null }>("SELECT to_regclass('dex_swap')::text AS t");
    expect(rows[0]!.t).toBe("dex_swap");
  });

  it("needs a name and valid tables before subscribing", async () => {
    const store = new PostgresEventStore(new PGlite());
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    await expect(storeTables(client, store, TABLES, { name: "" })).rejects.toThrow(/needs a name/);
    await expect(storeTables(client, store, [], { name: "x" })).rejects.toThrow(/Invalid event tables/);
  });

  it("recovers schema after a caller rolls back a transaction around the first writeTables", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await db.exec("BEGIN");
    await store.writeTables(TABLES, [swap(1n)]);
    await db.exec("ROLLBACK");
    await store.writeTables(TABLES, [swap(2n), deposit(2n, "9")], { name: "t", cursor: "c1" });
    expect((await rows(db, "dex_swap")).map((r) => r.version)).toEqual([2]);
    expect((await rows(db, "fungible_asset_deposit")).map((r) => r.amount)).toEqual([9]);
    expect(await store.readCursor("t")).toBe("c1");
  });

  it("recovers custom tables when the driver reports a missing relation message without SQLSTATE code", async () => {
    const db = new PGlite();
    let dropOnce = true;
    const client: PostgresClient = {
      async query<T = Record<string, unknown>>(sql: string, params?: unknown[]) {
        if (dropOnce && sql.includes('INSERT INTO "dex_swap"')) {
          dropOnce = false;
          await db.query('DROP TABLE "dex_swap"');
          throw new Error('relation "dex_swap" does not exist');
        }
        return db.query<T>(sql, params);
      },
    };
    const store = new PostgresEventStore(client);
    await store.writeTables(TABLES, [swap(3n)], { name: "t", cursor: "c2" });
    expect((await rows(db, "dex_swap")).map((r) => r.version)).toEqual([3]);
    expect(await store.readCursor("t")).toBe("c2");
  });
});
