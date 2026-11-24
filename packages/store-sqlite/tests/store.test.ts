import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";
import { SqliteEventStore, StoreAdapter } from "../src/index.js";
import type { StoredSale, StoredSwap } from "../src/index.js";
import { versionKey } from "../src/schema.js";

const U64_MAX = 2n ** 64n - 1n;

function ev(version: bigint, eventIndex = 0, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: "0x1::m::E", data: { n: "1" }, version, sequenceNumber: 0n, eventIndex, ...extra };
}

function rows(db: Database): StreamdEvent[] {
  return db
    .query("SELECT event FROM streamd_events ORDER BY version, event_index")
    .all()
    .map((r) => parseEvent((r as { event: string }).event));
}

describe("SqliteEventStore", () => {
  it("keeps one row per (version, eventIndex) and lets the later write win", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n), ev(1n, 1), ev(2n)]);
    store.write([ev(1n, 1, { state: ["later"] }), ev(2n)]);
    const stored = rows(db);
    expect(stored.map((e) => `${e.version}:${e.eventIndex}`)).toEqual(["1:0", "1:1", "2:0"]);
    expect(stored[1]!.state).toEqual(["later"]);
  });

  it("round-trips u64 versions, u128 creation numbers and bigint data exactly", () => {
    const db = new Database(":memory:");
    const event = ev(U64_MAX, 3, {
      data: { amount: 2n ** 127n + 1n },
      sequenceNumber: U64_MAX,
      timestamp: 2n ** 60n + 7n,
      guid: { creationNumber: 2n ** 128n - 1n, accountAddress: "0x1" },
      sender: "0xa",
      transactionHash: `0x${"ab".repeat(32)}`,
    });
    new SqliteEventStore(db).write([ev(9n), event]);
    const [, back] = rows(db);
    expect(back!.version).toBe(U64_MAX);
    expect(back!.sequenceNumber).toBe(U64_MAX);
    expect(back!.timestamp).toBe(2n ** 60n + 7n);
    expect(back!.guid).toEqual({ creationNumber: 2n ** 128n - 1n, accountAddress: "0x1" });
    expect(back!.data).toEqual({ amount: (2n ** 127n + 1n).toString() });
  });

  it("saves the checkpoint with the events and keeps it when the cursor is undefined", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    expect(store.readCursor("swaps")).toBeUndefined();
    store.write([ev(1n)], { name: "swaps", cursor: "c1" });
    store.write([ev(2n)], { name: "swaps", cursor: undefined });
    expect(store.readCursor("swaps")).toBe("c1");
    store.write([], { name: "swaps", cursor: "c2" });
    expect(store.readCursor("swaps")).toBe("c2");
  });

  it("writes nothing when the transaction fails partway", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n)], { name: "s", cursor: "c1" });
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_cursors BEGIN SELECT RAISE(ABORT, 'boom'); END`);
    db.exec(`CREATE TRIGGER boom2 BEFORE UPDATE ON streamd_cursors BEGIN SELECT RAISE(ABORT, 'boom'); END`);
    expect(() => store.write([ev(2n), ev(3n)], { name: "s", cursor: "c2" })).toThrow("boom");
    expect(rows(db).map((e) => e.version)).toEqual([1n]);
    expect(store.readCursor("s")).toBe("c1");
  });

  it("refuses invalid events and names before writing anything", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    expect(() => store.write([ev(1n), ev(U64_MAX + 1n)])).toThrow(StreamdError);
    expect(() => store.write([ev(-1n)])).toThrow(StreamdError);
    expect(() => store.write([ev(1n, -1)])).toThrow(StreamdError);
    expect(() => store.write([ev(1n)], { name: "", cursor: "c" })).toThrow(StreamdError);
    expect(rows(db)).toEqual([]);
    expect(() => new SqliteEventStore({} as never)).toThrow(StreamdError);
  });

  it("reports the write's own error when SQLite has already rolled back", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_events BEGIN SELECT RAISE(ROLLBACK, 'real cause'); END`);
    expect(() => store.write([ev(1n)], { name: "s", cursor: "c" })).toThrow("real cause");
    expect(store.readCursor("s")).toBeUndefined();
  });

  it("writes inside a transaction the caller holds, and rolls back only its own part", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec("BEGIN");
    store.write([ev(1n)]);
    expect(() => store.write([ev(2n), { ...ev(3n), type: 5 as never }])).toThrow(StreamdError);
    db.exec("COMMIT");
    expect(rows(db).map((e) => e.version)).toEqual([1n]);
  });

  it("refuses an event it could not read back", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    for (const bad of [{ sequenceNumber: "x" }, { type: "" }, { timestamp: 5 }]) {
      expect(() => store.write([ev(1n), ev(2n, 0, bad as never)])).toThrow(StreamdError);
    }
    expect(rows(db)).toEqual([]);
  });

  it("reopens an existing database without losing rows", () => {
    const db = new Database(":memory:");
    new SqliteEventStore(db).write([ev(5n)], { name: "s", cursor: "c" });
    const again = new SqliteEventStore(db);
    expect(again.readCursor("s")).toBe("c");
    expect(rows(db)).toHaveLength(1);
  });
});

describe("SqliteEventStore.prune", () => {
  const versions = (db: Database) => rows(db).map((e) => e.version);

  it("deletes below the version and keeps the rest", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n), ev(2n), ev(2n, 1), ev(3n)]);
    store.prune({ beforeVersion: 2n });
    expect(versions(db)).toEqual([2n, 2n, 3n]);
  });

  it("leaves saved cursors", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n)], { name: "swaps", cursor: "c" });
    store.prune({ beforeVersion: 5n });
    expect(versions(db)).toEqual([]);
    expect(store.readCursor("swaps")).toBe("c");
  });

  it("throws on an invalid version and changes nothing", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n)]);
    expect(() => store.prune({ beforeVersion: -1n })).toThrow(StreamdError);
    expect(() => store.prune({ beforeVersion: 2 as unknown as bigint })).toThrow(StreamdError);
    expect(() => store.prune(undefined as unknown as { beforeVersion: bigint })).toThrow(StreamdError);
    expect(versions(db)).toEqual([1n]);
  });

  it("rolls the delete back when recording the edge fails", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n)]);
    db.run("DROP TABLE streamd_cursors");
    expect(() => store.prune({ beforeVersion: 5n })).toThrow();
    expect(versions(db)).toEqual([1n]);
  });

  it("keeps the name holding the edge away from callers", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const name = "streamd:pruned-before";
    expect(() => store.write([], { name, cursor: "x" })).toThrow("reserved");
    expect(() => store.readCursor(name)).toThrow("reserved");
  });
});

describe("SqliteEventStore cursors", () => {
  function populated() {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([ev(1n), ev(5n)], { name: "swaps", cursor: "c-swaps" });
    store.write([ev(6n)], { name: "Bids", cursor: "c-bids" });
    store.write([], { name: "bids", cursor: "c-lower" });
    store.prune({ beforeVersion: 2n });
    return { db, store };
  }
  const edge = (db: Database) =>
    (db.query("SELECT cursor FROM streamd_cursors WHERE name = 'streamd:pruned-before'").get() as { cursor: string })
      .cursor;

  it("lists saved cursors by name in byte order, without the pruned edge", () => {
    expect(new SqliteEventStore(new Database(":memory:")).cursors()).toEqual([]);
    const { store } = populated();
    expect(store.cursors()).toEqual([
      { name: "Bids", cursor: "c-bids" },
      { name: "bids", cursor: "c-lower" },
      { name: "swaps", cursor: "c-swaps" },
    ]);
  });

  it("deletes one cursor, reports whether it existed, and leaves the events", () => {
    const { db, store } = populated();
    expect(store.deleteCursor("swaps")).toBe(true);
    expect(store.deleteCursor("swaps")).toBe(false);
    expect(store.readCursor("swaps")).toBeUndefined();
    expect(store.cursors().map((c) => c.name)).toEqual(["Bids", "bids"]);
    expect(rows(db).map((e) => e.version)).toEqual([5n, 6n]);
    expect(edge(db)).toBe(versionKey(2n));
  });

  it("refuses the reserved name and invalid names, leaving the edge", () => {
    const { db, store } = populated();
    for (const bad of ["streamd:pruned-before", "", 7]) {
      expect(() => store.deleteCursor(bad as string)).toThrow(StreamdError);
    }
    expect(edge(db)).toBe(versionKey(2n));
  });
});

function tr(version: bigint, eventIndex: number, data: Partial<Transfer> = {}): StreamdEvent<Transfer> {
  return {
    type: "0x1::fungible_asset::Deposit",
    data: { asset: "0xa", amount: 5n, ...data },
    version,
    sequenceNumber: 0n,
    eventIndex,
  };
}

function transferRows(db: Database): Record<string, unknown>[] {
  return db.query("SELECT * FROM streamd_transfers ORDER BY version, event_index").all() as Record<string, unknown>[];
}

describe("SqliteEventStore transfers", () => {
  const U256_MAX = 2n ** 256n - 1n;

  it("stores each record's fields in columns and the record as JSON, the later write winning", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTransfers([tr(1n, 2, { amount: U256_MAX, from: "0xf", to: "0xt", symbol: "APT", decimals: 8 }), tr(2n, 0)], {
      name: "transfers:t",
      cursor: "c1",
    });
    store.writeTransfers([tr(2n, 0, { amount: 9n, to: "0xt" })]);
    const [first, second] = transferRows(db);
    expect(first).toEqual({
      version: versionKey(1n),
      event_index: 2,
      asset: "0xa",
      amount: U256_MAX.toString(),
      from_address: "0xf",
      to_address: "0xt",
      symbol: "APT",
      decimals: 8,
      transfer: expect.any(String),
    });
    expect(parseEvent(first!.transfer as string).data).toEqual({
      asset: "0xa",
      amount: U256_MAX.toString(),
      from: "0xf",
      to: "0xt",
      symbol: "APT",
      decimals: 8,
    });
    expect(second).toMatchObject({ amount: "9", from_address: null, to_address: "0xt", symbol: null, decimals: null });
    expect(transferRows(db)).toHaveLength(2);
    expect(store.readCursor("transfers:t")).toBe("c1");
  });

  it("refuses a malformed record and writes nothing from that call", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const bad: Partial<Transfer>[] = [
      { amount: -1n },
      { amount: U256_MAX + 1n },
      { amount: 5 as unknown as bigint },
      { asset: "" },
      { to: 7 as unknown as string },
      { decimals: 1.5 },
    ];
    for (const data of bad) {
      expect(() => store.writeTransfers([tr(1n, 0), tr(2n, 0, data)], { name: "t", cursor: "c" })).toThrow(StreamdError);
    }
    expect(() => store.writeTransfers([{ ...tr(1n, 0), data: null as unknown as Transfer }])).toThrow(StreamdError);
    store.writeTransfers([]);
    expect(transferRows(db)).toEqual([]);
    expect(store.readCursor("t")).toBeUndefined();
  });

  it("prunes transfers below the edge with the events", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTransfers([tr(1n, 0), tr(2n, 0), tr(3n, 0)]);
    store.write([ev(1n), ev(3n)]);
    store.prune({ beforeVersion: 3n });
    expect(transferRows(db).map((r) => r.version)).toEqual([versionKey(3n)]);
    expect(rows(db).map((e) => e.version)).toEqual([3n]);
  });

  it("leaves the table out of the event schema, so an older database still opens read-only", () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-"));
    const path = join(dir, "events.db");
    try {
      new SqliteEventStore(new Database(path)).write([ev(1n)]);
      const readonly = new Database(path, { readonly: true });
      expect(() => new StoreAdapter(readonly)).not.toThrow();
      readonly.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("recreates streamd_transfers when a caller rolls back the first writeTransfers", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec("BEGIN");
    store.writeTransfers([tr(1n, 0)]);
    db.exec("ROLLBACK");
    store.writeTransfers([tr(2n, 0)], { name: "t", cursor: "c1" });
    expect(transferRows(db).map((r) => r.version)).toEqual([versionKey(2n)]);
    expect(store.readCursor("t")).toBe("c1");
  });
});

function sw(version: bigint, eventIndex: number, data: Partial<StoredSwap> = {}): StreamdEvent<StoredSwap> {
  return {
    type: "0x1::pool_v3::SwapEventV3",
    data: { protocol: "hyperion", poolId: "0xp", assetIn: "0xa", amountIn: 5n, assetOut: "0xb", amountOut: 7n, ...data },
    version,
    sequenceNumber: 0n,
    eventIndex,
  };
}

function sa(version: bigint, eventIndex: number, data: Partial<StoredSale> = {}): StreamdEvent<StoredSale> {
  return {
    type: "0x1::events::ListingFilledEvent",
    data: { marketplace: "wapal", kind: "listing", token: "0xt", seller: "0xs", buyer: "0xb", price: 100n, ...data },
    version,
    sequenceNumber: 0n,
    eventIndex,
  };
}

const tableRows = (db: Database, table: string) =>
  db.query(`SELECT * FROM ${table} ORDER BY version, event_index`).all() as Record<string, unknown>[];

describe("SqliteEventStore swaps and sales", () => {
  const U256_MAX = 2n ** 256n - 1n;

  it("stores each swap's fields in columns and the record as JSON, with its cursor, the later write winning", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeSwaps([sw(1n, 2, { trader: "0xtr", amountIn: U256_MAX }), sw(2n, 0)], { name: "swaps:s", cursor: "c1" });
    store.writeSwaps([sw(2n, 0, { amountOut: 9n })]);
    const [first, second] = tableRows(db, "streamd_swaps");
    expect(first).toEqual({
      version: versionKey(1n),
      event_index: 2,
      protocol: "hyperion",
      pool_id: "0xp",
      trader: "0xtr",
      asset_in: "0xa",
      amount_in: U256_MAX.toString(),
      asset_out: "0xb",
      amount_out: "7",
      swap: expect.any(String),
    });
    expect(parseEvent(first!.swap as string).data).toMatchObject({ trader: "0xtr", amountIn: U256_MAX.toString() });
    expect(second).toMatchObject({ trader: null, amount_out: "9" });
    expect(tableRows(db, "streamd_swaps")).toHaveLength(2);
    expect(store.readCursor("swaps:s")).toBe("c1");
  });

  it("stores each sale's fields in columns and the record as JSON, with its cursor", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeSales([sa(1n, 0, { collection: "0xc", royalty: 3n, fee: 2n }), sa(2n, 1)], { name: "sales:s", cursor: "c2" });
    const [first, second] = tableRows(db, "streamd_sales");
    expect(first).toEqual({
      version: versionKey(1n),
      event_index: 0,
      marketplace: "wapal",
      kind: "listing",
      token: "0xt",
      collection: "0xc",
      seller: "0xs",
      buyer: "0xb",
      price: "100",
      royalty: "3",
      fee: "2",
      sale: expect.any(String),
    });
    expect(second).toMatchObject({ collection: null, royalty: null, fee: null });
    expect(store.readCursor("sales:s")).toBe("c2");
  });

  it("refuses a malformed swap or sale and writes nothing from that call", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const swaps: Partial<StoredSwap>[] = [
      { amountIn: -1n },
      { amountOut: U256_MAX + 1n },
      { amountIn: 5 as unknown as bigint },
      { poolId: "" },
      { protocol: undefined as unknown as string },
      { trader: 7 as unknown as string },
    ];
    for (const data of swaps) {
      expect(() => store.writeSwaps([sw(1n, 0), sw(2n, 0, data)], { name: "s", cursor: "c" })).toThrow(StreamdError);
    }
    const sales: Partial<StoredSale>[] = [
      { price: -1n },
      { royalty: 1 as unknown as bigint },
      { token: "" },
      { buyer: undefined as unknown as string },
      { collection: 7 as unknown as string },
    ];
    for (const data of sales) {
      expect(() => store.writeSales([sa(1n, 0), sa(2n, 0, data)], { name: "s", cursor: "c" })).toThrow(StreamdError);
    }
    expect(() => store.writeSwaps([{ ...sw(1n, 0), data: null as unknown as StoredSwap }])).toThrow(StreamdError);
    expect(() => store.writeSales([{ ...sa(1n, 0), data: null as unknown as StoredSale }])).toThrow(StreamdError);
    store.writeSwaps([]);
    expect(tableRows(db, "streamd_swaps")).toEqual([]);
    expect(tableRows(db, "streamd_sales")).toEqual([]);
    expect(store.readCursor("s")).toBeUndefined();
  });

  it("prunes swaps and sales below the edge with the events", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeSwaps([sw(1n, 0), sw(3n, 0)]);
    store.writeSales([sa(2n, 0), sa(4n, 0)]);
    store.prune({ beforeVersion: 3n });
    expect(tableRows(db, "streamd_swaps").map((r) => r.version)).toEqual([versionKey(3n)]);
    expect(tableRows(db, "streamd_sales").map((r) => r.version)).toEqual([versionKey(4n)]);
  });

  it("adds the tables to a database that has only streamd_transfers", () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTransfers([tr(1n, 0)]);
    db.exec("DROP TABLE streamd_swaps; DROP TABLE streamd_sales;");
    store.prune({ beforeVersion: 1n });
    store.writeSales([sa(2n, 0)]);
    expect(tableRows(db, "streamd_swaps")).toEqual([]);
    expect(tableRows(db, "streamd_sales")).toHaveLength(1);
    expect(transferRows(db)).toHaveLength(1);
  });
});
