import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { SqliteEventStore } from "../src/index.js";
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
