import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { SqliteDedupStore } from "../src/index.js";

function held(db: Database): string[] {
  return (db.query("SELECT id FROM streamd_dedup ORDER BY rowid").all() as { id: string }[]).map((r) => r.id);
}

describe("SqliteDedupStore", () => {
  it("answers per id in order, and a new instance over the same database sees old ids", () => {
    const db = new Database(":memory:");
    const store = new SqliteDedupStore(db);
    expect(store.seen(["1:0", "2:0"])).toEqual([false, false]);
    store.add(["1:0"]);
    expect(store.seen(["2:0", "1:0"])).toEqual([false, true]);
    expect(new SqliteDedupStore(db).seen(["1:0", "2:0"])).toEqual([true, false]);
  });

  it("shares ids between instances", () => {
    const db = new Database(":memory:");
    const a = new SqliteDedupStore(db);
    const b = new SqliteDedupStore(db);
    a.add(["1:0"]);
    b.add(["2:0"]);
    expect(a.seen(["2:0"])).toEqual([true]);
    expect(b.seen(["1:0"])).toEqual([true]);
  });

  it("adds a held or repeated id once, keeping its first place", () => {
    const db = new Database(":memory:");
    const store = new SqliteDedupStore(db, { maxEntries: 2 });
    store.add(["a", "a"]);
    store.add(["b"]);
    store.add(["a"]);
    expect(held(db)).toEqual(["a", "b"]);
    store.add(["c"]);
    expect(held(db)).toEqual(["b", "c"]);
  });

  it("forgets the oldest past maxEntries, across the table", () => {
    const db = new Database(":memory:");
    const store = new SqliteDedupStore(db, { maxEntries: 3 });
    store.add(["a", "b"]);
    new SqliteDedupStore(db, { maxEntries: 3 }).add(["c", "d"]);
    expect(store.seen(["a", "b", "c", "d"])).toEqual([false, true, true, true]);
  });

  it("keeps the newest maxEntries of a batch larger than the bound", () => {
    const db = new Database(":memory:");
    const store = new SqliteDedupStore(db, { maxEntries: 2 });
    store.add(["x"]);
    store.add(["a", "b", "c"]);
    expect(held(db)).toEqual(["b", "c"]);
  });

  it("keeps ids as given, with no interpretation", () => {
    const db = new Database(":memory:");
    const store = new SqliteDedupStore(db);
    const odd = ["x'); DROP TABLE streamd_dedup; --", "", "a\0b", "net/1:0"];
    store.add(odd);
    expect(store.seen(odd)).toEqual([true, true, true, true]);
    expect(store.seen(["a"])).toEqual([false]);
  });

  it("refuses bad options and ids", () => {
    const db = new Database(":memory:");
    for (const maxEntries of [0, -1, 1.5, Number.NaN]) {
      expect(() => new SqliteDedupStore(db, { maxEntries })).toThrow(StreamdError);
    }
    expect(() => new SqliteDedupStore({} as never)).toThrow(StreamdError);
    const store = new SqliteDedupStore(db);
    expect(() => store.add([1] as never)).toThrow(StreamdError);
    expect(() => store.seen("1:0" as never)).toThrow(StreamdError);
  });
});
