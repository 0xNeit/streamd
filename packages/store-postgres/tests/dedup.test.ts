import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { PGlite } from "./helpers.js";
import { PostgresDedupStore } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

async function held(db: PGlite): Promise<string[]> {
  const { rows } = await db.query<{ id: string }>("SELECT id FROM streamd_dedup ORDER BY seq");
  return rows.map((r) => r.id);
}

describe("PostgresDedupStore", () => {
  it("answers per id in order, and a new instance over the same database sees old ids", async () => {
    const db = new PGlite();
    const store = new PostgresDedupStore(db);
    expect(await store.seen(["1:0", "2:0"])).toEqual([false, false]);
    await store.add(["1:0"]);
    expect(await store.seen(["2:0", "1:0"])).toEqual([false, true]);
    expect(await new PostgresDedupStore(db).seen(["1:0", "2:0"])).toEqual([true, false]);
  });

  it("shares ids between instances", async () => {
    const db = new PGlite();
    const a = new PostgresDedupStore(db);
    const b = new PostgresDedupStore(db);
    await a.add(["1:0"]);
    await b.add(["2:0"]);
    expect(await a.seen(["2:0"])).toEqual([true]);
    expect(await b.seen(["1:0"])).toEqual([true]);
  });

  it("adds a held or repeated id once, keeping its first place", async () => {
    const db = new PGlite();
    const store = new PostgresDedupStore(db, { maxEntries: 2 });
    await store.add(["a", "a"]);
    await store.add(["b"]);
    await store.add(["a"]);
    expect(await held(db)).toEqual(["a", "b"]);
    await store.add(["c"]);
    expect(await held(db)).toEqual(["b", "c"]);
  });

  it("forgets the oldest past maxEntries, across the table", async () => {
    const db = new PGlite();
    const store = new PostgresDedupStore(db, { maxEntries: 3 });
    await store.add(["a", "b"]);
    await new PostgresDedupStore(db, { maxEntries: 3 }).add(["c", "d"]);
    expect(await store.seen(["a", "b", "c", "d"])).toEqual([false, true, true, true]);
  });

  it("keeps the newest maxEntries of a batch larger than the bound", async () => {
    const db = new PGlite();
    const store = new PostgresDedupStore(db, { maxEntries: 2 });
    await store.add(["x"]);
    await store.add(["a", "b", "c"]);
    expect(await held(db)).toEqual(["b", "c"]);
  });

  it("writes in one query per add once the table exists", async () => {
    const db = new PGlite();
    const sent: string[] = [];
    const client: PostgresClient = {
      query: async (text, params) => {
        sent.push(text);
        return db.query(text, params) as Promise<{ rows: Record<string, unknown>[] }>;
      },
    };
    const store = new PostgresDedupStore(client, { maxEntries: 2 });
    await store.add(["a"]);
    sent.length = 0;
    await store.add(["b", "c"]);
    expect(sent.length).toBe(1);
    expect(await held(db)).toEqual(["b", "c"]);
  });

  it("keeps ids as given, with no interpretation", async () => {
    const db = new PGlite();
    const store = new PostgresDedupStore(db);
    const odd = ["x'); DROP TABLE streamd_dedup; --", "", '{"a",b}', "net/1:0"];
    await store.add(odd);
    expect(await store.seen(odd)).toEqual([true, true, true, true]);
    expect(await store.seen(["a"])).toEqual([false]);
  });

  it("refuses bad options and ids", async () => {
    const db = new PGlite();
    for (const maxEntries of [0, -1, 1.5, Number.NaN]) {
      expect(() => new PostgresDedupStore(db, { maxEntries })).toThrow(StreamdError);
    }
    expect(() => new PostgresDedupStore({} as never)).toThrow(StreamdError);
    const store = new PostgresDedupStore(db);
    await expect(store.add([1] as never)).rejects.toThrow(StreamdError);
    await expect(store.add(["a\0b"])).rejects.toThrow(StreamdError);
    await expect(store.seen("1:0" as never)).rejects.toThrow(StreamdError);
  });
});
