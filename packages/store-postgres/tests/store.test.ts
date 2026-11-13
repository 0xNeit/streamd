import { describe, expect, it } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { PostgresEventStore } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

const U64_MAX = 2n ** 64n - 1n;

function ev(version: bigint, eventIndex = 0, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: "0x1::m::E", data: { n: "1" }, version, sequenceNumber: 0n, eventIndex, ...extra };
}

async function rows(db: PGlite): Promise<StreamdEvent[]> {
  const { rows } = await db.query<{ event: string }>("SELECT event FROM streamd_events ORDER BY version, event_index");
  return rows.map((r) => parseEvent(r.event));
}

describe("PostgresEventStore", () => {
  it("keeps one row per (version, eventIndex) and lets the later write win, within a batch too", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n), ev(1n, 1), ev(2n)]);
    await store.write([ev(1n, 1, { state: ["later"] }), ev(2n), ev(2n, 0, { state: ["last"] })]);
    const stored = await rows(db);
    expect(stored.map((e) => `${e.version}:${e.eventIndex}`)).toEqual(["1:0", "1:1", "2:0"]);
    expect(stored[1]!.state).toEqual(["later"]);
    expect(stored[2]!.state).toEqual(["last"]);
  });

  it("round-trips u64 versions, u128 creation numbers and bigint data exactly", async () => {
    const db = new PGlite();
    const event = ev(U64_MAX, 3, {
      data: { amount: 2n ** 127n + 1n },
      sequenceNumber: U64_MAX,
      timestamp: 2n ** 60n + 7n,
      guid: { creationNumber: 2n ** 128n - 1n, accountAddress: "0x1" },
      sender: "0xa",
      transactionHash: `0x${"ab".repeat(32)}`,
    });
    await new PostgresEventStore(db).write([event]);
    const [stored] = await rows(db);
    expect(stored).toEqual({ ...event, data: { amount: (2n ** 127n + 1n).toString() } });
    const { rows: raw } = await db.query<Record<string, string>>(
      "SELECT version::text, sender, guid_address, guid_creation::text FROM streamd_events",
    );
    expect(raw[0]).toEqual({
      version: U64_MAX.toString(),
      sender: `0x${"0".repeat(63)}a`,
      guid_address: `0x${"0".repeat(63)}1`,
      guid_creation: (2n ** 128n - 1n).toString(),
    });
  });

  it("stores text Postgres's jsonb cannot hold, such as a NUL character", async () => {
    const db = new PGlite();
    const event = ev(1n, 0, { data: { s: "a\u0000b", lone: "\ud800" } });
    await new PostgresEventStore(db).write([event]);
    expect((await rows(db))[0]!.data).toEqual({ s: "a\u0000b", lone: "\ud800" });
  });

  it("writes nothing when one event of a batch is bad", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const bad = [
      ev(-1n),
      ev(U64_MAX + 1n),
      ev(1n, -1),
      ev(1n, 2 ** 31),
    ];
    for (const event of bad) {
      await expect(store.write([ev(5n), event], { name: "a", cursor: "c" })).rejects.toBeInstanceOf(StreamdError);
    }
    await store.write([]);
    expect(await rows(db)).toEqual([]);
    expect(await store.readCursor("a")).toBeUndefined();
  });

  it("writes nothing when the statement fails partway", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n)], { name: "s", cursor: "c1" });
    await db.exec(`
      CREATE FUNCTION boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$;
      CREATE TRIGGER boom BEFORE INSERT OR UPDATE ON streamd_cursors FOR EACH ROW EXECUTE FUNCTION boom();
    `);
    await expect(store.write([ev(2n), ev(3n)], { name: "s", cursor: "c2" })).rejects.toThrow("boom");
    expect((await rows(db)).map((e) => e.version)).toEqual([1n]);
    expect(await store.readCursor("s")).toBe("c1");
  });

  it("saves the cursor with the batch, and leaves it when the checkpoint has none", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    expect(await store.readCursor("tail")).toBeUndefined();
    await store.write([ev(1n)], { name: "tail", cursor: "c1" });
    await store.write([ev(2n)], { name: "tail", cursor: undefined });
    expect(await store.readCursor("tail")).toBe("c1");
    await store.write([], { name: "tail", cursor: "c2" });
    expect(await store.readCursor("tail")).toBe("c2");
    expect(await store.readCursor("other")).toBeUndefined();
    expect((await rows(db)).length).toBe(2);
  });

  it("refuses bad names and cursors, and a client without query()", async () => {
    const store = new PostgresEventStore(new PGlite());
    await expect(store.write([], { name: "", cursor: "c" })).rejects.toThrow("non-empty string");
    await expect(store.readCursor("")).rejects.toThrow("non-empty string");
    await expect(store.write([], { name: "n", cursor: 5 as unknown as string })).rejects.toThrow("string or undefined");
    expect(() => new PostgresEventStore({} as PostgresClient)).toThrow("query()");
  });

  it("creates the schema once per client and shares a database with a second store", async () => {
    const db = new PGlite();
    const seen: string[] = [];
    const client: PostgresClient = { query: (text, params) => (seen.push(text), db.query(text, params)) };
    const store = new PostgresEventStore(client);
    await Promise.all([store.write([ev(1n)]), store.readCursor("x")]);
    await store.write([ev(2n)]);
    expect(seen.filter((t) => t.includes("CREATE TABLE IF NOT EXISTS streamd_events"))).toHaveLength(1);
    await new PostgresEventStore(db).write([ev(3n)]);
    expect((await rows(db)).length).toBe(3);
  });

  it("tries the schema again after a failure", async () => {
    const db = new PGlite();
    let fail = true;
    const client: PostgresClient = {
      query: (text, params) => {
        if (fail) {
          fail = false;
          return Promise.reject(new Error("connection reset"));
        }
        return db.query(text, params);
      },
    };
    const store = new PostgresEventStore(client);
    await expect(store.write([ev(1n)])).rejects.toThrow("connection reset");
    await store.write([ev(1n)]);
    expect((await rows(db)).length).toBe(1);
  });
});

describe("PostgresEventStore.prune", () => {
  const versions = async (db: PGlite) => (await rows(db)).map((e) => e.version);
  const edge = async (db: PGlite) =>
    (await db.query<{ cursor: string }>("SELECT cursor FROM streamd_cursors WHERE name = 'streamd:pruned-before'"))
      .rows[0]?.cursor;

  it("deletes below the version and keeps the rest", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n), ev(2n), ev(2n, 1), ev(3n)]);
    expect(await store.prune({ beforeVersion: 2n })).toBeUndefined();
    expect(await versions(db)).toEqual([2n, 2n, 3n]);
  });

  it("leaves saved cursors", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n)], { name: "swaps", cursor: "c" });
    await store.prune({ beforeVersion: 5n });
    expect(await versions(db)).toEqual([]);
    expect(await store.readCursor("swaps")).toBe("c");
  });

  it("prunes a store that was never written to", async () => {
    const db = new PGlite();
    await new PostgresEventStore(db).prune({ beforeVersion: U64_MAX });
    expect(await edge(db)).toBe(U64_MAX.toString());
  });

  it("rejects an invalid version and changes nothing", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n)]);
    for (const bad of [{ beforeVersion: -1n }, { beforeVersion: U64_MAX + 1n }, { beforeVersion: 2 }, undefined]) {
      await expect(store.prune(bad as { beforeVersion: bigint })).rejects.toBeInstanceOf(StreamdError);
    }
    expect(await versions(db)).toEqual([1n]);
    expect(await edge(db)).toBeUndefined();
  });

  it("rolls the delete back when recording the edge fails", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n)]);
    await db.exec(`
      CREATE FUNCTION boom() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$;
      CREATE TRIGGER boom BEFORE INSERT OR UPDATE ON streamd_cursors FOR EACH ROW EXECUTE FUNCTION boom();
    `);
    await expect(store.prune({ beforeVersion: 5n })).rejects.toThrow("boom");
    expect(await versions(db)).toEqual([1n]);
  });

  it("only raises the edge, comparing versions across digit counts", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.prune({ beforeVersion: 10n });
    await store.prune({ beforeVersion: 9n });
    expect(await edge(db)).toBe("10");
    await store.prune({ beforeVersion: 100n });
    expect(await edge(db)).toBe("100");
  });

  it("still deletes below a lower version, as rows written late below the edge", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.prune({ beforeVersion: 10n });
    await store.write([ev(2n), ev(5n)]);
    await store.prune({ beforeVersion: 3n });
    expect(await versions(db)).toEqual([5n]);
    expect(await edge(db)).toBe("10");
  });

  it("keeps the name holding the edge away from callers", async () => {
    const store = new PostgresEventStore(new PGlite());
    const name = "streamd:pruned-before";
    await expect(store.write([], { name, cursor: "x" })).rejects.toThrow("reserved");
    await expect(store.readCursor(name)).rejects.toThrow("reserved");
  });

  it("lands a prune and a write sent together, each whole", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([ev(1n), ev(2n)]);
    await Promise.all([
      store.prune({ beforeVersion: 5n }),
      store.write([ev(6n), ev(7n)], { name: "s", cursor: "c" }),
      store.prune({ beforeVersion: 3n }),
    ]);
    expect(await versions(db)).toEqual([6n, 7n]);
    expect(await store.readCursor("s")).toBe("c");
    expect(await edge(db)).toBe("5");
  });
});
