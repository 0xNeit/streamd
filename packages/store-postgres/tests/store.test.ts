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
  const { rows } = await db.query<{ event: unknown }>("SELECT event FROM streamd_events ORDER BY version, event_index");
  return rows.map((r) => parseEvent(JSON.stringify(r.event)));
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
