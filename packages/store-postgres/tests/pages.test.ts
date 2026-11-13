import { describe, expect, it } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { CachingAdapter, StreamdError, type EventAdapter, type EventDescriptor, type EventPage } from "@streamd/core";
import { PostgresPageStore } from "../src/index.js";
import type { PostgresClient } from "../src/index.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };

async function count(db: PGlite): Promise<number> {
  const { rows } = await db.query<{ n: number }>("SELECT COUNT(*)::int AS n FROM streamd_pages");
  return rows[0]!.n;
}

/** A client that counts the queries it forwards, after the schema is in place. */
function counting(db: PGlite): PostgresClient & { sent: string[] } {
  const sent: string[] = [];
  return {
    sent,
    query: async (text, params) => {
      sent.push(text);
      return db.query(text, params) as Promise<{ rows: Record<string, unknown>[] }>;
    },
  };
}

describe("PostgresPageStore", () => {
  it("keeps pages across instances, and replaces a key", async () => {
    const db = new PGlite();
    await new PostgresPageStore(db, { namespace: "mainnet" }).set("k", "one");
    const again = new PostgresPageStore(db, { namespace: "mainnet" });
    expect(await again.get("k")).toBe("one");
    await again.set("k", "two");
    expect(await again.get("k")).toBe("two");
    expect(await again.get("missing")).toBeUndefined();
    expect(await count(db)).toBe(1);
  });

  it("keeps namespaces apart", async () => {
    const db = new PGlite();
    const mainnet = new PostgresPageStore(db, { namespace: "mainnet" });
    const testnet = new PostgresPageStore(db, { namespace: "testnet" });
    await mainnet.set("k", "m");
    expect(await testnet.get("k")).toBeUndefined();
    await testnet.set("k", "t");
    expect(await mainnet.get("k")).toBe("m");
  });

  it("keeps the newest written up to maxEntries, a rewrite counting as new and a read not", async () => {
    const db = new PGlite();
    const store = new PostgresPageStore(db, { namespace: "n", maxEntries: 2 });
    await store.set("a", "1");
    await store.set("b", "2");
    await store.set("a", "3");
    expect(await store.get("b")).toBe("2");
    await store.set("c", "4");
    expect(await store.get("b")).toBeUndefined();
    expect(await store.get("a")).toBe("3");
    expect(await store.get("c")).toBe("4");
    expect(await count(db)).toBe(2);
  });

  it("bounds the whole table, every namespace together, and keeps one at maxEntries 1", async () => {
    const db = new PGlite();
    await new PostgresPageStore(db, { namespace: "x" }).set("k", "x");
    const bounded = new PostgresPageStore(db, { namespace: "y", maxEntries: 1 });
    await bounded.set("k", "y1");
    expect(await count(db)).toBe(1);
    await bounded.set("k", "y2");
    await bounded.set("j", "y3");
    expect(await count(db)).toBe(1);
    expect(await bounded.get("j")).toBe("y3");
  });

  it("trims a table already past the bound, keeping an old key it rewrites", async () => {
    const db = new PGlite();
    const unbounded = new PostgresPageStore(db, { namespace: "n" });
    for (const key of ["a", "b", "c", "d"]) await unbounded.set(key, key);
    const bounded = new PostgresPageStore(db, { namespace: "n", maxEntries: 2 });
    await bounded.set("a", "again");
    expect(await count(db)).toBe(2);
    expect(await bounded.get("a")).toBe("again");
    expect(await bounded.get("d")).toBe("d");
  });

  it("sends one statement per set, bounded or not", async () => {
    const db = new PGlite();
    const client = counting(db);
    await new PostgresPageStore(client, { namespace: "n" }).get("warm");
    for (const store of [new PostgresPageStore(client, { namespace: "n" }), new PostgresPageStore(client, { namespace: "n", maxEntries: 3 })]) {
      client.sent.length = 0;
      await store.set("k", "v");
      expect(client.sent).toHaveLength(1);
    }
  });

  it("refuses bad arguments before sending anything", async () => {
    const db = counting(new PGlite());
    expect(() => new PostgresPageStore({} as never, { namespace: "n" })).toThrow(StreamdError);
    expect(() => new PostgresPageStore(undefined as never, { namespace: "n" })).toThrow(StreamdError);
    expect(() => new PostgresPageStore(db, {} as never)).toThrow("namespace");
    expect(() => new PostgresPageStore(db, undefined as never)).toThrow("namespace");
    expect(() => new PostgresPageStore(db, { namespace: "" })).toThrow("namespace");
    expect(() => new PostgresPageStore(db, { namespace: "a\0b" })).toThrow("NUL");
    for (const maxEntries of [0, -1, 1.5, Number.NaN]) {
      expect(() => new PostgresPageStore(db, { namespace: "n", maxEntries })).toThrow("maxEntries");
    }
    expect(db.sent).toEqual([]);
  });

  it("creates only the page table, and retries a schema that failed", async () => {
    const db = new PGlite();
    let fail = true;
    const flaky: PostgresClient = {
      query: async (text, params) => {
        if (fail) throw new Error("down");
        return db.query(text, params) as Promise<{ rows: Record<string, unknown>[] }>;
      },
    };
    const store = new PostgresPageStore(flaky, { namespace: "n" });
    await expect(store.get("k")).rejects.toThrow("down");
    fail = false;
    await store.set("k", "v");
    expect(await store.get("k")).toBe("v");
    const { rows } = await db.query<{ name: string }>(
      "SELECT table_name AS name FROM information_schema.tables WHERE table_name LIKE 'streamd_%'",
    );
    expect(rows.map((r) => r.name)).toEqual(["streamd_pages"]);
  });

  it("lets CachingAdapter answer a restart with no fetch, and treats a corrupt row as a miss", async () => {
    const db = new PGlite();
    const page: EventPage = {
      events: [{ type: "0x1::m::E", data: { n: "1" }, version: 5n, sequenceNumber: 0n, eventIndex: 0 }],
      nextCursor: "6",
      hasMore: true,
      final: true,
      throughVersion: 5n,
    };
    let fetches = 0;
    const source: EventAdapter = { fetchEvents: async () => (fetches++, page) };
    const options = { fromVersion: 1n };
    await new CachingAdapter(source, { store: new PostgresPageStore(db, { namespace: "n" }) }).fetchEvents(descriptor, options);
    const restarted = new CachingAdapter(source, { store: new PostgresPageStore(db, { namespace: "n" }) });
    expect(await restarted.fetchEvents(descriptor, options)).toEqual(page);
    expect(fetches).toBe(1);

    await db.query("UPDATE streamd_pages SET page = '{not json'");
    const errors: unknown[] = [];
    const corrupt = new CachingAdapter(source, {
      store: new PostgresPageStore(db, { namespace: "n" }),
      onStoreError: (err) => errors.push(err),
    });
    expect(await corrupt.fetchEvents(descriptor, options)).toEqual(page);
    expect(fetches).toBe(2);
    expect(errors).toHaveLength(1);
  });
});
