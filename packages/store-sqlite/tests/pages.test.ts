import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { CachingAdapter, StreamdError, type EventAdapter, type EventDescriptor, type EventPage } from "@streamd/core";
import { SqlitePageStore } from "../src/index.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };

function count(db: Database): number {
  return (db.query("SELECT COUNT(*) AS n FROM streamd_pages").get() as { n: number }).n;
}

describe("SqlitePageStore", () => {
  it("keeps pages across instances, and replaces a key", () => {
    const db = new Database(":memory:");
    new SqlitePageStore(db, { namespace: "mainnet" }).set("k", "one");
    const again = new SqlitePageStore(db, { namespace: "mainnet" });
    expect(again.get("k")).toBe("one");
    again.set("k", "two");
    expect(again.get("k")).toBe("two");
    expect(again.get("missing")).toBeUndefined();
    expect(count(db)).toBe(1);
  });

  it("keeps namespaces apart", () => {
    const db = new Database(":memory:");
    const mainnet = new SqlitePageStore(db, { namespace: "mainnet" });
    const testnet = new SqlitePageStore(db, { namespace: "testnet" });
    mainnet.set("k", "m");
    expect(testnet.get("k")).toBeUndefined();
    testnet.set("k", "t");
    expect(mainnet.get("k")).toBe("m");
  });

  it("keeps the newest written up to maxEntries, a rewrite counting as new", () => {
    const db = new Database(":memory:");
    const store = new SqlitePageStore(db, { namespace: "n", maxEntries: 2 });
    store.set("a", "1");
    store.set("b", "2");
    store.set("a", "3");
    store.set("c", "4");
    expect(store.get("b")).toBeUndefined();
    expect(store.get("a")).toBe("3");
    expect(store.get("c")).toBe("4");
    expect(count(db)).toBe(2);
  });

  it("refuses bad arguments", () => {
    const db = new Database(":memory:");
    expect(() => new SqlitePageStore({} as never, { namespace: "n" })).toThrow(StreamdError);
    expect(() => new SqlitePageStore(db, {} as never)).toThrow("namespace");
    expect(() => new SqlitePageStore(db, undefined as never)).toThrow("namespace");
    expect(() => new SqlitePageStore(db, { namespace: "" })).toThrow("namespace");
    for (const maxEntries of [0, -1, 1.5]) {
      expect(() => new SqlitePageStore(db, { namespace: "n", maxEntries })).toThrow("maxEntries");
    }
  });

  it("lets CachingAdapter answer a restart with no fetch", async () => {
    const db = new Database(":memory:");
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
    await new CachingAdapter(source, { store: new SqlitePageStore(db, { namespace: "n" }) }).fetchEvents(descriptor, options);
    const restarted = new CachingAdapter(source, { store: new SqlitePageStore(db, { namespace: "n" }) });
    expect(await restarted.fetchEvents(descriptor, options)).toEqual(page);
    expect(fetches).toBe(1);
  });
});
