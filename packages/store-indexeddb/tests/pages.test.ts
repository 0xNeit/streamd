import { describe, expect, it } from "bun:test";
import { CachingAdapter, StreamdError, type EventAdapter, type EventDescriptor, type EventPage } from "@streamd/core";
import { IndexedDbPageStore } from "../src/index.js";
import { FakeIndexedDb } from "./fake-idb.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };

describe("IndexedDbPageStore", () => {
  it("keeps pages across instances, and replaces a key", async () => {
    const indexedDB = new FakeIndexedDb();
    await new IndexedDbPageStore({ namespace: "mainnet", indexedDB }).set("k", "one");
    const again = new IndexedDbPageStore({ namespace: "mainnet", indexedDB });
    expect(await again.get("k")).toBe("one");
    await again.set("k", "two");
    expect(await again.get("k")).toBe("two");
    expect(await again.get("missing")).toBeUndefined();
    expect(indexedDB.keys()).toEqual(["mainnet\nk"]);
  });

  it("keeps namespaces apart", async () => {
    const indexedDB = new FakeIndexedDb();
    const mainnet = new IndexedDbPageStore({ namespace: "mainnet", indexedDB });
    const testnet = new IndexedDbPageStore({ namespace: "testnet", indexedDB });
    await mainnet.set("k", "m");
    expect(await testnet.get("k")).toBeUndefined();
    await testnet.set("k", "t");
    expect(await mainnet.get("k")).toBe("m");
  });

  it("keeps the newest written up to maxEntries, a rewrite counting as new", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", maxEntries: 2, indexedDB });
    await store.set("a", "1");
    await store.set("b", "2");
    await store.set("a", "3");
    await store.set("c", "4");
    expect(await store.get("b")).toBeUndefined();
    expect(await store.get("a")).toBe("3");
    expect(await store.get("c")).toBe("4");
    expect(indexedDB.keys()).toEqual(["n\na", "n\nc"]);
  });

  it("evicts across namespaces, and trims a database already past the limit", async () => {
    const indexedDB = new FakeIndexedDb();
    const open = new IndexedDbPageStore({ namespace: "x", indexedDB });
    for (const key of ["1", "2", "3", "4"]) await open.set(key, key);
    await new IndexedDbPageStore({ namespace: "y", maxEntries: 2, indexedDB }).set("5", "5");
    expect(indexedDB.keys()).toEqual(["x\n4", "y\n5"]);
  });

  it("rejects a write the quota refuses, keeping what was there", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", indexedDB });
    await store.set("a", "1");
    indexedDB.abortWrites = { name: "QuotaExceededError" };
    const failed = Promise.race([store.set("b", "2"), Bun.sleep(200).then(() => "hung")]);
    await expect(failed).rejects.toThrow(StreamdError);
    await expect(failed).rejects.toThrow("quota");
    indexedDB.abortWrites = undefined;
    expect(await store.get("a")).toBe("1");
    expect(await store.get("b")).toBeUndefined();
  });

  it("keeps the page count as it writes, so a limit needs no scan", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", indexedDB });
    await store.set("a", "1");
    await store.set("b", "2");
    await store.set("a", "3");
    expect(indexedDB.meta.get("count")).toBe(2);
    await new IndexedDbPageStore({ namespace: "n", maxEntries: 1, indexedDB }).set("c", "4");
    expect(indexedDB.meta.get("count")).toBe(1);
    expect(indexedDB.keys()).toEqual(["n\nc"]);
  });

  it("keeps exactly maxEntries pages", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", maxEntries: 2, indexedDB });
    await store.set("a", "1");
    await store.set("b", "2");
    expect(indexedDB.keys()).toEqual(["n\na", "n\nb"]);
  });

  it("keeps every write when several land at once", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", maxEntries: 3, indexedDB });
    await Promise.all(["a", "b", "a", "c", "d"].map((key, i) => store.set(key, String(i))));
    expect(indexedDB.keys()).toEqual(["n\na", "n\nc", "n\nd"]);
    expect(await store.get("a")).toBe("2");
  });

  it("creates one auto-increment store with a unique key index", async () => {
    const indexedDB = new FakeIndexedDb();
    await new IndexedDbPageStore({ namespace: "n", indexedDB }).get("a");
    expect(indexedDB.schema).toEqual({ store: "pages", autoIncrement: true, index: ["key", "key", { unique: true }] });
  });

  it("rejects a failed read, and a failed write rolls back", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", maxEntries: 5, indexedDB });
    await store.set("a", "1");
    indexedDB.failRequests = "get";
    await expect(store.get("a")).rejects.toThrow("IndexedDB read failed");
    indexedDB.failRequests = "add";
    const failed = store.set("a", "2");
    await expect(failed).rejects.toThrow("IndexedDB write failed");
    await expect(failed).rejects.not.toThrow("quota");
    indexedDB.failRequests = undefined;
    expect(await store.get("a")).toBe("1");
  });

  it("closes its connection for another tab's upgrade and reopens on the next call", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", indexedDB });
    await store.set("a", "1");
    indexedDB.versionChange();
    expect(indexedDB.connections.map((db) => (db as { closed?: boolean }).closed)).toEqual([true]);
    expect(await store.get("a")).toBe("1");
    expect(indexedDB.opens).toBe(2);
  });

  it("retries a failed open on the next call", async () => {
    const indexedDB = new FakeIndexedDb();
    indexedDB.failOpens = 1;
    const store = new IndexedDbPageStore({ namespace: "n", indexedDB });
    await expect(store.get("a")).rejects.toThrow("IndexedDB open failed");
    await store.set("a", "1");
    expect(await store.get("a")).toBe("1");
    expect(indexedDB.opens).toBe(2);
  });

  it("reads a record that is not text as a miss", async () => {
    const indexedDB = new FakeIndexedDb();
    const store = new IndexedDbPageStore({ namespace: "n", indexedDB });
    await store.set("a", "1");
    indexedDB.row("n\na")!.page = { events: [] };
    expect(await store.get("a")).toBeUndefined();
  });

  it("rejects every call where there is no IndexedDB, without throwing at construction", async () => {
    const store = new IndexedDbPageStore({ namespace: "n" });
    await expect(store.get("a")).rejects.toThrow("IndexedDB is not available");
    await expect(store.set("a", "1")).rejects.toThrow(StreamdError);
  });

  it("refuses bad arguments", () => {
    expect(() => new IndexedDbPageStore({} as never)).toThrow("namespace");
    expect(() => new IndexedDbPageStore(undefined as never)).toThrow("namespace");
    expect(() => new IndexedDbPageStore({ namespace: "" })).toThrow("namespace");
    expect(() => new IndexedDbPageStore({ namespace: "n", indexedDB: {} as never })).toThrow(StreamdError);
    for (const maxEntries of [0, -1, 1.5]) {
      expect(() => new IndexedDbPageStore({ namespace: "n", maxEntries })).toThrow("maxEntries");
    }
  });

  it("lets CachingAdapter answer a reload with no fetch, bigints restored", async () => {
    const indexedDB = new FakeIndexedDb();
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
    await new CachingAdapter(source, { store: new IndexedDbPageStore({ namespace: "n", indexedDB }) }).fetchEvents(descriptor, options);
    const reloaded = new CachingAdapter(source, { store: new IndexedDbPageStore({ namespace: "n", indexedDB }) });
    expect(await reloaded.fetchEvents(descriptor, options)).toEqual(page);
    expect(fetches).toBe(1);
  });

  it("lets CachingAdapter fall back to the source when the store fails", async () => {
    const indexedDB = new FakeIndexedDb();
    indexedDB.failOpens = 2;
    const errors: unknown[] = [];
    const page: EventPage = { events: [], nextCursor: "1", hasMore: false, final: true };
    const source: EventAdapter = { fetchEvents: async () => page };
    const adapter = new CachingAdapter(source, {
      store: new IndexedDbPageStore({ namespace: "n", indexedDB }),
      onStoreError: (err) => errors.push(err),
    });
    expect(await adapter.fetchEvents(descriptor, { fromVersion: 1n })).toEqual(page);
    expect(errors).toHaveLength(2);
  });
});
