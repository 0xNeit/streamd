import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { MemoryDedupStore } from "../src/store.js";

describe("MemoryDedupStore", () => {
  it("reports added ids as seen, in the order asked", () => {
    const store = new MemoryDedupStore();
    expect(store.seen(["1:0", "2:0"])).toEqual([false, false]);
    store.add(["2:0"]);
    expect(store.seen(["1:0", "2:0"])).toEqual([false, true]);
  });

  it("forgets the oldest ids past maxEntries", () => {
    const store = new MemoryDedupStore({ maxEntries: 2 });
    store.add(["1:0", "2:0"]);
    store.add(["3:0"]);
    expect(store.seen(["1:0", "2:0", "3:0"])).toEqual([false, true, true]);
  });

  it("keeps an id once however often it is added", () => {
    const store = new MemoryDedupStore({ maxEntries: 2 });
    store.add(["1:0", "1:0", "2:0"]);
    store.add(["1:0"]);
    expect(store.seen(["1:0", "2:0"])).toEqual([true, true]);
    store.add(["3:0", "4:0", "5:0"]);
    expect(store.seen(["1:0", "2:0", "3:0", "4:0", "5:0"])).toEqual([false, false, false, true, true]);
  });

  it("refuses a maxEntries that is not a positive integer", () => {
    for (const maxEntries of [0, -1, 1.5, Number.NaN]) {
      expect(() => new MemoryDedupStore({ maxEntries })).toThrow(StreamdError);
    }
  });
});
