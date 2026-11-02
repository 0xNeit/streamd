import { describe, it, expect } from "bun:test";
import { CursorManager } from "../src/cursor.js";
import type { StreamdEvent } from "../src/types.js";

function ev(version: number, eventIndex = 0, sequenceNumber = 0): StreamdEvent {
  return {
    type: "0x1::test::TestEvent",
    data: null,
    version: BigInt(version),
    sequenceNumber: BigInt(sequenceNumber),
    eventIndex,
  };
}

describe("CursorManager", () => {
  it("tracks the adapter resume token", () => {
    const cursor = new CursorManager("10");
    expect(cursor.token).toBe("10");
    cursor.token = "20";
    expect(cursor.token).toBe("20");
  });

  it("starts with no token", () => {
    expect(new CursorManager().token).toBeUndefined();
  });

  it("deduplicates seen events", () => {
    const cursor = new CursorManager();
    expect(cursor.hasSeen(ev(1))).toBe(false);
    cursor.mark(ev(1));
    expect(cursor.hasSeen(ev(1))).toBe(true);
    expect(cursor.hasSeen(ev(2))).toBe(false);
  });

  it("keys on eventIndex, not sequenceNumber", () => {
    // Module events in one transaction share sequenceNumber 0; keying on it
    // would collapse them into a single event.
    const cursor = new CursorManager();
    cursor.mark(ev(1, 0));

    expect(cursor.hasSeen(ev(1, 1))).toBe(false);
    expect(cursor.hasSeen(ev(1, 0, 99))).toBe(true);
  });

  it("evicts oldest entries when exceeding window size", () => {
    const cursor = new CursorManager();

    // Fill beyond the 1000 limit
    for (let i = 0; i < 1100; i++) {
      cursor.mark(ev(i));
    }

    // Oldest entries should be evicted
    expect(cursor.hasSeen(ev(0))).toBe(false);
    expect(cursor.hasSeen(ev(99))).toBe(false);

    // Recent entries should still be present
    expect(cursor.hasSeen(ev(1099))).toBe(true);
    expect(cursor.hasSeen(ev(500))).toBe(true);
  });

  it("does not double-mark the same event", () => {
    const cursor = new CursorManager();
    cursor.mark(ev(1));
    cursor.mark(ev(1)); // should not grow internal structures
    expect(cursor.hasSeen(ev(1))).toBe(true);
  });
});
