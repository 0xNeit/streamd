import { describe, it, expect } from "bun:test";
import { CursorManager } from "../src/cursor.js";
import { StreamdError } from "../src/errors.js";
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

    // Boundary and recent entries should still be present
    expect(cursor.hasSeen(ev(100))).toBe(true);
    expect(cursor.hasSeen(ev(1099))).toBe(true);
    expect(cursor.hasSeen(ev(500))).toBe(true);
  });

  it("does not double-mark the same event or evict other entries", () => {
    const cursor = new CursorManager(undefined, 2);
    cursor.mark(ev(1));
    cursor.mark(ev(2));
    // Re-marking ev(1) must not grow the window or evict ev(2).
    cursor.mark(ev(1));
    expect(cursor.hasSeen(ev(1))).toBe(true);
    expect(cursor.hasSeen(ev(2))).toBe(true);

    // Marking a third distinct event evicts the oldest (ev(1)), keeping ev(2) and ev(3).
    cursor.mark(ev(3));
    expect(cursor.hasSeen(ev(1))).toBe(false);
    expect(cursor.hasSeen(ev(2))).toBe(true);
    expect(cursor.hasSeen(ev(3))).toBe(true);
  });

  it("respects a custom dedupWindow size", () => {
    const cursor = new CursorManager("10", 3);
    expect(cursor.token).toBe("10");

    cursor.mark(ev(1));
    cursor.mark(ev(2));
    cursor.mark(ev(3));
    expect(cursor.hasSeen(ev(1))).toBe(true);
    expect(cursor.hasSeen(ev(2))).toBe(true);
    expect(cursor.hasSeen(ev(3))).toBe(true);

    cursor.mark(ev(4));
    expect(cursor.hasSeen(ev(1))).toBe(false);
    expect(cursor.hasSeen(ev(2))).toBe(true);
    expect(cursor.hasSeen(ev(3))).toBe(true);
    expect(cursor.hasSeen(ev(4))).toBe(true);
  });

  it("disables deduplication when dedupWindow is 0", () => {
    const cursor = new CursorManager("5", 0);
    expect(cursor.token).toBe("5");

    cursor.mark(ev(1));
    expect(cursor.hasSeen(ev(1))).toBe(false);
  });

  it("throws StreamdError when dedupWindow is invalid", () => {
    for (const bad of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      expect(() => new CursorManager(undefined, bad)).toThrow(StreamdError);
      expect(() => new CursorManager(undefined, bad)).toThrow(/Invalid dedupWindow/);
    }
  });
});
