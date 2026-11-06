import { describe, it, expect } from "bun:test";
import { mergeEvents } from "../src/merge.js";
import type { StreamdEvent } from "../src/types.js";

function ev(version: number, eventIndex = 0, type = "0x1::a::E"): StreamdEvent {
  return { type, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

/** A walk over fixed pages that records each pull and whether it was closed. */
function walk(pages: StreamdEvent[][], fail?: Error) {
  const state = { pulls: 0, closed: false };
  async function* run() {
    try {
      for (const page of pages) {
        state.pulls++;
        yield page;
      }
      if (fail !== undefined) {
        state.pulls++;
        throw fail;
      }
    } finally {
      state.closed = true;
    }
  }
  return { gen: run(), state };
}

async function collect(it: AsyncIterable<StreamdEvent>): Promise<string[]> {
  const out: string[] = [];
  for await (const e of it) out.push(`${e.version}:${e.eventIndex}`);
  return out;
}

describe("mergeEvents", () => {
  it("interleaves walks by version, then eventIndex", async () => {
    const a = walk([[ev(1), ev(4, 2)], [ev(9)]]);
    const b = walk([[ev(2), ev(4, 1)], [ev(4, 3), ev(10)]]);
    expect(await collect(mergeEvents([a.gen, b.gen]))).toEqual([
      "1:0", "2:0", "4:1", "4:2", "4:3", "9:0", "10:0",
    ]);
  });

  it("yields an event returned by more than one walk once", async () => {
    const a = walk([[ev(1), ev(3)]]);
    const b = walk([[ev(3), ev(5)]]);
    const c = walk([[ev(3)]]);
    expect(await collect(mergeEvents([a.gen, b.gen, c.gen]))).toEqual(["1:0", "3:0", "5:0"]);
  });

  it("counts limit after dedup and pulls no page it does not need", async () => {
    const a = walk([[ev(1), ev(2), ev(6)], [ev(8)]]);
    const b = walk([[ev(2), ev(3)], [ev(7)]]);
    expect(await collect(mergeEvents([a.gen, b.gen], 3))).toEqual(["1:0", "2:0", "3:0"]);
    expect(a.state.pulls).toBe(1);
    expect(b.state.pulls).toBe(1);
    expect(a.state.closed && b.state.closed).toBe(true);
  });

  it("handles empty walks and empty pages", async () => {
    const a = walk([]);
    const b = walk([[], [ev(5)], []]);
    expect(await collect(mergeEvents([a.gen, b.gen]))).toEqual(["5:0"]);
    expect(await collect(mergeEvents([]))).toEqual([]);
  });

  it("rejects with a walk's error and closes the others", async () => {
    const boom = new Error("boom");
    const a = walk([[ev(1)]], boom);
    const b = walk([[ev(2)], [ev(3)]]);
    await expect(collect(mergeEvents([a.gen, b.gen]))).rejects.toBe(boom);
    expect(b.state.closed).toBe(true);
  });

  it("closes every walk when the consumer leaves early", async () => {
    const a = walk([[ev(1)], [ev(3)]]);
    const b = walk([[ev(2)], [ev(4)]]);
    for await (const e of mergeEvents([a.gen, b.gen])) {
      expect(e.version).toBe(1n);
      break;
    }
    expect(a.state.closed && b.state.closed).toBe(true);
    expect(a.state.pulls + b.state.pulls).toBe(2);
  });
});
