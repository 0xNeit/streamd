import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import { runCompare } from "../src/compare.js";
import type { CompareOptions } from "../src/args.js";

const TYPE = "0x1::coin::DepositEvent";
const ev = (v: number, amount = "1"): StreamdEvent => ({ type: TYPE, data: { amount }, version: BigInt(v), sequenceNumber: 0n, eventIndex: 0 });
const OPTIONS: CompareOptions = {
  descriptor: { kind: "module", eventType: [TYPE] },
  network: "mainnet",
  against: "https://b.example/v1",
  fromVersion: 1n,
  toVersion: 6n,
};

async function run(a: EventAdapter, b: EventAdapter, options = OPTIONS) {
  const lines: Record<string, unknown>[] = [];
  const errors: string[] = [];
  const code = await runCompare(options, {
    a,
    b,
    write: (line) => void lines.push(JSON.parse(line)),
    error: (m) => void errors.push(m),
  });
  return { code, lines, errors, summary: lines.at(-1)! };
}

describe("runCompare", () => {
  const all = [ev(1), ev(2), ev(3), ev(4), ev(5), ev(6)];

  it("exits 0 with only a summary when the nodes agree", async () => {
    const { code, lines, summary } = await run(new MemoryAdapter(all), new MemoryAdapter(all));
    expect(code).toBe(0);
    expect(lines).toHaveLength(1);
    expect(summary).toMatchObject({ kind: "summary", ok: true, window: { fromVersion: "1", toVersion: "6" }, matched: 6, errors: {} });
  });

  it("prints missing, extra and different events as lines, then exits 1", async () => {
    const a = new MemoryAdapter([ev(1), ev(2), ev(3, "9"), ev(4), ev(6)]);
    const b = new MemoryAdapter([ev(1), ev(3), ev(4), ev(5), ev(6)]);
    const { code, lines, summary } = await run(a, b);
    expect(code).toBe(1);
    expect(lines.slice(0, -1)).toEqual([
      { kind: "missing", event: { type: TYPE, data: { amount: "1" }, version: "2", sequenceNumber: "0", eventIndex: 0 } },
      { kind: "extra", event: { type: TYPE, data: { amount: "1" }, version: "5", sequenceNumber: "0", eventIndex: 0 } },
      {
        kind: "different",
        id: "3:0",
        fields: ["data"],
        a: { type: TYPE, data: { amount: "9" }, version: "3", sequenceNumber: "0", eventIndex: 0 },
        b: { type: TYPE, data: { amount: "1" }, version: "3", sequenceNumber: "0", eventIndex: 0 },
      },
    ]);
    expect(summary).toMatchObject({ ok: false, matched: 3 });
  });

  it("exits 1 when a node fails, reporting it in the summary", async () => {
    const down: EventAdapter = { fetchEvents: async () => Promise.reject(new StreamdNetworkError("node down", 503)) };
    const { code, summary } = await run(new MemoryAdapter(all), down);
    expect(code).toBe(1);
    expect(summary).toMatchObject({ ok: false, window: null });
    expect(String((summary.errors as { b?: string }).b)).toContain("node down");
  });

  it("passes the page bounds through, and reports a refused range as an error", async () => {
    const pages: number[] = [];
    const counting: EventAdapter = { fetchEvents: (d, o) => (pages.push(o.limit ?? 0), new MemoryAdapter(all).fetchEvents(d, o)) };
    const { summary } = await run(counting, new MemoryAdapter(all), { ...OPTIONS, limit: 2, maxPages: 2 });
    expect(pages).toEqual([2, 2]);
    expect(summary).toMatchObject({ pagesRead: { a: 2 } });
    const bad = await run(new MemoryAdapter(all), new MemoryAdapter(all), { ...OPTIONS, fromVersion: 9n });
    expect(bad.code).toBe(1);
    expect(bad.errors[0]).toStartWith("streamd: ");
  });
});
