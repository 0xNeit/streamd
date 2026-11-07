import { describe, it, expect } from "bun:test";
import { compareAdapters } from "../src/compare.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, EventPage, StreamdEvent } from "../src/types.js";

const TYPE = "0x1::test::Ping";
const desc: EventDescriptor = { kind: "module", eventType: TYPE };
const range = { fromVersion: 10n, toVersion: 20n };

function ev(version: number, eventIndex = 0, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: TYPE, data: { n: "1" }, version: BigInt(version), sequenceNumber: 0n, eventIndex, ...extra };
}

/** Answers every fetch with the next of `pages`, then the last one again; counts calls. */
function scripted(pages: unknown[]): EventAdapter & { calls: number } {
  const adapter = {
    calls: 0,
    async fetchEvents() {
      const page = pages[Math.min(adapter.calls, pages.length - 1)];
      adapter.calls++;
      return page as EventPage;
    },
  };
  return adapter;
}

const page = (events: StreamdEvent[], more: Partial<EventPage> = {}): EventPage => ({
  events,
  nextCursor: String(events.at(-1)?.version ?? "x"),
  hasMore: false,
  ...more,
});

describe("compareAdapters", () => {
  it("reports two identical sources as ok over the whole range", async () => {
    const events = [ev(10), ev(12), ev(12, 1), ev(20), ev(25)];
    const report = await compareAdapters(new MemoryAdapter(events), new MemoryAdapter(events), desc, {
      ...range,
      limit: 2,
    });
    expect(report.ok).toBe(true);
    expect(report.window).toEqual({ fromVersion: 10n, toVersion: 20n });
    expect(report.matched).toBe(4);
    expect(report.pagesRead.a).toBeGreaterThan(1);
    expect(report.errors).toEqual({});
  });

  it("reports missing, extra and different events by eventId", async () => {
    const a = new MemoryAdapter([ev(11), ev(12), ev(13, 0, { data: { n: 2n, nested: [1, { x: "y" }] } }), ev(30)]);
    const b = new MemoryAdapter([
      ev(12),
      ev(13, 0, { data: { nested: [1, { x: "z" }], n: 2n }, sequenceNumber: 4n, type: "0x1::test::Pong" }),
      ev(14),
      ev(30),
    ]);
    const report = await compareAdapters(a, b, { kind: "module", eventType: [TYPE, "0x1::test::Pong"] }, {
      fromVersion: 10n,
      toVersion: 14n,
    });
    expect(report.ok).toBe(false);
    expect(report.missing.map((e) => e.version)).toEqual([11n]);
    expect(report.extra.map((e) => e.version)).toEqual([14n]);
    expect(report.different).toHaveLength(1);
    expect(report.different[0]!.id).toBe("13:0");
    expect(report.different[0]!.fields).toEqual(["type", "data", "sequenceNumber"]);
    expect(report.matched).toBe(1);
  });

  it("compares data deeply, bigint-safe and regardless of key order", async () => {
    const a = new MemoryAdapter([ev(11, 0, { data: { a: 1n, b: [2n, { c: "d" }] } })]);
    const b = new MemoryAdapter([ev(11, 0, { data: { b: [2n, { c: "d" }], a: 1n } })]);
    const c = new MemoryAdapter([ev(11, 0, { data: { a: "1", b: [2n, { c: "d" }] } })]);
    expect((await compareAdapters(a, b, desc, range)).ok).toBe(true);
    expect((await compareAdapters(a, c, desc, range)).different[0]!.fields).toEqual(["data"]);
  });

  it("compares optional fields only when both sides set them, normalizing addresses", async () => {
    const full = {
      transactionHash: "0xAB",
      timestamp: 5n,
      sender: "0x1",
      guid: { creationNumber: 3n, accountAddress: "0x1" },
      entryFunction: { function: "0x1::m::f", typeArguments: [], arguments: ["1"] },
      state: [1],
    };
    const padded = {
      ...full,
      transactionHash: "0xab",
      sender: "0x0000000000000000000000000000000000000000000000000000000000000001",
      guid: { creationNumber: 3n, accountAddress: "0x01" },
      state: [2],
    };
    const same = await compareAdapters(new MemoryAdapter([ev(11, 0, full)]), new MemoryAdapter([ev(11, 0, padded)]), desc, range);
    expect(same.ok).toBe(true);
    const bare = await compareAdapters(new MemoryAdapter([ev(11, 0, full)]), new MemoryAdapter([ev(11)]), desc, range);
    expect(bare.ok).toBe(true);
    const changed = {
      transactionHash: "0xcd",
      timestamp: 6n,
      sender: "0x2",
      guid: { creationNumber: 4n, accountAddress: "0x1" },
      entryFunction: { function: "0x1::m::g", typeArguments: [], arguments: ["1"] },
    };
    const report = await compareAdapters(new MemoryAdapter([ev(11, 0, full)]), new MemoryAdapter([ev(11, 0, changed)]), desc, range);
    expect(report.different[0]!.fields).toEqual(["guid", "transactionHash", "timestamp", "sender", "entryFunction"]);
  });

  it("compares only the window both sources covered, from throughVersion", async () => {
    const a = scripted([page([ev(11), ev(15), ev(18)], { throughVersion: 20n })]);
    const b = scripted([page([ev(11)], { throughVersion: 14n, hasMore: true }), page([], { throughVersion: 14n, hasMore: true })]);
    const report = await compareAdapters(a, b, desc, { ...range, maxPages: 2 });
    expect(report.coverage).toEqual({ a: 20n, b: 14n });
    expect(report.window).toEqual({ fromVersion: 10n, toVersion: 14n });
    expect(report.ok).toBe(true);
    expect(report.matched).toBe(1);
  });

  it("without throughVersion, covers the last version seen, or below it when cut short", async () => {
    const ended = scripted([page([ev(11), ev(13)])]);
    const cut = scripted([page([ev(11), ev(13)], { hasMore: true })]);
    const report = await compareAdapters(ended, cut, desc, { ...range, maxPages: 1 });
    expect(report.coverage).toEqual({ a: 13n, b: 12n });
    expect(report.window).toEqual({ fromVersion: 10n, toVersion: 12n });
    expect(report.matched).toBe(1);
  });

  it("has no window when one source covered nothing", async () => {
    const report = await compareAdapters(new MemoryAdapter([ev(11)]), scripted([page([])]), desc, range);
    expect(report.coverage.b).toBe(9n);
    expect(report.window).toBeUndefined();
    expect(report.ok).toBe(false);
    expect(report.missing).toEqual([]);
  });

  it("stops a walk once it passed toVersion, even if the source ignores it", async () => {
    const ignoring = scripted([page([ev(11), ev(30)], { hasMore: true })]);
    const report = await compareAdapters(ignoring, new MemoryAdapter([ev(11)]), desc, range);
    expect(ignoring.calls).toBe(1);
    expect(report.coverage.a).toBe(20n);
    expect(report.ok).toBe(true);
  });

  it("reports a rejecting or malformed source in errors and compares what it read", async () => {
    let calls = 0;
    const flaky: EventAdapter = {
      async fetchEvents() {
        if (calls++ > 0) throw new Error("boom");
        return page([ev(11), ev(13)], { hasMore: true, throughVersion: 13n });
      },
    };
    const report = await compareAdapters(flaky, new MemoryAdapter([ev(11), ev(13), ev(16)]), desc, range);
    expect(report.errors.a).toBe("fetchEvents rejected: Error: boom");
    expect(report.window).toEqual({ fromVersion: 10n, toVersion: 13n });
    expect(report.matched).toBe(2);
    expect(report.ok).toBe(false);

    const malformed = await compareAdapters(new MemoryAdapter([ev(11)]), scripted([{ events: "no" }]), desc, range);
    expect(malformed.errors.b).toBe("page 1: events is not an array");
    expect(malformed.pagesRead.b).toBe(0);
  });

  it("caps each list at maxReported and says so", async () => {
    const many = [ev(11), ev(12), ev(13), ev(14), ev(30)];
    const report = await compareAdapters(new MemoryAdapter(many), scripted([page([], { throughVersion: 20n })]), desc, {
      ...range,
      maxReported: 2,
    });
    expect(report.missing.map((e) => e.version)).toEqual([11n, 12n]);
    expect(report.truncated).toBe(true);
  });

  it("rejects an invalid range before any fetch", async () => {
    const a = scripted([page([])]);
    const bad = [
      undefined,
      { fromVersion: 1n },
      { fromVersion: -1n, toVersion: 2n },
      { fromVersion: 3n, toVersion: 2n },
      { fromVersion: 1, toVersion: 2n },
      { ...range, limit: 0 },
      { ...range, maxPages: 1.5 },
      { ...range, maxReported: -1 },
    ];
    for (const r of bad) {
      await expect(compareAdapters(a, a, desc, r as never)).rejects.toBeInstanceOf(StreamdError);
    }
    await expect(compareAdapters({} as never, a, desc, range)).rejects.toThrow("compareAdapters requires a valid EventAdapter");
    expect(a.calls).toBe(0);
  });
});
