import { describe, it, expect } from "bun:test";
import { checkAdapter } from "../src/conformance.js";
import type { AdapterCheckReport } from "../src/conformance.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "../src/types.js";

const TYPE = "0x1::test::Ping";
const desc: EventDescriptor = { kind: "module", eventType: TYPE };

function ev(version: number, eventIndex = 0): StreamdEvent {
  return { type: TYPE, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

function statuses(report: AdapterCheckReport): Record<string, string> {
  return Object.fromEntries(report.results.map((r) => [r.name, r.status]));
}

function failed(report: AdapterCheckReport): string[] {
  return report.results.filter((r) => r.status === "fail").map((r) => r.name);
}

/** An adapter serving `pages` in order, counting calls. */
function scripted(pages: unknown[]): EventAdapter & { calls: FetchOptions[] } {
  const calls: FetchOptions[] = [];
  return {
    calls,
    async fetchEvents(_d, options) {
      calls.push(options);
      return pages[Math.min(calls.length - 1, pages.length - 1)] as EventPage;
    },
  };
}

const seeded = () => new MemoryAdapter([1, 2, 3, 4, 5, 6, 7].map((v) => ev(v)));

describe("checkAdapter walk", () => {
  it("passes a MemoryAdapter on every walk rule", async () => {
    const report = await checkAdapter(seeded(), desc, { fromVersion: 2n, limit: 2 });
    expect(report.ok).toBe(true);
    expect(statuses(report)).toEqual({
      fetch: "pass",
      "page-shape": "pass",
      limit: "pass",
      "from-version": "pass",
      order: "pass",
      "cursor-advance": "pass",
    });
    expect(report.pagesFetched).toBe(3);
  });

  it("skips from-version when no fromVersion is given", async () => {
    const report = await checkAdapter(seeded(), desc);
    expect(report.ok).toBe(true);
    expect(report.results.find((r) => r.name === "from-version")?.status).toBe("skip");
  });

  it("never fetches more than maxPages", async () => {
    const page = { events: [], nextCursor: undefined, hasMore: true };
    let n = 0;
    const adapter: EventAdapter = {
      async fetchEvents() {
        n++;
        return { ...page, nextCursor: String(n) };
      },
    };
    const report = await checkAdapter(adapter, desc, { maxPages: 2 });
    expect(n).toBe(2);
    expect(report.pagesFetched).toBe(2);
    expect(report.ok).toBe(true);
  });

  it("asks with fromVersion first, then with the cursor alone", async () => {
    const adapter = scripted([
      { events: [ev(5)], nextCursor: "a", hasMore: true },
      { events: [ev(6)], nextCursor: "b", hasMore: false },
    ]);
    await checkAdapter(adapter, desc, { fromVersion: 5n, limit: 1 });
    expect(adapter.calls).toEqual([
      { fromVersion: 5n, limit: 1 },
      { cursor: "a", limit: 1 },
    ]);
  });

  it("fails fetch on a rejection and judges the pages before it", async () => {
    let n = 0;
    const adapter: EventAdapter = {
      async fetchEvents() {
        if (n++ === 0) return { events: [ev(1)], nextCursor: "1", hasMore: true };
        throw new TypeError("boom");
      },
    };
    const report = await checkAdapter(adapter, desc);
    expect(failed(report)).toEqual(["fetch"]);
    expect(report.results[0]!.message).toContain("TypeError: boom");
    expect(report.pagesFetched).toBe(1);
  });

  it("fails fetch on a synchronous throw and skips the page rules", async () => {
    const adapter = {
      fetchEvents() {
        throw new Error("sync");
      },
    } as unknown as EventAdapter;
    const report = await checkAdapter(adapter, desc, { fromVersion: 1n });
    expect(failed(report)).toEqual(["fetch"]);
    expect(statuses(report)).toMatchObject({ "page-shape": "skip", order: "skip", "from-version": "skip" });
  });

  it.each([
    ["a null page", null, "page is not an object"],
    ["missing hasMore", { events: [] }, "hasMore"],
    ["a numeric cursor", { events: [], hasMore: false, nextCursor: 3 }, "nextCursor"],
    ["a number version", { events: [{ ...ev(1), version: 1 }], hasMore: false }, "events[0] version"],
    ["a fractional eventIndex", { events: [{ ...ev(1), eventIndex: 0.5 }], hasMore: false }, "eventIndex"],
    ["a string timestamp", { events: [{ ...ev(1), timestamp: "1" }], hasMore: false }, "timestamp"],
    ["a numeric sender", { events: [{ ...ev(1), sender: 1 }], hasMore: false }, "sender"],
  ])("fails page-shape on %s", async (_label, page, message) => {
    const report = await checkAdapter(scripted([page]), desc);
    expect(failed(report)).toEqual(["page-shape"]);
    expect(report.results.find((r) => r.name === "page-shape")!.message).toContain(message);
  });

  it("fails limit on an over-full page", async () => {
    const report = await checkAdapter(
      scripted([{ events: [ev(1), ev(2), ev(3)], hasMore: false }]),
      desc,
      { limit: 2 },
    );
    expect(failed(report)).toEqual(["limit"]);
  });

  it("fails from-version on an event below it", async () => {
    const report = await checkAdapter(scripted([{ events: [ev(3), ev(4)], hasMore: false }]), desc, {
      fromVersion: 4n,
    });
    expect(failed(report)).toEqual(["from-version"]);
  });

  it("fails order on descending events within a page", async () => {
    const report = await checkAdapter(scripted([{ events: [ev(4), ev(3)], hasMore: false }]), desc);
    expect(failed(report)).toEqual(["order"]);
  });

  it("fails order on an event repeated across pages", async () => {
    const report = await checkAdapter(
      scripted([
        { events: [ev(1), ev(2, 1)], nextCursor: "1", hasMore: true },
        { events: [ev(2, 1)], nextCursor: "2", hasMore: false },
      ]),
      desc,
    );
    expect(failed(report)).toEqual(["order"]);
  });

  it("fails cursor-advance on hasMore without a cursor or with a stalled one", async () => {
    const missing = await checkAdapter(scripted([{ events: [], hasMore: true }]), desc);
    expect(failed(missing)).toEqual(["cursor-advance"]);
    const stalled = scripted([
      { events: [], nextCursor: "7", hasMore: true },
      { events: [], nextCursor: "7", hasMore: true },
    ]);
    const report = await checkAdapter(stalled, desc);
    expect(failed(report)).toEqual(["cursor-advance"]);
    expect(stalled.calls).toHaveLength(2);
  });

  it("rejects a non-adapter with StreamdError", async () => {
    await expect(checkAdapter({} as EventAdapter, desc)).rejects.toBeInstanceOf(StreamdError);
  });

  it.each([
    ["a zero limit", { limit: 0 }],
    ["a fractional maxPages", { maxPages: 1.5 }],
    ["a negative fromVersion", { fromVersion: -1n }],
    ["a number fromVersion", { fromVersion: 1 }],
    ["null options", null],
  ])("rejects %s with StreamdError before any call", async (_label, options) => {
    const adapter = scripted([{ events: [], hasMore: false }]);
    await expect(checkAdapter(adapter, desc, options as never)).rejects.toBeInstanceOf(StreamdError);
    expect(adapter.calls).toHaveLength(0);
  });
});
