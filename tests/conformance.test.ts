import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { checkAdapter } from "../src/conformance.js";
import type { AdapterCheckReport } from "../src/conformance.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { FullnodeScanAdapter } from "../src/adapters/fullnode.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  BlockInfo,
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

const WALK_RULES: string[] = ["fetch", "page-shape", "from-version", "order", "cursor-advance"];

function walkFailures(report: AdapterCheckReport): string[] {
  return failed(report).filter((name) => WALK_RULES.includes(name));
}

/**
 * Serves `pages` by cursor: no cursor is page 1, page N's `nextCursor` is page
 * N + 1. A cursor it never issued rejects as invalid input.
 */
function scripted(pages: unknown[]): EventAdapter & { calls: FetchOptions[] } {
  const calls: FetchOptions[] = [];
  return {
    calls,
    async fetchEvents(_d, options) {
      calls.push(options);
      if (options.cursor === undefined) return pages[0] as EventPage;
      const i = pages.findIndex((p) => (p as EventPage | null)?.nextCursor === options.cursor);
      if (i === -1) throw new StreamdError(`unknown cursor ${options.cursor}`);
      return pages[Math.min(i + 1, pages.length - 1)] as EventPage;
    },
  };
}

const seeded = () => new MemoryAdapter([1, 2, 3, 4, 5, 6, 7].map((v) => ev(v)));

describe("checkAdapter walk", () => {
  it("passes a MemoryAdapter on every rule", async () => {
    const report = await checkAdapter(seeded(), desc, { fromVersion: 2n, limit: 2 });
    expect(report.ok).toBe(true);
    expect(statuses(report)).toEqual({
      fetch: "pass",
      "page-shape": "pass",
      "from-version": "pass",
      order: "pass",
      "cursor-advance": "pass",
      "cursor-precedence": "pass",
      "foreign-cursor": "pass",
      "head-version": "pass",
      "earliest-version": "pass",
      block: "skip",
    });
    expect(report.results.map((r): string => r.name)).toEqual([...WALK_RULES, "cursor-precedence", "foreign-cursor", "head-version", "earliest-version", "block"]);
    expect(report.pagesFetched).toBe(3);
  });

  it("skips from-version when no fromVersion is given", async () => {
    const report = await checkAdapter(seeded(), desc);
    expect(report.ok).toBe(true);
    expect(report.results.find((r) => r.name === "from-version")?.status).toBe("skip");
  });

  it("walks at most maxPages, plus the two extra fetches", async () => {
    const page = { events: [], nextCursor: undefined, hasMore: true };
    let n = 0;
    const adapter: EventAdapter = {
      async fetchEvents() {
        n++;
        return { ...page, nextCursor: String(n) };
      },
    };
    const report = await checkAdapter(adapter, desc, { maxPages: 2 });
    expect(n).toBe(4);
    expect(report.pagesFetched).toBe(2);
  });

  it("asks with fromVersion first, then with the cursor alone", async () => {
    const adapter = scripted([
      { events: [ev(5)], nextCursor: "a", hasMore: true },
      { events: [ev(6)], nextCursor: "b", hasMore: false },
    ]);
    await checkAdapter(adapter, desc, { fromVersion: 5n, limit: 1 });
    expect(adapter.calls.slice(0, 2)).toEqual([
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
    expect(walkFailures(report)).toEqual(["fetch"]);
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
    expect(walkFailures(report)).toEqual(["fetch"]);
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

  it("accepts a page over the limit, as the contract allows", async () => {
    const report = await checkAdapter(
      scripted([{ events: [ev(1), ev(2), ev(3)], hasMore: false }]),
      desc,
      { limit: 2 },
    );
    expect(report.ok).toBe(true);
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
    expect(report.pagesFetched).toBe(2);
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

/** `seeded()` with some of its methods replaced. */
function broken(overrides: Partial<EventAdapter>): EventAdapter {
  const base = seeded();
  return {
    fetchEvents: (d, o) => base.fetchEvents(d, o),
    getHeadVersion: () => base.getHeadVersion(),
    getEarliestVersion: () => base.getEarliestVersion(),
    ...overrides,
  };
}

const walkOptions = { fromVersion: 2n, limit: 2 };

function result(report: AdapterCheckReport, name: string) {
  return report.results.find((r) => r.name === name)!;
}

describe("checkAdapter cursor rules", () => {
  it("fails cursor-precedence when fromVersion beats the cursor", async () => {
    const base = seeded();
    const adapter = broken({
      fetchEvents: (d, o) =>
        base.fetchEvents(d, o.fromVersion !== undefined ? { ...o, cursor: undefined } : o),
    });
    const report = await checkAdapter(adapter, desc, walkOptions);
    expect(failed(report)).toEqual(["cursor-precedence"]);
  });

  it("asks page 2's cursor with the walk's start, or 0n without one", async () => {
    const adapter = scripted([
      { events: [ev(1)], nextCursor: "a", hasMore: true },
      { events: [ev(2)], nextCursor: "b", hasMore: false },
    ]);
    await checkAdapter(adapter, desc, { limit: 1 });
    expect(adapter.calls[2]).toEqual({ cursor: "a", fromVersion: 0n, limit: 1 });
  });

  it("skips cursor-precedence on a one-page walk", async () => {
    const report = await checkAdapter(seeded(), desc);
    expect(result(report, "cursor-precedence").status).toBe("skip");
  });

  it.each([
    ["accepts it", async () => ({ events: [], hasMore: false }), "never issued"],
    ["rejects as a network error", async () => { throw new StreamdNetworkError("down", 503); }, "StreamdNetworkError"],
    ["rejects with a plain Error", async () => { throw new Error("nope"); }, "not a StreamdError"],
  ])("fails foreign-cursor when the adapter %s", async (_label, onForeign, message) => {
    const base = seeded();
    const adapter = broken({
      fetchEvents: (d, o) =>
        o.cursor?.startsWith("streamd-check:") ? (onForeign() as Promise<EventPage>) : base.fetchEvents(d, o),
    });
    const report = await checkAdapter(adapter, desc, walkOptions);
    expect(failed(report)).toEqual(["foreign-cursor"]);
    expect(result(report, "foreign-cursor").message).toContain(message);
  });
});

describe("checkAdapter optional methods", () => {
  it("skips every optional rule a bare adapter lacks", async () => {
    const base = seeded();
    const report = await checkAdapter({ fetchEvents: (d, o) => base.fetchEvents(d, o) }, desc);
    expect(report.ok).toBe(true);
    for (const name of ["head-version", "earliest-version", "block"]) {
      expect(result(report, name)).toEqual({ name, status: "skip", message: "not implemented (optional)" } as never);
    }
  });

  it.each([
    ["head below a seen event", { getHeadVersion: async () => 3n }, "head-version", "below seen event"],
    ["head not a bigint", { getHeadVersion: async () => 7 as unknown as bigint }, "head-version", "not a non-negative bigint"],
    ["head rejecting", { getHeadVersion: async () => { throw new Error("x"); } }, "head-version", "rejected"],
    ["earliest above a seen event", { getEarliestVersion: async () => 5n }, "earliest-version", "above seen event"],
  ])("judges %s", async (_label, overrides, name, message) => {
    const report = await checkAdapter(broken(overrides as Partial<EventAdapter>), desc, walkOptions);
    expect(failed(report)).toEqual([name]);
    expect(result(report, name).message).toContain(message);
  });

  it("fails earliest-version when it is above head", async () => {
    const report = await checkAdapter(
      broken({
        fetchEvents: async () => ({ events: [], nextCursor: "0", hasMore: false }),
        getHeadVersion: async () => 4n,
        getEarliestVersion: async () => 5n,
      }),
      desc,
    );
    expect(failed(report)).toContain("earliest-version");
    expect(result(report, "earliest-version").message).toContain("above head 4");
  });

  const block: BlockInfo = { height: 3n, firstVersion: 10n, lastVersion: 12n, timestamp: 1n };

  it("skips block without blockHeight and passes a matching block", async () => {
    const adapter = broken({ getBlock: async () => block });
    expect(result(await checkAdapter(adapter, desc), "block").message).toBe("blockHeight not given");
    const report = await checkAdapter(adapter, desc, { blockHeight: 3n });
    expect(result(report, "block").status).toBe("pass");
  });

  it.each([
    ["a height mismatch", { ...block, height: 4n }, "returned height 4"],
    ["an inverted range", { ...block, firstVersion: 13n }, "firstVersion is above lastVersion"],
    ["a number timestamp", { ...block, timestamp: 1 }, "timestamp is not a bigint"],
  ])("fails block on %s", async (_label, reply, message) => {
    const report = await checkAdapter(broken({ getBlock: async () => reply as BlockInfo }), desc, {
      blockHeight: 3n,
    });
    expect(failed(report)).toEqual(["block"]);
    expect(result(report, "block").message).toContain(message);
  });

  it("stays within maxPages + 2 fetches and one call per optional method", async () => {
    const base = seeded();
    const calls: string[] = [];
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => (calls.push("fetch"), base.fetchEvents(d, o)),
      getHeadVersion: () => (calls.push("head"), base.getHeadVersion()),
      getEarliestVersion: () => (calls.push("earliest"), base.getEarliestVersion()),
      getBlock: async () => (calls.push("block"), block),
    };
    const report = await checkAdapter(adapter, desc, { limit: 1, maxPages: 4, blockHeight: 3n });
    expect(report.ok).toBe(true);
    expect(calls.filter((c) => c === "fetch")).toHaveLength(6);
    expect(calls.filter((c) => c !== "fetch")).toEqual(["head", "earliest", "block"]);
  });
});

describe("checkAdapter against FullnodeScanAdapter", () => {
  /** A real `Aptos` over a stubbed transport serving versions 0..9, one Ping each. */
  function stubbedFullnode(): FullnodeScanAdapter {
    const route = (path: string, params: Record<string, unknown>): unknown => {
      if (path === "") return { ledger_version: "9", oldest_ledger_version: "0" };
      if (path === "transactions") {
        const start = Number(params.start ?? 0);
        const limit = Number(params.limit ?? 25);
        return Array.from({ length: 10 }, (_, v) => ({
          version: String(v),
          type: "user_transaction",
          events: [{ type: TYPE, data: {}, sequence_number: "0" }],
        })).slice(start, start + limit);
      }
      if (path === "blocks/by_height/3") {
        return { block_height: "3", block_timestamp: "5", first_version: "6", last_version: "7" };
      }
      throw new Error(`unexpected path ${path}`);
    };
    const client = {
      async provider(req: { url: string; params?: Record<string, unknown> }) {
        const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
        return { status: 200, statusText: "OK", data: route(path, req.params ?? {}), headers: {} };
      },
    };
    return new FullnodeScanAdapter(
      new Aptos(
        new AptosConfig({
          // The SDK caches ledger info per network name; one no other test uses keeps this head.
          network: Network.CUSTOM,
          fullnode: "http://fake/v1",
          client: client as never,
          clientConfig: { http2: false },
        }),
      ),
      { scanBatch: 2 },
    );
  }

  it("passes every rule on a module descriptor", async () => {
    const report = await checkAdapter(stubbedFullnode(), desc, { ...walkOptions, blockHeight: 3n });
    expect(report.results.filter((r) => r.status !== "pass")).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.pagesFetched).toBe(3);
  });
});
