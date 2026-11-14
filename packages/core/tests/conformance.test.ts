import { describe, it, expect } from "bun:test";
import { checkAdapter } from "../src/conformance.js";
import type { AdapterCheckReport } from "../src/conformance.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { FallbackAdapter } from "../src/adapters/fallback.js";
import { RoutingAdapter } from "../src/adapters/routing.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  BlockInfo,
  EventPage,
  FetchOptions,
  StreamdEvent,
  TransactionEvents,
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

const WALK_RULES: string[] = [
  "fetch",
  "page-shape",
  "from-version",
  "order",
  "cursor-advance",
  "through-version",
];

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
      "through-version": "pass",
      "cursor-precedence": "pass",
      "foreign-cursor": "pass",
      "head-version": "pass",
      "earliest-version": "pass",
      block: "skip",
      "block-heights": "skip",
      "block-sequence": "skip",
      "transaction-events": "skip",
      "transaction-events-by-version": "skip",
      view: "skip",
    });
    expect(report.results.map((r): string => r.name)).toEqual([
      ...WALK_RULES,
      "cursor-precedence",
      "foreign-cursor",
      "head-version",
      "earliest-version",
      "block",
      "block-heights",
      "block-sequence",
      "transaction-events",
      "transaction-events-by-version",
      "view",
    ]);
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
    [
      "an entryFunction without arguments",
      { events: [{ ...ev(1), entryFunction: { function: "0x1::a::b", typeArguments: [] } }], hasMore: false },
      "entryFunction",
    ],
    [
      "a number guid creationNumber",
      { events: [{ ...ev(1), guid: { creationNumber: 1, accountAddress: "0x1" } }], hasMore: false },
      "guid",
    ],
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

  it("skips through-version when no page sets it", async () => {
    const report = await checkAdapter(scripted([{ events: [ev(1)], hasMore: false }]), desc);
    expect(statuses(report)["through-version"]).toBe("skip");
  });

  it("fails through-version when it falls or a later event is not above it", async () => {
    const falling = scripted([
      { events: [ev(1)], nextCursor: "a", hasMore: true, throughVersion: 5n },
      { events: [], nextCursor: "b", hasMore: false, throughVersion: 4n },
    ]);
    expect(failed(await checkAdapter(falling, desc))).toEqual(["through-version"]);
    const early = scripted([
      { events: [ev(1)], nextCursor: "a", hasMore: true, throughVersion: 5n },
      { events: [ev(5)], nextCursor: "b", hasMore: false },
    ]);
    const report = await checkAdapter(early, desc);
    expect(failed(report)).toEqual(["through-version"]);
    expect(report.results.find((r) => r.name === "through-version")?.message).toContain("5:0");
  });

  it("fails page-shape on a throughVersion that is not a non-negative bigint", async () => {
    for (const throughVersion of [3, -1n]) {
      const report = await checkAdapter(scripted([{ events: [], hasMore: false, throughVersion }]), desc);
      expect(failed(report)).toEqual(["page-shape"]);
    }
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
    ["a view that is not an object", { view: "0x1::a::b" }],
    ["a malformed view function", { view: { request: { function: "coin::balance" } } }],
    ["a negative view version", { view: { request: { function: "0x1::a::b" }, version: -1n } }],
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
    const optional = [
      "head-version",
      "earliest-version",
      "block",
      "block-heights",
      "block-sequence",
      "transaction-events",
      "transaction-events-by-version",
      "view",
    ];
    for (const name of optional) {
      expect(result(report, name)).toEqual({ name, status: "skip", message: "not implemented (optional)" } as never);
    }
  });

  it.each([
    ["FallbackAdapter", () => new FallbackAdapter([seeded()])],
    ["RoutingAdapter", () => new RoutingAdapter({ default: seeded() })],
  ])("skips the block rules for a %s over sources without them", async (_label, make) => {
    const report = await checkAdapter(make(), desc, { fromVersion: 0n, blockHeight: 1n });
    for (const name of ["block", "block-heights", "block-sequence"]) {
      expect(result(report, name)).toEqual({ name, status: "skip", message: "not implemented (optional)" } as never);
    }
    expect(report.ok).toBe(true);
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
    ["a negative firstVersion", { ...block, firstVersion: -1n }, "firstVersion is negative"],
    ["a number timestamp", { ...block, timestamp: 1 }, "timestamp is not a bigint"],
  ])("fails block on %s", async (_label, reply, message) => {
    const report = await checkAdapter(broken({ getBlock: async () => reply as BlockInfo }), desc, {
      blockHeight: 3n,
    });
    expect(failed(report)).toEqual(["block"]);
    expect(result(report, "block").message).toContain(message);
  });

  it("stays within maxPages + 2 fetches, two getBlock calls and one per other optional method", async () => {
    const base = seeded();
    const blocks = chain();
    const calls: string[] = [];
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => (calls.push("fetch"), base.fetchEvents(d, o)),
      getHeadVersion: () => (calls.push("head"), base.getHeadVersion()),
      getEarliestVersion: () => (calls.push("earliest"), base.getEarliestVersion()),
      getBlockHeights: (d) => (calls.push("heights"), blocks.getBlockHeights!(d)),
      getBlock: (d, h) => (calls.push(`block ${h}`), blocks.getBlock!(d, h)),
      getTransactionEventsByVersion: async (v) => (calls.push("by-version"), transaction([ev(Number(v))], v)),
      view: async () => (calls.push("view"), []),
    };
    const report = await checkAdapter(adapter, desc, {
      limit: 1,
      maxPages: 4,
      blockHeight: 3n,
      view: { request: { function: "0x1::a::b" } },
    });
    expect(report.ok).toBe(true);
    expect(calls.filter((c) => c === "fetch")).toHaveLength(6);
    expect(calls.filter((c) => c !== "fetch")).toEqual([
      "head",
      "earliest",
      "heights",
      "block 3",
      "block 2",
      "by-version",
      "view",
    ]);
  });
});

/** Blocks `oldest` to `newest`, block h holding versions 10h to 10h + 9 at time 100h. */
function chain(oldest = 0n, newest = 9n): Pick<EventAdapter, "getBlock" | "getBlockHeights"> {
  return {
    getBlockHeights: async () => ({ oldest, newest }),
    getBlock: async (_d, height) => ({
      height,
      firstVersion: 10n * height,
      lastVersion: 10n * height + 9n,
      timestamp: 100n * height,
    }),
  };
}

describe("checkAdapter block rules", () => {
  it("passes contiguous blocks, defaulting the height to newest", async () => {
    const asked: bigint[] = [];
    const blocks = chain();
    const report = await checkAdapter(
      broken({ ...blocks, getBlock: (d, h) => (asked.push(h), blocks.getBlock!(d, h)) }),
      desc,
      walkOptions,
    );
    expect(report.ok).toBe(true);
    expect(statuses(report)).toMatchObject({ block: "pass", "block-heights": "pass", "block-sequence": "pass" });
    expect(asked).toEqual([9n, 8n]);
  });

  it("prefers blockHeight over newest", async () => {
    const asked: bigint[] = [];
    const blocks = chain();
    await checkAdapter(broken({ ...blocks, getBlock: (d, h) => (asked.push(h), blocks.getBlock!(d, h)) }), desc, {
      blockHeight: 4n,
    });
    expect(asked).toEqual([4n, 3n]);
  });

  it.each([
    ["rejecting", async () => { throw new Error("down"); }, "getBlockHeights rejected: Error: down"],
    ["inverted", async () => ({ oldest: 5n, newest: 4n }), "oldest is above newest"],
    ["a number newest", async () => ({ oldest: 0n, newest: 9 }), "newest is not a non-negative bigint"],
  ])("fails block-heights when %s and skips the rules needing its height", async (_label, getBlockHeights, message) => {
    const report = await checkAdapter(broken({ ...chain(), getBlockHeights } as Partial<EventAdapter>), desc);
    expect(failed(report)).toEqual(["block-heights"]);
    expect(result(report, "block-heights").message).toContain(message);
    expect(result(report, "block").message).toBe("blockHeight not given and getBlockHeights did not pass");
    expect(result(report, "block-sequence").status).toBe("skip");
  });

  const shifted = (change: (height: bigint) => Partial<BlockInfo>): Partial<EventAdapter> => {
    const blocks = chain();
    return {
      ...blocks,
      getBlock: async (d, h) => ({ ...(await blocks.getBlock!(d, h)), ...change(h) }),
    };
  };

  it.each([
    ["a gap", shifted((h) => (h === 8n ? { lastVersion: 88n } : {})), "ends at version 88 but block 9 starts at 90"],
    ["an overlap", shifted((h) => (h === 9n ? { firstVersion: 89n } : {})), "block 9 starts at 89"],
    ["a falling timestamp", shifted((h) => (h === 9n ? { timestamp: 1n } : {})), "timestamp 1 is below block 8's 800"],
    [
      "the block before failing",
      shifted((h) => (h === 8n ? { height: 7n } : {})),
      "getBlock(8) returned height 7",
    ],
  ])("fails block-sequence on %s", async (_label, overrides, message) => {
    const report = await checkAdapter(broken(overrides), desc, walkOptions);
    expect(failed(report)).toEqual(["block-sequence"]);
    expect(result(report, "block-sequence").message).toContain(message);
  });

  it("skips block-sequence at height 0, below oldest, and when block failed", async () => {
    const zero = await checkAdapter(broken(chain()), desc, { blockHeight: 0n });
    expect(result(zero, "block-sequence").message).toBe("no block before height 0");
    const edge = await checkAdapter(broken(chain(5n, 9n)), desc, { blockHeight: 5n });
    expect(result(edge, "block-sequence").message).toBe("block 4 is below oldest 5");
    const bad = await checkAdapter(broken(shifted(() => ({ height: 1n }))), desc, walkOptions);
    expect(failed(bad)).toEqual(["block"]);
    expect(result(bad, "block-sequence").message).toBe("block 9 did not pass");
  });

  it("fails on a synchronous throw instead of rejecting", async () => {
    const getBlock = () => {
      throw new TypeError("sync");
    };
    const report = await checkAdapter(broken({ ...chain(), getBlock } as Partial<EventAdapter>), desc);
    expect(failed(report)).toEqual(["block"]);
    expect(result(report, "block").message).toBe("getBlock rejected: TypeError: sync");
  });
});

const hashOf = (version: bigint) => `0x${version.toString(16).padStart(64, "0")}`;

/** Two events per transaction at versions 1 to 4, each carrying its transaction's hash. */
function hashedEvents(): StreamdEvent[] {
  return [1, 2, 3, 4].flatMap((v) => [0, 1].map((i) => ({ ...ev(v, i), transactionHash: hashOf(BigInt(v)) })));
}

/** A transaction as both lookups should report it, from `events`. */
function transaction(events: StreamdEvent[], version: bigint): TransactionEvents {
  return { version, success: true, vmStatus: "Executed successfully", events: events.filter((e) => e.version === version) };
}

/** Serves the walk and both lookups correctly from `hashedEvents()`, with overrides. */
function lookups(overrides: Partial<EventAdapter> = {}): EventAdapter {
  const events = hashedEvents();
  const base = new MemoryAdapter(events);
  return {
    fetchEvents: (d, o) => base.fetchEvents(d, o),
    getTransactionEvents: async (hash) => {
      const found = events.find((e) => e.transactionHash === hash);
      return found && transaction(events, found.version);
    },
    getTransactionEventsByVersion: async (version) =>
      events.some((e) => e.version === version) ? transaction(events, version) : undefined,
    ...overrides,
  };
}

const LOOKUPS = ["transaction-events", "transaction-events-by-version"];

describe("checkAdapter transaction lookups", () => {
  it("passes a MemoryAdapter seeded with hashed events", async () => {
    const report = await checkAdapter(new MemoryAdapter(hashedEvents()), desc, { fromVersion: 2n });
    expect(report.ok).toBe(true);
    expect(statuses(report)).toMatchObject({
      "transaction-events": "pass",
      "transaction-events-by-version": "skip",
    });
  });

  it("passes both lookups, asking for the first hashed walked event", async () => {
    const asked: unknown[] = [];
    const correct = lookups();
    const report = await checkAdapter(
      lookups({
        getTransactionEvents: (hash, d) => (asked.push(hash, d), correct.getTransactionEvents!(hash)),
        getTransactionEventsByVersion: (v, d) => (asked.push(v, d), correct.getTransactionEventsByVersion!(v)),
      }),
      desc,
      { fromVersion: 3n },
    );
    expect(report.ok).toBe(true);
    expect(statuses(report)).toMatchObject({ "transaction-events": "pass", "transaction-events-by-version": "pass" });
    expect(asked).toEqual([hashOf(3n), desc, 3n, desc]);
  });

  const wrong = (change: (t: TransactionEvents) => unknown) => async (version: bigint) =>
    change(transaction(hashedEvents(), version)) as TransactionEvents;

  it.each([
    ["nothing found", { getTransactionEvents: async () => undefined }, "transaction-events", "found nothing for walked event 1:0"],
    ["a wrong version", { getTransactionEvents: async () => transaction(hashedEvents(), 2n) }, "transaction-events", "returned version 2, but walked event 1:0 is at 1"],
    ["a missing success", { getTransactionEventsByVersion: wrong((t) => ({ ...t, success: undefined })) }, "transaction-events-by-version", "success is not a boolean"],
    ["a malformed event", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: [{ ...t.events[0], version: 1 }] })) }, "transaction-events-by-version", "events[0] version"],
    ["an event at another version", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: [...t.events, ev(2, 5)] })) }, "transaction-events-by-version", "event 2:5 is not at version 1"],
    ["descending eventIndex", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: [...t.events].reverse() })) }, "transaction-events-by-version", "eventIndex 0 follows 1"],
    ["the walked event missing", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: t.events.slice(1) })) }, "transaction-events-by-version", "does not hold walked event 1:0"],
    ["a different type", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: t.events.map((e) => ({ ...e, type: "0x1::other::Pong" })) })) }, "transaction-events-by-version", "does not hold walked event 1:0 (0x1::test::Ping)"],
    ["a by-version answer that disagrees", { getTransactionEventsByVersion: wrong((t) => ({ ...t, events: t.events.slice(0, 1) })) }, "transaction-events-by-version", "disagrees with getTransactionEvents: [1:0] vs [1:0,1:1]"],
    ["a rejection", { getTransactionEvents: async () => { throw new StreamdNetworkError("down", 503); } }, "transaction-events", "getTransactionEvents(0x"],
    ["a synchronous throw", { getTransactionEventsByVersion: () => { throw new Error("sync"); } }, "transaction-events-by-version", "rejected: Error: sync"],
  ])("fails on %s", async (_label, overrides, name, message) => {
    const report = await checkAdapter(lookups(overrides as Partial<EventAdapter>), desc);
    expect(failed(report)).toEqual([name]);
    expect(result(report, name).message).toContain(message);
  });

  it("looks up by version the first walked event when no lookup by hash ran", async () => {
    const asked: bigint[] = [];
    const events = [ev(5), ev(6)];
    const report = await checkAdapter(
      {
        fetchEvents: (d, o) => new MemoryAdapter(events).fetchEvents(d, o),
        getTransactionEventsByVersion: async (v) => (asked.push(v), transaction(events, v)),
      },
      desc,
    );
    expect(report.ok).toBe(true);
    expect(asked).toEqual([5n]);
  });

  const handleDesc: EventDescriptor = {
    kind: "handle",
    accountAddress: "0x1",
    eventHandleStruct: "0x1::test::Handles",
    fieldName: "pings",
  };

  it("matches a handle event by sequenceNumber, not eventIndex", async () => {
    const walked = { ...ev(8, 7), sequenceNumber: 7n };
    const page = scripted([{ events: [walked], hasMore: false }]);
    const at = (sequenceNumber: bigint) => async (): Promise<TransactionEvents> => ({
      version: 8n,
      success: true,
      vmStatus: "",
      events: [{ ...ev(8, 0), sequenceNumber }],
    });
    const ok = await checkAdapter({ ...page, getTransactionEventsByVersion: at(7n) }, handleDesc);
    expect(result(ok, "transaction-events-by-version").status).toBe("pass");
    const off = await checkAdapter({ ...page, getTransactionEventsByVersion: at(6n) }, handleDesc);
    expect(failed(off)).toEqual(["transaction-events-by-version"]);
  });

  it.each([
    ["a resource descriptor", { kind: "resource", accountAddress: "0x1", resourceType: TYPE }, "resource changes are not transaction events"],
    ["an object descriptor", { kind: "object", objectId: "0x1" }, "object changes are not transaction events"],
  ])("skips both lookups for %s", async (_label, descriptor, message) => {
    const page = scripted([{ events: [ev(1)], hasMore: false }]);
    const report = await checkAdapter(lookups({ fetchEvents: page.fetchEvents }), descriptor as EventDescriptor);
    for (const name of LOOKUPS) expect(result(report, name).message).toBe(message);
  });

  it("skips without a walked event, and by hash without a hashed one", async () => {
    const empty = await checkAdapter(lookups({ fetchEvents: async () => ({ events: [], hasMore: false }) }), desc);
    for (const name of LOOKUPS) expect(result(empty, name).message).toBe("no walked event to look up");
    const unhashed = await checkAdapter(lookups({ fetchEvents: (d, o) => seeded().fetchEvents(d, o) }), desc);
    expect(result(unhashed, "transaction-events").message).toBe("no walked event carries a transactionHash");
  });
});

describe("checkAdapter view", () => {
  const request = { function: "0x1::coin::balance", typeArguments: ["0x1::aptos_coin::AptosCoin"], functionArguments: ["0x1"] } as const;

  function viewing(reply: () => unknown, base: Partial<EventAdapter> = {}) {
    const asked: unknown[] = [];
    const adapter = broken({
      view: async (r, v, d) => (asked.push(r, v, d), reply() as unknown[]),
      ...base,
    });
    return { adapter, asked };
  }

  it("skips without the method, and without a request", async () => {
    const absent = await checkAdapter(seeded(), desc, { view: { request } });
    expect(result(absent, "view").message).toBe("not implemented (optional)");
    const { adapter, asked } = viewing(() => []);
    expect(result(await checkAdapter(adapter, desc), "view").message).toBe("view request not given");
    expect(asked).toHaveLength(0);
  });

  it("reads at the given version, else the first walked event's, else the head", async () => {
    const given = viewing(() => ["1"]);
    const report = await checkAdapter(given.adapter, desc, { ...walkOptions, view: { request, version: 4n } });
    expect(result(report, "view").status).toBe("pass");
    expect(given.asked).toEqual([request, 4n, desc]);
    const walked = viewing(() => ["1"]);
    await checkAdapter(walked.adapter, desc, { ...walkOptions, view: { request } });
    expect(walked.asked[1]).toBe(2n);
    const empty = async () => ({ events: [], hasMore: false });
    const head = viewing(() => ["1"], { fetchEvents: empty, getHeadVersion: async () => 9n });
    await checkAdapter(head.adapter, desc, { view: { request } });
    expect(head.asked[1]).toBe(9n);
    const none = viewing(() => ["1"], { fetchEvents: empty, getHeadVersion: undefined });
    const skipped = await checkAdapter(none.adapter, desc, { view: { request } });
    expect(result(skipped, "view").message).toBe("no version to read at");
    expect(none.asked).toHaveLength(0);
  });

  it.each([
    ["a non-array result", () => ({ value: "1" }), "view(0x1::coin::balance) at 2 resolved object, not an array"],
    ["undefined", () => undefined, "resolved undefined, not an array"],
    ["a rejection", () => { throw new StreamdNetworkError("pruned", 410); }, "view rejected: StreamdNetworkError: pruned"],
  ])("fails view on %s", async (_label, reply, message) => {
    const { adapter } = viewing(reply);
    const report = await checkAdapter(adapter, desc, { ...walkOptions, view: { request } });
    expect(failed(report)).toEqual(["view"]);
    expect(result(report, "view").message).toContain(message);
  });
});
