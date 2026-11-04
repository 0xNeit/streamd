import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventDescriptor, StreamdEvent } from "../src/types.js";

const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";
const IGNORED = "0x1::test::Ignored";
const GENERIC = "0x1::pool::Swap<0x1::a::CoinA, 0x1::b::CoinB>";

const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };
const accountDesc: EventDescriptor = { kind: "account", accountAddress: "0x1" };
const handleDesc: EventDescriptor = {
  kind: "handle",
  accountAddress: "0x1",
  eventHandleStruct: "0x1::test::Store",
  fieldName: "pings",
};

function ev(
  version: number,
  eventIndex = 0,
  type = PING,
  data: unknown = { v: version, i: eventIndex },
): StreamdEvent {
  return {
    type,
    data,
    version: BigInt(version),
    sequenceNumber: BigInt(eventIndex),
    eventIndex,
  };
}

describe("MemoryAdapter", () => {
  it("returns an empty terminal page when constructed with no events", async () => {
    const adapter = new MemoryAdapter();
    const page = await adapter.fetchEvents(moduleDesc);

    expect(page).toEqual({
      events: [],
      nextCursor: "0",
      hasMore: false,
    });
  });

  it("copies and sorts input events by ascending (version, eventIndex) without mutating caller arrays", async () => {
    const input = [ev(10, 0), ev(5, 2), ev(5, 0), ev(5, 1), ev(1, 0)];
    const snapshot = [...input];
    const adapter = new MemoryAdapter(input);

    // Mutating the caller's array after construction must not affect the adapter.
    input.pop();
    input.reverse();
    expect(snapshot).toHaveLength(5);

    const first = await adapter.fetchEvents(moduleDesc);
    expect(
      first.events.map((e) => [e.version, e.eventIndex]),
    ).toEqual([
      [1n, 0],
      [5n, 0],
      [5n, 1],
      [5n, 2],
      [10n, 0],
    ]);
    expect(first.nextCursor).toBe("5");
    expect(first.hasMore).toBe(false);

    // Mutating a returned page's events array must not affect later fetches.
    first.events.length = 0;
    const second = await adapter.fetchEvents(moduleDesc);
    expect(second.events).toHaveLength(5);
  });

  it("matches single and multi-type module descriptors and deduplicates repeated types", async () => {
    const adapter = new MemoryAdapter([
      ev(1, 0, IGNORED),
      ev(1, 1, PONG),
      ev(1, 2, PING),
      ev(2, 0, GENERIC),
    ]);

    const single = await adapter.fetchEvents(moduleDesc);
    expect(single.events.map((e) => [e.version, e.eventIndex, e.type])).toEqual([
      [1n, 2, PING],
    ]);

    const multi = await adapter.fetchEvents({
      kind: "module",
      eventType: [PING, PONG, GENERIC, PING],
    });
    expect(multi.events.map((e) => [e.version, e.eventIndex, e.type])).toEqual([
      [1n, 1, PONG],
      [1n, 2, PING],
      [2n, 0, GENERIC],
    ]);
  });

  it("matches account descriptors with and without eventTypes, and handle descriptors", async () => {
    const adapter = new MemoryAdapter([
      ev(1, 0, PING),
      ev(2, 0, PONG),
      ev(3, 0, IGNORED),
    ]);

    const allAccount = await adapter.fetchEvents(accountDesc);
    expect(allAccount.events.map((e) => e.version)).toEqual([1n, 2n, 3n]);

    const filteredAccount = await adapter.fetchEvents({
      ...accountDesc,
      eventTypes: [PONG],
    });
    expect(filteredAccount.events.map((e) => e.version)).toEqual([2n]);

    const emptyTypesAccount = await adapter.fetchEvents({
      ...accountDesc,
      eventTypes: [],
    });
    expect(emptyTypesAccount.events).toEqual([]);
    expect(emptyTypesAccount.nextCursor).toBe("3");

    const handlePage = await adapter.fetchEvents(handleDesc);
    expect(handlePage.events.map((e) => e.version)).toEqual([1n, 2n, 3n]);
  });

  it("pages by cursor across same-version events and sets hasMore accurately", async () => {
    const adapter = new MemoryAdapter([
      ev(10, 0),
      ev(10, 1),
      ev(10, 2),
    ]);

    const p1 = await adapter.fetchEvents(moduleDesc, { limit: 2 });
    expect(p1.events.map((e) => e.eventIndex)).toEqual([0, 1]);
    expect(p1.nextCursor).toBe("2");
    expect(p1.hasMore).toBe(true);

    const p2 = await adapter.fetchEvents(moduleDesc, {
      cursor: p1.nextCursor,
      limit: 2,
    });
    expect(p2.events.map((e) => e.eventIndex)).toEqual([2]);
    expect(p2.nextCursor).toBe("3");
    expect(p2.hasMore).toBe(false);

    const p3 = await adapter.fetchEvents(moduleDesc, {
      cursor: p2.nextCursor,
      limit: 2,
    });
    expect(p3).toEqual({
      events: [],
      nextCursor: "3",
      hasMore: false,
    });
  });

  it("defaults limit to 25 and reports hasMore only when a 26th event remains", async () => {
    const exact25 = new MemoryAdapter(
      Array.from({ length: 25 }, (_, i) => ev(i + 1)),
    );
    const page25 = await exact25.fetchEvents(moduleDesc);
    expect(page25.events).toHaveLength(25);
    expect(page25.nextCursor).toBe("25");
    expect(page25.hasMore).toBe(false);

    const twentySix = new MemoryAdapter(
      Array.from({ length: 26 }, (_, i) => ev(i + 1)),
    );
    const first = await twentySix.fetchEvents(moduleDesc);
    expect(first.events).toHaveLength(25);
    expect(first.nextCursor).toBe("25");
    expect(first.hasMore).toBe(true);

    const second = await twentySix.fetchEvents(moduleDesc, {
      cursor: first.nextCursor,
    });
    expect(second.events.map((e) => e.version)).toEqual([26n]);
    expect(second.nextCursor).toBe("26");
    expect(second.hasMore).toBe(false);
  });

  it("advances nextCursor past non-matching events even when events is empty", async () => {
    const adapter = new MemoryAdapter([
      ev(1, 0, IGNORED),
      ev(2, 0, IGNORED),
      ev(3, 0, IGNORED),
    ]);

    const page = await adapter.fetchEvents(moduleDesc);
    expect(page).toEqual({
      events: [],
      nextCursor: "3",
      hasMore: false,
    });
  });

  it("honours cursor in preference to fromVersion and offset", async () => {
    const adapter = new MemoryAdapter([ev(1), ev(5), ev(10), ev(20)]);

    // cursor "1" points at version 5; fromVersion 999n and offset 3 are ignored.
    const page = await adapter.fetchEvents(moduleDesc, {
      cursor: "1",
      fromVersion: 999n,
      offset: 3,
      limit: 2,
    });
    expect(page.events.map((e) => e.version)).toEqual([5n, 10n]);
    expect(page.nextCursor).toBe("3");
    expect(page.hasMore).toBe(true);
  });

  it("filters by fromVersion and toVersion and stops with hasMore false at toVersion", async () => {
    const adapter = new MemoryAdapter([ev(2), ev(5), ev(8), ev(12)]);

    const bounded = await adapter.fetchEvents(moduleDesc, {
      fromVersion: 5n,
      toVersion: 8n,
      limit: 2,
    });
    expect(bounded.events.map((e) => e.version)).toEqual([5n, 8n]);
    expect(bounded.nextCursor).toBe("3");
    // Event at index 3 has version 12 > toVersion 8, so no more events in range.
    expect(bounded.hasMore).toBe(false);

    const moreInRange = await adapter.fetchEvents(moduleDesc, {
      fromVersion: 2n,
      toVersion: 8n,
      limit: 2,
    });
    expect(moreInRange.events.map((e) => e.version)).toEqual([2n, 5n]);
    expect(moreInRange.nextCursor).toBe("2");
    expect(moreInRange.hasMore).toBe(true);

    const beforeAll = await adapter.fetchEvents(moduleDesc, {
      toVersion: 1n,
    });
    expect(beforeAll).toEqual({
      events: [],
      nextCursor: "0",
      hasMore: false,
    });
  });

  it("combines offset with fromVersion when cursor is omitted, and handles out-of-range cursors and limit 0", async () => {
    const adapter = new MemoryAdapter([ev(2), ev(5), ev(8), ev(12)]);

    const withOffset = await adapter.fetchEvents(moduleDesc, {
      offset: 1,
      fromVersion: 8n,
    });
    expect(withOffset.events.map((e) => e.version)).toEqual([8n, 12n]);
    expect(withOffset.nextCursor).toBe("4");

    const zeroLimit = await adapter.fetchEvents(moduleDesc, {
      offset: 2,
      limit: 0,
    });
    expect(zeroLimit).toEqual({
      events: [],
      nextCursor: "2",
      hasMore: false,
    });

    const pastEnd = await adapter.fetchEvents(moduleDesc, {
      cursor: "999999999999999999999",
    });
    expect(pastEnd).toEqual({
      events: [],
      nextCursor: "999999999999999999999",
      hasMore: false,
    });
  });

  it("reverses page order when orderBy is desc", async () => {
    const adapter = new MemoryAdapter([ev(1), ev(2), ev(3)]);
    const page = await adapter.fetchEvents(moduleDesc, {
      limit: 2,
      orderBy: "desc",
    });
    expect(page.events.map((e) => e.version)).toEqual([2n, 1n]);
    expect(page.nextCursor).toBe("2");
    expect(page.hasMore).toBe(true);
  });

  describe("invalid input rejection", () => {
    const badInput: [string, EventDescriptor, Record<string, unknown>][] = [
      ["handle address", { ...handleDesc, accountAddress: "nope" }, {}],
      ["account address", { ...accountDesc, accountAddress: "nope" }, {}],
      ["empty module eventType array", { kind: "module", eventType: [] }, {}],
      [
        "empty module eventType string",
        { kind: "module", eventType: "" as `${string}::${string}::${string}` },
        {},
      ],
      [
        "two-segment module eventType",
        {
          kind: "module",
          eventType: "0x1::only_two" as `${string}::${string}::${string}`,
        },
        {},
      ],
      [
        "four-segment module eventType",
        {
          kind: "module",
          eventType: "0x1::a::b::c" as `${string}::${string}::${string}`,
        },
        {},
      ],
      [
        "module eventType array with bad entry and limit 0",
        {
          kind: "module",
          eventType: [PING, "0x1::bad" as `${string}::${string}::${string}`],
        },
        { limit: 0 },
      ],
      [
        "unknown descriptor kind",
        { kind: "other" } as unknown as EventDescriptor,
        {},
      ],
      ["non-numeric cursor", moduleDesc, { cursor: "abc" }],
      ["empty cursor", moduleDesc, { cursor: "" }],
      ["negative cursor", moduleDesc, { cursor: "-1" }],
      ["negative offset", moduleDesc, { offset: -1 }],
      ["fractional offset", moduleDesc, { offset: 1.5 }],
      ["negative limit", moduleDesc, { limit: -1 }],
      ["fractional limit", moduleDesc, { limit: 1.5 }],
      ["NaN limit", moduleDesc, { limit: Number.NaN }],
    ];

    for (const [name, descriptor, options] of badInput) {
      it(`refuses a bad ${name} with StreamdError, not StreamdNetworkError`, async () => {
        const adapter = new MemoryAdapter([ev(1)]);
        const err = await adapter
          .fetchEvents(descriptor, options)
          .catch((e: unknown) => e);

        expect(err).toBeInstanceOf(StreamdError);
        expect(err).not.toBeInstanceOf(StreamdNetworkError);
      });
    }
  });
});
