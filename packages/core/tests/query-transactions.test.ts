import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { EventQuery } from "../src/query.js";
import { StreamdClient } from "../src/client.js";
import { defineEvent } from "../src/define.js";
import { StreamdError } from "../src/errors.js";
import { whereField } from "../src/filter.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventFilter,
  StreamdEvent,
  TransactionFilter,
  TransactionGroup,
} from "../src/types.js";

const A = "0x1::test::A";
const B = "0x1::test::B";
const as: EventDescriptor = { kind: "module", eventType: A };
const bs: EventDescriptor = { kind: "module", eventType: B };
const both: EventDescriptor = { kind: "module", eventType: [A, B] };

function ev(version: number, eventIndex: number, type = A, keep = true): StreamdEvent {
  return {
    type,
    data: { keep },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: `0x${version}`,
    sender: "0xa",
    timestamp: BigInt(version) * 1000n,
  };
}

/** `adapter` counting its `fetchEvents` calls. */
function counted(adapter: EventAdapter) {
  const calls = { n: 0 };
  const fetchEvents = adapter.fetchEvents.bind(adapter);
  adapter.fetchEvents = (d, o) => (calls.n++, fetchEvents(d, o));
  return { adapter, calls };
}

const shape = (groups: TransactionGroup[]) =>
  groups.map((g) => `${g.version}:${g.events.map((e) => e.eventIndex).join(",")}`);

describe("EventQuery.getTransactions", () => {
  const history = [ev(10, 0), ev(11, 0), ev(11, 1), ev(11, 2), ev(12, 0), ev(13, 0)];

  it("groups a range by version, fields as onTransaction takes them", async () => {
    const groups = await new EventQuery(new MemoryAdapter(history)).getTransactions(as, {}, 2);
    expect(shape(groups)).toEqual(["10:0", "11:0,1,2", "12:0", "13:0"]);
    expect(groups[1]).toMatchObject({ version: 11n, transactionHash: "0x11", sender: "0xa", timestamp: 11000n });
  });

  it("keeps the last group whole when limit lands on a page boundary inside it", async () => {
    // Pages of two: [10:0, 11:0], [11:1, 11:2], [12:0, 13:0].
    const { adapter, calls } = counted(new MemoryAdapter(history));
    const groups = await new EventQuery(adapter).getTransactions(as, { limit: 2 }, 2);
    expect(shape(groups)).toEqual(["10:0", "11:0,1,2"]);
    // The third page showed version 12, so the walk stopped there.
    expect(calls.n).toBe(3);
  });

  it("stops fetching once the next version shows up", async () => {
    const { adapter, calls } = counted(new MemoryAdapter(history));
    const groups = await new EventQuery(adapter).getTransactions(as, { limit: 1 }, 2);
    expect(shape(groups)).toEqual(["10:0"]);
    expect(calls.n).toBe(1);
  });

  it("merges a list, an event both descriptors select landing once", async () => {
    const events = [ev(10, 0), ev(10, 1, B), ev(11, 0, B), ev(12, 0), ev(12, 1, B)];
    const query = new EventQuery(new MemoryAdapter(events));
    const groups = await query.getTransactions([as, bs, both], {}, 1);
    expect(shape(groups)).toEqual(["10:0,1", "11:0", "12:0,1"]);
    expect(groups[0]!.events.map((e) => e.type)).toEqual([A, B]);
  });

  it("applies where per event before grouping, so an unmatched transaction yields nothing", async () => {
    const events = [ev(10, 0), ev(11, 0, A, false), ev(11, 1, A, false), ev(12, 0, A, false), ev(12, 1)];
    const query = new EventQuery(new MemoryAdapter(events));
    const groups = await query.getTransactions(as, { where: whereField("keep", true), limit: 2 }, 2);
    expect(shape(groups)).toEqual(["10:0", "12:1"]);
  });

  it("returns [] for an empty range, limit 0 and a where matching nothing", async () => {
    const query = new EventQuery(new MemoryAdapter(history));
    expect(await query.getTransactions(as, { fromVersion: 100n })).toEqual([]);
    expect(await query.getTransactions(as, { fromVersion: 11n, toVersion: 10n })).toEqual([]);
    expect(await query.getTransactions(as, { limit: 0 })).toEqual([]);
    expect(await query.getTransactions(as, { where: () => false })).toEqual([]);
    expect(await new EventQuery(new MemoryAdapter()).getTransactions([as, bs])).toEqual([]);
  });

  it("honours the version bounds", async () => {
    const query = new EventQuery(new MemoryAdapter(history));
    const groups = await query.getTransactions(as, { fromVersion: 11n, toVersion: 12n }, 2);
    expect(shape(groups)).toEqual(["11:0,1,2", "12:0"]);
  });

  it("returns the newest limit groups first for desc", async () => {
    const query = new EventQuery(new MemoryAdapter(history));
    expect(shape(await query.getTransactions(as, { orderBy: "desc", limit: 2 }, 2))).toEqual(["13:0", "12:0"]);
    expect(shape(await query.getTransactions(as, { orderBy: "desc" }, 2))).toEqual(["13:0", "12:0", "11:0,1,2", "10:0"]);
  });

  it("reads withState for the returned groups' events only", async () => {
    const adapter: EventAdapter = new MemoryAdapter(history);
    const versions: bigint[] = [];
    adapter.view = async (_request, version) => (versions.push(version), [String(version)]);
    const withState = () => ({ function: "0x1::m::f" as const });
    const groups = await new EventQuery(adapter).getTransactions(as, { limit: 2, withState }, 2);
    expect(versions).toEqual([10n, 11n, 11n, 11n]);
    expect(groups[1]!.events.map((e) => e.state)).toEqual([["11"], ["11"], ["11"]]);
  });

  it("refuses onCursor, offset, an empty list and a bad orderBy", async () => {
    const query = new EventQuery(new MemoryAdapter(history));
    const refused = (filter: EventFilter) => query.getTransactions(as, filter as TransactionFilter);
    await expect(refused({ onCursor: () => {} })).rejects.toThrow(StreamdError);
    await expect(refused({ offset: 1 })).rejects.toThrow(StreamdError);
    await expect(refused({ orderBy: "up" as "asc" })).rejects.toThrow(StreamdError);
    await expect(query.getTransactions([])).rejects.toThrow(StreamdError);
  });
});

describe("StreamdClient.getTransactions", () => {
  it("returns typed groups from the client's adapter", async () => {
    const typed = defineEvent<{ keep: boolean }>(A);
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(10, 0), ev(10, 1), ev(11, 0)]) });
    const groups = await client.getTransactions(typed, { limit: 1 });
    const keep: boolean = groups[0]!.events[1]!.data.keep;
    expect(keep).toBe(true);
    expect(shape(groups as TransactionGroup[])).toEqual(["10:0,1"]);
    client.destroy();
  });
});
