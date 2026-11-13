import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { defineEvent } from "../src/define.js";
import { StreamdError } from "../src/errors.js";
import { count, groupBy, latest, sum } from "../src/reducers.js";
import type { EventOf, Reduction, StreamdEvent } from "../src/types.js";

const TYPE = "0x1::test::Deposit";
const Deposit = defineEvent<{ store: string; amount: string }>(TYPE);

function ev(version: number, data: unknown, eventIndex = 0): StreamdEvent {
  return { type: TYPE, data, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

const fold = <S>(f: (s: S, e: StreamdEvent) => S, initial: S, events: StreamdEvent[]) =>
  events.reduce(f, initial);

describe("count", () => {
  it("starts at 0n and adds one per event", () => {
    expect(count().initial).toBe(0n);
    expect(fold(count(), 0n, [ev(1, {}), ev(2, {}), ev(3, {})])).toBe(3n);
  });
});

describe("sum", () => {
  it("adds decoded bigints, decimal strings past 2^53 and safe numbers at full precision", () => {
    const events = [ev(1, { amount: 5n }), ev(2, { amount: "18446744073709551615" }), ev(3, { amount: 7 })];
    expect(sum("amount").initial).toBe(0n);
    expect(fold(sum("amount"), 0n, events)).toBe(18446744073709551627n);
  });

  it("reads nested and array-index paths, as a string or a key array", () => {
    const events = [ev(1, { legs: [{ x: { amount: "2" } }] }), ev(2, { legs: [{ x: { amount: "3" } }] })];
    expect(fold(sum("legs.0.x.amount"), 0n, events)).toBe(5n);
    expect(fold(sum(["legs", 0, "x", "amount"]), 0n, events)).toBe(5n);
  });

  it("skips a missing or null field", () => {
    const events = [ev(1, { amount: "4" }), ev(2, {}), ev(3, { amount: null }), ev(4, "text")];
    expect(fold(sum("amount"), 0n, events)).toBe(4n);
  });

  it("throws on a present field that is not an integer", () => {
    const s = sum("amount");
    for (const amount of ["1.5", "abc", "", 1.5, 2 ** 60, { value: "1" }, true]) {
      expect(() => s(0n, ev(1, { amount }) as EventOf<unknown>)).toThrow(StreamdError);
    }
  });

  it("refuses a malformed path at construction", () => {
    expect(() => sum("")).toThrow(StreamdError);
    expect(() => sum("a..b")).toThrow(StreamdError);
    expect(() => groupBy("", count())).toThrow(StreamdError);
  });
});

describe("latest", () => {
  it("starts undefined and keeps the most recent event", () => {
    const events = [ev(1, {}), ev(2, {}, 3)];
    expect(latest().initial).toBeUndefined();
    expect(fold<StreamdEvent | undefined>(latest(), undefined, events)).toBe(events[1]);
  });
});

describe("groupBy", () => {
  it("folds the inner reducer per key, from its initial value", () => {
    const events = [ev(1, { store: "0xa" }), ev(2, { store: "0xb" }), ev(3, { store: "0xa" }), ev(4, { store: 7 }), ev(5, { store: 7n })];
    const g = groupBy("store", count());
    expect(g.initial).toEqual(new Map());
    expect(fold(g, g.initial, events)).toEqual(new Map([["0xa", 2n], ["0xb", 1n], ["7", 2n]]));
  });

  it("keys as reported, without padding addresses", () => {
    const events = [ev(1, { store: "0x0a" }), ev(2, { store: "0x000a" })];
    expect([...fold(groupBy("store", count()), new Map(), events).keys()]).toEqual(["0x0a", "0x000a"]);
  });

  it("skips a missing or null key and throws on an object or array", () => {
    const g = groupBy("store", count());
    expect(fold(g, new Map(), [ev(1, {}), ev(2, { store: null })])).toEqual(new Map());
    expect(() => g(new Map(), ev(1, { store: { inner: 1 } }) as EventOf<unknown>)).toThrow(StreamdError);
    expect(() => g(new Map(), ev(1, { store: ["0xa"] }) as EventOf<unknown>)).toThrow(StreamdError);
  });

  it("nests, sums, and starts an inner latest() from undefined", () => {
    const events = [ev(1, { a: "x", b: "p", amount: "2" }), ev(2, { a: "x", b: "q", amount: "3" }), ev(3, { a: "y", b: "p", amount: "4" })];
    expect(fold(groupBy("a", groupBy("b", sum("amount"))), new Map(), events)).toEqual(
      new Map([["x", new Map([["p", 2n], ["q", 3n]])], ["y", new Map([["p", 4n]])]]),
    );
    const newest = fold(groupBy("a", latest()), new Map(), events);
    expect(newest.get("x")).toBe(events[1]);
    expect(newest.get("y")).toBe(events[2]);
  });

  it("never mutates the map it is given", () => {
    const g = groupBy("store", count());
    const before = new Map([["0xa", 1n]]);
    const after = g(before, ev(1, { store: "0xa" }) as EventOf<unknown>);
    expect(before).toEqual(new Map([["0xa", 1n]]));
    expect(after).toEqual(new Map([["0xa", 2n]]));
    expect(g.initial.size).toBe(0);
  });
});

describe("reducers through client.reduce", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const HISTORY = [
    ev(1, { store: "0xa", amount: "10" }),
    ev(2, { store: "0xb", amount: "20" }),
    ev(3, { store: "0xa", amount: "30" }, 1),
  ];
  const LIVE = [ev(4, { store: "0xb", amount: "40" }), ev(5, { store: "0xc", amount: "50" })];

  it("folds the history, then live events, with count, sum and groupBy", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const n = client.reduce(Deposit, count(), 0n, { fromVersion: 0n });
    const total = client.reduce(Deposit, sum("amount"), 0n, { fromVersion: 0n });
    const byStore = client.reduce(Deposit, groupBy("store", count()), new Map(), { fromVersion: 0n });
    const newest = client.reduce(Deposit, latest(), undefined, { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    expect(await n.ready).toBe(3n);
    expect(await total.ready).toBe(60n);
    expect(await byStore.ready).toEqual(new Map([["0xa", 2n], ["0xb", 1n]]));
    expect((await newest.ready)?.version).toBe(3n);
    memory.push(...LIVE);
    await advanceTimersByTimeAsync(3000);
    expect(n.value).toBe(5n);
    expect(total.value).toBe(150n);
    expect(byStore.value).toEqual(new Map([["0xa", 2n], ["0xb", 2n], ["0xc", 1n]]));
    expect(newest.value?.version).toBe(5n);
    client.destroy();
  });

  it("resumes from a saved groupBy value and cursor to the value of an unbroken run", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const first = client.reduce(Deposit, groupBy("store", sum("amount")), new Map(), { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    await first.ready;
    const saved = JSON.stringify({
      value: [...first.value].map(([k, v]) => [k, v.toString()]),
      cursor: first.cursor,
    });
    first.unsubscribe();
    memory.push(...LIVE);

    const fetches: unknown[] = [];
    const counting = new StreamdClient({
      adapter: { fetchEvents: (d, o) => (fetches.push(o), memory.fetchEvents(d, o)) },
    });
    const { value, cursor } = JSON.parse(saved) as { value: [string, string][]; cursor: string };
    const resumed = counting.reduce(
      Deposit,
      groupBy("store", sum("amount")),
      new Map(value.map(([k, v]) => [k, BigInt(v)])),
      { cursor },
    );
    await advanceTimersByTimeAsync(3000);
    expect(resumed.value).toEqual(new Map([["0xa", 40n], ["0xb", 60n], ["0xc", 50n]]));
    expect(fetches.length).toBeGreaterThan(0);
    expect(fetches.every((o) => (o as { cursor?: string }).cursor !== undefined)).toBe(true);
    client.destroy();
    counting.destroy();
  });

  it("stops with onError on an amount that is not an integer", async () => {
    const memory = new MemoryAdapter(HISTORY);
    const client = new StreamdClient({ adapter: memory });
    const errors: Error[] = [];
    const total = client.reduce(Deposit, sum("amount"), 0n, { fromVersion: 0n, onError: (e) => void errors.push(e) });
    await advanceTimersByTimeAsync(10);
    await total.ready;
    memory.push(ev(4, { store: "0xa", amount: "1e3" }), ev(5, { store: "0xa", amount: "5" }));
    await advanceTimersByTimeAsync(3000);
    expect(total.active).toBe(false);
    expect(total.value).toBe(60n);
    expect(errors[0]?.message).toBe("Reducer threw for 4:0");
    client.destroy();
  });

  it("infers the reduction types", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    const n: Reduction<bigint> = client.reduce(Deposit, count(), 0n);
    const total: Reduction<bigint> = client.reduce(Deposit, sum("amount"), 0n);
    const g = groupBy("store", count());
    const byStore = client.reduce(Deposit, g, g.initial);
    const groups: Map<string, bigint> = byStore.value;
    // @ts-expect-error the groups are bigints, not strings
    const wrong: Map<string, string> = byStore.value;
    const newest = client.reduce(Deposit, latest(), undefined);
    const store: string | undefined = newest.value?.data.store;
    void [n, total, groups, store, wrong];
    client.destroy();
  });
});
