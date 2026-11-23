import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { defineEvent } from "../src/define.js";
import { StreamdError } from "../src/errors.js";
import { buckets, count, distinct, groupBy, latest, max, min, sum, type Distinct } from "../src/reducers.js";
import type { EventOf, Reduction, StreamdEvent } from "../src/types.js";

const TYPE = "0x1::test::Deposit";
const Deposit = defineEvent<{ store: string; amount: string }>(TYPE);

function ev(version: number, data: unknown, eventIndex = 0): StreamdEvent {
  return { type: TYPE, data, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

const at = (version: number, timestamp: bigint, data: unknown = {}): StreamdEvent => ({ ...ev(version, data), timestamp });

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

describe("min and max", () => {
  it("start undefined and track the extremes at full precision", () => {
    const events = [ev(1, { amount: "18446744073709551615" }), ev(2, { amount: 5n }), ev(3, { amount: 7 }), ev(4, { amount: "-3" })];
    expect(min("amount").initial).toBeUndefined();
    expect(max("amount").initial).toBeUndefined();
    expect(fold(min("amount"), undefined, events)).toBe(-3n);
    expect(fold(max("amount"), undefined, events)).toBe(18446744073709551615n);
  });

  it("pass over a missing or null field, staying undefined when nothing is read", () => {
    expect(fold(min("amount"), undefined, [ev(1, {}), ev(2, { amount: null })])).toBeUndefined();
    expect(fold(max("amount"), undefined, [ev(1, { amount: "2" }), ev(2, {}), ev(3, { amount: null })])).toBe(2n);
  });

  it("throw on a present field that is not an integer, and refuse a malformed path", () => {
    for (const amount of ["1.5", "abc", 1.5, { value: "1" }, true]) {
      expect(() => min("amount")(undefined, ev(1, { amount }) as EventOf<unknown>)).toThrow("min: field");
      expect(() => max("amount")(undefined, ev(1, { amount }) as EventOf<unknown>)).toThrow("max: field");
    }
    expect(() => min("")).toThrow(StreamdError);
    expect(() => max("a..b")).toThrow(StreamdError);
  });
});

describe("distinct", () => {
  const values = (d: Distinct) => [...d.values];

  it("keeps each value once, keyed as reported", () => {
    const d = distinct("store");
    expect(values(d.initial)).toEqual([]);
    expect(d.initial.capped).toBe(false);
    const events = [ev(1, { store: "0x0a" }), ev(2, { store: "0x000a" }), ev(3, { store: "0x0a" }), ev(4, { store: 7 }), ev(5, { store: 7n })];
    const out = fold(d, d.initial, events);
    expect(values(out)).toEqual(["0x0a", "0x000a", "7"]);
    expect(out.capped).toBe(false);
  });

  it("skips a missing or null field and throws on an object or array", () => {
    const d = distinct("store");
    expect(values(fold(d, d.initial, [ev(1, {}), ev(2, { store: null })]))).toEqual([]);
    expect(() => d(d.initial, ev(1, { store: { inner: 1 } }) as EventOf<unknown>)).toThrow("distinct: field");
    expect(() => d(d.initial, ev(1, { store: ["0xa"] }) as EventOf<unknown>)).toThrow(StreamdError);
  });

  it("returns a new state for a new value and the same state for one already held", () => {
    const d = distinct("store");
    const one = d(d.initial, ev(1, { store: "0xa" }) as EventOf<unknown>);
    expect(one).not.toBe(d.initial);
    expect(d.initial.values.size).toBe(0);
    expect(d(one, ev(2, { store: "0xa" }) as EventOf<unknown>)).toBe(one);
  });

  it("stops adding at max and flags capped", () => {
    const d = distinct("store", { max: 2 });
    const events = [ev(1, { store: "a" }), ev(2, { store: "b" }), ev(3, { store: "a" })];
    const full = fold(d, d.initial, events);
    expect(full.capped).toBe(false);
    const capped = d(full, ev(4, { store: "c" }) as EventOf<unknown>);
    expect(values(capped)).toEqual(["a", "b"]);
    expect(capped.capped).toBe(true);
    expect(d(capped, ev(5, { store: "d" }) as EventOf<unknown>)).toBe(capped);
  });

  it("refuses a bad max at construction", () => {
    for (const bad of [0, -1, 1.5, Infinity, Number.NaN]) {
      expect(() => distinct("store", { max: bad })).toThrow("distinct: max");
    }
  });
});

describe("buckets", () => {
  const MINUTE = 60_000_000n;

  it("folds inner per epoch-aligned window, keyed by its start", () => {
    const b = buckets(MINUTE, count());
    expect(b.initial).toEqual(new Map());
    const events = [at(1, 0n), at(2, MINUTE - 1n), at(3, MINUTE), at(4, 3n * MINUTE + 5n), at(5, 3n * MINUTE + 6n)];
    expect(fold(b, b.initial, events)).toEqual(new Map([[0n, 2n], [MINUTE, 1n], [3n * MINUTE, 2n]]));
  });

  it("floors a negative timestamp to the window before zero", () => {
    expect([...fold(buckets(10n, count()), new Map(), [at(1, -1n), at(2, -10n), at(3, -11n)]).keys()]).toEqual([-10n, -20n]);
  });

  it("starts each window from inner.initial and nests with groupBy", () => {
    const events = [at(1, 5n, { store: "a", amount: "2" }), at(2, 15n, { store: "a", amount: "3" }), at(3, 16n, { store: "b", amount: "4" })];
    expect(fold(buckets(10n, groupBy("store", sum("amount"))), new Map(), events)).toEqual(
      new Map([[0n, new Map([["a", 2n]])], [10n, new Map([["a", 3n], ["b", 4n]])]]),
    );
    const newest = fold(buckets(10n, latest()), new Map(), events);
    expect(newest.get(10n)).toBe(events[2]);
  });

  it("throws on an event without a timestamp", () => {
    expect(() => buckets(10n, count())(new Map(), ev(7, {}, 2) as EventOf<unknown>)).toThrow("buckets: event 7:2 has no timestamp");
  });

  it("keeps at most keep windows, dropping the earliest", () => {
    const b = buckets(10n, count(), { keep: 2 });
    const kept = fold(b, b.initial, [at(1, 1n), at(2, 11n), at(3, 21n), at(4, 22n)]);
    expect(kept).toEqual(new Map([[10n, 1n], [20n, 2n]]));
    expect(b(kept, at(5, 3n) as EventOf<unknown>)).toEqual(kept);
  });

  it("refuses a bad interval or keep at construction", () => {
    for (const bad of [0n, -1n, 10 as unknown as bigint]) {
      expect(() => buckets(bad, count())).toThrow("buckets: interval");
    }
    for (const keep of [0, -1, 1.5, Infinity]) {
      expect(() => buckets(10n, count(), { keep })).toThrow("buckets: keep");
    }
  });

  it("never mutates the map it is given", () => {
    const b = buckets(10n, count());
    const before = new Map([[0n, 1n]]);
    const after = b(before, at(1, 3n) as EventOf<unknown>);
    expect(before).toEqual(new Map([[0n, 1n]]));
    expect(after).toEqual(new Map([[0n, 2n]]));
    expect(b.initial.size).toBe(0);
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

  it("folds the history, then live events, with min, max, distinct and buckets", async () => {
    const stamped = (events: StreamdEvent[]) => events.map((e) => ({ ...e, timestamp: e.version * 10n }));
    const memory = new MemoryAdapter(stamped(HISTORY));
    const client = new StreamdClient({ adapter: memory });
    const least = client.reduce(Deposit, min("amount"), undefined, { fromVersion: 0n });
    const most = client.reduce(Deposit, max("amount"), undefined, { fromVersion: 0n });
    const stores = distinct("store");
    const unique = client.reduce(Deposit, stores, stores.initial, { fromVersion: 0n });
    const perWindow = buckets(20n, sum("amount"));
    const windows = client.reduce(Deposit, perWindow, perWindow.initial, { fromVersion: 0n });
    await advanceTimersByTimeAsync(10);
    expect(await least.ready).toBe(10n);
    expect(await most.ready).toBe(30n);
    expect([...(await unique.ready).values]).toEqual(["0xa", "0xb"]);
    expect(await windows.ready).toEqual(new Map([[0n, 10n], [20n, 50n]]));
    memory.push(...stamped(LIVE));
    await advanceTimersByTimeAsync(3000);
    expect(least.value).toBe(10n);
    expect(most.value).toBe(50n);
    expect([...unique.value.values]).toEqual(["0xa", "0xb", "0xc"]);
    expect(windows.value).toEqual(new Map([[0n, 10n], [20n, 50n], [40n, 90n]]));
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
    const lowest: Reduction<bigint | undefined> = client.reduce(Deposit, min("amount"), undefined);
    const d = distinct("store");
    const seen: ReadonlySet<string> = client.reduce(Deposit, d, d.initial).value.values;
    const w = buckets(60_000_000n, groupBy("store", count()));
    const perWindow: Map<bigint, Map<string, bigint>> = client.reduce(Deposit, w, w.initial).value;
    void [lowest, seen, perWindow];
    void [n, total, groups, store, wrong];
    client.destroy();
  });
});
