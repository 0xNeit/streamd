import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { normalizeAddress } from "../src/filter.js";
import type { EventAdapter, StreamdEvent, WatchOptions } from "../src/types.js";

const A = "0xa";
const B = "0xb";
const C = "0xc";

function ev(version: number, sender: string): StreamdEvent {
  return {
    type: "0x1::test::Event",
    data: { version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
    sender,
  };
}

/** Answers each `account` descriptor from the events its address sent; `failing` addresses always throw. */
function bySender(events: StreamdEvent[], failing: string[] = []): EventAdapter {
  return {
    fetchEvents: vi.fn(async (d, o) => {
      if (d.kind !== "account") throw new Error("account descriptors only");
      const address = normalizeAddress(d.accountAddress);
      if (failing.some((f) => normalizeAddress(f) === address)) throw new StreamdNetworkError("down");
      const own = events.filter((e) => normalizeAddress(e.sender!) === address);
      return new MemoryAdapter(own).fetchEvents(d, o);
    }),
  };
}

function setup(events: StreamdEvent[], failing: string[] = []) {
  const adapter = bySender(events, failing);
  const client = new StreamdClient({ adapter });
  const log: string[] = [];
  const errors: Error[] = [];
  const watch = (addresses: string[], options: WatchOptions = {}) =>
    client.watch(addresses, (e, address) => void log.push(`${e.version}@${address}`), {
      onError: (err) => void errors.push(err),
      ...options,
    });
  return { adapter, client, log, errors, watch };
}

describe("watch", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("delivers each watched address's events with that address, and no one else's", async () => {
    const { client, log, watch } = setup([ev(1, A), ev(2, B), ev(3, C), ev(4, A)]);
    const w = watch([A, B]);
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log.sort()).toEqual(["1@0xa", "2@0xb", "4@0xa"]);
    expect(w.addresses).toEqual([]);
    expect(w.active).toBe(false);
  });

  it("refuses an invalid address at watch() and add(), and cursor or id", () => {
    const { client, watch } = setup([]);
    expect(() => watch(["nope"])).toThrow(StreamdError);
    const w = watch([]);
    expect(() => w.add("nope")).toThrow("Not a valid Aptos address: nope");
    expect(() => watch([A], { cursor: "1" } as WatchOptions)).toThrow(StreamdError);
    expect(() => watch([A], { id: "x" } as WatchOptions)).toThrow(StreamdError);
    expect(w.remove("nope")).toBe(false);
    client.destroy();
  });

  it("watches an address spelled twice once, keeping the first spelling", async () => {
    const long = `0x${"0".repeat(63)}a`;
    const { client, log, watch } = setup([ev(1, A)]);
    const w = watch([A, long]);
    expect(w.addresses).toEqual([A]);
    expect(w.add(long)).toBe(false);
    await advanceTimersByTimeAsync(10);
    client.destroy();
    expect(log).toEqual(["1@0xa"]);
  });

  it("applies fromVersion to the initial addresses only", async () => {
    const { client, log, watch } = setup([ev(2, A), ev(2, B), ev(6, A), ev(7, B)]);
    const w = watch([A], { fromVersion: 5n });
    await advanceTimersByTimeAsync(10);
    expect(w.add(B)).toBe(true);
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log).toEqual(["6@0xa", "2@0xb", "7@0xb"]);
    expect(w.add(C)).toBe(false);
  });

  it("remove() stops the address's walk", async () => {
    const events = [ev(1, A), ev(2, B)];
    const { client, log, watch } = setup(events);
    const w = watch([A, B], { pollInterval: 100 });
    await advanceTimersByTimeAsync(10);
    expect(w.remove(A)).toBe(true);
    expect(w.remove(A)).toBe(false);
    expect(w.addresses).toEqual([B]);
    events.push(ev(3, A), ev(4, B));
    await advanceTimersByTimeAsync(250);
    client.destroy();

    expect(log.sort()).toEqual(["1@0xa", "2@0xb", "4@0xb"]);
  });

  it("starts walks added while paused paused, and delivers after resume()", async () => {
    const { client, log, adapter, watch } = setup([ev(1, A), ev(2, B)]);
    const w = watch([A]);
    w.pause();
    expect(w.add(B)).toBe(true);
    await advanceTimersByTimeAsync(10);
    expect(log).toEqual([]);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect(w.paused).toBe(true);
    expect(w.active).toBe(false);
    w.resume();
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(log.sort()).toEqual(["1@0xa", "2@0xb"]);
  });

  it("drops an address whose walk gives up and keeps the others", async () => {
    const events = [ev(1, A)];
    const { client, log, errors, watch } = setup(events, [B]);
    const w = watch([A, B], { maxErrors: 2, pollInterval: 10, maxBackoff: 10 });
    await advanceTimersByTimeAsync(100);
    expect(w.addresses).toEqual([A]);
    expect(w.active).toBe(true);
    events.push(ev(2, A));
    await advanceTimersByTimeAsync(30);
    client.destroy();

    expect(log).toEqual(["1@0xa", "2@0xa"]);
    expect(errors.length).toBeGreaterThanOrEqual(2);
  });

  it("fires onCaughtUp once, after every initial address caught up", async () => {
    const { client, watch } = setup([ev(1, A), ev(2, B)]);
    const onCaughtUp = vi.fn();
    const w = watch([A, B], { onCaughtUp, pollInterval: 10 });
    w.add(C);
    await advanceTimersByTimeAsync(50);
    client.destroy();
    expect(onCaughtUp).toHaveBeenCalledTimes(1);

    const empty = vi.fn();
    watch([], { onCaughtUp: empty });
    await advanceTimersByTimeAsync(1);
    expect(empty).toHaveBeenCalledTimes(1);
    client.destroy();
  });

  it("reports a throwing callback and carries on", async () => {
    const client = new StreamdClient({ adapter: bySender([ev(1, A), ev(2, A)]) });
    const errors: Error[] = [];
    const seen: bigint[] = [];
    client.watch(
      [A],
      (e) => {
        seen.push(e.version);
        if (e.version === 1n) throw new Error("boom");
      },
      { onError: (err) => void errors.push(err) },
    );
    await advanceTimersByTimeAsync(10);
    client.destroy();

    expect(seen).toEqual([1n, 2n]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(StreamdError);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(Error);
  });

  it("is not a listener, and ends on unsubscribe, dispose and abort", async () => {
    const events = [ev(1, A)];
    const { client, log, watch } = setup(events);
    const w = watch([A], { pollInterval: 10 });
    expect(client.listenerCount()).toBe(0);
    client.removeAllListeners();
    await advanceTimersByTimeAsync(5);
    expect(log).toEqual(["1@0xa"]);
    w.unsubscribe();
    expect(w.active).toBe(false);
    expect(w.add(B)).toBe(false);

    const disposed = watch([A], { pollInterval: 10, fromVersion: 2n });
    disposed[Symbol.dispose]();
    const controller = new AbortController();
    const aborted = watch([A], { pollInterval: 10, fromVersion: 2n, signal: controller.signal });
    controller.abort();
    const early = watch([A], { pollInterval: 10, fromVersion: 2n, signal: controller.signal });
    expect(early.active).toBe(false);
    expect(early.addresses).toEqual([]);
    expect(disposed.active).toBe(false);
    expect(aborted.active).toBe(false);
    events.push(ev(3, A));
    await advanceTimersByTimeAsync(50);
    expect(log).toEqual(["1@0xa"]);
    client.destroy();
  });

  it("reports a combined status", async () => {
    const { client, watch } = setup([ev(1, A), ev(5, B)]);
    const w = watch([]);
    expect(w.status).toEqual({
      active: true,
      cursor: undefined,
      lastVersion: undefined,
      consecutiveErrors: 0,
      lastSuccessAt: undefined,
    });
    w.add(A);
    w.add(B);
    await advanceTimersByTimeAsync(10);
    expect(w.status.lastVersion).toBe(5n);
    expect(w.status.lastSuccessAt).toBeNumber();
    expect(w.cursor).toBeUndefined();
    client.destroy();
    expect(w.status.active).toBe(false);
  });
});

describe("watch positions", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const long = (a: string) => normalizeAddress(a)!;
  const settle = () => advanceTimersByTimeAsync(10);

  it("reports each address's position, long-form, and leaves out one with none", async () => {
    const { client, watch } = setup([ev(1, A), ev(2, B), ev(3, A)]);
    const w = watch([A, B], { fromVersion: 0n });
    expect(w.positions).toEqual({});
    await settle();
    expect(Object.keys(w.positions).sort()).toEqual([long(A), long(B)].sort());
    expect(typeof w.positions[long(A)]).toBe("string");
    client.destroy();
  });

  it("continues each address from its position after a restart", async () => {
    const events = [ev(1, A), ev(2, B)];
    const first = setup(events);
    const w = first.watch([A, B], { fromVersion: 0n });
    await settle();
    first.client.destroy();
    const saved = JSON.parse(JSON.stringify(w.positions)) as Record<string, string>;

    const next = setup([...events, ev(3, A), ev(4, B)]);
    next.watch([A, B], { positions: saved, fromVersion: 0n });
    await settle();
    next.client.destroy();
    expect(next.log.sort()).toEqual(["3@0xa", "4@0xb"]);
  });

  it("starts an address without a position as usual", async () => {
    const events = [ev(1, A), ev(2, B)];
    const first = setup(events);
    const w = first.watch([A], { fromVersion: 0n });
    await settle();
    first.client.destroy();

    const next = setup([...events, ev(3, A)]);
    next.watch([A, B], { positions: w.positions, fromVersion: 0n });
    await settle();
    next.client.destroy();
    expect(next.log.sort()).toEqual(["2@0xb", "3@0xa"]);
  });

  it("stays readable after unsubscribe and destroy", async () => {
    const a = setup([ev(1, A)]);
    const w1 = a.watch([A], { fromVersion: 0n });
    await settle();
    w1.unsubscribe();
    expect(Object.keys(w1.positions)).toEqual([long(A)]);

    const b = setup([ev(1, A)]);
    const w2 = b.watch([A], { fromVersion: 0n });
    await settle();
    b.client.destroy();
    expect(Object.keys(w2.positions)).toEqual([long(A)]);
  });

  it("keeps the position of a walk that gave up", async () => {
    let failing = false;
    const memory = new MemoryAdapter([ev(1, A)]);
    const adapter: EventAdapter = {
      fetchEvents: async (d, o) => {
        if (failing) throw new StreamdNetworkError("down");
        return memory.fetchEvents(d, o);
      },
    };
    const client = new StreamdClient({ adapter });
    const w = client.watch([A], () => {}, { fromVersion: 0n, maxErrors: 1, pollInterval: 1, onError: () => {} });
    await settle();
    const before = w.positions[long(A)];
    expect(before).toBeDefined();
    failing = true;
    await settle();
    expect(w.addresses).toEqual([]);
    expect(w.positions[long(A)]).toBe(before!);
    client.destroy();
  });

  it("carries a position for an unwatched address, which add uses once", async () => {
    const events = [ev(1, B), ev(2, B)];
    const first = setup(events);
    const w = first.watch([B], { fromVersion: 0n });
    await settle();
    first.client.destroy();
    const saved = w.positions;

    const next = setup([...events, ev(3, B)]);
    const resumed = next.watch([], { positions: saved });
    expect(resumed.positions).toEqual(saved);
    resumed.add(B);
    await settle();
    expect(next.log).toEqual(["3@0xb"]);

    resumed.remove(B);
    expect(resumed.positions).toEqual({});
    resumed.add(B);
    await settle();
    // With no position left it starts as on() would, which on MemoryAdapter is from the top.
    expect(next.log).toEqual(["3@0xb", "1@0xb", "2@0xb", "3@0xb"]);
    next.client.destroy();
  });

  it("treats short and long keys as the same address", async () => {
    const events = [ev(1, A)];
    const first = setup(events);
    const w = first.watch([A], { fromVersion: 0n });
    await settle();
    first.client.destroy();

    const next = setup([...events, ev(2, A)]);
    next.watch([A], { positions: { [A]: w.positions[long(A)]! }, fromVersion: 0n });
    await settle();
    next.client.destroy();
    expect(next.log).toEqual(["2@0xa"]);
  });

  it("refuses invalid positions at the call", () => {
    const { watch } = setup([]);
    for (const bad of [[], null, "x", 1]) {
      expect(() => watch([A], { positions: bad as never })).toThrow(StreamdError);
    }
    expect(() => watch([A], { positions: { nope: "1" } })).toThrow(StreamdError);
    expect(() => watch([A], { positions: { [A]: "" } })).toThrow(StreamdError);
    expect(() => watch([A], { positions: { [A]: 1 as unknown as string } })).toThrow(StreamdError);
  });
});
