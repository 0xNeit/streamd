import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { changesOnly } from "../src/changes.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { every, not } from "../src/filter.js";
import type { EventDescriptor, StreamdEvent, SyncEventPredicate } from "../src/types.js";

const STORE = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
const OTHER = "0x1::account::Account";
const store: EventDescriptor = { kind: "resource", accountAddress: "0xa", resourceType: STORE };

function write(version: number, data: unknown, type = STORE, eventIndex = 0): StreamdEvent {
  return { type, data, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}
const coin = (value: string, frozen = false) => ({ coin: { value }, frozen });
const versions = (events: StreamdEvent[]) => events.map((e) => e.version);
const kept = (where: SyncEventPredicate, events: StreamdEvent[]) =>
  versions(events.filter((e) => where(e)));

describe("changesOnly", () => {
  it("drops unchanged writes and keeps changed ones over getEvents", async () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([
        write(1, coin("5")),
        write(2, coin("5")),
        write(3, coin("6")),
        write(4, coin("6")),
        write(5, coin("5")),
      ]),
    });
    const events = await client.getEvents(store, { fromVersion: 0n, where: changesOnly() });
    expect(versions(events)).toEqual([1n, 3n, 5n]);
  });

  it("compares the value at a path, a missing value as undefined", () => {
    const where = changesOnly("coin.value");
    expect(
      kept(where, [
        write(1, coin("5")),
        write(2, coin("5", true)),
        write(3, coin("6", true)),
        write(4, { frozen: true }),
        write(5, { coin: {} }),
        write(6, coin("6")),
      ]),
    ).toEqual([1n, 3n, 4n, 6n]);
    expect(() => changesOnly("coin..value")).toThrow(StreamdError);
    expect(() => changesOnly([])).toThrow(StreamdError);
  });

  it("treats a delete as a change, and two in a row as equal", () => {
    const events = [
      write(1, coin("5")),
      write(2, null),
      write(3, null),
      write(4, coin("5")),
    ];
    expect(kept(changesOnly(), events)).toEqual([1n, 2n, 4n]);
    expect(kept(changesOnly("coin.value"), events)).toEqual([1n, 2n, 4n]);
    // Under a path, a delete still differs from a write that lacked the field.
    expect(kept(changesOnly("coin.value"), [write(1, {}), write(2, null)])).toEqual([1n, 2n]);
  });

  it("compares deeply, keys in any order, bigints by value", () => {
    const where = changesOnly();
    expect(
      kept(where, [
        write(1, { a: 1n, b: [1, { c: "x" }] }),
        write(2, { b: [1, { c: "x" }], a: 1n }),
        write(3, { a: "1", b: [1, { c: "x" }] }),
        write(4, { a: "1", b: [1, { c: "y" }] }),
      ]),
    ).toEqual([1n, 3n, 4n]);
  });

  it("keeps state per type and per instance", () => {
    const where = changesOnly();
    expect(
      kept(where, [
        write(1, coin("5")),
        write(2, coin("5"), OTHER),
        write(3, coin("5")),
        write(4, coin("5"), OTHER),
        write(5, coin("6"), OTHER),
      ]),
    ).toEqual([1n, 2n, 5n]);
    const fresh = changesOnly();
    expect(fresh(write(6, coin("5")))).toBe(true);
    expect(where(write(6, coin("5")))).toBe(false);
  });

  it("answers an event tested again as it did the first time", () => {
    const where = changesOnly();
    const events = [write(1, coin("5")), write(2, coin("5")), write(3, coin("6"))];
    expect(kept(where, events)).toEqual([1n, 3n]);
    expect(kept(where, events)).toEqual([1n, 3n]);
    expect(where(write(4, coin("6")))).toBe(false);
  });

  it("passes an earlier event it does not remember, without moving state", () => {
    const where = changesOnly();
    expect(where(write(5, coin("5")))).toBe(true);
    expect(where(write(3, coin("9")))).toBe(true);
    expect(where(write(5, coin("5"), STORE, 1))).toBe(false);
    expect(where(write(6, coin("9")))).toBe(true);
  });

  it("stays synchronous under every and not", () => {
    const where = every(changesOnly(), () => true);
    expect(where(write(1, coin("5")))).toBe(true);
    const repeats = not(changesOnly());
    expect(kept(repeats, [write(1, coin("5")), write(2, coin("5")), write(3, coin("6"))])).toEqual([2n]);
  });

  it("still delivers a change after a failed poll tests it again", async () => {
    const adapter = new MemoryAdapter([
      write(1, coin("5")),
      write(2, coin("5")),
      write(3, coin("6")),
      write(4, coin("6")),
      write(5, coin("7")),
    ]);
    let flaked = false;
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter }).on(store, (e) => void seen.push(e.version), {
      fromVersion: 0n,
      pollInterval: 1,
      onError: (err) => errors.push(err),
      where: every(changesOnly(), async (e) => {
        if (e.version === 3n && !flaked) {
          flaked = true;
          throw new StreamdNetworkError("lookup flaked", 503);
        }
        return true;
      }),
    });
    for (let i = 0; i < 200 && seen.length < 3; i++) await new Promise((r) => setTimeout(r, 2));
    sub.unsubscribe();
    expect(errors).toHaveLength(1);
    expect(seen).toEqual([1n, 3n, 5n]);
  });
});
