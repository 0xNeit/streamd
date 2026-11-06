import { describe, it, expect } from "bun:test";
import { resolveFromTime, validateFromTime } from "../src/time.js";
import { StreamdError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor } from "../src/types.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::test::Event" };

/**
 * A chain whose block `oldest + i` is stamped `stamps[i]` and holds versions
 * `10h..10h+9`, counting `getBlock` calls.
 */
function chain(stamps: readonly bigint[], oldest = 0n) {
  const looked: bigint[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    getBlockHeights: async () => ({ oldest, newest: oldest + BigInt(stamps.length - 1) }),
    getBlock: async (_d, height) => {
      looked.push(height);
      const timestamp = stamps[Number(height - oldest)];
      if (timestamp === undefined) throw new Error(`block ${height} out of range`);
      return { height, firstVersion: height * 10n, lastVersion: height * 10n + 9n, timestamp };
    },
  };
  return { adapter, looked };
}

/** The answer by linear scan, to compare the search against. */
function expected(stamps: readonly bigint[], t: bigint, oldest = 0n): bigint {
  const i = stamps.findIndex((s) => s >= t);
  return i === -1 ? (oldest + BigInt(stamps.length - 1)) * 10n + 10n : (oldest + BigInt(i)) * 10n;
}

describe("resolveFromTime", () => {
  const uniform = Array.from({ length: 1000 }, (_, i) => 1_000_000n + BigInt(i) * 250_000n);

  it("finds the first block stamped at or after the time, in few lookups", async () => {
    for (const t of [1_000_001n, 1_250_000n, 50_000_123n, 200_000_000n, uniform.at(-1)!]) {
      const { adapter, looked } = chain(uniform, 5000n);
      expect(await resolveFromTime(adapter, descriptor, t)).toBe(expected(uniform, t, 5000n));
      expect(looked.length).toBeLessThanOrEqual(15);
    }
  });

  it("matches a linear scan on uneven block times and runs of equal stamps", async () => {
    const stamps = [10n, 10n, 11n, 11n, 11n, 11n, 50n, 51n, 52n, 900n, 900n, 901n, 5000n];
    for (let t = 0n; t <= 5001n; t += t < 60n ? 1n : 97n) {
      const { adapter } = chain(stamps);
      expect(await resolveFromTime(adapter, descriptor, t)).toBe(expected(stamps, t));
    }
  });

  it("clamps a time before the oldest block to that block, in one lookup", async () => {
    const { adapter, looked } = chain(uniform, 7n);
    expect(await resolveFromTime(adapter, descriptor, 0n)).toBe(70n);
    expect(looked).toEqual([7n]);
  });

  it("starts just past the head for a future time", async () => {
    const { adapter, looked } = chain(uniform);
    expect(await resolveFromTime(adapter, descriptor, 10n ** 18n)).toBe(9990n + 10n);
    expect(looked).toEqual([0n, 999n]);
  });

  it("handles a single served block", async () => {
    expect(await resolveFromTime(chain([100n], 4n).adapter, descriptor, 100n)).toBe(40n);
    expect(await resolveFromTime(chain([100n], 4n).adapter, descriptor, 101n)).toBe(50n);
  });

  it("rejects an adapter missing either method, naming both", async () => {
    const { adapter } = chain(uniform);
    for (const partial of [
      { ...adapter, getBlock: undefined },
      { ...adapter, getBlockHeights: undefined },
    ]) {
      const err = await resolveFromTime(partial, descriptor, 1n).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect((err as Error).message).toContain("getBlockHeights and getBlock");
    }
  });

  it("stops between lookups once its caller has given up", async () => {
    const { adapter, looked } = chain(uniform);
    const err = await resolveFromTime(adapter, descriptor, 50_000_123n, () => true).catch(
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(StreamdError);
    expect(looked).toEqual([0n, 999n]);
  });
});

describe("validateFromTime", () => {
  it("accepts a non-negative bigint alone or with no start", () => {
    expect(() => validateFromTime({})).not.toThrow();
    expect(() => validateFromTime({ fromTime: 0n })).not.toThrow();
  });

  it.each([
    [{ fromTime: -1n }, "Invalid fromTime"],
    [{ fromTime: 5 as unknown as bigint }, "Invalid fromTime"],
    [{ fromTime: 1n, fromVersion: 3n }, "fromTime or fromVersion"],
    [{ fromTime: 1n, fromVersion: "earliest" }, "fromTime or fromVersion"],
    [{ fromTime: 1n, fromBlock: 3n }, "fromTime or fromBlock"],
  ])("rejects %p", (options, message) => {
    expect(() => validateFromTime(options)).toThrow(message);
  });
});
