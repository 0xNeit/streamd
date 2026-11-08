import { describe, it, expect } from "bun:test";
import { resolveFromTime, validateFromTime } from "../src/time.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
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

  it("refuses malformed block heights and a block that answers another height", async () => {
    const { adapter } = chain(uniform);
    const cases: [EventAdapter, string][] = [
      [{ ...adapter, getBlockHeights: async () => ({ oldest: 9n, newest: 2n }) }, "oldest is above newest"],
      [{ ...adapter, getBlockHeights: async () => ({ oldest: "0", newest: 9n }) as never }, "getBlockHeights oldest"],
      [{ ...adapter, getBlock: async (d, h) => ({ ...(await adapter.getBlock!(d, h)), height: h + 1n }) }, "getBlock(0) returned height 1"],
    ];
    for (const [bad, message] of cases) {
      const err = await resolveFromTime(bad, descriptor, 50_000_123n).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect((err as Error).message).toContain(message);
    }
  });

  it("reads block heights once, so a getter cannot change them after the check", async () => {
    const { adapter } = chain(uniform);
    let reads = 0;
    const heights = {
      newest: 999n,
      get oldest() {
        return ++reads > 1 ? 5000n : 0n;
      },
    };
    const flipping = { ...adapter, getBlockHeights: async () => heights };

    expect(await resolveFromTime(flipping, descriptor, 1_250_000n)).toBe(expected(uniform, 1_250_000n));
    expect(reads).toBe(1);
  });

  it("stops between lookups once its caller has given up", async () => {
    const { adapter, looked } = chain(uniform);
    const err = await resolveFromTime(adapter, descriptor, 50_000_123n, () => true).catch(
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(StreamdError);
    expect(looked).toEqual([]);
  });

  /** `chain(...)` whose blocks below `edge.below` answer 410, as a pruning fullnode does. */
  function pruning(stamps: readonly bigint[], edge: { below: bigint }) {
    const { adapter, looked } = chain(stamps);
    const getBlock = adapter.getBlock!;
    adapter.getBlock = async (d, height) => {
      if (height < edge.below) {
        looked.push(height);
        throw new StreamdNetworkError(`block ${height} pruned`, 410);
      }
      return getBlock(d, height);
    };
    return { adapter, looked };
  }

  it("moves past pruned blocks the reported oldest height still names", async () => {
    const served = uniform.slice(40);
    const { adapter } = pruning(uniform, { below: 40n });
    expect(await resolveFromTime(adapter, descriptor, 0n)).toBe(400n);
    for (const t of [served[200]!, served[200]! + 1n, served.at(-1)!]) {
      const { adapter } = pruning(uniform, { below: 40n });
      expect(await resolveFromTime(adapter, descriptor, t)).toBe(expected(uniform, t));
    }
  });

  it("moves past a block pruned while the search runs", async () => {
    const edge = { below: 0n };
    const { adapter } = pruning(uniform, edge);
    const getBlock = adapter.getBlock!;
    let calls = 0;
    adapter.getBlock = async (d, height) => {
      if (++calls === 3) edge.below = 600n;
      return getBlock(d, height);
    };
    const t = uniform[700]!;
    expect(await resolveFromTime(adapter, descriptor, t)).toBe(expected(uniform, t));
    const early = await resolveFromTime(pruning(uniform, { below: 600n }).adapter, descriptor, uniform[10]!);
    expect(early / 10n).toBeGreaterThanOrEqual(600n);
  });

  it("clamps to the exact first served block however far the edge is past the reported oldest", async () => {
    const stamps = Array.from({ length: 4000 }, (_, i) => BigInt(i) * 1000n);
    const { adapter } = pruning(stamps, { below: 1025n });
    expect(await resolveFromTime(adapter, descriptor, 0n)).toBe(10250n);
    for (const t of [1_026_000n, 1_500_000n, 3_999_000n]) {
      const { adapter } = pruning(stamps, { below: 1025n });
      expect(await resolveFromTime(adapter, descriptor, t)).toBe(expected(stamps, t));
    }
  });

  it("clamps when the edge passes the search's upper probe while it runs", async () => {
    const stamps = Array.from({ length: 10_000 }, (_, i) => BigInt(i) * 1000n);
    const edge = { below: 0n };
    const { adapter } = pruning(stamps, edge);
    const getBlock = adapter.getBlock!;
    adapter.getBlock = async (d, height) => {
      const block = getBlock(d, height);
      edge.below++;
      return block;
    };
    const first = await resolveFromTime(adapter, descriptor, 2000n);
    expect(first % 10n).toBe(0n);
    expect(first / 10n).toBeGreaterThan(2n);
    expect(first / 10n).toBeLessThanOrEqual(edge.below);
  });

  it("stops stepping past pruned blocks once its caller has given up", async () => {
    for (const t of [0n, uniform[500]!]) {
      const { adapter, looked } = pruning(uniform, { below: 900n });
      const err = await resolveFromTime(adapter, descriptor, t, () => looked.length >= 2)
        .catch((e: unknown) => e);
      expect((err as Error).message).toBe("fromTime search stopped");
      expect(looked.length).toBe(2);
    }
  });

  it("still rejects other failures, and a source pruned through its newest block", async () => {
    const { adapter } = chain(uniform);
    const failing = { ...adapter, getBlock: async () => { throw new StreamdNetworkError("down", 500); } };
    const down = await resolveFromTime(failing, descriptor, 1n).catch((e: unknown) => e);
    expect((down as StreamdNetworkError).statusCode).toBe(500);

    const gone = await resolveFromTime(pruning(uniform, { below: 5000n }).adapter, descriptor, 1n)
      .catch((e: unknown) => e);
    expect(gone).toBeInstanceOf(StreamdNetworkError);
    expect((gone as StreamdNetworkError).statusCode).toBe(410);
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
