import { describe, it, expect } from "bun:test";
import { StreamdError } from "../src/errors.js";
import { points, type PointsOptions, type PointsState } from "../src/points.js";
import type { StreamdEvent } from "../src/types.js";

const A = `0x${"a".padStart(64, "0")}`;
const B = `0x${"b".padStart(64, "0")}`;

function ev(version: number, fields: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: "0x1::q::Done", data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0, sender: "0xa", ...fields };
}

const run = (options: PointsOptions, events: StreamdEvent[], from?: PointsState) => {
  const f = points(options);
  return events.reduce(f, from ?? f.initial);
};

describe("points", () => {
  it("awards the sender 1n per event by default, keyed by long-form address", () => {
    const state = run({ rules: [{ id: "any" }] }, [ev(1), ev(2, { sender: "0x0A" }), ev(3, { sender: "0xb" })]);
    expect(state.get(A)).toEqual({ address: A, points: 2n, awards: { any: 2 }, reached: {}, version: 2n });
    expect(state.get(B)?.points).toBe(1n);
  });

  it("reads the address from a data path and points from a function, negative included", () => {
    const state = run(
      { rules: [{ id: "r", address: "who.addr", points: (e) => BigInt((e.data as { n: number }).n) }] },
      [ev(1, { data: { who: { addr: "0xb" }, n: 5 } }), ev(2, { data: { who: { addr: "0xb" }, n: -7 } })],
    );
    expect(state.get(B)?.points).toBe(-2n);
    expect(state.has(A)).toBe(false);
  });

  it("applies where, and each matching rule awards independently", () => {
    const state = run(
      {
        rules: [
          { id: "all", points: 1n },
          { id: "big", points: 10n, where: (e) => e.type.endsWith("Big") },
        ],
      },
      [ev(1), ev(2, { type: "0x1::q::Big" })],
    );
    expect(state.get(A)).toMatchObject({ points: 12n, awards: { all: 2, big: 1 } });
  });

  it("adds firstTime once per address and rule", () => {
    const state = run({ rules: [{ id: "r", points: 2n, firstTime: 100n }] }, [ev(1), ev(2), ev(3, { sender: "0xb" })]);
    expect(state.get(A)?.points).toBe(104n);
    expect(state.get(B)?.points).toBe(102n);
  });

  it("stops a rule at its per-address limit", () => {
    const state = run({ rules: [{ id: "r", limit: 2 }, { id: "s" }] }, [ev(1), ev(2), ev(3)]);
    expect(state.get(A)).toMatchObject({ points: 5n, awards: { r: 2, s: 3 }, version: 3n });
  });

  it("returns the same state when nothing is awarded", () => {
    const f = points({ rules: [{ id: "r", limit: 1 }] });
    const once = f(f.initial, ev(1));
    expect(f(once, ev(2))).toBe(once);
  });

  it("skips a missing or null address and throws on a non-string or non-address", () => {
    const f = points({ rules: [{ id: "r", address: "who" }] });
    expect(f(f.initial, ev(1, { data: {} }))).toBe(f.initial);
    expect(f(f.initial, ev(1, { data: { who: null } }))).toBe(f.initial);
    expect(points({ rules: [{ id: "r" }] })(f.initial, ev(1, { sender: undefined }))).toBe(f.initial);
    expect(() => f(f.initial, ev(1, { data: { who: 7 } }))).toThrow(StreamdError);
    expect(() => f(f.initial, ev(1, { data: { who: "alice" } }))).toThrow(StreamdError);
  });

  it("throws on an async where or a non-bigint points result", () => {
    const asyncWhere = points({ rules: [{ id: "r", where: (() => Promise.resolve(true)) as never }] });
    expect(() => asyncWhere(asyncWhere.initial, ev(1))).toThrow(StreamdError);
    const badPoints = points({ rules: [{ id: "r", points: (() => 1) as never }] });
    expect(() => badPoints(badPoints.initial, ev(1))).toThrow(StreamdError);
  });

  it("records a threshold at its first crossing and keeps it after decay", () => {
    const options: PointsOptions = {
      rules: [{ id: "r", points: 10n }],
      thresholds: [{ id: "bronze", points: 10n }, { id: "silver", points: 20n }],
      decay: { interval: 100n, keep: { numerator: 1n, denominator: 2n } },
    };
    const state = run(options, [ev(1, { timestamp: 0n }), ev(2, { timestamp: 10n }), ev(3, { timestamp: 500n })]);
    // 20 at t=10, then five halvings floor to 0, plus 10.
    expect(state.get(A)).toMatchObject({ points: 10n, reached: { bronze: 1n, silver: 2n }, updatedAt: 500n });
  });

  describe("decay", () => {
    const decay = { interval: 10n, keep: { numerator: 2n, denominator: 3n } };
    const f = points({ rules: [{ id: "r", points: 100n }], decay });

    it("sets the clock at the first award", () => {
      expect(f(f.initial, ev(1, { timestamp: 7n })).get(A)).toMatchObject({ points: 100n, updatedAt: 7n });
    });

    it("applies nothing within the first interval", () => {
      const s = [ev(1, { timestamp: 0n }), ev(2, { timestamp: 9n })].reduce(f, f.initial);
      expect(s.get(A)).toMatchObject({ points: 200n, updatedAt: 0n });
    });

    it("applies one interval, floored, and advances the clock by whole intervals", () => {
      const s = [ev(1, { timestamp: 0n }), ev(2, { timestamp: 15n })].reduce(f, f.initial);
      // floor(100 * 2 / 3) = 66
      expect(s.get(A)).toMatchObject({ points: 166n, updatedAt: 10n });
    });

    it("applies many intervals as one exact power, floored toward zero", () => {
      const s = [ev(1, { timestamp: 0n }), ev(2, { timestamp: 45n })].reduce(f, f.initial);
      // floor(100 * 16 / 81) = 19, then the award
      expect(s.get(A)).toMatchObject({ points: 119n, updatedAt: 40n });
      // floor(2 * 9 / 16) = 1, where flooring each interval would give 0
      const g = points({ rules: [{ id: "r", points: (e) => (e.version === 1n ? 2n : 0n) }], decay: { interval: 1n, keep: { numerator: 3n, denominator: 4n } } });
      expect([ev(1, { timestamp: 0n }), ev(2, { timestamp: 2n })].reduce(g, g.initial).get(A)?.points).toBe(1n);
      // negative scores floor toward zero: -100 * 2 / 3 = -66
      const n = points({ rules: [{ id: "r", points: (e) => (e.version === 1n ? -100n : 0n) }], decay });
      expect([ev(1, { timestamp: 0n }), ev(2, { timestamp: 10n })].reduce(n, n.initial).get(A)?.points).toBe(-66n);
    });

    it("throws on an event without a timestamp", () => {
      expect(() => f(f.initial, ev(1))).toThrow(StreamdError);
    });

    it("leaves updatedAt off without decay", () => {
      expect(run({ rules: [{ id: "r" }] }, [ev(1, { timestamp: 5n })]).get(A)?.updatedAt).toBeUndefined();
    });
  });

  it("leaves initial and earlier states unchanged, copying only changed scores", () => {
    const f = points({ rules: [{ id: "r" }] });
    const s1 = f(f.initial, ev(1));
    const s2 = f(s1, ev(2, { sender: "0xb" }));
    const s3 = f(s2, ev(3));
    expect(f.initial.size).toBe(0);
    expect(s1.size).toBe(1);
    expect(s1.get(A)?.points).toBe(1n);
    expect(s2.get(A)).toBe(s1.get(A)!);
    expect(s3.get(A)?.points).toBe(2n);
    expect(s3.get(B)).toBe(s2.get(B)!);
  });

  it("refuses every malformed option", () => {
    const bad: unknown[] = [
      undefined,
      {},
      { rules: [] },
      { rules: [null] },
      { rules: [{ id: "" }] },
      { rules: [{ id: "r" }, { id: "r" }] },
      { rules: [{ id: "r", where: 1 }] },
      { rules: [{ id: "r", points: 1 }] },
      { rules: [{ id: "r", firstTime: 1 }] },
      { rules: [{ id: "r", limit: 0 }] },
      { rules: [{ id: "r", limit: 1.5 }] },
      { rules: [{ id: "r", address: "" }] },
      { rules: [{ id: "r" }], thresholds: {} },
      { rules: [{ id: "r" }], thresholds: [{ id: "", points: 1n }] },
      { rules: [{ id: "r" }], thresholds: [{ id: "t", points: 1n }, { id: "t", points: 2n }] },
      { rules: [{ id: "r" }], thresholds: [{ id: "t", points: 1 }] },
      { rules: [{ id: "r" }], decay: { interval: 0n, keep: { numerator: 1n, denominator: 2n } } },
      { rules: [{ id: "r" }], decay: { interval: 1n } },
      { rules: [{ id: "r" }], decay: { interval: 1n, keep: { numerator: 1n, denominator: 0n } } },
      { rules: [{ id: "r" }], decay: { interval: 1n, keep: { numerator: -1n, denominator: 2n } } },
      { rules: [{ id: "r" }], decay: { interval: 1n, keep: { numerator: 3n, denominator: 2n } } },
    ];
    for (const options of bad) expect(() => points(options as PointsOptions)).toThrow(StreamdError);
  });
});
