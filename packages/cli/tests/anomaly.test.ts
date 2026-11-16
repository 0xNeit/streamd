import { describe, expect, it } from "bun:test";
import { band, decimal, K_PATTERN, outside } from "../src/anomaly.js";

const big = (...values: number[]) => values.map(BigInt);
const shown = (b: ReturnType<typeof band>) => ({ median: decimal(b.median), mad: decimal(b.mad), low: decimal(b.low), high: decimal(b.high) });

describe("band", () => {
  it("takes the middle value of an odd baseline, in any order", () => {
    const b = band(big(4, 100, 1, 3, 2), "3");
    expect(shown(b)).toEqual({ median: "3", mad: "1", low: "-1.4478", high: "7.4478" });
    expect(outside(b, 8n)).toBe("above");
    expect(outside(b, 7n)).toBeUndefined();
    expect(outside(b, -1n)).toBeUndefined();
    expect(outside(b, -2n)).toBe("below");
  });

  it("averages the two middle values of an even baseline", () => {
    expect(shown(band(big(1, 2, 3, 4), "2"))).toEqual({ median: "2.5", mad: "1", low: "-0.4652", high: "5.4652" });
    expect(shown(band(big(0, 0, 1, 2), "1"))).toEqual({ median: "0.5", mad: "0.5", low: "-0.2413", high: "1.2413" });
  });

  it("keeps negative values exact", () => {
    expect(shown(band(big(-10, -4, -6), "1"))).toEqual({ median: "-6", mad: "2", low: "-8.9652", high: "-3.0348" });
  });

  it("collapses to the median when the baseline is all equal", () => {
    const b = band(big(5, 5, 5, 5), "3");
    expect(shown(b)).toEqual({ median: "5", mad: "0", low: "5", high: "5" });
    expect(outside(b, 5n)).toBeUndefined();
    expect(outside(b, 6n)).toBe("above");
    expect(outside(b, 4n)).toBe("below");
    expect(shown(band(big(0, 0, 0), "3"))).toEqual({ median: "0", mad: "0", low: "0", high: "0" });
  });

  it("does not fire on a bound, only past it", () => {
    // MAD 5000 and k 2 make the half-width exactly 14826.
    const b = band(big(0, 5000, 10000), "2");
    expect(shown(b)).toEqual({ median: "5000", mad: "5000", low: "-9826", high: "19826" });
    expect(outside(b, 19826n)).toBeUndefined();
    expect(outside(b, 19827n)).toBe("above");
    expect(outside(b, -9826n)).toBeUndefined();
    expect(outside(b, -9827n)).toBe("below");
  });

  it("takes a fractional k to four places", () => {
    expect(shown(band(big(0, 5000, 10000), "0.0001"))).toMatchObject({ low: "4999.2587", high: "5000.7413" });
    expect(shown(band(big(1, 2, 3), "2.5"))).toMatchObject({ low: "-1.7065", high: "5.7065" });
  });

  it("stays exact past 2^64", () => {
    const base = 2n ** 64n;
    const b = band([base, base + 2n, base + 4n], "1");
    expect(shown(b)).toEqual({ median: `${base + 2n}`, mad: "2", low: "18446744073709551615.0348", high: "18446744073709551620.9652" });
    expect(outside(b, base + 4n)).toBeUndefined();
    expect(outside(b, base + 5n)).toBe("above");
  });
});

describe("K_PATTERN", () => {
  it("accepts decimals to four places and refuses anything else", () => {
    for (const k of ["3", "0.5", "2.25", "10.0001", "0"]) expect(K_PATTERN.test(k)).toBe(true);
    for (const k of ["", "-1", "1.", ".5", "1.00001", "01", "1e3", " 1"]) expect(K_PATTERN.test(k)).toBe(false);
  });
});
