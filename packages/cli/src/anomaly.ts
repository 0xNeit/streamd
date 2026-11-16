/**
 * Exact median-absolute-deviation bands over bigints. Every quantity is kept
 * as a numerator over `DENOMINATOR`: a median is a multiple of 1/2, a MAD at
 * worst of 1/4, `k` has at most 4 fractional digits and 1.4826 has 4, so 4 * 10^8
 * holds them all as integers.
 */
const DENOMINATOR = 400_000_000n;
/** 1.4826 * 10^4: scales the MAD to a standard deviation for normally distributed data. */
const MAD_SCALE = 14_826n;
const K_DIGITS = 4;

/** A non-negative decimal with at most four fractional digits; zero is refused where `k` is read. */
export const K_PATTERN = /^(?:0|[1-9]\d*)(?:\.\d{1,4})?$/;

export interface Band {
  /** Numerators over the shared denominator; read them with `decimal`. */
  median: bigint;
  mad: bigint;
  low: bigint;
  high: bigint;
}

/** `k` as an integer of ten-thousandths. */
function scaledK(k: string): bigint {
  const [whole, fraction = ""] = k.split(".");
  return BigInt(whole! + fraction.padEnd(K_DIGITS, "0"));
}

/** Twice the median of `values`, which is exact; `values` must be sorted and non-empty. */
function twiceMedian(values: readonly bigint[]): bigint {
  const mid = values.length >> 1;
  return values.length % 2 === 1 ? 2n * values[mid]! : values[mid - 1]! + values[mid]!;
}

const ascending = (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0);
const abs = (n: bigint) => (n < 0n ? -n : n);

/** The band `median ± k * 1.4826 * MAD` of a non-empty baseline. */
export function band(baseline: readonly bigint[], k: string): Band {
  const m2 = twiceMedian([...baseline].sort(ascending));
  const mad4 = twiceMedian(baseline.map((x) => abs(2n * x - m2)).sort(ascending));
  const median = m2 * (DENOMINATOR / 2n);
  const width = scaledK(k) * MAD_SCALE * mad4;
  return { median, mad: mad4 * (DENOMINATOR / 4n), low: median - width, high: median + width };
}

/** Which side of the band `value` falls on, strictly outside it, or undefined inside or on a bound. */
export function outside(b: Band, value: bigint): "above" | "below" | undefined {
  const scaled = value * DENOMINATOR;
  return scaled > b.high ? "above" : scaled < b.low ? "below" : undefined;
}

/** A band numerator as an exact decimal string, trailing zeros trimmed. */
export function decimal(numerator: bigint): string {
  // 10^10 / DENOMINATOR is 25, so the quotient is exact at ten places.
  const tenths = abs(numerator) * 25n;
  const digits = tenths.toString().padStart(11, "0");
  const fraction = digits.slice(-10).replace(/0+$/, "");
  const text = digits.slice(0, -10) + (fraction === "" ? "" : `.${fraction}`);
  return numerator < 0n ? `-${text}` : text;
}
