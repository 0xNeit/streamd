import { describe, it, expect } from "bun:test";
import {
  StreamdError,
  StreamdNetworkError,
  parseRetryAfter,
  retryAfterHint,
} from "../src/errors.js";

describe("parseRetryAfter", () => {
  const now = Date.parse("Wed, 21 Oct 2015 07:28:00 GMT");

  it("reads delta-seconds as milliseconds", () => {
    expect(parseRetryAfter("5", now)).toBe(5000);
    expect(parseRetryAfter(" 120 ", now)).toBe(120_000);
    expect(parseRetryAfter("0", now)).toBe(0);
  });

  it("reads each HTTP-date format as the time until it", () => {
    expect(parseRetryAfter("Wed, 21 Oct 2015 07:28:07 GMT", now)).toBe(7000);
    expect(parseRetryAfter("Wednesday, 21-Oct-15 07:28:07 GMT", now)).toBe(7000);
    // asctime carries no zone, so Date.parse reads it as local time.
    expect(parseRetryAfter("Wed Oct 21 07:28:07 2015", now)).toBeNumber();
  });

  it("gives 0 for a date already past", () => {
    expect(parseRetryAfter("Wed, 21 Oct 2015 07:27:00 GMT", now)).toBe(0);
  });

  it("is undefined for an absent or malformed value", () => {
    for (const value of [undefined, null, "", "  ", "-1", "1.5", "5s", "soon", "Wed, nope"]) {
      expect(parseRetryAfter(value, now)).toBeUndefined();
    }
  });
});

describe("retryAfterHint", () => {
  const network = (retryAfter?: number) =>
    new StreamdNetworkError("rate limited", 429, undefined, retryAfter);

  it("returns a finite non-negative hint from a network error", () => {
    expect(retryAfterHint(network(5000))).toBe(5000);
    expect(retryAfterHint(network(0))).toBe(0);
  });

  it("ignores missing, invalid, and non-network hints", () => {
    expect(retryAfterHint(network())).toBeUndefined();
    expect(retryAfterHint(network(NaN))).toBeUndefined();
    expect(retryAfterHint(network(-1))).toBeUndefined();
    expect(retryAfterHint(network(Infinity))).toBeUndefined();
    expect(retryAfterHint(new StreamdError("bad input"))).toBeUndefined();
    expect(retryAfterHint({ retryAfter: 5000 })).toBeUndefined();
  });
});
