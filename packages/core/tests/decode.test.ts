import { describe, it, expect } from "bun:test";
import { fieldDecoder } from "../src/decode.js";
import { StreamdError } from "../src/errors.js";
import type { StreamdEvent } from "../src/types.js";

function ev(data: unknown): StreamdEvent {
  return { type: "0x1::test::Event", data, version: 7n, sequenceNumber: 0n, eventIndex: 2 };
}

describe("fieldDecoder", () => {
  it("returns undefined when no fields are named", () => {
    expect(fieldDecoder(undefined)).toBeUndefined();
    expect(fieldDecoder([])).toBeUndefined();
  });

  it("rejects an option that is not a list of non-empty names", () => {
    expect(() => fieldDecoder("amount" as unknown as string[])).toThrow(StreamdError);
    expect(() => fieldDecoder([""])).toThrow(StreamdError);
    expect(() => fieldDecoder([1 as unknown as string])).toThrow(StreamdError);
  });

  it("turns named decimal strings into bigint, u128-sized included", () => {
    const decode = fieldDecoder(["amount", "supply"])!;
    const u128Max = "340282366920938463463374607431768211455";
    const out = decode(ev({ amount: "100", supply: u128Max, store: "0x1" }));

    expect(out.data).toEqual({ amount: 100n, supply: BigInt(u128Max), store: "0x1" });
    expect(out.version).toBe(7n);
  });

  it("keeps bigint values and leaves absent fields absent", () => {
    const decode = fieldDecoder(["amount", "fee"])!;
    const out = decode(ev({ amount: 5n }));

    expect(out.data).toEqual({ amount: 5n });
    expect(Object.hasOwn(out.data as object, "fee")).toBe(false);
  });

  it("returns the event untouched when no named field is present or data is not an object", () => {
    const decode = fieldDecoder(["amount"])!;
    for (const data of [{ other: "1" }, null, "1", ["1"]]) {
      const input = ev(data);
      expect(decode(input)).toBe(input);
    }
  });

  it("throws a StreamdError for a named field that is not an unsigned integer string", () => {
    const decode = fieldDecoder(["amount"])!;
    for (const value of [1, "-1", "0x10", "1.5", "", "abc", { value: "1" }, null]) {
      expect(() => decode(ev({ amount: value }))).toThrow(/"amount" of 7:2/);
    }
  });

  it("does not mutate the input event", () => {
    const input = ev({ amount: "9" });
    const out = fieldDecoder(["amount"])!(input);

    expect(input.data).toEqual({ amount: "9" });
    expect(out).not.toBe(input);
  });
});
