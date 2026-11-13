import { describe, expect, it } from "bun:test";
import { valueKey, withLatestFunctions } from "../src/key.js";

describe("valueKey", () => {
  it("ignores object key order", () => {
    expect(valueKey({ kind: "module", eventType: "0x1::a::B" })).toBe(
      valueKey({ eventType: "0x1::a::B", kind: "module" }),
    );
  });

  it("keeps bigint apart from numbers and strings", () => {
    expect(valueKey({ v: 1n })).not.toBe(valueKey({ v: 1 }));
    expect(valueKey({ v: 1n })).not.toBe(valueKey({ v: "1" }));
    expect(valueKey({ v: 1n })).toBe(valueKey({ v: 1n }));
    expect(valueKey({ v: -5n })).not.toBe(valueKey({ v: 5n }));
  });

  it("keys functions by presence, not identity", () => {
    expect(valueKey({ where: () => true })).toBe(valueKey({ where: () => false }));
    expect(valueKey({ where: () => true })).not.toBe(valueKey({}));
  });

  it("keys a Map by its entries", () => {
    expect(valueKey(new Map([["0xa", 1n]]))).toBe(valueKey(new Map([["0xa", 1n]])));
    expect(valueKey(new Map([["0xa", 1n]]))).not.toBe(valueKey(new Map([["0xa", 2n]])));
    expect(valueKey(new Map([["0xa", 1n]]))).not.toBe(valueKey(new Map()));
    expect(valueKey(new Map())).toBe(valueKey(new Map()));
    expect(valueKey({ m: new Map([["x", new Map([["y", 1n]])]]) })).not.toBe(
      valueKey({ m: new Map([["x", new Map([["y", 2n]])]]) }),
    );
  });

  it("treats undefined fields as absent and preserves array order", () => {
    expect(valueKey({ a: undefined, b: [1n, 2n] })).toBe(valueKey({ b: [1n, 2n] }));
    expect(valueKey({ b: [1n, 2n] })).not.toBe(valueKey({ b: [2n, 1n] }));
    expect(valueKey([{ y: 1, x: 2 }])).toBe(valueKey([{ x: 2, y: 1 }]));
    expect(valueKey(undefined)).toBe("undefined");
  });
});

describe("withLatestFunctions", () => {
  it("calls the function current at call time and copies other fields", () => {
    let current = { limit: 3, where: (n: number) => n > 1 };
    const wrapped = withLatestFunctions(current, () => current);
    expect(wrapped.limit).toBe(3);
    expect(wrapped.where(2)).toBe(true);
    current = { limit: 3, where: (n: number) => n > 5 };
    expect(wrapped.where(2)).toBe(false);
  });
});
