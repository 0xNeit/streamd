import { describe, it, expect } from "bun:test";
import {
  every,
  involvesAddress,
  normalizeAddress,
  ofType,
  some,
} from "../src/filter.js";
import type { StreamdEvent } from "../src/types.js";

const LONG_ONE =
  "0x0000000000000000000000000000000000000000000000000000000000000001";

function ev(data: unknown, type = "0x1::test::Event"): StreamdEvent {
  return { type, data, version: 1n, sequenceNumber: 0n, eventIndex: 0 };
}

describe("normalizeAddress", () => {
  it("expands short forms to the canonical long form", () => {
    expect(normalizeAddress("0x1")).toBe(LONG_ONE);
    expect(normalizeAddress(LONG_ONE)).toBe(LONG_ONE);
  });

  it("rejects non-addresses", () => {
    expect(normalizeAddress("hello")).toBeUndefined();
    expect(normalizeAddress("1")).toBeUndefined();
    expect(normalizeAddress("0xZZ")).toBeUndefined();
    // Too long to be an address
    expect(normalizeAddress(`0x${"a".repeat(65)}`)).toBeUndefined();
  });
});

describe("involvesAddress", () => {
  it("matches across differing address spellings", () => {
    // The whole point: 0x1 and its zero-padded form are the same account, so
    // raw string comparison would silently miss.
    const predicate = involvesAddress("0x1");
    expect(predicate(ev({ owner: LONG_ONE }))).toBe(true);
    expect(predicate(ev({ owner: "0x1" }))).toBe(true);
  });

  it("searches nested objects and arrays", () => {
    const predicate = involvesAddress("0x1");
    expect(predicate(ev({ inner: { deep: { owner: "0x1" } } }))).toBe(true);
    expect(predicate(ev({ holders: ["0x2", "0x1"] }))).toBe(true);
  });

  it("does not match unrelated addresses", () => {
    const predicate = involvesAddress("0x1");
    expect(predicate(ev({ owner: "0x2" }))).toBe(false);
    expect(predicate(ev({ amount: "100" }))).toBe(false);
  });

  it("restricts the search when fields are given", () => {
    const predicate = involvesAddress("0x1", { fields: ["to"] });
    expect(predicate(ev({ to: "0x1", from: "0x2" }))).toBe(true);
    expect(predicate(ev({ to: "0x2", from: "0x1" }))).toBe(false);
  });

  it("honours maxDepth", () => {
    const deep = ev({ a: { b: { c: { owner: "0x1" } } } });
    expect(involvesAddress("0x1", { maxDepth: 8 })(deep)).toBe(true);
    expect(involvesAddress("0x1", { maxDepth: 1 })(deep)).toBe(false);
  });

  it("tolerates non-object payloads", () => {
    const predicate = involvesAddress("0x1");
    expect(predicate(ev(null))).toBe(false);
    expect(predicate(ev("0x1"))).toBe(true);
    expect(predicate(ev(42))).toBe(false);
  });

  it("rejects an invalid target address up front", () => {
    expect(() => involvesAddress("not-an-address")).toThrow(TypeError);
  });
});

describe("predicate combinators", () => {
  it("ofType matches exact event types", () => {
    const predicate = ofType("0x1::a::A", "0x1::b::B");
    expect(predicate(ev({}, "0x1::a::A"))).toBe(true);
    expect(predicate(ev({}, "0x1::c::C"))).toBe(false);
  });

  it("every requires all predicates", () => {
    const predicate = every(ofType("0x1::a::A"), involvesAddress("0x1"));
    expect(predicate(ev({ owner: "0x1" }, "0x1::a::A"))).toBe(true);
    expect(predicate(ev({ owner: "0x2" }, "0x1::a::A"))).toBe(false);
  });

  it("some requires at least one predicate", () => {
    const predicate = some(ofType("0x1::a::A"), involvesAddress("0x1"));
    expect(predicate(ev({ owner: "0x2" }, "0x1::a::A"))).toBe(true);
    expect(predicate(ev({ owner: "0x1" }, "0x1::z::Z"))).toBe(true);
    expect(predicate(ev({ owner: "0x2" }, "0x1::z::Z"))).toBe(false);
  });
});
