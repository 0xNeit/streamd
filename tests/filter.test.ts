import { describe, it, expect } from "bun:test";
import { StreamdError } from "../src/errors.js";
import {
  every,
  involvesAddress,
  normalizeAddress,
  not,
  ofModule,
  ofType,
  some,
  whereField,
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

describe("whereField", () => {
  it("matches a top-level field with address normalisation", () => {
    const byShort = whereField("owner", "0x1");
    const byLong = whereField("owner", LONG_ONE);

    expect(byShort(ev({ owner: LONG_ONE }))).toBe(true);
    expect(byShort(ev({ owner: "0x1" }))).toBe(true);
    expect(byLong(ev({ owner: "0x1" }))).toBe(true);
    expect(
      whereField("owner", "0xa")(
        ev({
          owner:
            "0x000000000000000000000000000000000000000000000000000000000000000A",
        }),
      ),
    ).toBe(true);
    expect(byShort(ev({ owner: "0x2" }))).toBe(false);
    expect(byShort(ev({ owner: "not-an-address" }))).toBe(false);
    expect(byShort(ev({ owner: 1 }))).toBe(false);
  });

  it("traverses nested objects and arrays via dotted string or segment array", () => {
    const payload = {
      inner: {
        deep: { owner: LONG_ONE },
        holders: [{ owner: "0x2" }, { owner: "0x1" }],
      },
    };

    expect(whereField("inner.deep.owner", "0x1")(ev(payload))).toBe(true);
    expect(
      whereField(["inner", "deep", "owner"], "0x1")(ev(payload)),
    ).toBe(true);
    expect(whereField("inner.holders.1.owner", "0x1")(ev(payload))).toBe(true);
    expect(
      whereField(["inner", "holders", 1, "owner"], "0x1")(ev(payload)),
    ).toBe(true);
    expect(whereField("inner.holders.0.owner", "0x1")(ev(payload))).toBe(false);
  });

  it("treats array path segments as literal keys without splitting on dots", () => {
    const payload = { "a.b": { "c.d": "0x1" } };

    expect(whereField(["a.b", "c.d"], LONG_ONE)(ev(payload))).toBe(true);
    expect(whereField("a.b.c.d", LONG_ONE)(ev(payload))).toBe(false);
  });

  it("matches non-address strings, numbers, bigints, booleans, and null", () => {
    const payload = {
      amount: "1000",
      label: "transfer",
      count: 42,
      zero: 0,
      seq: 99n,
      locked: true,
      flag: false,
      empty: null,
    };

    expect(whereField("amount", "1000")(ev(payload))).toBe(true);
    expect(whereField("amount", "999")(ev(payload))).toBe(false);
    expect(whereField("label", "transfer")(ev(payload))).toBe(true);
    expect(whereField("label", "mint")(ev(payload))).toBe(false);
    expect(whereField("count", 42)(ev(payload))).toBe(true);
    expect(whereField("count", 43)(ev(payload))).toBe(false);
    expect(whereField("count", "42")(ev(payload))).toBe(false);
    expect(whereField("zero", 0)(ev(payload))).toBe(true);
    expect(whereField("seq", 99n)(ev(payload))).toBe(true);
    expect(whereField("seq", 100n)(ev(payload))).toBe(false);
    expect(whereField("locked", true)(ev(payload))).toBe(true);
    expect(whereField("locked", false)(ev(payload))).toBe(false);
    expect(whereField("flag", false)(ev(payload))).toBe(true);
    expect(whereField("empty", null)(ev(payload))).toBe(true);
    expect(whereField("missing", null)(ev(payload))).toBe(false);
  });

  it("returns false for missing paths, non-object payloads, out-of-bounds indices, and prototype keys", () => {
    const pred = whereField("a.b", "0x1");
    expect(pred(ev(null))).toBe(false);
    expect(pred(ev(undefined))).toBe(false);
    expect(pred(ev("0x1"))).toBe(false);
    expect(pred(ev(42))).toBe(false);
    expect(pred(ev({ a: null }))).toBe(false);
    expect(pred(ev({ a: "0x1" }))).toBe(false);
    expect(pred(ev({ a: [LONG_ONE] }))).toBe(false);

    const arrPayload = ev({ items: ["0x1"] });
    expect(whereField("items.0", LONG_ONE)(arrPayload)).toBe(true);
    expect(whereField("items.1", LONG_ONE)(arrPayload)).toBe(false);
    expect(whereField("items.01", LONG_ONE)(arrPayload)).toBe(false);
    expect(whereField("items.-1", LONG_ONE)(arrPayload)).toBe(false);
    expect(whereField("items.length", 1)(arrPayload)).toBe(false);

    expect(whereField("toString", "anything")(ev({}))).toBe(false);
    expect(whereField("constructor.name", "Object")(ev({}))).toBe(false);
    expect(whereField("__proto__", null)(ev({}))).toBe(false);
  });

  it("throws StreamdError up front on invalid paths or values", () => {
    for (const badPath of ["", ".a", "a.", "a..b"]) {
      expect(() => whereField(badPath, "0x1")).toThrow(StreamdError);
    }
    for (const badSegments of [
      [],
      [""],
      ["a", ""],
      [-1],
      [1.5],
      [Number.NaN],
      [Number.POSITIVE_INFINITY],
      [null as unknown as string],
    ]) {
      expect(() => whereField(badSegments, "0x1")).toThrow(StreamdError);
    }
    expect(() => whereField(null as unknown as string, "0x1")).toThrow(
      StreamdError,
    );
    for (const badValue of [
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      undefined as unknown as string,
    ]) {
      expect(() => whereField("owner", badValue)).toThrow(StreamdError);
    }
  });
});

describe("ofModule", () => {
  const POOL = `0x${"ab".repeat(32)}`;

  it("matches every struct in the module, generics included", () => {
    const predicate = ofModule("0x1::coin");
    expect(predicate(ev({}, "0x1::coin::CoinDeposit"))).toBe(true);
    expect(
      predicate(ev({}, "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>")),
    ).toBe(true);
  });

  it("ignores the module when it only appears inside generics", () => {
    const predicate = ofModule("0x1::coin");
    expect(predicate(ev({}, "0x1::account::X<0x1::coin::C>"))).toBe(false);
  });

  it("treats short and long address spellings as the same", () => {
    expect(ofModule("0x1::coin")(ev({}, `${LONG_ONE}::coin::C`))).toBe(true);
    expect(ofModule(`${LONG_ONE}::coin`)(ev({}, "0x1::coin::C"))).toBe(true);
  });

  it("does not match another address, a prefixed module, or a different case", () => {
    const predicate = ofModule("0x1::coin");
    expect(predicate(ev({}, "0x2::coin::C"))).toBe(false);
    expect(predicate(ev({}, "0x1::coin_v2::C"))).toBe(false);
    expect(predicate(ev({}, "0x1::Coin::C"))).toBe(false);
  });

  it("matches any of several modules, and nothing when given none", () => {
    const predicate = ofModule("0x1::coin", `${POOL}::pool`);
    expect(predicate(ev({}, "0x1::coin::C"))).toBe(true);
    expect(predicate(ev({}, `${POOL}::pool::Swap`))).toBe(true);
    expect(predicate(ev({}, "0x1::account::A"))).toBe(false);
    expect(ofModule()(ev({}, "0x1::coin::C"))).toBe(false);
  });

  it("returns false for malformed event types", () => {
    const predicate = ofModule("0x1::coin");
    for (const type of ["", "0x1::coin", "coin::C::D", "0xZZ::coin::C"]) {
      expect(predicate(ev({}, type))).toBe(false);
    }
  });

  it("rejects arguments that are not address::module", () => {
    for (const bad of [
      "",
      "0x1",
      "coin",
      "0xZZ::coin",
      "0x1::coin::C",
      "0x1::",
      "0x1::1coin",
      "0x1::co-in",
      42 as unknown as string,
    ]) {
      expect(() => ofModule(bad)).toThrow(StreamdError);
    }
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

  it("not negates a predicate and composes with every, some, ofType, involvesAddress, whereField, and nested not", () => {
    const notA = not(ofType("0x1::a::A"));
    expect(notA(ev({}, "0x1::a::A"))).toBe(false);
    expect(notA(ev({}, "0x1::b::B"))).toBe(true);

    // Double negation restores the original result.
    const doubleNot = not(notA);
    expect(doubleNot(ev({}, "0x1::a::A"))).toBe(true);
    expect(doubleNot(ev({}, "0x1::b::B"))).toBe(false);

    // Exclude self-transfers: involves 0x1, but `from` is not 0x1 (with address normalisation).
    const incomingToOne = every(
      involvesAddress("0x1"),
      not(whereField("from", "0x1")),
    );
    expect(incomingToOne(ev({ from: "0x2", to: LONG_ONE }))).toBe(true);
    expect(incomingToOne(ev({ from: LONG_ONE, to: "0x1" }))).toBe(false);
    expect(incomingToOne(ev({ from: "0x2", to: "0x3" }))).toBe(false);

    // Negate a `some(...)` union (neither type A nor involving 0x1).
    const neither = not(some(ofType("0x1::a::A"), involvesAddress("0x1")));
    expect(neither(ev({ owner: "0x2" }, "0x1::b::B"))).toBe(true);
    expect(neither(ev({ owner: "0x2" }, "0x1::a::A"))).toBe(false);
    expect(neither(ev({ owner: LONG_ONE }, "0x1::b::B"))).toBe(false);
  });

  it("not passes the exact event to the inner predicate and propagates throws", () => {
    const seen: StreamdEvent[] = [];
    const target = ev({ ok: true }, "0x1::a::A");
    const recording = not((e) => {
      seen.push(e);
      return Boolean((e.data as { ok?: boolean }).ok);
    });
    expect(recording(target)).toBe(false);
    expect(seen).toEqual([target]);

    const boom = new Error("inner predicate failed");
    const throwing = not(() => {
      throw boom;
    });
    expect(() => throwing(target)).toThrow(boom);
  });

  it("not throws StreamdError up front when given a non-function", () => {
    for (const bad of [
      undefined,
      null,
      true,
      42,
      "not-a-fn",
      {},
      [],
    ] as unknown[]) {
      expect(() => not(bad as never)).toThrow(StreamdError);
      expect(() => not(bad as never)).toThrow(/Predicate must be a function/);
    }
  });
});
