import { describe, expect, it } from "bun:test";
import type { StreamdEvent } from "@streamd/core";
import { filterPredicate, parseFilter } from "../src/filter.js";
import type { FilterFlags } from "../src/filter.js";

const LONG_ONE = `0x${"0".repeat(63)}1`;

function ev(data: unknown, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: "0x1::m::E", data, version: 1n, sequenceNumber: 0n, eventIndex: 0, ...extra };
}

function matches(flags: FilterFlags, event: StreamdEvent): boolean {
  const filter = parseFilter(flags);
  if (filter === undefined) throw new Error("expected a filter");
  return filterPredicate(filter)(event) as boolean;
}

describe("parseFilter", () => {
  it("is undefined when no flag is set", () => {
    expect(parseFilter({})).toBeUndefined();
    expect(parseFilter({ sender: [], function: [], where: [] })).toBeUndefined();
  });

  it("normalizes senders and parses each operator, trimming spaces", () => {
    expect(parseFilter({ sender: ["0x1"], where: ["a = 5", "b==x", "c!=1", "d<1", "e<=1", "f>1", "g >= -2"] })).toEqual({
      senders: [LONG_ONE],
      functions: [],
      where: [
        { path: "a", op: "==", value: "5" },
        { path: "b", op: "==", value: "x" },
        { path: "c", op: "!=", value: "1" },
        { path: "d", op: "<", value: "1" },
        { path: "e", op: "<=", value: "1" },
        { path: "f", op: ">", value: "1" },
        { path: "g", op: ">=", value: "-2" },
      ],
    });
  });

  it.each([
    [{ sender: ["alice"] }, /--sender must be an address: alice/],
    [{ function: ["0x1::coin"] }, /--function: Function must be written address::module::function/],
    [{ where: ["amount"] }, /--where must be <field><op><value>.*: amount/],
    [{ where: ["amount=>5"] }, /--where must be <field><op><value>/],
    [{ where: ["amount===5"] }, /--where must be <field><op><value>/],
    [{ where: ["amount<>5"] }, /--where must be <field><op><value>/],
    [{ where: ["amount>x"] }, /--where amount>x: > needs an integer value/],
    [{ where: ["==5"] }, /--where ==5: Field path must not be empty/],
    [{ where: ["a..b==5"] }, /--where a\.\.b==5: Invalid field path/],
  ] as [FilterFlags, RegExp][])("rejects %j", (flags, message) => {
    expect(() => parseFilter(flags)).toThrow(message);
  });
});

describe("filterPredicate", () => {
  it("compares integers at full precision, as strings, numbers or nested", () => {
    const big = "340282366920938463463374607431768211455";
    expect(matches({ where: [`amount>=${big}`] }, ev({ amount: big }))).toBe(true);
    expect(matches({ where: [`amount>${big}`] }, ev({ amount: big }))).toBe(false);
    expect(matches({ where: ["n==7"] }, ev({ n: 7 }))).toBe(true);
    expect(matches({ where: ["legs.1.amount<10"] }, ev({ legs: [{ amount: "50" }, { amount: "9" }] }))).toBe(true);
    // A missing field never matches an integer comparison, under != too.
    expect(matches({ where: ["missing!=1"] }, ev({}))).toBe(false);
  });

  it("matches strings, addresses in long form and booleans", () => {
    expect(matches({ where: ["kind==burn"] }, ev({ kind: "burn" }))).toBe(true);
    expect(matches({ where: ["kind!=burn"] }, ev({ kind: "mint" }))).toBe(true);
    expect(matches({ where: ["kind!=burn"] }, ev({ kind: "burn" }))).toBe(false);
    expect(matches({ where: [`owner==0x1`] }, ev({ owner: LONG_ONE }))).toBe(true);
    expect(matches({ where: ["ok==true"] }, ev({ ok: true }))).toBe(true);
    expect(matches({ where: ["ok==true"] }, ev({ ok: "true" }))).toBe(true);
    expect(matches({ where: ["ok==false"] }, ev({ ok: true }))).toBe(false);
  });

  it("ANDs clauses and flag kinds, and ORs repeated senders and functions", () => {
    const fn = { function: "0x1::aptos_account::transfer", typeArguments: [], arguments: [] };
    const event = ev({ amount: "10" }, { sender: "0x2", entryFunction: fn });
    expect(matches({ sender: ["0x3", "0x2"] }, event)).toBe(true);
    expect(matches({ sender: ["0x3"] }, event)).toBe(false);
    expect(matches({ function: ["0x1::coin::transfer", "0x1::aptos_account::transfer"] }, event)).toBe(true);
    expect(matches({ sender: ["0x2"], function: ["0x1::aptos_account::transfer"], where: ["amount>5", "amount<20"] }, event)).toBe(true);
    expect(matches({ sender: ["0x2"], where: ["amount>5", "amount<9"] }, event)).toBe(false);
    expect(matches({ sender: ["0x2"] }, ev({}))).toBe(false);
  });
});
