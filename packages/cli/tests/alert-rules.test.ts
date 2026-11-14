import { describe, expect, it } from "bun:test";
import { parseRules } from "../src/alert-rules.js";

const TYPE = "0x1::transaction_fee::FeeStatement";
const LONG_ONE = `0x${"0".repeat(63)}1`;
const base = { name: "fees", descriptor: { eventType: TYPE }, window: "1m", threshold: { count: 3 } };

const parse = (rules: unknown[]) => parseRules(JSON.stringify({ rules }), "rules.json");
const failure = (rule: Record<string, unknown>) => () => parse([{ ...base, ...rule }]);

describe("parseRules", () => {
  it("reads a count rule with a filter and decode", () => {
    const [rule] = parse([
      { ...base, where: ["amount>=10"], sender: ["0x1"], function: ["0x1::coin::transfer"], decode: ["amount"], window: "30s" },
    ]);
    expect(rule).toMatchObject({
      name: "fees",
      descriptor: { kind: "module", eventType: [TYPE] },
      filter: { senders: [LONG_ONE], functions: ["0x1::coin::transfer"], where: [{ path: "amount", op: ">=", value: "10" }] },
      decode: ["amount"],
      window: 30_000_000n,
      threshold: { count: 3n },
    });
  });

  it("reads a sum rule over an account, with gte exact past 2^64", () => {
    const [rule] = parse([
      { ...base, descriptor: { account: "0x1", eventType: [TYPE] }, window: "2h", threshold: { sum: "amount", gte: "18446744073709551617" } },
    ]);
    expect(rule!.descriptor).toEqual({ kind: "account", accountAddress: LONG_ONE, eventTypes: [TYPE] });
    expect(rule!.window).toBe(7_200_000_000n);
    expect(rule!.threshold).toEqual({ sum: "amount", gte: 18446744073709551617n });
    expect(rule!.filter).toBeUndefined();
    expect(rule!.decode).toBeUndefined();
  });

  it("reads a resource descriptor and a safe-integer gte", () => {
    const [rule] = parse([
      { ...base, descriptor: { account: "0x1", resource: "0x1::account::Account" }, window: "1d", threshold: { sum: "n", gte: -5 } },
    ]);
    expect(rule!.descriptor).toEqual({ kind: "resource", accountAddress: LONG_ONE, resourceType: "0x1::account::Account" });
    expect(rule!.window).toBe(86_400_000_000n);
    expect(rule!.threshold).toEqual({ sum: "n", gte: -5n });
  });

  it("fingerprints what a rule counts, not its name", () => {
    const [a, b, c] = parse([base, { ...base, name: "other" }, { ...base, name: "third", window: "2m" }]);
    expect(a!.fingerprint).toBe(b!.fingerprint);
    expect(a!.fingerprint).not.toBe(c!.fingerprint);
  });

  it("refuses a malformed file", () => {
    expect(() => parseRules("{", "rules.json")).toThrow("rules.json: not valid JSON");
    expect(() => parseRules("[]", "rules.json")).toThrow('rules.json: must be a JSON object with "rules"');
    expect(() => parseRules("{}", "rules.json")).toThrow('rules.json: "rules" must be a non-empty array');
    expect(() => parseRules('{"rules":[]}', "rules.json")).toThrow('rules.json: "rules" must be a non-empty array');
    expect(() => parseRules('{"rules":[],"to":"x"}', "rules.json")).toThrow('rules.json: unknown field "to"');
  });

  it("names a rule by position when its name is the problem", () => {
    expect(() => parse([{ ...base, name: "" }])).toThrow("rules.json: rule #1: name: must be a non-empty string");
    expect(() => parse(["x"])).toThrow("rules.json: rule #1: name: must be a non-empty string");
    expect(() => parse([base, base])).toThrow('rules.json: rule #2: name: "fees" is used by an earlier rule');
  });

  it("names the rule and field of every other error", () => {
    const at = 'rules.json: rule "fees" (#1): ';
    expect(failure({ extra: 1 })).toThrow(`${at}unknown field "extra"`);
    expect(failure({ window: undefined })).toThrow(`${at}window: is required`);
    expect(failure({ descriptor: "x" })).toThrow(`${at}descriptor: must be an object`);
    expect(failure({ descriptor: {} })).toThrow(`${at}descriptor: needs eventType or account`);
    expect(failure({ descriptor: { eventType: [] } })).toThrow(`${at}descriptor: eventType must be a string or a non-empty array of strings`);
    expect(failure({ descriptor: { eventType: "nope" } })).toThrow(`${at}descriptor: Not a valid Move event type: nope`);
    expect(failure({ descriptor: { account: "zz" } })).toThrow(`${at}descriptor: Not a valid Aptos address: zz`);
    expect(failure({ descriptor: { account: 1 } })).toThrow(`${at}descriptor: account must be a string`);
    expect(failure({ descriptor: { resource: "0x1::a::A" } })).toThrow(`${at}descriptor: resource needs account, and no eventType`);
    expect(failure({ descriptor: { account: "0x1", handle: "0x1::a::A", field: "f" } })).toThrow(
      `${at}descriptor: handle descriptors are not supported, since their events carry no timestamp`,
    );
    expect(failure({ descriptor: { kind: "module" } })).toThrow(`${at}descriptor: unknown field "kind"`);
    expect(failure({ where: "amount>1" })).toThrow(`${at}where: must be an array of strings`);
    expect(failure({ where: ["amount"] })).toThrow(`${at}where must be <field><op><value>`);
    expect(failure({ where: ["amount<x"] })).toThrow(`${at}where amount<x: < needs an integer value`);
    expect(failure({ sender: ["zz"] })).toThrow(`${at}sender must be an address: zz`);
    expect(failure({ decode: ["a..b"] })).toThrow(`${at}decode: decode path "a..b" needs a field first and no empty segments`);
    for (const window of ["0m", "5", "1w", 60]) {
      expect(failure({ window })).toThrow(`${at}window: must be a duration such as 30s, 5m, 1h or 1d`);
    }
  });

  it("refuses a threshold that is not exactly a count or a sum", () => {
    const at = 'rules.json: rule "fees" (#1): threshold: ';
    const shape = `${at}must be { "count": n } or { "sum": "<path>", "gte": "<integer>" }`;
    expect(failure({ threshold: 3 })).toThrow(`${at}must be an object`);
    expect(failure({ threshold: {} })).toThrow(shape);
    expect(failure({ threshold: { count: 1, sum: "a", gte: "1" } })).toThrow(shape);
    expect(failure({ threshold: { sum: "a" } })).toThrow(shape);
    for (const count of [0, -1, 1.5, "3", 2 ** 53]) {
      expect(failure({ threshold: { count } })).toThrow(`${at}count must be a positive integer`);
    }
    expect(failure({ threshold: { sum: 1, gte: "1" } })).toThrow(`${at}sum must be a field path`);
    expect(failure({ threshold: { sum: "a..b", gte: "1" } })).toThrow(`${at}sum: Invalid field path: "a..b"`);
    for (const gte of ["1.5", "", "1e3", 1.5, null]) {
      expect(failure({ threshold: { sum: "a", gte } })).toThrow(`${at}gte must be an integer, as a decimal string or a safe integer`);
    }
  });

  it("checks descriptors by Sui's rules under --chain sui", () => {
    const [rule] = parseRules(JSON.stringify({ rules: [{ ...base, descriptor: { account: "0x2" } }] }), "f", true);
    expect(rule!.descriptor).toEqual({ kind: "account", accountAddress: `0x${"0".repeat(63)}2` });
    expect(() =>
      parseRules(JSON.stringify({ rules: [{ ...base, descriptor: { account: "0x2", resource: "0x2::a::A" } }] }), "f", true),
    ).toThrow('f: rule "fees" (#1): descriptor: resource is not available with chain sui');
  });
});
