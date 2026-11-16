import { describe, expect, it } from "bun:test";
import type { StreamdEvent } from "@streamd/core";
import { changeLine, changeRecord, diffValues, prettyChange, valueLine } from "../src/inspect.js";

const TYPE = "0x1::block::BlockResource";

function change(data: unknown, overrides: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: TYPE, data, version: 100n, sequenceNumber: 0n, eventIndex: 3, ...overrides };
}

describe("diffValues", () => {
  it("lists changed leaves by dotted path, keys sorted", () => {
    const before = { z: "1", a: { counter: "5", guid: { id: "x" } } };
    const after = { z: "2", a: { counter: "6", guid: { id: "x" } } };
    expect(diffValues(before, after)).toEqual([
      { path: "a.counter", before: "5", after: "6" },
      { path: "z", before: "1", after: "2" },
    ]);
  });

  it("is empty for equal values", () => {
    expect(diffValues({ a: [1, { b: true }] }, { a: [1, { b: true }] })).toEqual([]);
  });

  it("reports added and removed keys with one side missing", () => {
    const changes = diffValues({ gone: { x: 1 }, kept: 1 }, { kept: 1, added: [1] });
    expect(changes).toEqual([{ path: "added", after: [1] }, { path: "gone", before: { x: 1 } }]);
    expect("before" in changes[0]!).toBe(false);
    expect("after" in changes[1]!).toBe(false);
  });

  it("walks arrays by index as they grow and shrink", () => {
    expect(diffValues({ items: [{ amount: "1" }] }, { items: [{ amount: "2" }, { amount: "3" }] })).toEqual([
      { path: "items.0.amount", before: "1", after: "2" },
      { path: "items.1", after: { amount: "3" } },
    ]);
    expect(diffValues({ items: ["a", "b"] }, { items: ["a"] })).toEqual([{ path: "items.1", before: "b" }]);
  });

  it("reports a change of shape whole at its path", () => {
    expect(diffValues({ v: { a: 1 } }, { v: [1] })).toEqual([{ path: "v", before: { a: 1 }, after: [1] }]);
    expect(diffValues({ v: { a: 1 } }, { v: "s" })).toEqual([{ path: "v", before: { a: 1 }, after: "s" }]);
    expect(diffValues({ v: null }, { v: { a: 1 } })).toEqual([{ path: "v", before: null, after: { a: 1 } }]);
  });
});

describe("changeRecord", () => {
  it("diffs an update and carries the transaction context", () => {
    const event = change({ height: "2" }, { transactionHash: "0xab", timestamp: 1_700_000_000_000_000n, sender: "0x1" });
    expect(changeRecord(event, { height: "1" })).toEqual({
      version: 100n,
      transactionHash: "0xab",
      timestamp: 1_700_000_000_000_000n,
      sender: "0x1",
      change: "updated",
      changes: [{ path: "height", before: "1", after: "2" }],
    });
  });

  it("keeps an unchanged write as an update with no changes", () => {
    expect(changeRecord(change({ a: 1 }), { a: 1 })).toEqual({ version: 100n, change: "updated", changes: [] });
  });

  it("gives a creation, an unknown before and a delete their own kinds", () => {
    expect(changeRecord(change({ a: 1 }), null)).toEqual({ version: 100n, change: "created", value: { a: 1 } });
    expect(changeRecord(change({ a: 1 }), undefined)).toEqual({ version: 100n, change: "written", value: { a: 1 } });
    expect(changeRecord(change(null), { a: 1 })).toEqual({ version: 100n, change: "deleted" });
    expect(changeRecord(change(null), undefined)).toEqual({ version: 100n, change: "deleted" });
  });
});

describe("changeLine", () => {
  it("prints one JSON line with bigints as decimal strings and missing sides left out", () => {
    const record = changeRecord(change({ n: 2n, extra: "x" }, { timestamp: 5n }), { n: 1n });
    expect(changeLine(record)).toBe(
      '{"version":"100","timestamp":"5","change":"updated","changes":[{"path":"extra","after":"x"},{"path":"n","before":"1","after":"2"}]}\n',
    );
  });
});

describe("prettyChange", () => {
  it("prints a header and one line per changed path", () => {
    const record = changeRecord(change({ a: "2", b: "x" }, { timestamp: 1_700_000_000_000_000n }), { a: "1", c: true });
    expect(prettyChange(record)).toBe(
      [
        "100  2023-11-14 22:13:20  updated",
        '  a: "1" -> "2"',
        '  b: (absent) -> "x"',
        "  c: true -> (absent)",
        "",
      ].join("\n"),
    );
  });

  it("prints whole values, deletes and empty updates", () => {
    expect(prettyChange(changeRecord(change({ a: 1 }), null))).toBe('100  -  created\n  {"a":1}\n');
    expect(prettyChange(changeRecord(change({ a: 1 }), undefined))).toBe('100  -  written\n  {"a":1}\n');
    expect(prettyChange(changeRecord(change(null), { a: 1 }))).toBe("100  -  deleted\n");
    expect(prettyChange(changeRecord(change({ a: 1 }), { a: 1 }))).toBe("100  -  updated\n  (no field changed)\n");
  });

  it("shows control characters from on-chain text as ?", () => {
    const out = prettyChange(changeRecord(change({ name: "a\u009b31mb" }), { name: "a" }));
    expect(out).toBe('100  -  updated\n  name: "a" -> "a?31mb"\n');
  });
});

describe("valueLine", () => {
  it("prints the version and value as JSON, null when absent", () => {
    expect(valueLine(7n, { n: 1n }, false)).toBe('{"version":"7","value":{"n":"1"}}\n');
    expect(valueLine(7n, null, false)).toBe('{"version":"7","value":null}\n');
  });

  it("prints indented JSON for pretty, control characters as ?", () => {
    expect(valueLine(7n, { s: "x\u0085y" }, true)).toBe('{\n  "s": "x?y"\n}\n');
  });
});
