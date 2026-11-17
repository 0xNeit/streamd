import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { SuiAdapter } from "../src/adapter.js";
import { callsOf } from "../src/calls.js";
import { LONG, digest, fakeLedger, ptb, withCalls, type Command } from "./helpers.js";

const POOL = LONG("ab");
const swap: Command = { package: POOL, module: "pool", function: "swap", typeArguments: [`${LONG("2")}::sui::SUI`], arguments: [] };

describe("callsOf", () => {
  it("resolves every argument kind against the inputs", () => {
    const inputs = [
      { kind: 1, pure: new Uint8Array([1, 2, 3]) },
      { kind: 3, objectId: "0x6" },
      { kind: 2, objectId: LONG("c01") },
      { kind: 4, objectId: LONG("ec") },
      { kind: 5 },
    ];
    const args = [
      { kind: 1 },
      { kind: 2, input: 0 },
      { kind: 2, input: 1 },
      { kind: 2, input: 2 },
      { kind: 2, input: 3 },
      { kind: 2, input: 4 },
      { kind: 3, result: 0 },
      { kind: 3, result: 0, subresult: 1 },
    ];
    const call = callsOf(ptb(["split", { ...swap, arguments: args }], inputs))(POOL, "pool");
    expect(call).toEqual({
      function: `${POOL}::pool::swap`,
      typeArguments: [`${LONG("2")}::sui::SUI`],
      arguments: ["GasCoin", { Pure: "AQID" }, { Object: LONG("6") }, { Object: LONG("c01") }, { Object: LONG("ec") }, { Input: 4 }, { Result: 0 }, { NestedResult: [0, 1] }],
    });
  });

  it("matches the one command on the event's package and module, in either address form", () => {
    const other: Command = { package: "0xcd", module: "router", function: "route" };
    const calls = callsOf(ptb([swap, other]));
    expect(calls("0xab", "pool")?.function).toBe(`${POOL}::pool::swap`);
    expect(calls(LONG("cd"), "router")?.function).toBe(`${LONG("cd")}::router::route`);
    expect(calls(POOL, "pool")).toBe(calls("0xab", "pool"));
  });

  it("gives none when several commands or none match", () => {
    const calls = callsOf(ptb([swap, { ...swap, function: "flash" }]));
    expect(calls(POOL, "pool")).toBeUndefined();
    expect(calls(POOL, "other")).toBeUndefined();
    expect(calls(LONG("ff"), "pool")).toBeUndefined();
    expect(calls(undefined, "pool")).toBeUndefined();
  });

  it("gives none for transactions that are not programmable", () => {
    expect(callsOf({ kind: { data: { oneofKind: "consensusCommitPrologue" } } })(POOL, "pool")).toBeUndefined();
    expect(callsOf(undefined)(POOL, "pool")).toBeUndefined();
    expect(callsOf({})(POOL, "pool")).toBeUndefined();
    const broken = { kind: { data: { oneofKind: "programmableTransaction", programmableTransaction: { commands: "x" as never } } } };
    expect(callsOf(broken)(POOL, "pool")).toBeUndefined();
    expect(callsOf(ptb([null as never, swap], "x" as never))(POOL, "pool")?.function).toBe(`${POOL}::pool::swap`);
  });

  it("drops only the malformed command", () => {
    const bad: Command[] = [
      { ...swap, module: "not a module" },
      { ...swap, package: "0xzz", module: "a" },
      { ...swap, module: "b", function: "1x" },
      { ...swap, module: "c", arguments: [{ kind: 2, input: 9 }] },
      { ...swap, module: "d", arguments: [{ kind: 3, result: 99 }] },
      { ...swap, module: "e", arguments: [{ kind: 0 }] },
      { ...swap, module: "f", typeArguments: [1 as unknown as string] },
    ];
    const calls = callsOf(ptb([swap, ...bad]));
    expect(calls(POOL, "pool")?.function).toBe(`${POOL}::pool::swap`);
    expect(calls("0xzz", "a")).toBeUndefined();
    for (const module of ["b", "c", "d", "e", "f"]) expect(calls(POOL, module)).toBeUndefined();
  });
});

describe("SuiAdapter entryFunctions and browser-safe cursors", () => {
  const SWAP = `${POOL}::pool::Swap` as const;
  const desc = { kind: "module" as const, eventType: SWAP };
  const events = [
    { cp: 1n, ti: 0n, ei: 0, type: SWAP, pkg: POOL, module: "pool" },
    { cp: 1n, ti: 1n, ei: 0, type: SWAP, pkg: POOL, module: "pool" },
  ];

  it("skips batchGetTransactions when entryFunctions is false and validates the flag", async () => {
    const { client: base } = fakeLedger([1, 2], events);
    const { client, batches } = withCalls(base, { D1t0: ptb([swap]), D1t1: ptb([swap]) });

    const skipped = await new SuiAdapter(client, { entryFunctions: false }).fetchEvents(desc, { fromVersion: 0n });
    expect(batches).toEqual([]);
    expect(skipped.events.map((e) => e.entryFunction)).toEqual([undefined, undefined]);

    const attached = await new SuiAdapter(client).fetchEvents(desc, { fromVersion: 0n });
    expect(batches).toEqual([["D1t0", "D1t1"]]);
    expect(attached.events.map((e) => e.entryFunction?.function)).toEqual([`${POOL}::pool::swap`, `${POOL}::pool::swap`]);
    expect(() => new SuiAdapter(client, { entryFunctions: "no" as never })).toThrow(StreamdError);
  });

  it("encodes and decodes cursors without Buffer and rejects malformed tokens", async () => {
    const { client } = fakeLedger([1, 2], events);
    const saved = globalThis.Buffer;
    (globalThis as Record<string, unknown>).Buffer = undefined;
    try {
      const sui = new SuiAdapter(client);
      const p1 = await sui.fetchEvents(desc, { fromVersion: 0n, limit: 1 });
      expect(p1.events).toHaveLength(1);
      expect(p1.nextCursor).toBeDefined();
      const p2 = await sui.fetchEvents(desc, { cursor: p1.nextCursor, limit: 5 });
      expect(p2.events).toHaveLength(1);
      expect(p2.events[0]!.version).toBeGreaterThan(p1.events[0]!.version);

      const carried = await sui.fetchEvents(desc, { fromVersion: 2n, limit: 1 });
      expect(carried.nextCursor).toContain(".2");
      const resumed = await sui.fetchEvents(desc, { cursor: carried.nextCursor, limit: 5 });
      expect(resumed.events.map((e) => e.version)).toEqual([2n]);

      for (const bad of ["", "a", "ab=", "$$"]) {
        await expect(sui.fetchEvents(desc, { cursor: bad })).rejects.toThrow(StreamdError);
      }
    } finally {
      (globalThis as Record<string, unknown>).Buffer = saved;
    }
  });
});
