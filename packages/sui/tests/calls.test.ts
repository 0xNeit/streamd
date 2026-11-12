import { describe, expect, it } from "bun:test";
import { callsOf } from "../src/calls.js";
import { LONG, ptb, type Command } from "./helpers.js";

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
    for (const module of ["a", "b", "c", "d", "e", "f"]) expect(calls(POOL, module)).toBeUndefined();
  });
});
