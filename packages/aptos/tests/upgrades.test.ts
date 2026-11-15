import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import type { MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { diffModules } from "../src/upgrades.js";

const A = `0x${"ab".repeat(32)}`;

type Fn = { name: string; visibility?: string; is_entry?: boolean; is_view?: boolean; params?: string[]; return?: string[] };
type St = { name: string; is_event?: boolean; fields?: { name: string; type: string }[]; variants?: unknown[] };

const fn = (f: Fn) => ({
  visibility: "public",
  is_entry: false,
  is_view: false,
  generic_type_params: [],
  params: [],
  return: [],
  ...f,
});
const st = (s: St) => ({
  is_native: false,
  is_event: false,
  is_enum: false,
  abilities: ["drop", "store"],
  generic_type_params: [],
  fields: [],
  ...s,
});
const mod = (name: string, functions: Fn[], structs: St[] = [], address = "0xabababababababababababababababababababababababababababababababab") =>
  ({
    bytecode: "0x",
    abi: { address, name, friends: [], exposed_functions: functions.map(fn), structs: structs.map(st) },
  }) as unknown as MoveModuleBytecode;

const none = { added: [], changed: [], removed: [] };

describe("diffModules", () => {
  it("reports added, changed and removed signatures in each category", () => {
    const before = [
      mod(
        "pool",
        [
          { name: "swap", is_entry: true, params: ["&signer", "u64"] },
          { name: "close", is_entry: true },
          { name: "price", is_view: true, return: ["u64"] },
          { name: "old_view", is_view: true },
        ],
        [
          { name: "Swapped", is_event: true, fields: [{ name: "amount", type: "u64" }] },
          { name: "Closed", is_event: true },
        ],
      ),
    ];
    const after = [
      mod(
        "pool",
        [
          { name: "swap", is_entry: true, params: ["&signer", "u64", "u64"] },
          { name: "deposit", is_entry: true, visibility: "private" },
          { name: "price", is_view: true, return: ["u128"] },
          { name: "reserves", is_view: true, return: ["u64", "u64"] },
        ],
        [
          { name: "Swapped", is_event: true, fields: [{ name: "amount", type: "u64" }, { name: "fee", type: "u64" }] },
          { name: "Deposited", is_event: true },
        ],
      ),
    ];
    const diff = diffModules(before, after);
    const p = `${A}::pool::`;
    expect(diff.entryFunctions.added.map((f) => [f.name, f.visibility])).toEqual([[`${p}deposit`, "private"]]);
    expect(diff.entryFunctions.removed.map((f) => f.name)).toEqual([`${p}close`]);
    expect(diff.entryFunctions.changed).toEqual([
      {
        before: { name: `${p}swap`, visibility: "public", typeParameters: [], params: ["&signer", "u64"], returns: [] },
        after: { name: `${p}swap`, visibility: "public", typeParameters: [], params: ["&signer", "u64", "u64"], returns: [] },
      },
    ]);
    expect(diff.views.added.map((f) => f.name)).toEqual([`${p}reserves`]);
    expect(diff.views.removed.map((f) => f.name)).toEqual([`${p}old_view`]);
    expect(diff.views.changed.map((c) => [c.before.returns, c.after.returns])).toEqual([[["u64"], ["u128"]]]);
    expect(diff.events.added.map((e) => e.name)).toEqual([`${p}Deposited`]);
    expect(diff.events.removed.map((e) => e.name)).toEqual([`${p}Closed`]);
    expect(diff.events.changed).toHaveLength(1);
    expect(diff.events.changed[0]!.after).toEqual({
      name: `${p}Swapped`,
      abilities: ["drop", "store"],
      typeParameters: [],
      fields: [{ name: "amount", type: "u64" }, { name: "fee", type: "u64" }],
    });
  });

  it("counts a struct gaining #[event] as an added event and a function becoming a view as an added view", () => {
    const before = [mod("m", [{ name: "get" }], [{ name: "E" }])];
    const after = [mod("m", [{ name: "get", is_view: true }], [{ name: "E", is_event: true }])];
    const diff = diffModules(before, after);
    expect(diff.events.added.map((e) => e.name)).toEqual([`${A}::m::E`]);
    expect(diff.views.added.map((f) => f.name)).toEqual([`${A}::m::get`]);
    expect(diffModules(after, before).events.removed.map((e) => e.name)).toEqual([`${A}::m::E`]);
  });

  it("reports nothing for unchanged modules, ignoring public functions and plain structs", () => {
    const modules = [mod("m", [{ name: "f", is_entry: true }, { name: "helper" }], [{ name: "Plain" }, { name: "E", is_event: true }])];
    const changedHelper = [mod("m", [{ name: "f", is_entry: true }, { name: "helper", params: ["u8"] }], [{ name: "Plain", fields: [{ name: "x", type: "u8" }] }, { name: "E", is_event: true }])];
    expect(diffModules(modules, modules)).toEqual({ events: none, entryFunctions: none, views: none });
    expect(diffModules(modules, changedHelper)).toEqual({ events: none, entryFunctions: none, views: none });
  });

  it("reports a whole new module as added, keyed in long form and sorted", () => {
    const after = [
      mod("zeta", [{ name: "b", is_entry: true }, { name: "a", is_entry: true }], [], "0xAB"),
      mod("alpha", [{ name: "c", is_entry: true }], [{ name: "Ev", is_event: true, variants: [{ name: "V1", fields: [{ name: "x", type: "u8" }] }] }], "0xab"),
    ];
    const diff = diffModules([], after);
    const short = `0x${"0".repeat(62)}ab`;
    expect(diff.entryFunctions.added.map((f) => f.name)).toEqual([`${short}::alpha::c`, `${short}::zeta::a`, `${short}::zeta::b`]);
    expect(diff.events.added).toEqual([
      { name: `${short}::alpha::Ev`, abilities: ["drop", "store"], typeParameters: [], fields: [], variants: [{ name: "V1", fields: [{ name: "x", type: "u8" }] }] },
    ]);
  });

  it("keeps type parameter constraints and phantoms", () => {
    const generic = (phantom: boolean) =>
      ({
        bytecode: "0x",
        abi: {
          address: A,
          name: "g",
          friends: [],
          exposed_functions: [{ ...fn({ name: "go", is_entry: true }), generic_type_params: [{ constraints: ["store"] }] }],
          structs: [{ ...st({ name: "E", is_event: true }), generic_type_params: [{ constraints: [], is_phantom: phantom }] }],
        },
      }) as unknown as MoveModuleBytecode;
    const diff = diffModules([generic(false)], [generic(true)]);
    expect(diff.events.changed.map((c) => [c.before.typeParameters, c.after.typeParameters])).toEqual([
      [[{ constraints: [], isPhantom: false }], [{ constraints: [], isPhantom: true }]],
    ]);
    expect(diffModules([], [generic(false)]).entryFunctions.added[0]!.typeParameters).toEqual([{ constraints: ["store"] }]);
  });

  it("refuses a module without an ABI or with a malformed one", () => {
    const noAbi = { bytecode: "0x" } as MoveModuleBytecode;
    expect(() => diffModules([], [noAbi])).toThrow(StreamdError);
    const broken = mod("m", [{ name: "f", is_entry: true }]);
    (broken.abi!.exposed_functions[0] as unknown as Record<string, unknown>).params = "u64";
    expect(() => diffModules([broken], [])).toThrow("Fullnode returned module m without a readable ABI");
    const badAddress = mod("m", [], [], "not-an-address");
    expect(() => diffModules([], [badAddress])).toThrow(StreamdError);
    const badEvent = mod("m", [], [{ name: "E", is_event: true, fields: [{ name: "x" } as never] }]);
    expect(() => diffModules([], [badEvent])).toThrow(StreamdError);
  });
});
