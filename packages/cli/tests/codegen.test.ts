import { describe, expect, it } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeAddress, StreamdError } from "@streamd/core";
import { parseModuleAbi } from "../src/abi.js";
import type { ModuleAbi } from "../src/abi.js";
import { generateEventTypes } from "../src/codegen.js";
import type { ModuleRef } from "../src/codegen.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/abi/", import.meta.url));
const DEX = "0x00000000000000000000000000000000000000000000000000000000000cafe0";
const BEEF = "0x000000000000000000000000000000000000000000000000000000000000beef";

const fixtures = readdirSync(FIXTURES).map((f) =>
  parseModuleAbi(JSON.parse(readFileSync(join(FIXTURES, f), "utf8"))),
);

function loader(abis: ModuleAbi[] = fixtures) {
  const calls: string[] = [];
  const load = async (address: string, module: string) => {
    calls.push(`${address}::${module}`);
    const found = abis.find(
      (a) => normalizeAddress(a.address) === normalizeAddress(address) && a.name === module,
    );
    if (found === undefined) throw new StreamdError(`Module ${address}::${module} not found`);
    return found;
  };
  return { load, calls };
}

const TARGETS: ModuleRef[] = [
  { address: "0x1", module: "coin" },
  { address: "0x1", module: "fungible_asset" },
  { address: DEX, module: "dex" },
];

const body = (source: string) => source.slice(source.indexOf("import"));

function abi(address: string, name: string, structs: unknown[]): ModuleAbi {
  return parseModuleAbi({ abi: { address, name, structs } });
}

const struct = (name: string, fields: [string, string][], isEvent = true) => ({
  name,
  is_event: isEvent,
  is_enum: false,
  generic_type_params: [],
  fields: fields.map(([n, type]) => ({ name: n, type })),
  variants: [],
});

describe("generateEventTypes", () => {
  it("maps fields per the ABI mapping and loads each module once", async () => {
    const { load, calls } = loader();
    const source = await generateEventTypes(TARGETS, load);
    expect(source).toContain('import { defineEvent } from "@streamd/core";');
    expect(source).toContain(
      [
        "export interface Swap<T0 = unknown, T1 = unknown> {",
        "  pool: { inner: string };",
        "  amount_in: string;",
        "  amount_out: string;",
        "  path: string[];",
        "  memo: string;",
        "  fills: number[][];",
        "  fee: Fee;",
        "  snapshot: Snapshot<string>;",
        "  last: { vec: [] | [T1] };",
        "}",
        "export const Swap = <T0 = unknown, T1 = unknown>(t0: string, t1: string) =>",
        `  defineEvent<Swap<T0, T1>>(\`${DEX}::dex::Swap<\${t0}, \${t1}>\`);`,
      ].join("\n"),
    );
    expect(source).toContain(
      [
        "export type OrderEvent =",
        '  | { __variant__: "Placed"; order_id: string; time_in_force: TimeInForce; trigger: { vec: [] | [TriggerCondition] } }',
        '  | { __variant__: "Filled"; order_id: string; pnl: string; delta: number }',
        '  | { __variant__: "Cancelled" };',
        `export const OrderEvent = defineEvent<OrderEvent>("${DEX}::dex::OrderEvent");`,
      ].join("\n"),
    );
    expect(source).toContain("export interface Snapshot<T0 = unknown> {\n  value: T0;\n}");
    expect(source).toContain("  maximum_supply: { vec: [] | [string] };\n");
    expect(source).toContain("  decimals: number;\n");
    expect(source).toContain("  coin_type: TypeInfo;\n");
    // Only what the events reach: the unused struct with a function-typed field is skipped.
    expect(source).not.toContain("Unused");
    expect(calls.sort()).toEqual(
      ["0x1::coin", "0x1::fungible_asset", "0x1::type_info", "0x5::order_book_types",
        `${DEX}::dex`, `${BEEF}::math`].sort(),
    );
  });

  it("qualifies names two modules share and leaves unique ones alone", async () => {
    const source = await generateEventTypes(TARGETS, loader().load);
    expect(source).toContain('export const FungibleAsset_Deposit = defineEvent<FungibleAsset_Deposit>("0x1::fungible_asset::Deposit");');
    expect(source).toContain(`export const Dex_Deposit = defineEvent<Dex_Deposit>("${DEX}::dex::Deposit");`);
    expect(source).toContain("export const Coin_Deposit = <T0 = unknown>(t0: string) =>\n  defineEvent<Coin_Deposit<T0>>(`0x1::coin::Deposit<${t0}>`);");
    expect(source).toContain('export const CoinDeposit = defineEvent<CoinDeposit>("0x1::coin::CoinDeposit");');
    expect(source).toContain('export const Frozen = defineEvent<Frozen>("0x1::fungible_asset::Frozen");');
  });

  it("adds the address when the module name collides too, and the target order does not matter", async () => {
    const abis = [
      abi(DEX, "dex", [struct("Deposit", [["a", "u8"]])]),
      abi(BEEF, "dex", [struct("Deposit", [["b", "u8"]]), struct("T0", [["c", "u8"]]), struct("lower", [["d", "u8"]])]),
    ];
    const forward = await generateEventTypes(
      [{ address: DEX, module: "dex" }, { address: BEEF, module: "dex" }],
      loader(abis).load,
    );
    const reversed = await generateEventTypes(
      [{ address: BEEF, module: "dex" }, { address: DEX, module: "dex" }],
      loader(abis).load,
    );
    expect(body(forward)).toBe(body(reversed));
    expect(forward).toContain(`export interface Dex_Deposit_${DEX} {\n  a: number;\n}`);
    expect(forward).toContain(`export interface Dex_Deposit_${BEEF} {\n  b: number;\n}`);
    expect(forward).toContain("export interface Dex_T0 {");
    expect(forward).toContain("export interface Dex_lower {");
  });

  it("fails naming what is missing or unsupported", async () => {
    const run = (structs: unknown[]) =>
      generateEventTypes([{ address: DEX, module: "dex" }], loader([abi(DEX, "dex", structs)]).load);
    await expect(run([struct("Plain", [["a", "u8"]], false)])).rejects.toThrow(
      `Module ${DEX}::dex declares no event structs`,
    );
    await expect(run([struct("E", [["a", `${DEX}::dex::Gone`]])])).rejects.toThrow(
      `Struct Gone not found in ${DEX}::dex, needed by field a of ${DEX}::dex::E`,
    );
    await expect(run([struct("E", [["a", "0x7::other::S"]])])).rejects.toThrow(
      "Module 0x7::other not found",
    );
    await expect(run([struct("E", [["f", "|u64|u64 has copy"]])])).rejects.toThrow(
      `Unsupported Move type in field f of ${DEX}::dex::E: |u64|u64 has copy`,
    );
    await expect(run([struct("E", [["a", "T0"]])])).rejects.toThrow("names T0");
    await expect(run([struct("E", [["a", `${DEX}::dex::E<u8>`]])])).rejects.toThrow(
      "wrong number of type arguments",
    );
  });

  it("refuses an ABI for a different module than the one asked for", async () => {
    const load = async () => abi(DEX, "other", [struct("E", [["a", "u8"]])]);
    await expect(generateEventTypes([{ address: DEX, module: "dex" }], load)).rejects.toThrow(
      `Fullnode returned ${DEX}::other for ${DEX}::dex`,
    );
  });

  it("emits a file that typechecks and types event.data through the client", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-codegen-"));
    try {
      writeFileSync(join(dir, "events.ts"), await generateEventTypes(TARGETS, loader().load));
      writeFileSync(join(dir, "usage.ts"), USAGE);
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022",
            module: "ESNext",
            moduleResolution: "bundler",
            strict: true,
            noUnusedLocals: true,
            noUnusedParameters: true,
            noEmit: true,
            skipLibCheck: true,
            types: [],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["events.ts", "usage.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
      expect(result.exitCode).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});

// Each @ts-expect-error fails the compile if its line does typecheck.
const USAGE = `
import type { StreamdClient } from "@streamd/core";
import { Coin_Deposit, Creation, FungibleAsset_Deposit, FungibleAsset_DepositDecode, OrderEvent, PairCreation, Swap } from "./events.js";

declare const client: StreamdClient;
const is = <T>(_value: T) => {};

client.on(FungibleAsset_Deposit, (e) => {
  is<string>(e.data.store);
  // @ts-expect-error u64 is a decimal string
  is<number>(e.data.amount);
});
client.on(FungibleAsset_Deposit, () => {}, { decode: FungibleAsset_DepositDecode });
client.on(OrderEvent, (e) => {
  if (e.data.__variant__ === "Filled") {
    is<number>(e.data.delta);
    is<string>(e.data.pnl);
  }
  if (e.data.__variant__ === "Placed" && e.data.time_in_force.__variant__ === "IOC") {
    is<[] | [{ __variant__: "PriceMoveAbove" | "PriceMoveBelow" | "TimeBased"; _0: string }]>(e.data.trigger.vec);
  }
  // @ts-expect-error order_id is not on every variant
  is(e.data.order_id);
});
client.on(Swap<{ coin: true }>("0x1::aptos_coin::AptosCoin", "0x1::aptos_coin::AptosCoin"), (e) => {
  is<number>(e.data.fee.bps);
  is<string>(e.data.snapshot.value);
  is<string>(e.data.pool.inner);
  is<string[]>(e.data.path);
  is<string>(e.data.memo);
  is<number[][]>(e.data.fills);
  is<[] | [unknown]>(e.data.last.vec);
});
client.on(Coin_Deposit("0x1::aptos_coin::AptosCoin"), (e) => is<string>(e.data.account));
client.on(PairCreation, (e) => is<string>(e.data.coin_type.module_name));
client.on(Creation, (e) => {
  is<[] | [string]>(e.data.maximum_supply.vec);
  is<number>(e.data.decimals);
  // @ts-expect-error not a field
  is(e.data.nope);
});
`;

describe("generateEventTypes decode lists", () => {
  const generate = (...structs: unknown[]) =>
    generateEventTypes([{ address: "0x1", module: "m" }], loader([abi("0x1", "m", structs)]).load);

  it("lists an event's own unsigned wide integers, in field order", async () => {
    const source = await generate(
      struct("Trade", [["size", "u64"], ["who", "address"], ["fee", "u128"], ["big", "u256"]]),
    );
    expect(source).toContain('export const TradeDecode = ["size", "fee", "big"] as const;');
  });

  it("leaves out everything decode cannot read", async () => {
    const source = await generate(
      struct("Inner", [["amount", "u64"]], false),
      struct("Trade", [
        ["narrow", "u8"],
        ["word", "u32"],
        ["signed", "i64"],
        ["huge", "i256"],
        ["flag", "bool"],
        ["list", "vector<u64>"],
        ["maybe", "0x1::option::Option<u64>"],
        ["nested", "0x1::m::Inner"],
        ["kept", "u64"],
      ]),
    );
    expect(source).toContain('export const TradeDecode = ["kept"] as const;');
    expect(source).not.toContain("InnerDecode");
  });

  it("writes no list when nothing qualifies, or for an enum", async () => {
    const source = await generate(struct("Quiet", [["who", "address"]]), {
      name: "Kind",
      is_event: true,
      is_enum: true,
      generic_type_params: [],
      variants: [{ name: "A", fields: [{ name: "n", type: "u64" }] }],
      fields: [],
    });
    expect(source).not.toContain("Decode");
  });

  it("writes a plain list for a generic event, leaving out its type parameters", async () => {
    const source = await generate({
      ...struct("Pair", [["amount", "u64"], ["value", "T0"]]),
      generic_type_params: [{ constraints: [] }],
    });
    expect(source).toContain('export const PairDecode = ["amount"] as const;');
  });

  it("steps around a struct that already has the name", async () => {
    const source = await generate(struct("Trade", [["n", "u64"]]), struct("TradeDecode", [["who", "address"]]));
    expect(source).toContain('export const TradeDecode_ = ["n"] as const;');
    expect(source).toContain('export const TradeDecode = defineEvent<TradeDecode>(');
  });
});

describe("generateEventTypes view builders", () => {
  const fn = (name: string, params: string[], generics = 0, isView = true) => ({
    name,
    visibility: "public",
    is_entry: false,
    is_view: isView,
    generic_type_params: Array.from({ length: generics }, () => ({ constraints: [] })),
    params,
    return: [],
  });
  const module = (address: string, name: string, functions: unknown[], structs: unknown[] = []) =>
    parseModuleAbi({ abi: { address, name, structs, exposed_functions: functions } });
  const generate = (...abis: ModuleAbi[]) =>
    generateEventTypes(abis.map((a) => ({ address: a.address, module: a.name })), loader(abis).load);

  it("writes a builder taking type arguments, then arguments", async () => {
    const source = await generate(module("0x1", "coin", [fn("balance", ["address"], 1)]));
    expect(source).toContain('import type { ViewRequest } from "@streamd/core";');
    expect(source).not.toContain("defineEvent");
    expect(source).toContain(`/** \`0x1::coin::balance\`, a view */
export const balanceView = (t0: string, arg0: string): ViewRequest => ({
  function: "0x1::coin::balance",
  typeArguments: [t0],
  functionArguments: [arg0],
});`);
  });

  it("maps every argument kind to what the JSON view API takes", async () => {
    const source = await generate(
      module("0x1", "m", [
        fn("many", [
          "bool",
          "u8",
          "u32",
          "u64",
          "u256",
          "address",
          "vector<u8>",
          "vector<u64>",
          "vector<address>",
          "0x1::string::String",
          "0x1::object::Object<0x1::m::S>",
          "0x1::option::Option<u64>",
          "T0",
        ], 1),
      ]),
    );
    expect(source).toContain(
      "(t0: string, arg0: boolean, arg1: number, arg2: number, arg3: string | bigint, arg4: string | bigint, " +
        "arg5: string, arg6: string, arg7: (string | bigint)[], arg8: string[], arg9: string, arg10: string, " +
        "arg11: { vec: [] | [string | bigint] }, arg12: unknown)",
    );
  });

  it("ignores functions that are not views, and takes a snapshot with none", async () => {
    const source = await generate(
      module("0x1", "m", [fn("seen", []), fn("hidden", [], 0, false)], [struct("E", [["a", "u64"]])]),
    );
    expect(source).toContain("seenView");
    expect(source).not.toContain("hidden");
  });

  it("comments out a view JSON cannot call", async () => {
    const source = await generate(
      module("0x1", "m", [
        fn("signed", ["&signer"]),
        fn("owned", ["signer"]),
        fn("custom", ["0x1::m::Thing"]),
        fn("nested", ["vector<0x1::m::Thing>"]),
        fn("fine", []),
      ]),
    );
    for (const name of ["signed", "owned", "custom", "nested"]) {
      expect(source).toContain(`// \`0x1::m::${name}\` takes an argument`);
      expect(source).not.toContain(`${name}View`);
    }
    expect(source).toContain("fineView");
  });

  it("qualifies a function name two modules share, and camel-cases", async () => {
    const source = await generate(
      module("0x1", "one_two", [fn("get_value", [])]),
      module("0x2", "other", [fn("get_value", []), fn("only_here", [])]),
    );
    expect(source).toContain("export const oneTwo_getValueView");
    expect(source).toContain("export const other_getValueView");
    expect(source).toContain("export const onlyHereView");
  });

  it("separates the same module name at two addresses", async () => {
    const source = await generate(module("0x1", "m", [fn("f", [])]), module("0x2", "m", [fn("f", [])]));
    expect(source).toContain("export const m_fView_0x1");
    expect(source).toContain("export const m_fView_0x2");
  });

  it("leaves event-only output without a view import, and fails only with neither", async () => {
    const events = await generate(module("0x1", "m", [], [struct("E", [["a", "u64"]])]));
    expect(events).not.toContain("ViewRequest");
    await expect(generate(module("0x1", "m", [], [struct("S", [["a", "u64"]], false)]))).rejects.toThrow(
      "declares no event structs, event handles or view functions",
    );
  });

  it("keeps order stable whatever order the targets come in", async () => {
    const a = module("0x1", "a", [fn("x", []), fn("y", [])]);
    const b = module("0x1", "b", [fn("z", [])]);
    const first = await generateEventTypes([{ address: "0x1", module: "a" }, { address: "0x1", module: "b" }], loader([a, b]).load);
    const second = await generateEventTypes([{ address: "0x1", module: "b" }, { address: "0x1", module: "a" }], loader([a, b]).load);
    expect(body(first)).toBe(body(second));
  });
});

describe("generateEventTypes whole accounts", () => {
  const helper = abi(DEX, "helper", [struct("Config", [["fee", "u64"]], false)]);
  const pool = parseModuleAbi({
    abi: {
      address: DEX,
      name: "pool",
      structs: [struct("Swapped", [["amount", "u64"]])],
      exposed_functions: [
        { name: "reserves", visibility: "public", is_entry: false, is_view: true, generic_type_params: [], params: [], return: [] },
      ],
    },
  });
  const lister = (abis: ModuleAbi[]) => {
    const calls: string[] = [];
    const list = async (address: string) => {
      calls.push(address);
      return abis.filter((a) => normalizeAddress(a.address) === normalizeAddress(address));
    };
    return { list, calls };
  };

  it("expands an account to its modules, skipping those with nothing, without loading them", async () => {
    const { load, calls } = loader([]);
    const { list, calls: listed } = lister([helper, pool]);
    const source = await generateEventTypes([{ address: DEX }], load, { list });
    expect(listed).toEqual([DEX]);
    expect(calls).toEqual([]);
    expect(source).toContain("export interface Swapped");
    expect(source).not.toContain("Config");
    expect(source).toContain(`ABIs of ${DEX}.`);
  });

  it("generates a module both named and covered by its account once", async () => {
    const { load, calls } = loader([helper, pool]);
    const { list } = lister([helper, pool]);
    const once = await generateEventTypes([{ address: DEX }], load, { list });
    const both = await generateEventTypes([{ address: DEX }, { address: DEX, module: "pool" }], load, { list });
    expect(calls).toEqual([]);
    expect(once.match(/export const reservesView /g)).toHaveLength(1);
    expect(body(both)).toBe(body(once));
    const named = await generateEventTypes([{ address: DEX, module: "pool" }, { address: DEX }], load, { list });
    expect(body(named)).toBe(body(once));
  });

  it("still fails on a named module with nothing, and on an account yielding nothing", async () => {
    const { load } = loader([helper, pool]);
    const { list } = lister([helper, pool]);
    await expect(generateEventTypes([{ address: DEX }, { address: DEX, module: "helper" }], load, { list })).rejects.toThrow(
      `Module ${DEX}::helper declares no event structs, event handles or view functions`,
    );
    await expect(generateEventTypes([{ address: DEX }], load, { list: lister([helper]).list })).rejects.toThrow(
      `Account ${DEX} has no modules with event structs, event handles or view functions`,
    );
    await expect(generateEventTypes([{ address: BEEF }], load, { list })).rejects.toThrow(`Account ${BEEF} has no modules`);
  });

  it("refuses a listing naming another account, and an account target with no lister", async () => {
    const { load } = loader();
    await expect(generateEventTypes([{ address: BEEF }], load, { list: async () => [pool] })).rejects.toThrow(
      `Fullnode listed ${DEX}::pool for account ${BEEF}`,
    );
    await expect(generateEventTypes([{ address: DEX }], load)).rejects.toThrow("No module listing");
  });

  it("follows references out of a listed module through load", async () => {
    const outer = abi(DEX, "outer", [struct("Wrapped", [["inner", `${BEEF}::lib::Inner`]])]);
    const lib = abi(BEEF, "lib", [struct("Inner", [["x", "u8"]], false)]);
    const { load, calls } = loader([lib]);
    const source = await generateEventTypes([{ address: DEX }], load, { list: lister([outer]).list });
    expect(calls).toEqual([`${BEEF}::lib`]);
    expect(source).toContain("export interface Inner");
  });
});

describe("generateEventTypes handle builders", () => {
  const resource = (name: string, fields: [string, string][], generics = 0, abilities = ["key"]) => ({
    ...struct(name, fields, false),
    abilities,
    generic_type_params: Array.from({ length: generics }, () => ({ constraints: [] })),
  });
  const store = abi(DEX, "vault", [
    resource("Vault", [["deposit_events", `0x1::event::EventHandle<${DEX}::vault::Moved>`], ["total", "u64"]]),
    resource("Shelf", [["moves", `0x1::event::EventHandle<${DEX}::vault::Moved>`]], 0, ["store"]),
    struct("Moved", [["amount", "u64"]], false),
  ]);
  const generate = (...abis: ModuleAbi[]) =>
    generateEventTypes(abis.map((a) => ({ address: a.address, module: a.name })), loader(abis).load);

  it("writes a typed handle builder per EventHandle field of a resource, and the payload's type", async () => {
    const source = await generate(store);
    expect(source).toContain('import type { HandleEventDescriptor } from "@streamd/core";');
    expect(source).toContain("export interface Moved {\n  amount: string;\n}");
    expect(source).toContain(
      [
        `/** \`deposit_events\` on \`${DEX}::vault::Vault\`, a legacy event handle */`,
        "export const Vault_DepositEvents = (accountAddress: string): HandleEventDescriptor<Moved> => ({",
        '  kind: "handle",',
        "  accountAddress,",
        `  eventHandleStruct: "${DEX}::vault::Vault",`,
        '  fieldName: "deposit_events",',
        "});",
      ].join("\n"),
    );
    expect(source).not.toContain("Shelf");
    expect(source).not.toContain("defineEvent");
  });

  it("takes a generic resource's type arguments, and type parameters only when the payload uses them", async () => {
    const generic = abi("0x1", "coin", [
      resource("CoinStore", [
        ["deposit_events", "0x1::coin::DepositEvent"],
        ["withdraw_events", "0x1::event::EventHandle<0x1::coin::WithdrawEvent>"],
        ["typed_events", "0x1::event::EventHandle<0x1::coin::Typed<T0>>"],
      ], 1),
      struct("WithdrawEvent", [["amount", "u64"]], false),
      { ...struct("Typed", [["value", "T0"]], false), generic_type_params: [{ constraints: [] }] },
    ]);
    const source = await generate(generic);
    expect(source).toContain(
      "export const CoinStore_WithdrawEvents = (accountAddress: string, t0: string): HandleEventDescriptor<WithdrawEvent> => ({",
    );
    expect(source).toContain("  eventHandleStruct: `0x1::coin::CoinStore<${t0}>`,");
    expect(source).toContain(
      "export const CoinStore_TypedEvents = <T0 = unknown>(accountAddress: string, t0: string): HandleEventDescriptor<Typed<T0>> => ({",
    );
    expect(source).not.toContain("CoinStore_DepositEvents");
  });

  it("qualifies a builder name two modules share, and steps around a generated type", async () => {
    const other = abi(BEEF, "vault", [
      resource("Vault", [["deposit_events", "0x1::event::EventHandle<u64>"]]),
    ]);
    const shared = await generate(store, other);
    expect(shared).toContain(`export const Vault_Vault_DepositEvents_${BEEF} = `);
    expect(shared).toContain(`export const Vault_Vault_DepositEvents_${DEX} = `);
    const clash = abi(DEX, "m", [
      resource("A", [["b", `0x1::event::EventHandle<${DEX}::m::A_B>`]]),
      struct("A_B", [["x", "u8"]], false),
    ]);
    expect(await generate(clash)).toContain("export const M_A_B = ");
  });

  it("counts handles as something to generate, and leaves them out on request", async () => {
    const only = abi(DEX, "only", [resource("R", [["e", "0x1::event::EventHandle<u64>"]])]);
    expect(await generate(only)).toContain("HandleEventDescriptor<string>");
    const lookalike = abi(DEX, "look", [
      resource("R", [["e", "0x1::other::EventHandle<u64>"]]),
      struct("E", [["x", "u8"]]),
    ]);
    expect(await generate(lookalike)).not.toContain("HandleEventDescriptor");
    await expect(
      generateEventTypes([{ address: DEX, module: "only" }], loader([only]).load, { handles: false }),
    ).rejects.toThrow("declares no event structs, event handles or view functions");
  });

  it("emits handle builders that typecheck and type event.data through the client", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-handles-"));
    try {
      writeFileSync(join(dir, "vault.ts"), await generate(store));
      writeFileSync(
        join(dir, "use.ts"),
        [
          'import type { StreamdClient } from "@streamd/core";',
          'import { Vault_DepositEvents } from "./vault.js";',
          "declare const client: StreamdClient;",
          "const is = <T>(_value: T) => {};",
          'client.on(Vault_DepositEvents("0x1"), (e) => is<string>(e.data.amount));',
          "// @ts-expect-error amount is a decimal string",
          'client.on(Vault_DepositEvents("0x1"), (e) => is<number>(e.data.amount));',
        ].join("\n"),
      );
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022", module: "ESNext", moduleResolution: "bundler", strict: true,
            noUnusedLocals: true, noUnusedParameters: true, noEmit: true, skipLibCheck: true, types: [],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["vault.ts", "use.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
      expect(result.exitCode).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});

describe("generateEventTypes validators", () => {
  const generic = { ...struct("Boxed", [["item", "T0"], ["count", "u64"]], false), generic_type_params: [{ constraints: [] }] };
  const shapes = abi(DEX, "shapes", [
    struct("Everything", [
      ["small", "u8"], ["signed", "i32"], ["big", "u128"], ["neg", "i64"], ["flag", "bool"], ["who", "address"],
      ["bytes", "vector<u8>"], ["name", "0x1::string::String"], ["maybe", "0x1::option::Option<u64>"],
      ["obj", "0x1::object::Object<0x1::object::ObjectCore>"], ["list", "vector<vector<u16>>"],
      ["inner", `${DEX}::shapes::Inner`], ["boxed", `${DEX}::shapes::Boxed<u64>`], ["pick", `${DEX}::shapes::Pick`],
    ]),
    struct("Inner", [["x", "u64"]], false),
    generic,
    {
      name: "Pick", is_event: false, is_enum: true, generic_type_params: [], fields: [],
      variants: [{ name: "None", fields: [] }, { name: "Some", fields: [{ name: "amount", type: "u64" }] }],
    },
  ]);
  const good = {
    small: 1, signed: -2, big: "340282366920938463463374607431768211455", neg: "-9", flag: true, who: "0x1",
    bytes: "0xabcd", name: "n", maybe: { vec: ["5"] }, obj: { inner: "0xa" }, list: [[1, 2], []],
    inner: { x: "3" }, boxed: { item: { anything: true }, count: "4" }, pick: { __variant__: "Some", amount: "6" },
  };

  async function load(validators = true) {
    // Inside the package, so the generated import of @streamd/core resolves at runtime.
    const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".validators-"));
    const file = join(dir, "shapes.ts");
    writeFileSync(file, await generateEventTypes([{ address: DEX, module: "shapes" }], loader([shapes]).load, { validators }));
    return { dir, file, source: readFileSync(file, "utf8"), mod: (await import(file)) as Record<string, (v: unknown) => boolean> };
  }

  it("accepts the fullnode's JSON for every mapping, decoded bigints and extra fields", async () => {
    const { dir, mod } = await load();
    try {
      expect(mod.isEverything!(good)).toBe(true);
      expect(mod.isEverything!({ ...good, big: 5n, neg: -5n, added: "later field" })).toBe(true);
      expect(mod.isEverything!({ ...good, maybe: { vec: [] }, pick: { __variant__: "None" }, bytes: "0x" })).toBe(true);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses each wrong shape", async () => {
    const { dir, mod } = await load();
    try {
      const bad: Record<string, unknown>[] = [
        { small: 1.5 }, { small: "1" }, { big: 7 }, { big: "-1" }, { big: -1n }, { neg: "x" }, { flag: "true" },
        { who: "1" }, { who: "0xZZ" }, { bytes: "0xabc" }, { name: 3 }, { maybe: { vec: ["1", "2"] } },
        { maybe: { vec: [1] } }, { obj: "0xa" }, { obj: { inner: "nope" } }, { list: [[1.5]] }, { list: [1] },
        { inner: { x: 3 } }, { boxed: { item: 1, count: 4 } }, { pick: { __variant__: "Other" } },
        { pick: { __variant__: "Some" } },
      ];
      for (const change of bad) expect(mod.isEverything!({ ...good, ...change })).toBe(false);
      const { flag: _flag, ...missing } = good;
      expect(mod.isEverything!(missing)).toBe(false);
      for (const v of [null, undefined, [], "x", 1]) expect(mod.isEverything!(v)).toBe(false);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("names around a clash, writes nothing without the option, and typechecks narrowing unknown", async () => {
    const plain = await load(false);
    expect(plain.source).not.toContain("streamdIs");
    rmSync(plain.dir, { recursive: true, force: true });
    // The guard for an event FooView would be isFooView, which the builder for view is_foo already is.
    const clash = parseModuleAbi({
      abi: {
        address: DEX,
        name: "clash",
        structs: [struct("FooView", [["a", "u8"]])],
        exposed_functions: [
          { name: "is_foo", visibility: "public", is_entry: false, is_view: true, generic_type_params: [], params: [], return: [] },
        ],
      },
    });
    const clashed = await generateEventTypes([{ address: DEX, module: "clash" }], loader([clash]).load, { validators: true });
    expect(clashed).toContain("export const isFooView = ");
    expect(clashed).toContain("export function isFooView_(value: unknown): value is FooView");
    const { dir, source } = await load();
    try {
      expect(source).not.toContain("&& true");
      writeFileSync(
        join(dir, "use.ts"),
        [
          'import { isEverything, type Everything } from "./shapes.js";',
          "declare const payload: unknown;",
          "if (isEverything(payload)) { const e: Everything = payload; void e; }",
        ].join("\n"),
      );
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022", module: "ESNext", moduleResolution: "bundler", strict: true, noUnusedLocals: true,
            noUnusedParameters: true, noPropertyAccessFromIndexSignature: true, noEmit: true, skipLibCheck: true, types: [],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["shapes.ts", "use.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});
