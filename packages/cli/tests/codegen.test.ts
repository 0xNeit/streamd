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
import { Coin_Deposit, Creation, FungibleAsset_Deposit, OrderEvent, PairCreation, Swap } from "./events.js";

declare const client: StreamdClient;
const is = <T>(_value: T) => {};

client.on(FungibleAsset_Deposit, (e) => {
  is<string>(e.data.store);
  // @ts-expect-error u64 is a decimal string
  is<number>(e.data.amount);
});
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
