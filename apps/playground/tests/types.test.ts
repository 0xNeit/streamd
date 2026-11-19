import { describe, expect, it } from "bun:test";
import type { Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { generateFor, moduleLoader, moduleOf } from "../src/types.js";

const struct = (name: string, isEvent: boolean, fields: { name: string; type: string }[], params = 0) => ({
  name,
  is_event: isEvent,
  abilities: ["drop", "store"],
  generic_type_params: Array.from({ length: params }, () => ({ constraints: [] })),
  fields,
});

const MODULES: Record<string, unknown> = {
  "0xcafe::pool": {
    abi: {
      address: "0xcafe",
      name: "pool",
      structs: [
        struct("Swap", true, [{ name: "amount", type: "u64" }, { name: "pair", type: "0xcafe::pair::Pair" }]),
        struct("Moved", true, [{ name: "value", type: "T0" }], 1),
      ],
    },
  },
  "0xcafe::pair": { abi: { address: "0xcafe", name: "pair", structs: [struct("Pair", false, [{ name: "left", type: "address" }])] } },
};

function fakeAptos(fail?: unknown) {
  const calls: string[] = [];
  const aptos = {
    getAccountModule: async ({ accountAddress, moduleName }: { accountAddress: string; moduleName: string }) => {
      calls.push(`${accountAddress}::${moduleName}`);
      if (fail !== undefined) throw fail;
      const found = MODULES[`${accountAddress}::${moduleName}`];
      if (found === undefined) throw Object.assign(new Error("Module not found"), { status: 404 });
      return found;
    },
  } as unknown as Aptos;
  return { aptos, calls };
}

describe("moduleOf", () => {
  it("names the declaring module, ignoring type arguments", () => {
    expect(moduleOf("0x1::fungible_asset::Deposit")).toEqual({ address: "0x1", module: "fungible_asset" });
    expect(moduleOf("0xcafe::pool::Moved<0x1::aptos_coin::AptosCoin>")).toEqual({ address: "0xcafe", module: "pool" });
  });
});

describe("generateFor", () => {
  it("generates the type's module and the structs it references, as codegen does", async () => {
    const { aptos, calls } = fakeAptos();
    const source = await generateFor(aptos, "0xcafe::pool::Swap");
    expect(source).toContain('export const Swap = defineEvent<Swap>("0xcafe::pool::Swap");');
    expect(source).toContain("export interface Pair {");
    expect(source).toContain("export const Moved = <T0 = unknown>(t0: string) =>");
    expect(calls).toEqual(["0xcafe::pool", "0xcafe::pair"]);
  });

  it("names a missing module", async () => {
    const { aptos } = fakeAptos();
    const err = await generateFor(aptos, "0xcafe::gone::Swap").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toBe("Module 0xcafe::gone not found");
  });

  it("wraps any other failure with the module and keeps its status", async () => {
    const cause = Object.assign(new Error("Too Many Requests"), { status: 429 });
    const err = await moduleLoader(fakeAptos(cause).aptos)("0xcafe", "pool").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toBe("Fullnode request for module 0xcafe::pool failed: Too Many Requests");
    expect(err.statusCode).toBe(429);
  });

  it("refuses a response with no ABI", async () => {
    const aptos = { getAccountModule: async () => ({ bytecode: "0x00" }) } as unknown as Aptos;
    await expect(moduleLoader(aptos)("0xcafe", "pool")).rejects.toThrow(StreamdError);
  });
});
