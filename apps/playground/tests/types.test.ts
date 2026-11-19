import { afterEach, describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { clientFor, describeError } from "../src/client.js";
import type { PlaygroundNetwork } from "../src/client.js";
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

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

/** An anonymous client whose fullnode answers from `modules`, or with `fail`'s status and body. */
function fakeAptos(fail?: { status: number; body: string }, network: PlaygroundNetwork = "mainnet", modules = MODULES) {
  const calls: string[] = [];
  globalThis.fetch = (async (input: string | URL) => {
    const url = new URL(String(input));
    const [, address, module] = url.pathname.match(/\/accounts\/([^/]+)\/module\/([^/]+)$/) ?? [];
    calls.push(`${url.host} ${address}::${module}`);
    if (fail !== undefined) return new Response(fail.body, { status: fail.status });
    const found = modules[`${address}::${module}`];
    if (found === undefined) return Response.json({ message: "Module not found", error_code: "module_not_found" }, { status: 404 });
    return Response.json(found);
  }) as unknown as typeof fetch;
  const { aptos, client } = clientFor(network, "");
  client.destroy();
  return { aptos, calls: () => calls.map((c) => c.split(" ")[1]), hosts: () => calls.map((c) => c.split(" ")[0]) };
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
    expect(calls()).toEqual(["0xcafe::pool", "0xcafe::pair"]);
  });

  it("names a missing module", async () => {
    const { aptos } = fakeAptos();
    const err = await generateFor(aptos, "0xcafe::gone::Swap").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toBe("Module 0xcafe::gone not found");
  });

  it("reports a plain-text 429 as a rate limit", async () => {
    const err = await moduleLoader(fakeAptos({ status: 429, body: "Per anonymous IP rate limit exceeded" }).aptos)("0xcafe", "pool").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toStartWith("Fullnode request for module 0xcafe::pool failed:");
    expect(err.statusCode).toBe(429);
    expect(describeError(err, false)).toContain("Add an API key");
  });

  it("reads each network's own ABI, with nothing cached across networks", async () => {
    const testnetPool = { abi: { address: "0xcafe", name: "pool", structs: [struct("TestOnly", true, [])] } };
    expect(await generateFor(fakeAptos().aptos, "0xcafe::pool::Swap")).toContain("export const Swap =");
    const testnet = fakeAptos(undefined, "testnet", { ...MODULES, "0xcafe::pool": testnetPool });
    const source = await generateFor(testnet.aptos, "0xcafe::pool::TestOnly");
    expect(source).toContain("export const TestOnly =");
    expect(source).not.toContain("export const Swap =");
    expect(testnet.hosts()[0]).toContain("testnet");
  });

  it("refuses a response with no ABI", async () => {
    const { aptos } = fakeAptos(undefined, "mainnet", { "0xcafe::pool": { bytecode: "0x00" } });
    await expect(moduleLoader(aptos)("0xcafe", "pool")).rejects.toThrow(StreamdError);
  });
});
