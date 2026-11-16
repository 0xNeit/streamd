import { describe, expect, it } from "bun:test";
import { getAptosFullNode } from "@aptos-labs/ts-sdk";
import { StreamdError } from "@streamd/core";
import { aptosCodegenLoader, aptosConfig } from "../src/chain.js";

function rawModule(address: string, name: string) {
  return {
    bytecode: "0x1",
    abi: {
      address,
      name,
      structs: [
        {
          name: "PingEvent",
          is_event: true,
          is_enum: false,
          abilities: ["drop", "store"],
          generic_type_params: [],
          fields: [{ name: "value", type: "u64" }],
        },
      ],
      exposed_functions: [],
    },
  };
}

describe("aptosCodegenLoader", () => {
  const config = aptosConfig({ network: "mainnet", nodeUrl: "https://node.example/v1" });

  it("requests the module ABI path and parses the response", async () => {
    const calls: unknown[] = [];
    const fetchFn = (async (req: unknown) => {
      calls.push(req);
      return { data: rawModule("0x1", "coin"), headers: {}, status: 200, statusText: "OK", request: {}, config: {} };
    }) as unknown as typeof getAptosFullNode;

    const { load } = aptosCodegenLoader(config, fetchFn);
    const abi = await load("0x1", "coin");
    expect(calls).toEqual([{ aptosConfig: config, originMethod: "codegen", path: "accounts/0x1/module/coin" }]);
    expect(abi.address).toBe("0x1");
    expect(abi.name).toBe("coin");
    expect(abi.structs.map((s) => s.name)).toEqual(["PingEvent"]);
  });

  it("maps 404 to Module X not found and wraps other request failures as StreamdError", async () => {
    const missing = aptosCodegenLoader(config, (async () => {
      throw Object.assign(new Error("Request failed"), { status: 404 });
    }) as unknown as typeof getAptosFullNode);
    await expect(missing.load("0x1", "nope")).rejects.toThrow(new StreamdError("Module 0x1::nope not found"));

    const serverError = aptosCodegenLoader(config, (async () => {
      throw new Error("socket hang up");
    }) as unknown as typeof getAptosFullNode);
    await expect(serverError.load("0x1", "coin")).rejects.toThrow(
      new StreamdError("Fullnode request for module 0x1::coin failed: socket hang up"),
    );

    const badAbi = aptosCodegenLoader(config, (async () => ({
      data: { bytecode: "0x1" },
      headers: {},
    })) as unknown as typeof getAptosFullNode);
    await expect(badAbi.load("0x1", "coin")).rejects.toThrow(/malformed module ABI/);
  });

  it("paginates account modules across cursors and stops on empty or repeating cursor", async () => {
    const seen: unknown[] = [];
    const pages = [
      { data: [rawModule("0x1", "a")], headers: { "x-aptos-cursor": "c1" } },
      { data: [rawModule("0x1", "b")], headers: { "x-aptos-cursor": "c1" } },
    ];
    let index = 0;
    const { list } = aptosCodegenLoader(config, (async (req: unknown) => {
      seen.push(req);
      return pages[index++];
    }) as unknown as typeof getAptosFullNode);

    const abis = await list("0x1");
    expect(abis.map((m) => m.name)).toEqual(["a", "b"]);
    expect(seen).toEqual([
      { aptosConfig: config, originMethod: "codegen", path: "accounts/0x1/modules", params: { limit: 100 } },
      { aptosConfig: config, originMethod: "codegen", path: "accounts/0x1/modules", params: { limit: 100, start: "c1" } },
    ]);

    let emptyCalls = 0;
    const emptyStop = aptosCodegenLoader(config, (async () => {
      emptyCalls++;
      return { data: [rawModule("0x1", "only")], headers: { "x-aptos-cursor": "" } };
    }) as unknown as typeof getAptosFullNode);
    expect((await emptyStop.list("0x1")).map((m) => m.name)).toEqual(["only"]);
    expect(emptyCalls).toBe(1);
  });

  it("refuses malformed module lists and wraps list request errors", async () => {
    const malformed = aptosCodegenLoader(config, (async () => ({
      data: { not: "an array" },
      headers: {},
    })) as unknown as typeof getAptosFullNode);
    await expect(malformed.list("0x1")).rejects.toThrow(new StreamdError("Malformed module list for 0x1"));

    const failed = aptosCodegenLoader(config, (async () => {
      throw Object.assign(new Error("rate limited"), { status: 429 });
    }) as unknown as typeof getAptosFullNode);
    await expect(failed.list("0x1")).rejects.toThrow(
      new StreamdError("Fullnode request for the modules of 0x1 failed: rate limited"),
    );
  });
});
