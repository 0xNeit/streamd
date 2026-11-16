import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getAptosFullNode } from "@aptos-labs/ts-sdk";
import { StreamdError } from "@streamd/core";
import { aptosCodegenLoader, aptosConfig } from "../src/chain.js";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const MAIN = resolve(ROOT, "packages/cli/src/main.ts");

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

describe("main.ts CLI entry point", () => {
  const dirs: string[] = [];
  const routerAbi = {
    bytecode: "0x1",
    abi: {
      address: "0x867ed1f6bf916171b1de3ee92849b8978b7d1b9e0a8cc982a3d19d535dfd9c0c",
      name: "router",
      friends: [],
      exposed_functions: [
        {
          name: "get_primary_name",
          visibility: "public",
          is_entry: false,
          is_view: true,
          generic_type_params: [],
          params: ["address"],
          return: ["0x1::option::Option<0x1::string::String>"],
        },
      ],
      structs: [],
    },
  };
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(request) {
      const url = new URL(request.url);
      if (url.pathname.startsWith("/fail/v1/accounts/")) {
        return Response.json({ type: "0x1::coin::CoinStore", data: {} });
      }
      if (url.pathname.endsWith("/module/router")) {
        return Response.json(routerAbi);
      }
      if (url.pathname === "/v1/view") {
        return Response.json([{ vec: [] }]);
      }
      if (url.pathname === "/v1/accounts/0x1/module/coin") {
        return Response.json(rawModule("0x1", "coin"));
      }
      return new Response("not found", { status: 404 });
    },
  });

  afterAll(() => {
    server.stop(true);
    for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
  });

  async function runCli(...args: string[]) {
    const child = Bun.spawn([process.execPath, MAIN, ...args], {
      cwd: ROOT,
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    return { exitCode, stdout, stderr };
  }

  it("prints usage for --help with exit 0 and exits 2 on bad flags", async () => {
    const help = await runCli("--help");
    expect(help.exitCode).toBe(0);
    expect(help.stdout).toContain("Usage:");
    expect(help.stdout).toContain("streamd codegen");
    expect(help.stderr).toBe("");

    const bad = await runCli("query", "--not-a-flag");
    expect(bad.exitCode).toBe(2);
    expect(bad.stdout).toBe("");
    expect(bad.stderr).toContain("streamd:");
    expect(bad.stderr).toContain("--help");
  });

  it("dispatches name and codegen commands against a local fullnode", async () => {
    const nodeUrl = `http://127.0.0.1:${server.port}/v1`;
    const named = await runCli("name", "0x1", "--node-url", nodeUrl);
    expect(named.exitCode).toBe(0);
    expect(named.stderr).toBe("");
    expect(JSON.parse(named.stdout)).toEqual({
      name: null,
      address: `0x${"1".padStart(64, "0")}`,
    });

    const stdoutGen = await runCli("codegen", "0x1::coin", "--node-url", nodeUrl);
    expect(stdoutGen.exitCode).toBe(0);
    expect(stdoutGen.stderr).toBe("");
    expect(stdoutGen.stdout).toContain("export interface PingEvent");
    expect(stdoutGen.stdout).toContain("defineEvent");

    const dir = mkdtempSync(join(tmpdir(), "streamd-main-test-"));
    dirs.push(dir);
    const outFile = join(dir, "events.ts");
    const fileGen = await runCli("codegen", "0x1::coin", "--node-url", nodeUrl, "--out", outFile);
    expect(fileGen.exitCode).toBe(0);
    expect(fileGen.stdout).toBe("");
    expect(readFileSync(outFile, "utf8")).toBe(stdoutGen.stdout);

    const checkMatch = await runCli("codegen", "0x1::coin", "--node-url", nodeUrl, "--out", outFile, "--check");
    expect(checkMatch.exitCode).toBe(0);

    writeFileSync(outFile, "// stale\n");
    const checkDrift = await runCli("codegen", "0x1::coin", "--node-url", nodeUrl, "--out", outFile, "--check");
    expect(checkDrift.exitCode).toBe(1);
    expect(checkDrift.stderr).toContain("streamd:");
  });

  it("writes the --stats JSON line to stderr even when query fails", async () => {
    const failed = await runCli(
      "query",
      "--account",
      "0x1",
      "--handle",
      "0x1::coin::CoinStore",
      "--field",
      "missing",
      "--from-version",
      "1",
      "--to-version",
      "2",
      "--node-url",
      `http://127.0.0.1:${server.port}/fail/v1`,
      "--stats",
    );
    expect(failed.exitCode).toBe(1);
    expect(failed.stdout).toBe("");
    expect(failed.stderr).toContain("streamd:");
    const statsLine = failed.stderr.split("\n").find((line) => line.startsWith('{"stats":'));
    expect(statsLine).toBeDefined();
    const parsed = JSON.parse(statsLine!) as {
      stats: { nodes: Array<Record<string, { requests: number; errors: number }>> };
    };
    expect(parsed.stats.nodes).toHaveLength(1);
    expect(parsed.stats.nodes[0]!.fetchEvents).toMatchObject({ requests: 1, errors: 1 });
  });
});
