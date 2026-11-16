import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "bun:test";
import { AptosClient, FullnodeScanAdapter } from "@streamd/aptos";
import { SuiAdapter, SuiClient } from "@streamd/sui";
import { ArchiveFallbackAdapter, PoolAdapter, StreamdNetworkError } from "@streamd/core";
import type { EventPage } from "@streamd/core";
import { chainFor, readThrough, sendOnce } from "../src/chain.js";
import { isMissingModule } from "../src/sui.js";

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

/** The URLs the adapter's head read reaches, with every request refused. */
async function reached(adapter: { getHeadVersion(): Promise<bigint> }): Promise<string[]> {
  const urls: string[] = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    urls.push(input instanceof Request ? input.url : String(input));
    throw new Error("offline");
  }) as unknown as typeof fetch;
  await adapter.getHeadVersion().catch(() => {});
  return urls;
}

describe("chainFor", () => {
  it("gives an Aptos client and fullnode adapters by default", async () => {
    const chain = await chainFor({ network: "mainnet" });
    expect(chain.client()).toBeInstanceOf(AptosClient);
    expect(chain.adapter()).toBeInstanceOf(FullnodeScanAdapter);
    expect(chain.adapter("https://b.example/v1")).toBeInstanceOf(FullnodeScanAdapter);
  });

  it("gives a SuiClient and SuiAdapters on the preset, a --node-url and an --against URL", async () => {
    const preset = await chainFor({ chain: "sui", network: "testnet" });
    expect(preset.client()).toBeInstanceOf(SuiClient);
    expect(preset.adapter()).toBeInstanceOf(SuiAdapter);
    expect((await reached(preset.adapter()))[0]).toStartWith("https://fullnode.testnet.sui.io:443/");
    const custom = await chainFor({ chain: "sui", network: "mainnet", nodeUrl: "https://a.example:443" });
    expect((await reached(custom.adapter()))[0]).toStartWith("https://a.example:443/");
    expect((await reached(custom.adapter("https://b.example:443")))[0]).toStartWith("https://b.example:443/");
  });
});

/** A node that names itself in every cursor, and records each request's cursor. */
function fakeNode(name: string) {
  const seen: (string | undefined)[] = [];
  return {
    seen,
    async fetchEvents(_d: unknown, options: { cursor?: string } = {}): Promise<EventPage> {
      seen.push(options.cursor);
      return { events: [], nextCursor: `${name}-${seen.length}`, hasMore: false };
    },
    getHeadVersion: async () => 1n,
  };
}

/** The fullnode URL behind an Aptos adapter, or behind each node of a pool, read from their configs. */
type Node = { nodes?: Node[]; primary?: Node; archive?: Node; aptos?: { config: { fullnode?: string; clientConfig?: { API_KEY?: string } } } };
const nodesOf = (adapter: unknown): Node[] => {
  const { nodes, primary, archive } = adapter as Node;
  if (primary !== undefined) return [...nodesOf(primary), ...nodesOf(archive)];
  return nodes ?? [adapter as Node];
};
const fullnodes = (adapter: unknown) => nodesOf(adapter).map((n) => n.aptos?.config.fullnode);
const keys = (adapter: unknown) => nodesOf(adapter).map((n) => n.aptos?.config.clientConfig?.API_KEY);

const TYPE = { kind: "module", eventType: "0x1::coin::DepositEvent" } as const;

describe("readThrough", () => {
  it("is the one node itself when there are no others", () => {
    const one = fakeNode("a");
    expect(readThrough(one)).toBe(one);
    expect(readThrough(one, [])).toBe(one);
  });

  it("pools several, each walk staying on the node it started on", async () => {
    const [a, b] = [fakeNode("a"), fakeNode("b")];
    const pool = readThrough(a, [b]);
    expect(pool).toBeInstanceOf(PoolAdapter);
    const first = await pool.fetchEvents(TYPE, {});
    const second = await pool.fetchEvents(TYPE, {});
    expect([first.nextCursor, second.nextCursor]).toEqual(["a-1", "b-1"]);
    await pool.fetchEvents(TYPE, { cursor: second.nextCursor });
    expect(b.seen).toEqual([undefined, "b-1"]);
    expect(await pool.getHeadVersion()).toBe(1n);
  });
});

/** A node whose every read fails with `status`. */
function failing(status: number) {
  return {
    async fetchEvents(): Promise<EventPage> {
      throw new StreamdNetworkError(`HTTP ${status}`, status);
    },
    getHeadVersion: async () => 7n,
  };
}

describe("readThrough with an archive", () => {
  it("sends what the fullnode side refuses as pruned to the archive", async () => {
    const archive = fakeNode("archive");
    const reads = readThrough(failing(410), [], archive);
    expect(reads).toBeInstanceOf(ArchiveFallbackAdapter);
    expect((await reads.fetchEvents(TYPE, {})).nextCursor).toBe("archive-1");
    expect(await reads.getHeadVersion()).toBe(7n);
  });

  it("keeps a pool as the fullnode side and moves nothing but a 410", async () => {
    const archive = fakeNode("archive");
    const pruned = readThrough(failing(410), [failing(410)], archive);
    expect((await pruned.fetchEvents(TYPE, {})).nextCursor).toBe("archive-1");
    const limited = readThrough(failing(500), [failing(500)], archive);
    await expect(limited.fetchEvents(TYPE, {})).rejects.toThrow("HTTP 500");
    expect(archive.seen).toHaveLength(1);
  });
});

describe("chainFor with an archive", () => {
  const ARCHIVE = "https://archive.mainnet.aptoslabs.com/v1";

  it("reads Aptos mainnet's archive for default, with the key only without --node-url", async () => {
    const plain = await chainFor({ network: "mainnet", archive: "default", apiKey: "k" });
    expect(plain.adapter()).toBeInstanceOf(ArchiveFallbackAdapter);
    expect(fullnodes(plain.adapter())).toEqual([undefined, ARCHIVE]);
    expect(keys(plain.adapter())).toEqual(["k", "k"]);
    const custom = await chainFor({ network: "mainnet", nodeUrl: "https://a.example/v1", archive: "default", apiKey: "k" });
    expect(keys(custom.adapter())).toEqual(["k", undefined]);
    expect(fullnodes((custom.client() as unknown as { adapter: unknown }).adapter)).toEqual(["https://a.example/v1", ARCHIVE]);
  });

  it("reads an archive URL with no key, behind a pool", async () => {
    const chain = await chainFor({
      network: "testnet",
      nodeUrl: "https://a.example/v1",
      extraNodeUrls: ["https://b.example/v1"],
      archive: "https://archive.example/v1",
      apiKey: "k",
    });
    expect(fullnodes(chain.adapter())).toEqual(["https://a.example/v1", "https://b.example/v1", "https://archive.example/v1"]);
    expect(keys(chain.adapter())).toEqual(["k", undefined, undefined]);
  });
});

describe("chainFor with several nodes", () => {
  it("spreads Aptos reads over every --node-url, the client included", async () => {
    const chain = await chainFor({ network: "mainnet", nodeUrl: "https://a.example/v1", extraNodeUrls: ["https://b.example/v1"], apiKey: "k" });
    const urls = ["https://a.example/v1", "https://b.example/v1"];
    expect(fullnodes(chain.adapter())).toEqual(urls);
    expect(fullnodes((chain.client() as unknown as { adapter: unknown }).adapter)).toEqual(urls);
    expect(keys(chain.adapter())).toEqual(["k", undefined]);
    expect(fullnodes(chain.adapter("https://c.example/v1"))).toEqual(["https://c.example/v1"]);
  });

  it("spreads Sui reads over every gRPC URL", async () => {
    const chain = await chainFor({ chain: "sui", network: "mainnet", nodeUrl: "https://a.example:443", extraNodeUrls: ["https://b.example:443"] });
    const adapter = chain.adapter();
    expect((await reached(adapter))[0]).toStartWith("https://a.example:443/");
    expect((await reached(adapter))[0]).toStartWith("https://b.example:443/");
    expect(chain.client()).toBeInstanceOf(SuiClient);
  });

  it("keeps the client on one fullnode adapter with one --node-url", async () => {
    const chain = await chainFor({ network: "mainnet", nodeUrl: "https://a.example/v1" });
    expect((chain.client() as unknown as { adapter: unknown }).adapter).toBeInstanceOf(FullnodeScanAdapter);
  });
});

describe("chainFor nodes", () => {
  it("gives one unpooled, uncounted adapter per Aptos node, the key on the first", async () => {
    const chain = await chainFor({ network: "mainnet", nodeUrl: "https://a.example/v1", extraNodeUrls: ["https://b.example/v1"], apiKey: "k" });
    const nodes = chain.nodes();
    expect(nodes).toHaveLength(2);
    for (const n of nodes) expect(n).toBeInstanceOf(FullnodeScanAdapter);
    expect(nodes.map(fullnodes)).toEqual([["https://a.example/v1"], ["https://b.example/v1"]]);
    expect(nodes.map(keys)).toEqual([["k"], [undefined]]);
  });

  it("sends each node request once, so a 429 reaches the caller with its status", async () => {
    const chain = await chainFor({ network: "mainnet", nodeUrl: "https://a.example/v1", extraNodeUrls: ["https://b.example/v1"] });
    const seen: string[] = [];
    let status = 429;
    globalThis.fetch = (async (input: string | URL | Request) => {
      seen.push(String(input));
      return status === 429
        ? new Response("slow down", { status, headers: { "retry-after": "1" } })
        : Response.json({ ledger_version: "42", oldest_ledger_version: "0", block_height: "7", oldest_block_height: "0" });
    }) as unknown as typeof fetch;
    const [a] = chain.nodes();
    const err = await a!.getHeadVersion().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(429);
    expect(seen).toEqual(["https://a.example/v1"]);
    status = 200;
    expect(await a!.getHeadVersion()).toBe(42n);
    expect(await a!.getBlockHeights!(TYPE)).toEqual({ oldest: 0n, newest: 7n });
  });

  it("encodes params, drops unset headers, falls back to text, and abandons a request after the timeout", async () => {
    const requests: Request[] = [];
    globalThis.fetch = (async (input: string | URL, init?: RequestInit) => {
      requests.push(new Request(String(input), init));
      if (init?.signal) await new Promise((_, reject) => init.signal!.addEventListener("abort", () => reject(init.signal!.reason)));
      return new Response("not json", { status: 502, statusText: "Bad Gateway" });
    }) as unknown as typeof fetch;
    const { provider } = sendOnce();
    const response = await provider({ url: "https://a.example/v1/x", method: "POST", params: { start: 5n, skip: undefined }, headers: { a: "1", b: undefined }, body: { k: 1 } });
    expect(response).toMatchObject({ status: 502, statusText: "Bad Gateway", data: "not json" });
    expect(requests[0]!.url).toBe("https://a.example/v1/x?start=5");
    expect(requests[0]!.headers.get("a")).toBe("1");
    expect(requests[0]!.headers.has("b")).toBe(false);
    expect(await requests[0]!.text()).toBe('{"k":1}');
    await expect(sendOnce(20).provider({ url: "https://a.example/v1", method: "GET" })).rejects.toMatchObject({ name: "TimeoutError" });
  });

  it("gives one SuiAdapter per gRPC URL", async () => {
    const chain = await chainFor({ chain: "sui", network: "mainnet", nodeUrl: "https://a.example:443", extraNodeUrls: ["https://b.example:443"] });
    const [a, b] = chain.nodes();
    expect(a).toBeInstanceOf(SuiAdapter);
    expect((await reached(a!))[0]).toStartWith("https://a.example:443/");
    expect((await reached(b!))[0]).toStartWith("https://b.example:443/");
  });
});

describe("chainFor names", () => {
  it("puts an address in long form with no read, on both chains", async () => {
    globalThis.fetch = (async () => {
      throw new Error("no request expected");
    }) as unknown as typeof fetch;
    const long = `0x${"1".padStart(64, "0")}`;
    expect(await (await chainFor({ network: "mainnet" })).resolveName("0x1")).toBe(long);
    expect(await (await chainFor({ chain: "sui", network: "mainnet" })).resolveName("0x1")).toBe(long);
  });

  it("refuses a malformed address before any read", async () => {
    await expect((await chainFor({ network: "mainnet" })).resolveName("0xzz")).rejects.toThrow();
    await expect((await chainFor({ network: "mainnet" })).primaryName("0xzz")).rejects.toThrow();
    await expect((await chainFor({ chain: "sui", network: "mainnet" })).resolveName("nope!")).rejects.toThrow(
      /Not a valid Sui address or SuiNS name/,
    );
  });
});

describe("isMissingModule", () => {
  it("knows Node's code and Bun's message, and nothing else", () => {
    expect(isMissingModule(Object.assign(new Error("x"), { code: "ERR_MODULE_NOT_FOUND" }))).toBe(true);
    expect(isMissingModule(new Error("Cannot find package '@mysten/sui' from '/x/cli.js'"))).toBe(true);
    expect(isMissingModule(new SyntaxError("Unexpected token"))).toBe(false);
  });
});

describe("package manifests and tarballs", () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

  it("keeps @streamd/sui as an optional CLI peer and guards publishing with Bun", () => {
    const cli = JSON.parse(readFileSync(resolve(root, "packages/cli/package.json"), "utf8")) as {
      dependencies: Record<string, string>;
      devDependencies: Record<string, string>;
      peerDependencies: Record<string, string>;
      peerDependenciesMeta: Record<string, { optional?: boolean }>;
    };
    expect(cli.dependencies["@streamd/sui"]).toBeUndefined();
    expect(cli.devDependencies["@streamd/sui"]).toBe("workspace:*");
    expect(cli.peerDependencies["@streamd/sui"]).toBe("^0.1.0");
    expect(cli.peerDependenciesMeta["@streamd/sui"]).toEqual({ optional: true });
    const cleanEnv = Object.fromEntries(
      Object.entries(process.env).filter(([k]) => !k.startsWith("npm_")),
    );
    for (const dir of ["packages/core", "packages/cli"]) {
      const cwd = resolve(root, dir);
      const ok = Bun.spawnSync(["bun", "run", "prepublishOnly"], {
        cwd,
        env: cleanEnv,
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(ok.exitCode).toBe(0);

      const cleanNpm = Bun.spawnSync(["npm", "run", "prepublishOnly"], {
        cwd,
        env: cleanEnv,
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(cleanNpm.exitCode).toBe(1);
      expect(new TextDecoder().decode(cleanNpm.stderr)).toContain("bun publish");

      const nestedNpm = Bun.spawnSync(["npm", "run", "prepublishOnly"], {
        cwd,
        env: { ...cleanEnv, npm_config_user_agent: "bun/1.3.10 npm/? node/v24.3.0" },
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(nestedNpm.exitCode).toBe(1);
      expect(new TextDecoder().decode(nestedNpm.stderr)).toContain("bun publish");

      const winBun = Bun.spawnSync(["bun", "run", "prepublishOnly"], {
        cwd,
        env: {
          ...cleanEnv,
          npm_config_user_agent: "bun/1.4.3 npm/? node/v24.3.0 win32 x64",
          npm_execpath: "C:\\Users\\runner\\.bun\\bin\\bun.exe",
        },
        stdout: "pipe",
        stderr: "pipe",
      });
      expect(winBun.exitCode).toBe(0);
    }
  });

  it("includes README.md and LICENSE in @streamd/core and @streamd/cli tarballs", () => {
    for (const pkg of ["packages/core", "packages/cli"]) {
      const dir = resolve(root, pkg);
      expect(existsSync(resolve(dir, "README.md"))).toBe(true);
      expect(existsSync(resolve(dir, "LICENSE"))).toBe(true);
      const pack = Bun.spawnSync(["bun", "pm", "pack", "--dry-run"], { cwd: dir });
      expect(pack.exitCode).toBe(0);
      const output = pack.stdout.toString() + pack.stderr.toString();
      expect(output).toContain("README.md");
      expect(output).toContain("LICENSE");
    }
  });
});
