import { afterEach, describe, expect, it } from "bun:test";
import { AptosClient, FullnodeScanAdapter } from "@streamd/aptos";
import { SuiAdapter, SuiClient } from "@streamd/sui";
import { ArchiveFallbackAdapter, PoolAdapter, StreamdNetworkError } from "@streamd/core";
import type { EventPage } from "@streamd/core";
import { chainFor, readThrough } from "../src/chain.js";
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
