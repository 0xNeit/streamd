import { afterEach, describe, expect, it } from "bun:test";
import { AptosClient, FullnodeScanAdapter } from "@streamd/aptos";
import { SuiAdapter, SuiClient } from "@streamd/sui";
import { chainFor } from "../src/chain.js";
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
