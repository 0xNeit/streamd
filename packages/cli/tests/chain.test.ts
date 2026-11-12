import { afterEach, describe, expect, it } from "bun:test";
import { AptosClient, FullnodeScanAdapter } from "@streamd/aptos";
import { SuiAdapter, SuiClient } from "@streamd/sui";
import { chainFor } from "../src/chain.js";

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
