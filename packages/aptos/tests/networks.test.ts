import { describe, it, expect } from "bun:test";
import { AptosApiType, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { networks } from "../src/index.js";

describe("networks", () => {
  it.each(Object.entries(networks))("%s builds an AptosConfig with its own URLs", (_name, preset) => {
    const config = new AptosConfig({ ...preset, clientConfig: { http2: false } });
    expect(config.network).toBe(Network.CUSTOM);
    expect(config.getRequestUrl(AptosApiType.FULLNODE)).toBe(preset.fullnode);
    // The archive serves fullnode reads only; its Indexer is mainnet's own.
    if (preset.indexer !== undefined) expect(config.getRequestUrl(AptosApiType.INDEXER)).toBe(preset.indexer);
    else expect(() => config.getRequestUrl(AptosApiType.INDEXER)).toThrow();
    expect(new URL(preset.fullnode).protocol).toBe("https:");
    expect(Number.isSafeInteger(preset.chainId)).toBe(true);
  });

  it("passes to AptosConfig as is", () => {
    expect(new AptosConfig(networks.movementMainnet).fullnode).toBe(networks.movementMainnet.fullnode);
  });

  it("gives every network a distinct chain id", () => {
    const ids = Object.values(networks).map((p) => p.chainId);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("is frozen, presets included", () => {
    expect(Object.isFrozen(networks)).toBe(true);
    for (const preset of Object.values(networks)) expect(Object.isFrozen(preset)).toBe(true);
  });

  it("exports a mainnet archive preset", () => {
    expect(networks.aptosMainnetArchive).toEqual({
      network: "custom" as never,
      fullnode: "https://archive.mainnet.aptoslabs.com/v1",
      chainId: 1,
    });
    expect(Object.isFrozen(networks.aptosMainnetArchive)).toBe(true);
  });
});
