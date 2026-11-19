import { describe, expect, it } from "bun:test";
import { Network } from "@aptos-labs/ts-sdk";
import { StreamdNetworkError, StreamdPollingError } from "@streamd/core";
import { clientFor, describeError } from "../src/client.js";

describe("clientFor", () => {
  it("sends a key only when one is given, and points at the chosen network", () => {
    const keyed = clientFor("testnet", " key-1 ");
    expect(keyed.aptos.config.network).toBe(Network.TESTNET);
    expect(keyed.aptos.config.clientConfig?.API_KEY).toBe("key-1");
    const anonymous = clientFor("mainnet", "  ");
    expect(anonymous.aptos.config.network).toBe(Network.MAINNET);
    expect(anonymous.aptos.config.clientConfig?.API_KEY).toBeUndefined();
    keyed.client.destroy();
    anonymous.client.destroy();
  });
});

describe("describeError", () => {
  it("names a rate limit anywhere in the cause chain", () => {
    const limited = new StreamdNetworkError("Too Many Requests", 429);
    expect(describeError(limited, false)).toContain("Add an API key");
    expect(describeError(new StreamdPollingError("gave up", 5, limited), true)).toContain("even with this API key");
  });

  it("passes other messages through", () => {
    expect(describeError(new StreamdNetworkError("Bad Gateway", 502), false)).toBe("Bad Gateway");
    expect(describeError("plain", false)).toBe("plain");
  });
});
