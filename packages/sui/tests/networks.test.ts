import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { networks } from "../src/index.js";

describe("networks", () => {
  it.each(Object.entries(networks))("%s builds a SuiGrpcClient for its own network", (name, preset) => {
    expect(preset.network).toBe(name as never);
    expect(new URL(preset.baseUrl).protocol).toBe("https:");
    expect(new URL(preset.baseUrl).hostname).toBe(`fullnode.${name}.sui.io`);
    expect(preset.graphqlUrl).toBe(`https://graphql.${name}.sui.io/graphql`);
    expect(typeof new SuiGrpcClient(preset).ledgerService.listEvents).toBe("function");
  });

  it("is frozen, presets included", () => {
    expect(Object.isFrozen(networks)).toBe(true);
    for (const preset of Object.values(networks)) expect(Object.isFrozen(preset)).toBe(true);
  });
});
