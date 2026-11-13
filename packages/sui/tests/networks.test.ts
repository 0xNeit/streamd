import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdError } from "@streamd/core";
import { networks, type SuiTransport } from "../src/index.js";

const { mainnetArchive, ...fullnodes } = networks;

/** A transport that records each unary method it is asked to call. */
function recordingTransport() {
  const called: string[] = [];
  const refuse = () => {
    throw new Error("not used");
  };
  const transport = {
    mergeOptions: (options?: object) => ({ ...options }),
    unary: (method: { name: string }) => {
      called.push(method.name);
      return Promise.reject(new Error("offline"));
    },
    serverStreaming: refuse,
    clientStreaming: refuse,
    duplex: refuse,
  } as unknown as SuiTransport;
  return { transport, called };
}

describe("networks", () => {
  it.each(Object.entries(fullnodes))("%s builds a SuiGrpcClient for its own network", (name, preset) => {
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

  it("builds the mainnet archive's client over the transport given", async () => {
    expect(mainnetArchive.host).toBe("archive.mainnet.sui.io:443");
    const { transport, called } = recordingTransport();
    const preset = mainnetArchive(transport);
    expect(preset).toEqual({ network: "mainnet", transport });
    expect(Object.isFrozen(preset)).toBe(true);
    const grpc = new SuiGrpcClient(preset);
    expect(grpc.network).toBe("mainnet");
    await expect(Promise.resolve(grpc.ledgerService.getServiceInfo({}))).rejects.toThrow("offline");
    expect(called).toEqual(["GetServiceInfo"]);
  });

  it("refuses an archive preset without a native transport", () => {
    for (const bad of [undefined, "https://archive.mainnet.sui.io:443", { unary: () => undefined }]) {
      expect(() => mainnetArchive(bad as never)).toThrow(StreamdError);
    }
  });
});
