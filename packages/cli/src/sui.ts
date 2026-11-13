import { StreamdError } from "@streamd/core";
import type { Endpoint } from "./args.js";
import { readThrough } from "./chain.js";
import type { Chain } from "./chain.js";
import type { SuiNetworkPreset } from "@streamd/sui";

/**
 * `@streamd/sui` and the Sui SDK, imported only when a command runs with
 * `--chain sui`, so Aptos users need neither installed.
 */
export async function loadSui(): Promise<[typeof import("@mysten/sui/grpc"), typeof import("@streamd/sui")]> {
  try {
    return await Promise.all([import("@mysten/sui/grpc"), import("@streamd/sui")]);
  } catch (err) {
    if (isMissingModule(err)) throw new StreamdError("--chain sui needs @mysten/sui installed", err);
    throw err;
  }
}

/** Node reports a missing package by code, Bun by message. */
export function isMissingModule(err: unknown): boolean {
  return (err as { code?: unknown } | null)?.code === "ERR_MODULE_NOT_FOUND" || /Cannot find (package|module)/.test(String(err));
}

export async function suiChain(endpoint: Endpoint): Promise<Chain> {
  const [{ SuiGrpcClient }, sui] = await loadSui();
  const preset = sui.networks[endpoint.network as SuiNetworkPreset["network"]];
  const grpc = (baseUrl = endpoint.nodeUrl) => new SuiGrpcClient({ ...preset, ...(baseUrl !== undefined && { baseUrl }) });
  const node = grpc();
  // Transfers and balances read a SuiAdapter directly, so with several nodes they read the first.
  const reads = () => readThrough(new sui.SuiAdapter(node), endpoint.extraNodeUrls?.map((url) => new sui.SuiAdapter(grpc(url))));
  return {
    client: () => new sui.SuiClient({ sui: node, adapter: reads() }),
    adapter: (nodeUrl) => (nodeUrl === undefined ? reads() : new sui.SuiAdapter(grpc(nodeUrl))),
    check: (descriptor) => sui.checkDescriptor(node, descriptor),
    resolveName: (value) => sui.resolveName(node, value),
    primaryName: (address) => sui.primaryName(node, address),
  };
}
