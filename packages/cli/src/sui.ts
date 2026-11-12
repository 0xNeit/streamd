import { StreamdError } from "@streamd/core";
import type { Endpoint } from "./args.js";
import type { Chain } from "./chain.js";

/**
 * `@streamd/sui` and the Sui SDK, imported only when a command runs with
 * `--chain sui`, so Aptos users need neither installed.
 */
export async function loadSui(): Promise<[typeof import("@mysten/sui/grpc"), typeof import("@streamd/sui")]> {
  try {
    return await Promise.all([import("@mysten/sui/grpc"), import("@streamd/sui")]);
  } catch (err) {
    const missing = (err as { code?: unknown } | null)?.code === "ERR_MODULE_NOT_FOUND" || /Cannot find (package|module)/.test(String(err));
    if (missing) throw new StreamdError("--chain sui needs @mysten/sui installed", err);
    throw err;
  }
}

export async function suiChain(endpoint: Endpoint): Promise<Chain> {
  const [{ SuiGrpcClient }, sui] = await loadSui();
  const preset = sui.networks[endpoint.network as keyof typeof sui.networks];
  const grpc = (baseUrl = endpoint.nodeUrl) => new SuiGrpcClient({ ...preset, ...(baseUrl !== undefined && { baseUrl }) });
  const node = grpc();
  return {
    client: () => new sui.SuiClient({ sui: node }),
    adapter: (nodeUrl) => new sui.SuiAdapter(nodeUrl === undefined ? node : grpc(nodeUrl)),
    check: (descriptor) => sui.checkDescriptor(node, descriptor),
  };
}
