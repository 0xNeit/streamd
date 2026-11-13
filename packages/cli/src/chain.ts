import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient, FullnodeScanAdapter, checkDescriptor, networks } from "@streamd/aptos";
import type { EventAdapter, ModuleEventDescriptor, StreamdClient } from "@streamd/core";
import type { Endpoint } from "./args.js";
import type { TypeChecks } from "./check.js";
import { suiChain } from "./sui.js";

/** What the commands read through, so each is written once for both chains. */
export interface Chain {
  client(): StreamdClient & Pick<AptosClient, "eventsOf" | "transfers" | "getTransfers" | "balances">;
  /** An adapter over the endpoint's node, or over `nodeUrl`, which gets no API key. */
  adapter(nodeUrl?: string): EventAdapter & { getHeadVersion(): Promise<bigint> };
  check(descriptor: ModuleEventDescriptor): Promise<TypeChecks>;
}

const NETWORKS = {
  mainnet: { network: Network.MAINNET },
  testnet: { network: Network.TESTNET },
  devnet: { network: Network.DEVNET },
  "movement-mainnet": networks.movementMainnet,
  "movement-testnet": networks.movementTestnet,
} as const;

export function aptosConfig(options: Pick<Endpoint, "network" | "nodeUrl" | "apiKey">): AptosConfig {
  return new AptosConfig({
    ...NETWORKS[options.network],
    ...(options.nodeUrl !== undefined && { fullnode: options.nodeUrl }),
    clientConfig: {
      // The SDK's default HTTP/2 hangs under Bun, with no error.
      ...("Bun" in globalThis && { http2: false }),
      ...(options.apiKey !== undefined && { API_KEY: options.apiKey }),
    },
  });
}

function aptosChain(endpoint: Endpoint): Chain {
  const config = aptosConfig(endpoint);
  return {
    client: () => new AptosClient({ aptos: new Aptos(config) }),
    adapter: (nodeUrl) =>
      new FullnodeScanAdapter(new Aptos(nodeUrl === undefined ? config : aptosConfig({ network: endpoint.network, nodeUrl }))),
    check: (descriptor) => checkDescriptor(new Aptos(config), descriptor),
  };
}

/** The chain `endpoint` names; Sui's packages load only here, on `--chain sui`. */
export async function chainFor(endpoint: Endpoint): Promise<Chain> {
  return endpoint.chain === "sui" ? suiChain(endpoint) : aptosChain(endpoint);
}
