import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient, FullnodeScanAdapter, checkDescriptor, networks, primaryName, resolveName } from "@streamd/aptos";
import { ArchiveFallbackAdapter, PoolAdapter } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, StreamdClient } from "@streamd/core";
import type { Endpoint } from "./args.js";
import type { TypeChecks } from "./check.js";
import { suiChain } from "./sui.js";

/** What the commands read through, so each is written once for both chains. */
export interface Chain {
  client(): StreamdClient & Pick<AptosClient, "eventsOf" | "transfers" | "getTransfers" | "balances">;
  /** An adapter over the endpoint's nodes, or over `nodeUrl` alone, which gets no API key. */
  adapter(nodeUrl?: string): HeadAdapter;
  check(descriptor: ModuleEventDescriptor): Promise<TypeChecks>;
  /** The long-form address a name points to; an address comes back in long form with no read. */
  resolveName(nameOrAddress: string): Promise<string>;
  /** The address's primary name, checked forward, or `undefined` when it has none. */
  primaryName(address: string): Promise<string | undefined>;
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

type HeadAdapter = EventAdapter & { getHeadVersion(): Promise<bigint> };

/**
 * `first` itself, or reads spread over it and `others` through `PoolAdapter`,
 * with what those refuse as pruned sent to `archive`.
 */
export function readThrough(first: HeadAdapter, others: readonly HeadAdapter[] = [], archive?: EventAdapter): HeadAdapter {
  // Every node has getHeadVersion, so the pool has it too, passing its arguments through.
  const fullnode = others.length === 0 ? first : (new PoolAdapter([first, ...others]) as unknown as HeadAdapter);
  // The archive's head comes from the fullnode side, which has one.
  return archive === undefined ? fullnode : (new ArchiveFallbackAdapter(fullnode, archive) as unknown as HeadAdapter);
}

function aptosChain(endpoint: Endpoint): Chain {
  const config = aptosConfig(endpoint);
  const node = (nodeUrl: string) => new FullnodeScanAdapter(new Aptos(aptosConfig({ network: endpoint.network, nodeUrl })));
  const archive = () => {
    if (endpoint.archive !== "default") return endpoint.archive === undefined ? undefined : node(endpoint.archive);
    // The key goes to Aptos Labs' archive only when it already goes to Aptos Labs' fullnode.
    const apiKey = endpoint.nodeUrl === undefined ? endpoint.apiKey : undefined;
    const nodeUrl = networks.aptosMainnetArchive.fullnode;
    return new FullnodeScanAdapter(new Aptos(aptosConfig({ network: "mainnet", nodeUrl, ...(apiKey !== undefined && { apiKey }) })));
  };
  const reads = (aptos: Aptos) => readThrough(new FullnodeScanAdapter(aptos), endpoint.extraNodeUrls?.map(node), archive());
  return {
    client: () => {
      const aptos = new Aptos(config);
      return new AptosClient({ aptos, adapter: reads(aptos) });
    },
    adapter: (nodeUrl) => (nodeUrl === undefined ? reads(new Aptos(config)) : node(nodeUrl)),
    check: (descriptor) => checkDescriptor(new Aptos(config), descriptor),
    resolveName: (value) => resolveName(new Aptos(config), value),
    primaryName: (address) => primaryName(new Aptos(config), address),
  };
}

/** The chain `endpoint` names; Sui's packages load only here, on `--chain sui`. */
export async function chainFor(endpoint: Endpoint): Promise<Chain> {
  return endpoint.chain === "sui" ? suiChain(endpoint) : aptosChain(endpoint);
}
