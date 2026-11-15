import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import type { Client, ClientRequest, ClientResponse } from "@aptos-labs/ts-sdk";
import { AptosClient, FullnodeScanAdapter, checkDescriptor, networks, primaryName, resolveName } from "@streamd/aptos";
import { ArchiveFallbackAdapter, CachingAdapter, PoolAdapter } from "@streamd/core";
import type { CachingAdapterOptions, EventAdapter, ModuleEventDescriptor, StreamdClient } from "@streamd/core";
import type { Endpoint } from "./args.js";
import type { TypeChecks } from "./check.js";
import type { RequestStats } from "./stats.js";
import { suiChain } from "./sui.js";

/** What the commands read through, so each is written once for both chains. */
export interface Chain {
  client(): StreamdClient & Pick<AptosClient, "eventsOf" | "transfers" | "getTransfers" | "balances">;
  /** An adapter over the endpoint's nodes, or over `nodeUrl` alone, which gets no API key. */
  adapter(nodeUrl?: string): HeadAdapter;
  /** One adapter per node, unpooled and uncounted, the first with the API key, each request sent once. */
  nodes(): HeadAdapter[];
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

export function aptosConfig(options: Pick<Endpoint, "network" | "nodeUrl" | "apiKey">, client?: Client): AptosConfig {
  return new AptosConfig({
    ...NETWORKS[options.network],
    ...(client !== undefined && { client }),
    ...(options.nodeUrl !== undefined && { fullnode: options.nodeUrl }),
    clientConfig: {
      // The SDK's default HTTP/2 hangs under Bun, with no error.
      ...("Bun" in globalThis && { http2: false }),
      ...(options.apiKey !== undefined && { API_KEY: options.apiKey }),
    },
  });
}

/**
 * Each request once, through fetch. The SDK's own client retries a 429 twice,
 * waiting out its Retry-After, which would hide from `monitor` the throttling
 * it reports and spend the rate limit doing so.
 */
export async function sendOnce(request: ClientRequest<unknown>): Promise<ClientResponse<unknown>> {
  const url = new URL(request.url);
  for (const [key, value] of Object.entries(request.params ?? {})) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }
  const headers = Object.fromEntries(Object.entries(request.headers ?? {}).filter(([, value]) => value !== undefined));
  const body = request.body === undefined ? undefined : request.body instanceof Uint8Array ? request.body : JSON.stringify(request.body);
  const response = await fetch(url, { method: request.method, headers, ...(body !== undefined && { body }) });
  const text = await response.text();
  let data: unknown = text;
  try {
    data = JSON.parse(text);
  } catch {
    // Not JSON, such as a plain-text 429: the SDK reports the status either way.
  }
  return { status: response.status, statusText: response.statusText, data, headers: Object.fromEntries(response.headers) };
}

const ONCE = { provider: sendOnce } as Client;

export type HeadAdapter = EventAdapter & { getHeadVersion(): Promise<bigint> };

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

function aptosChain(endpoint: Endpoint, cache?: CachingAdapterOptions, stats?: RequestStats): Chain {
  const config = aptosConfig(endpoint);
  const node = (nodeUrl: string, client?: Client) =>
    new FullnodeScanAdapter(new Aptos(aptosConfig({ network: endpoint.network, nodeUrl }, client)));
  const archive = () => {
    if (endpoint.archive !== "default") return endpoint.archive === undefined ? undefined : node(endpoint.archive);
    // The key goes to Aptos Labs' archive only when it already goes to Aptos Labs' fullnode.
    const apiKey = endpoint.nodeUrl === undefined ? endpoint.apiKey : undefined;
    const nodeUrl = networks.aptosMainnetArchive.fullnode;
    return new FullnodeScanAdapter(new Aptos(aptosConfig({ network: "mainnet", nodeUrl, ...(apiKey !== undefined && { apiKey }) })));
  };
  const nodes = (client?: Client) => [
    new FullnodeScanAdapter(new Aptos(aptosConfig(endpoint, client))),
    ...(endpoint.extraNodeUrls?.map((url) => node(url, client)) ?? []),
  ];
  let shared: HeadAdapter | undefined;
  // Built once, so every client and adapter() share each node's counts.
  const reads = () => {
    if (shared !== undefined) return shared;
    const [first, ...others] = stats === undefined ? nodes() : nodes().map((n) => stats.track(n, "node"));
    const pruned = archive();
    const adapter = readThrough(first!, others, pruned && stats ? stats.track(pruned, "archive") : pruned);
    // Outermost, so a cached page reaches neither the pool nor the archive; the head passes through.
    return (shared = cache === undefined ? adapter : (new CachingAdapter(adapter, cache) as unknown as HeadAdapter));
  };
  return {
    client: () => new AptosClient({ aptos: new Aptos(config), adapter: reads() }),
    adapter: (nodeUrl) => (nodeUrl === undefined ? reads() : node(nodeUrl)),
    nodes: () => nodes(ONCE),
    check: (descriptor) => checkDescriptor(new Aptos(config), descriptor),
    resolveName: (value) => resolveName(new Aptos(config), value),
    primaryName: (address) => primaryName(new Aptos(config), address),
  };
}

/**
 * The chain `endpoint` names; Sui's packages load only here, on `--chain sui`.
 * `cache` wraps the endpoint's reads, on Aptos only: Sui pages are never final.
 * `stats` counts each node and archive behind those reads.
 */
export async function chainFor(endpoint: Endpoint, cache?: CachingAdapterOptions, stats?: RequestStats): Promise<Chain> {
  return endpoint.chain === "sui" ? suiChain(endpoint, stats) : aptosChain(endpoint, cache, stats);
}
