import { Aptos, AptosConfig, Network, getAptosFullNode } from "@aptos-labs/ts-sdk";
import type { Client } from "@aptos-labs/ts-sdk";
import { AptosClient, FullnodeScanAdapter, checkDescriptor, networks, primaryName, resolveName, sendOnce } from "@streamd/aptos";
import { parseModuleAbi } from "@streamd/codegen";
import type { ListModules, LoadModule, ModuleAbi } from "@streamd/codegen";
import { ArchiveFallbackAdapter, CachingAdapter, PoolAdapter, StreamdError, StreamdNetworkError, toStreamdError } from "@streamd/core";
import type { CachingAdapterOptions, EventAdapter, ModuleEventDescriptor, StreamdClient } from "@streamd/core";
import type { Endpoint } from "./args.js";
import type { TypeChecks } from "./check.js";
import { message } from "./format.js";
import type { RequestStats } from "./stats.js";
import { suiChain } from "./sui.js";

/** What the commands read through, so each is written once for both chains. */
export interface Chain {
  client(): StreamdClient & Pick<AptosClient, "eventsOf" | "transfers" | "getTransfers" | "iterateTransfers" | "balances">;
  /** An adapter over the endpoint's nodes, or over `nodeUrl` alone, which gets no API key. */
  adapter(nodeUrl?: string): HeadAdapter;
  /**
   * One adapter per node, unpooled and uncounted, the first with the API key,
   * each request sent once and, on Aptos, abandoned after `timeout` ms.
   */
  nodes(timeout?: number): HeadAdapter[];
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

/** The `--archive` node, with the API key it gets, or `undefined` without one. */
function archiveEndpoint(endpoint: Endpoint): Pick<Endpoint, "network" | "nodeUrl" | "apiKey"> | undefined {
  if (endpoint.archive === undefined) return undefined;
  if (endpoint.archive !== "default") return { network: endpoint.network, nodeUrl: endpoint.archive };
  // The key goes to Aptos Labs' archive only when it already goes to Aptos Labs' fullnode.
  const apiKey = endpoint.nodeUrl === undefined ? endpoint.apiKey : undefined;
  return { network: "mainnet", nodeUrl: networks.aptosMainnetArchive.fullnode, ...(apiKey !== undefined && { apiKey }) };
}

const pruned = (err: unknown) => err instanceof StreamdNetworkError && err.statusCode === 410;

/**
 * Reads one resource at a ledger version from the endpoint's first node, and
 * from `--archive` when that node refuses the version as pruned. Resolves to
 * the value, or `null` when the resource did not exist at that version.
 * `address` is long form. Aptos only.
 */
export function resourceReader(
  endpoint: Endpoint,
  address: string,
  resourceType: string,
  client?: Client,
): (version: bigint) => Promise<unknown> {
  // The type carries <, > and , in its type arguments.
  const path = `accounts/${address}/resource/${encodeURIComponent(resourceType)}`;
  const read = async (config: AptosConfig, version: bigint): Promise<unknown> => {
    try {
      const { data } = await getAptosFullNode<{}, object>({
        aptosConfig: config,
        originMethod: "inspect",
        path,
        params: { ledger_version: version.toString() },
      });
      if (typeof data !== "object" || data === null || !("data" in data)) {
        throw new StreamdError(`Fullnode returned a malformed ${resourceType} at version ${version}`);
      }
      return data.data;
    } catch (err) {
      const { status, data } = (err ?? {}) as { status?: unknown; data?: { error_code?: unknown } };
      if (status === 404 && data?.error_code === "resource_not_found") return null;
      throw toStreamdError(err, `${resourceType} at ${address}, version ${version}`);
    }
  };
  const first = aptosConfig(endpoint, client);
  const archive = archiveEndpoint(endpoint);
  return async (version) => {
    try {
      return await read(first, version);
    } catch (err) {
      if (archive === undefined || !pruned(err)) throw err;
      return read(aptosConfig(archive, client), version);
    }
  };
}

function aptosChain(endpoint: Endpoint, cache?: CachingAdapterOptions, stats?: RequestStats): Chain {
  const one = sendOnce();
  const config = aptosConfig(endpoint, one);
  const node = (nodeUrl: string, client: Client = one) =>
    new FullnodeScanAdapter(new Aptos(aptosConfig({ network: endpoint.network, nodeUrl }, client)));
  const archive = () => {
    const at = archiveEndpoint(endpoint);
    return at === undefined ? undefined : new FullnodeScanAdapter(new Aptos(aptosConfig(at, one)));
  };
  const nodes = (client: Client = one) => [
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
    nodes: (timeout) => nodes(sendOnce(timeout)),
    check: (descriptor) => checkDescriptor(new Aptos(config), descriptor),
    resolveName: (value) => resolveName(new Aptos(config), value),
    primaryName: (address) => primaryName(new Aptos(config), address),
  };
}

export interface AptosCodegenLoader {
  load: LoadModule;
  list: ListModules;
}

/**
 * Fetches and parses Move module ABIs for Aptos codegen through `fetchFn`.
 * Separated from `main.ts` so 404 mapping, error wrapping, paths, and pagination
 * can be unit-tested without hitting a live fullnode.
 */
export function aptosCodegenLoader(
  config: AptosConfig,
  fetchFn: typeof getAptosFullNode = getAptosFullNode,
): AptosCodegenLoader {
  return {
    load: async (address: string, module: string): Promise<ModuleAbi> => {
      let data: object;
      try {
        ({ data } = await fetchFn<{}, object>({
          aptosConfig: config,
          originMethod: "codegen",
          path: `accounts/${address}/module/${module}`,
        }));
      } catch (err) {
        if ((err as { status?: unknown } | null)?.status === 404) {
          throw new StreamdError(`Module ${address}::${module} not found`);
        }
        throw new StreamdError(`Fullnode request for module ${address}::${module} failed: ${message(err)}`);
      }
      return parseModuleAbi(data);
    },
    list: async (address: string): Promise<ModuleAbi[]> => {
      const abis: ModuleAbi[] = [];
      let start: string | undefined;
      do {
        let response: { data: unknown[]; headers: Record<string, unknown> };
        try {
          response = await fetchFn<{}, unknown[]>({
            aptosConfig: config,
            originMethod: "codegen",
            path: `accounts/${address}/modules`,
            params: { limit: 100, ...(start === undefined ? {} : { start }) },
          });
        } catch (err) {
          throw new StreamdError(`Fullnode request for the modules of ${address} failed: ${message(err)}`);
        }
        if (!Array.isArray(response.data)) throw new StreamdError(`Malformed module list for ${address}`);
        abis.push(...response.data.map(parseModuleAbi));
        const next = response.headers["x-aptos-cursor"];
        start = typeof next === "string" && next !== "" && next !== start ? next : undefined;
      } while (start !== undefined);
      return abis;
    },
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
