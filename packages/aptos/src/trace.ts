import { EventQuery, StreamdError, StreamdNetworkError, requireAddress } from "@streamd/core";
import type { AccountEventDescriptor, EventAdapter, RequestLimiter, Transfer } from "@streamd/core";
import { activityDescriptor, activitySource } from "./activity.js";
import { assetKey, requireAsset } from "./transfers.js";
import type { Activity, FundFlowGraph, TraceEdge, TraceNode, TraceOptions } from "./types.js";

/** One transaction's transfers touching the node read. */
export interface TracedTransaction {
  version: bigint;
  transfers: readonly Transfer[];
}

/** Every transaction a node sent or received over the trace's range. */
export type TraceReader = (address: string) => Promise<readonly TracedTransaction[]>;

export interface WalkOptions {
  hops: number;
  direction: "out" | "in" | "both";
  minAmount: bigint;
  /** In `assetKey` form. */
  asset?: string;
  maxNodes: number;
  stop: ReadonlySet<string>;
  target?: string;
}

/** Thrown by `countedAdapter` once the budget is spent; a caller's limit, so never retried. */
export class TraceBudgetError extends StreamdError {}

// What a node read can call: the methods activitySource passes on, and the head read.
const METHODS = ["fetchEvents", "getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const;

/**
 * `adapter` with every call counted, refusing those past `max`. `refused`
 * matters because transferSource reads a refused view as an absent store and
 * finishes the page without that transfer.
 */
export function countedAdapter(
  adapter: EventAdapter,
  max: number,
): { adapter: EventAdapter; requests: () => number; refused: () => boolean } {
  let requests = 0;
  let refused = false;
  const counted = {} as EventAdapter;
  for (const name of METHODS) {
    const method = adapter[name] as ((...args: unknown[]) => Promise<unknown>) | undefined;
    if (typeof method !== "function") continue;
    Object.assign(counted, {
      [name]: (...args: unknown[]) => {
        if (requests >= max) {
          refused = true;
          return Promise.reject(new TraceBudgetError(`trace spent its maxRequests of ${max}`));
        }
        requests++;
        return method.apply(adapter, args);
      },
    });
  }
  return { adapter: counted, requests: () => requests, refused: () => refused };
}

/**
 * Breadth first from `root`: each node within `hops` is read once, and every
 * transfer it took part in, in the directions followed, adds to the edge of
 * its `(from, to, asset)` and brings in the counterparty. A transfer read from
 * both of its ends counts once.
 */
export async function walkTransfers(
  read: TraceReader,
  root: string,
  options: WalkOptions,
): Promise<Omit<FundFlowGraph, "requests">> {
  const nodes = new Map<string, TraceNode>([[root, { address: root, hop: 0, expanded: false }]]);
  const edges = new Map<string, TraceEdge>();
  // Per transfer key, how many of it the graph holds: a node sees every transfer
  // it took part in, so the most any one node saw in a transaction is the count.
  const counted = new Map<string, number>();
  let stoppedBy: FundFlowGraph["stoppedBy"];
  const graph = () => ({
    root,
    nodes: [...nodes.values()],
    edges: [...edges.values()],
    ...(stoppedBy !== undefined && { stoppedBy }),
  });
  if (root === options.target) return (stoppedBy = "target"), graph();

  const queue = [root];
  for (let i = 0; i < queue.length; i++) {
    const node = nodes.get(queue[i]!)!;
    if (node.hop >= options.hops || options.stop.has(node.address)) continue;
    let history: readonly TracedTransaction[];
    try {
      history = await read(node.address);
    } catch (err) {
      if (!(err instanceof TraceBudgetError)) throw err;
      stoppedBy = "requests";
      break;
    }
    node.expanded = true;
    for (const { version, transfers } of history) {
      const inTransaction = new Map<string, number>();
      for (const transfer of transfers) {
        const { from, to, asset, amount } = transfer;
        if (from === undefined || to === undefined || from === to || amount < options.minAmount) continue;
        if (from !== node.address && to !== node.address) continue;
        if (options.asset !== undefined && assetKey(asset) !== options.asset) continue;
        const outward = from === node.address;
        if (options.direction === (outward ? "in" : "out")) continue;
        const key = `${version}|${from}|${to}|${asset}|${amount}`;
        const n = (inTransaction.get(key) ?? 0) + 1;
        inTransaction.set(key, n);
        if (n <= (counted.get(key) ?? 0)) continue;
        const other = outward ? to : from;
        if (!nodes.has(other)) {
          if (nodes.size >= options.maxNodes) return (stoppedBy = "nodes"), graph();
          nodes.set(other, { address: other, hop: node.hop + 1, expanded: false });
          queue.push(other);
        }
        counted.set(key, n);
        addEdge(edges, from, to, asset, amount, version);
        if (other === options.target) return (stoppedBy = "target"), graph();
      }
    }
  }
  return graph();
}

function addEdge(edges: Map<string, TraceEdge>, from: string, to: string, asset: string, amount: bigint, version: bigint): void {
  const key = `${from}|${to}|${asset}`;
  const edge = edges.get(key);
  if (edge === undefined) {
    edges.set(key, { from, to, asset, amount, transfers: 1, firstVersion: version, lastVersion: version });
    return;
  }
  edge.amount += amount;
  edge.transfers++;
  // Each node's history is read in ascending order, and the first end read sees every transfer on the edge.
  edge.lastVersion = version;
}

/**
 * The fewest edges funds could have taken from `from` to `to` inside `graph`,
 * following each edge's direction, or `undefined` when no chain of edges
 * connects them; `[]` when they are the same address.
 */
export function tracePath(graph: FundFlowGraph, from: string, to: string): TraceEdge[] | undefined {
  const [start, end] = [requireAddress(from), requireAddress(to)];
  const out = new Map<string, TraceEdge[]>();
  for (const edge of graph.edges) out.set(edge.from, [...(out.get(edge.from) ?? []), edge]);
  const via = new Map<string, TraceEdge | null>([[start, null]]);
  const queue = [start];
  for (let i = 0; i < queue.length && !via.has(end); i++) {
    for (const edge of out.get(queue[i]!) ?? []) {
      if (via.has(edge.to)) continue;
      via.set(edge.to, edge);
      queue.push(edge.to);
    }
  }
  if (!via.has(end)) return undefined;
  const path: TraceEdge[] = [];
  for (let edge = via.get(end); edge; edge = via.get(edge.from)) path.unshift(edge);
  return path;
}

const DIRECTIONS = new Set(["out", "in", "both"]);

function count(value: unknown, name: string, fallback: number, max = Number.MAX_SAFE_INTEGER): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > max) {
    throw new StreamdError(`trace ${name} must be an integer from 1 to ${max}`);
  }
  return value as number;
}

/** `options` checked at the call, addresses in long form. */
export function walkOptions(options: TraceOptions): WalkOptions & { maxRequests: number } {
  if (typeof options !== "object" || options === null) throw new StreamdError("trace options must be an object");
  const { direction = "both", minAmount = 0n, stop = [], target, fromVersion, toVersion } = options;
  if (!DIRECTIONS.has(direction)) throw new StreamdError(`trace direction must be "out", "in" or "both"`);
  if (typeof minAmount !== "bigint" || minAmount < 0n) throw new StreamdError("trace minAmount must be a non-negative bigint");
  if (fromVersion !== undefined && typeof fromVersion !== "bigint") throw new StreamdError("trace fromVersion must be a bigint");
  if (toVersion !== undefined && toVersion !== "head" && (typeof toVersion !== "bigint" || toVersion < 0n)) {
    throw new StreamdError(`trace toVersion must be a non-negative bigint or "head"`);
  }
  if (!Array.isArray(stop)) throw new StreamdError("trace stop must be a list of addresses");
  return {
    hops: count(options.hops, "hops", 1, 6),
    direction,
    minAmount,
    ...(options.asset !== undefined && { asset: requireAsset(options.asset) }),
    maxNodes: count(options.maxNodes, "maxNodes", 100),
    maxRequests: count(options.maxRequests, "maxRequests", 1000),
    stop: new Set(stop.map((a) => requireAddress(a))),
    ...(target !== undefined && { target: requireAddress(target) }),
  };
}

/** The head through `limiter`, retried as a query retries a page. */
async function headOf(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  descriptor: AccountEventDescriptor,
  retries: number,
  signal: AbortSignal | undefined,
): Promise<bigint> {
  for (let attempt = 0; ; attempt++) {
    try {
      const head = await limiter.run(() => adapter.getHeadVersion!(descriptor), signal, signal);
      if (typeof head !== "bigint" || head < 0n) throw new StreamdError(`getHeadVersion returned ${String(head)}`);
      return head;
    } catch (err) {
      if (!(err instanceof StreamdNetworkError) || attempt >= retries || signal?.aborted) throw err;
      await new Promise((resolve) => setTimeout(resolve, Math.min(500 * 2 ** attempt, 10_000)));
    }
  }
}

/**
 * The trace behind `AptosClient.trace`: every node read as `getActivity` reads
 * it, one at a time, over one range fixed at the start, through `limiter`.
 */
export async function trace(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  root: () => Promise<string>,
  options: TraceOptions,
  requestTimeout: number | undefined,
): Promise<FundFlowGraph> {
  const walk = walkOptions(options);
  const counted = countedAdapter(adapter, walk.maxRequests);
  const { retries, signal } = options;
  const address = await root();
  let { fromVersion, toVersion = "head" } = options;
  if (toVersion === "head" || (fromVersion !== undefined && fromVersion < 0n)) {
    if (counted.adapter.getHeadVersion === undefined) {
      throw new StreamdError(`trace needs an adapter with getHeadVersion, for a toVersion of "head" or a negative fromVersion`);
    }
    // Read once, so every node is read over the same range.
    const head = await headOf(counted.adapter, limiter, activityDescriptor(address), retries ?? 0, signal);
    if (toVersion === "head") toVersion = head;
    if (fromVersion !== undefined && fromVersion < 0n) fromVersion = head + fromVersion < 0n ? 0n : head + fromVersion;
  }
  // One source for the whole trace, so its store reads are memoized across nodes;
  // nodes are read one at a time, as one getActivity reads its pages.
  let node = address;
  const source = activitySource(counted.adapter, () => Promise.resolve(node), false, requestTimeout);
  const read: TraceReader = async (next) => {
    node = next;
    const records = await new EventQuery(source, limiter).getEvents<Activity>(activityDescriptor(node), {
      ...(fromVersion !== undefined && { fromVersion }),
      toVersion,
      ...(retries !== undefined && { retries }),
      ...(signal !== undefined && { signal }),
    });
    if (counted.refused()) throw new TraceBudgetError(`trace spent its maxRequests of ${walk.maxRequests}`);
    return records.map(({ version, data }) => ({ version, transfers: data.transfers }));
  };
  const graph = await walkTransfers(read, address, walk);
  return { ...graph, requests: counted.requests() };
}
