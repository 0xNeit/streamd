import { tracePath } from "@streamd/aptos";
import type { AptosClient, FundFlowGraph, TraceEdge, TraceOptions } from "@streamd/aptos";
import type { TraceCommandOptions } from "./args.js";
import { message } from "./format.js";

export interface TraceIO {
  client: Pick<AptosClient, "trace">;
  /** Writes the output; a returned promise is awaited, for backpressure. */
  write(text: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  signal: AbortSignal;
}

/** The trace's path between root and `target`, in the direction funds moved, or `null` when the graph has none. */
export function pathTo(graph: FundFlowGraph, target: string, direction: TraceCommandOptions["direction"] = "both"): TraceEdge[] | null {
  const out = direction === "in" ? undefined : tracePath(graph, graph.root, target);
  return out ?? (direction === "out" ? undefined : tracePath(graph, target, graph.root)) ?? null;
}

/** A DOT double-quoted string of `lines`: quotes and backslashes escaped, control characters dropped, DOT's own \n between lines. */
function quote(...lines: string[]): string {
  return `"${lines.map((line) => line.replace(/[\\"]/g, "\\$&").replace(/[\u0000-\u001f\u007f]/g, "")).join("\\n")}"`;
}

/** The graph as Graphviz DOT: the root drawn heavier, unexpanded nodes dashed, `path` edges bold. */
export function formatDot(graph: FundFlowGraph, path: readonly TraceEdge[] = []): string {
  const lines = [`digraph trace {`];
  if (graph.stoppedBy !== undefined) lines.push(`  comment=${quote(`stopped by ${graph.stoppedBy}`)};`);
  for (const node of graph.nodes) {
    const style = [node.address === graph.root && "penwidth=2", !node.expanded && "style=dashed"].filter(Boolean);
    lines.push(`  ${quote(node.address)} [label=${quote(node.address, `hop ${node.hop}`)}${style.map((s) => `, ${s}`).join("")}];`);
  }
  const bold = new Set(path);
  for (const edge of graph.edges) {
    const label = quote(edge.asset, `${edge.amount} (${edge.transfers} transfer${edge.transfers === 1 ? "" : "s"})`);
    lines.push(`  ${quote(edge.from)} -> ${quote(edge.to)} [label=${label}${bold.has(edge) ? ", style=bold" : ""}];`);
  }
  lines.push("}");
  return `${lines.join("\n")}\n`;
}

function json(value: unknown): string {
  return `${JSON.stringify(value, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v))}\n`;
}

function traceOptions(options: TraceCommandOptions, signal: AbortSignal): TraceOptions {
  const { hops, direction, fromVersion, toVersion, minAmount, asset, maxNodes, maxRequests, stop, target } = options;
  return {
    ...(hops !== undefined && { hops }),
    ...(direction !== undefined && { direction }),
    ...(fromVersion !== undefined && { fromVersion }),
    ...(toVersion !== undefined && { toVersion }),
    ...(minAmount !== undefined && { minAmount }),
    ...(asset !== undefined && { asset }),
    ...(maxNodes !== undefined && { maxNodes }),
    ...(maxRequests !== undefined && { maxRequests }),
    ...(stop !== undefined && { stop }),
    ...(target !== undefined && { target }),
    retries: 5,
    signal,
  };
}

/**
 * Traces the funds around an address and prints the graph as one JSON line
 * or as DOT, resolving to the exit code: 0 when printed (or the output
 * closed), 1 when the trace or a write failed, 130 when interrupted first.
 */
export async function runTrace(options: TraceCommandOptions, io: TraceIO): Promise<number> {
  let graph: FundFlowGraph;
  try {
    graph = await io.client.trace(options.address, traceOptions(options, io.signal));
  } catch (err) {
    if (io.signal.aborted) return 130;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  const path = options.target === undefined ? undefined : pathTo(graph, options.target, options.direction);
  try {
    await io.write(options.format === "dot" ? formatDot(graph, path ?? []) : json(path === undefined ? graph : { ...graph, path }));
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  return 0;
}
