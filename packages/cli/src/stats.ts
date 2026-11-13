import { adapterMetrics } from "@streamd/core";
import type { AdapterMetrics, EventAdapter } from "@streamd/core";

/**
 * `--stats`: one `adapterMetrics` per source a command reads, so the numbers
 * are calls that reached a node, not calls the pool, archive fallback or
 * cache answered or retried. Sources are named by position, never by URL,
 * since a URL may carry a key.
 */
export class RequestStats {
  private readonly nodes: AdapterMetrics[] = [];
  private archive: AdapterMetrics | undefined;

  /** `adapter`, counted as the next node or as the archive. */
  track<T extends EventAdapter>(adapter: T, role: "node" | "archive"): T {
    const metrics = adapterMetrics(adapter);
    if (role === "node") this.nodes.push(metrics);
    else this.archive = metrics;
    // The wrapper has exactly the methods `adapter` has.
    return metrics.adapter as T;
  }

  /** `{"stats":{"nodes":[...],"archive":{...}}}` and a newline, latencies in whole milliseconds. */
  line(): string {
    const stats = {
      nodes: this.nodes.map((n) => n.snapshot()),
      ...(this.archive !== undefined && { archive: this.archive.snapshot() }),
    };
    return `${JSON.stringify({ stats }, (_, value: unknown) => (typeof value === "number" ? Math.round(value) : value))}\n`;
  }
}

/** Calls `print` on each `SIGUSR1`, except on Windows, which has none; returns the remover. */
export function onStatsSignal(print: () => void, platform: NodeJS.Platform = process.platform): () => void {
  if (platform === "win32") return () => {};
  process.on("SIGUSR1", print);
  return () => process.off("SIGUSR1", print);
}
