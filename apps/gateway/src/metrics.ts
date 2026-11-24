import { toPrometheus } from "@streamd/core";
import type { PrometheusSource } from "@streamd/core";

/** What the gateway's own gauges report. */
export interface GatewayCounts {
  topics: number;
  streams: { sse: number; websocket: number };
  webhooks: { active: number; failed: number };
  buffered: number;
}

export const METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

function gauge(name: string, help: string, samples: [labels: string, value: number][]): string {
  return `# HELP ${name} ${help}\n# TYPE ${name} gauge\n${samples.map(([labels, value]) => `${name}${labels} ${value}\n`).join("")}`;
}

/**
 * The gateway's gauges, then `sources` under `streamd_gateway_adapter`. Label
 * values are fixed here; per-topic series would carry caller-chosen descriptors.
 */
export function renderMetrics(counts: GatewayCounts, sources: readonly PrometheusSource[] = []): string {
  const prefix = "streamd_gateway";
  return (
    gauge(`${prefix}_topics`, "Descriptors followed.", [["", counts.topics]]) +
    gauge(`${prefix}_streams`, "Open streams by transport.", [
      ['{transport="sse"}', counts.streams.sse],
      ['{transport="websocket"}', counts.streams.websocket],
    ]) +
    gauge(`${prefix}_webhooks`, "Registered webhooks by status.", [
      ['{status="active"}', counts.webhooks.active],
      ['{status="failed"}', counts.webhooks.failed],
    ]) +
    gauge(`${prefix}_buffered_events`, "Events held across all topics for resuming streams.", [["", counts.buffered]]) +
    toPrometheus(sources, { prefix: `${prefix}_adapter` })
  );
}
