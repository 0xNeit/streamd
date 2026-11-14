import { SAMPLES } from "./adapters/metrics.js";
import type { AdapterMetricsSnapshot, MethodMetrics } from "./adapters/metrics.js";
import { StreamdError } from "./errors.js";

export interface PrometheusSource {
  snapshot: AdapterMetricsSnapshot;
  /** Labels on this source's series, such as `{ node: "0" }`. */
  labels?: Readonly<Record<string, string>>;
}

export interface PrometheusOptions {
  /** Starts every metric name; default `streamd_adapter`. */
  prefix?: string;
  /** Labels on every series; a source's own label of the same name wins. */
  labels?: Readonly<Record<string, string>>;
}

type Label = [name: string, value: string];

const METRIC_NAME = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const LABEL_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const RESERVED = new Set(["method", "quantile"]);

const COUNTERS = [
  ["requests_total", "requests", "Adapter calls started, in-flight ones included."],
  ["errors_total", "errors", "Adapter calls that failed, rate-limited ones included."],
  ["rate_limited_total", "rateLimited", "Failed adapter calls with status 429 or a retry hint."],
] as const satisfies readonly (readonly [string, "requests" | "errors" | "rateLimited", string])[];

const QUANTILES = [
  ["0.5", "p50"],
  ["0.95", "p95"],
  ["0.99", "p99"],
  ["1", "max"],
] as const;

// A gauge, not a summary: the window is not cumulative and there is no sum or settled count.
const LATENCY_HELP = `Adapter call latency, nearest rank over the most recent ${SAMPLES} settled calls per method; quantile 1 is the maximum.`;

function isObject(value: unknown): value is object {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function labelsOf(value: unknown, where: string): Label[] {
  if (value === undefined) return [];
  if (!isObject(value)) throw new StreamdError(`toPrometheus: ${where} must be an object of label names to strings`);
  const labels = Object.entries(value);
  for (const [name, text] of labels) {
    if (!LABEL_NAME.test(name) || name.startsWith("__") || RESERVED.has(name)) {
      throw new StreamdError(`toPrometheus: not a usable label name: ${JSON.stringify(name)}`);
    }
    if (typeof text !== "string") throw new StreamdError(`toPrometheus: label ${name} must be a string`);
  }
  return labels as Label[];
}

/** Each source with its merged labels, refusing two that would write the same series. */
function sourcesOf(input: unknown, constant: Label[]): { snapshot: AdapterMetricsSnapshot; labels: Label[] }[] {
  if (!isObject(input) && !Array.isArray(input)) {
    throw new StreamdError("toPrometheus: expected a snapshot or a list of { snapshot, labels }");
  }
  const entries: unknown[] = Array.isArray(input) ? input : [{ snapshot: input }];
  const seen = new Set<string>();
  return entries.map((entry, i) => {
    if (!isObject(entry) || !isObject((entry as PrometheusSource).snapshot)) {
      throw new StreamdError(`toPrometheus: entry ${i} must be { snapshot, labels? } with a snapshot object`);
    }
    const { snapshot, labels } = entry as PrometheusSource;
    const merged = [...new Map([...constant, ...labelsOf(labels, `entry ${i} labels`)])];
    const key = JSON.stringify([...merged].sort(([a], [b]) => (a < b ? -1 : 1)));
    if (seen.has(key)) throw new StreamdError(`toPrometheus: entry ${i} repeats another entry's labels ${key}`);
    seen.add(key);
    return { snapshot, labels: merged };
  });
}

function escape(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

function sample(name: string, labels: Label[], value: number): string {
  return `${name}{${labels.map(([k, v]) => `${k}="${escape(v)}"`).join(",")}} ${value}\n`;
}

function methodsOf(snapshot: AdapterMetricsSnapshot): [string, MethodMetrics][] {
  return Object.entries(snapshot).filter((entry): entry is [string, MethodMetrics] => entry[1] !== undefined);
}

/**
 * Renders `adapterMetrics` snapshots as Prometheus text exposition format, one
 * family per metric with its HELP and TYPE once, however many sources there are.
 * Pass several as `{ snapshot, labels }` to tell a pool's nodes apart.
 */
export function toPrometheus(
  input: AdapterMetricsSnapshot | readonly PrometheusSource[],
  options: PrometheusOptions = {},
): string {
  const prefix = options.prefix ?? "streamd_adapter";
  if (typeof prefix !== "string" || !METRIC_NAME.test(prefix)) {
    throw new StreamdError(`toPrometheus: not a valid metric name prefix: ${JSON.stringify(prefix)}`);
  }
  const sources = sourcesOf(input, labelsOf(options.labels, "options.labels"));
  let out = "";
  const family = (name: string, type: string, help: string, samples: string[]) => {
    if (samples.length > 0) out += `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n${samples.join("")}`;
  };

  for (const [suffix, field, help] of COUNTERS) {
    const name = `${prefix}_${suffix}`;
    family(name, "counter", help, sources.flatMap(({ snapshot, labels }) =>
      methodsOf(snapshot).map(([method, metrics]) => sample(name, [...labels, ["method", method]], metrics[field])),
    ));
  }
  const name = `${prefix}_latency_seconds`;
  family(name, "gauge", LATENCY_HELP, sources.flatMap(({ snapshot, labels }) =>
    methodsOf(snapshot).flatMap(([method, { latency }]) =>
      latency === undefined
        ? []
        : QUANTILES.map(([quantile, key]) =>
            sample(name, [...labels, ["method", method], ["quantile", quantile]], latency[key] / 1000)),
    ),
  ));
  return out;
}
