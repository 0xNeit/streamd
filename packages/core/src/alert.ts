import { StreamdError } from "./errors.js";
import { parseEvent, serializeEvent } from "./serialize.js";
import type { StreamdEvent } from "./types.js";

/** The band rule of a `streamd alert` anomaly threshold, its defaults filled in. */
export interface AlertAnomalyRule {
  metric: "count" | "sum";
  /** The summed field path, with metric `sum` only. */
  sum?: string;
  /** A positive decimal with at most four fractional digits. */
  k: string;
  baseline: number;
  minBaseline: number;
  direction: "above" | "below" | "both";
}

/** The threshold of the rule an alert is about. */
export type AlertThreshold = { count: bigint } | { sum: string; gte: bigint } | { anomaly: AlertAnomalyRule };

/** How an anomaly alert's window left its band, as exact decimals with trailing zeros trimmed. */
export interface AlertAnomaly {
  side: "above" | "below";
  median: string;
  /** Unscaled, before `k * 1.4826`. */
  mad: string;
  low: string;
  high: string;
  baselineWindows: number;
}

/** One `streamd alert` delivery; `id` is `<rule>:<windowStart>`, the same on every redelivery. */
export interface AlertPayload {
  id: string;
  rule: string;
  /** Microseconds since the Unix epoch, as `windowEnd`. */
  windowStart: bigint;
  windowEnd: bigint;
  threshold: AlertThreshold;
  /** The window's count or sum. */
  value: bigint;
  /** Exactly when the threshold is an anomaly. */
  anomaly?: AlertAnomaly;
  /** The event that crossed the threshold. */
  event: StreamdEvent;
}

function thresholdJson(threshold: AlertThreshold): unknown {
  if ("count" in threshold) return { count: `${threshold.count}` };
  if ("gte" in threshold) return { sum: threshold.sum, gte: `${threshold.gte}` };
  const { metric, sum, k, baseline, minBaseline, direction } = threshold.anomaly;
  return {
    anomaly: { metric, ...(sum !== undefined && { sum }), k, baseline: `${baseline}`, minBaseline: `${minBaseline}`, direction },
  };
}

/** The JSON body `streamd alert` POSTs: integers as decimal strings, the event as `serializeEvent` writes it. */
export function serializeAlert(alert: AlertPayload): string {
  const { anomaly } = alert;
  const head = JSON.stringify({
    id: alert.id,
    rule: alert.rule,
    windowStart: `${alert.windowStart}`,
    windowEnd: `${alert.windowEnd}`,
    threshold: thresholdJson(alert.threshold),
    value: `${alert.value}`,
    ...(anomaly !== undefined && { anomaly: { ...anomaly, baselineWindows: `${anomaly.baselineWindows}` } }),
  });
  // The event goes in as serializeEvent writes it, so its bigints keep their fixed form.
  return `${head.slice(0, -1)},"event":${serializeEvent(alert.event)}}`;
}

const UNSIGNED = /^(?:0|[1-9]\d*)$/;
const SIGNED = /^-?(?:0|[1-9]\d*)$/;
const DECIMAL = /^-?(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/;
const K = /^(?:0|[1-9]\d*)(?:\.\d{0,3}[1-9])?$/;

type Fields = Record<string, unknown>;

const fail = (field: string, expected: string): never => {
  throw new StreamdError(`Cannot parse alert: "${field}" must be ${expected}`);
};

function object(value: unknown, field: string, keys: readonly string[]): Fields {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return fail(field, "an object");
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) fail(`${field}.${key}`, "absent (unknown field)");
  }
  return value as Fields;
}

function text(fields: Fields, key: string, at: string): string {
  const value = fields[key];
  return typeof value === "string" && value !== "" ? value : fail(at, "a non-empty string");
}

function integer(fields: Fields, key: string, at: string, pattern: RegExp, expected: string): bigint {
  const value = fields[key];
  return typeof value === "string" && pattern.test(value) ? BigInt(value) : fail(at, expected);
}

function count(fields: Fields, key: string, at: string, min: number, max: number): number {
  const value = integer(fields, key, at, UNSIGNED, `a decimal string from ${min} to ${max}`);
  return value >= BigInt(min) && value <= BigInt(max) ? Number(value) : fail(at, `a decimal string from ${min} to ${max}`);
}

function oneOf<T extends string>(fields: Fields, key: string, at: string, values: readonly T[]): T {
  const value = fields[key];
  return values.includes(value as T) ? (value as T) : fail(at, values.map((v) => JSON.stringify(v)).join(" or "));
}

const ANOMALY_RULE_KEYS = ["metric", "sum", "k", "baseline", "minBaseline", "direction"];

function anomalyRule(value: unknown): AlertAnomalyRule {
  const at = "threshold.anomaly";
  const raw = object(value, at, ANOMALY_RULE_KEYS);
  const metric = oneOf(raw, "metric", `${at}.metric`, ["count", "sum"] as const);
  if (metric === "count" && raw.sum !== undefined) fail(`${at}.sum`, 'absent with metric "count"');
  const sum = metric === "sum" ? text(raw, "sum", `${at}.sum`) : undefined;
  const k = raw.k;
  if (typeof k !== "string" || !K.test(k) || !/[1-9]/.test(k)) fail(`${at}.k`, "a positive decimal with at most 4 fractional digits");
  const baseline = count(raw, "baseline", `${at}.baseline`, 3, 1000);
  const minBaseline = count(raw, "minBaseline", `${at}.minBaseline`, 1, baseline);
  const direction = oneOf(raw, "direction", `${at}.direction`, ["above", "below", "both"] as const);
  return { metric, ...(sum !== undefined && { sum }), k: k as string, baseline, minBaseline, direction };
}

function threshold(value: unknown): AlertThreshold {
  const raw = object(value, "threshold", ["count", "sum", "gte", "anomaly"]);
  const keys = Object.keys(raw).sort().join(",");
  if (keys === "count") {
    const n = integer(raw, "count", "threshold.count", UNSIGNED, "a positive decimal string");
    return n > 0n ? { count: n } : fail("threshold.count", "a positive decimal string");
  }
  if (keys === "gte,sum") {
    return { sum: text(raw, "sum", "threshold.sum"), gte: integer(raw, "gte", "threshold.gte", SIGNED, "an integer as a decimal string") };
  }
  if (keys === "anomaly") return { anomaly: anomalyRule(raw.anomaly) };
  return fail("threshold", '{ "count" }, { "sum", "gte" } or { "anomaly" }');
}

function anomaly(value: unknown, rule: AlertAnomalyRule): AlertAnomaly {
  const raw = object(value, "anomaly", ["side", "median", "mad", "low", "high", "baselineWindows"]);
  const side = oneOf(raw, "side", "anomaly.side", ["above", "below"] as const);
  const [median, mad, low, high] = (["median", "mad", "low", "high"] as const).map((key) => {
    const v = raw[key];
    return typeof v === "string" && DECIMAL.test(v) ? v : fail(`anomaly.${key}`, "an exact decimal string");
  }) as [string, string, string, string];
  const baselineWindows = count(raw, "baselineWindows", "anomaly.baselineWindows", rule.minBaseline, rule.baseline);
  return { side, median, mad, low, high, baselineWindows };
}

const PAYLOAD_KEYS = ["id", "rule", "windowStart", "windowEnd", "threshold", "value", "anomaly", "event"];

/**
 * Reads a body `serializeAlert` wrote (a `streamd alert` delivery), refusing
 * anything else with a `StreamdError` that names the field.
 */
export function parseAlert(body: string): AlertPayload {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch (err) {
    throw new StreamdError("Cannot parse alert: not valid JSON", err);
  }
  const raw = object(parsed, "alert", PAYLOAD_KEYS);
  const rule = text(raw, "rule", "rule");
  const windowStart = integer(raw, "windowStart", "windowStart", UNSIGNED, "an unsigned decimal string");
  const id = text(raw, "id", "id");
  if (id !== `${rule}:${windowStart}`) fail("id", "<rule>:<windowStart>");
  const windowEnd = integer(raw, "windowEnd", "windowEnd", UNSIGNED, "an unsigned decimal string");
  if (windowEnd <= windowStart) fail("windowEnd", "after windowStart");
  const t = threshold(raw.threshold);
  const value = integer(raw, "value", "value", SIGNED, "an integer as a decimal string");
  if ("anomaly" in t ? raw.anomaly === undefined : raw.anomaly !== undefined) {
    fail("anomaly", "present exactly when the threshold is an anomaly");
  }
  const band = "anomaly" in t ? anomaly(raw.anomaly, t.anomaly) : undefined;
  if (raw.event === undefined) fail("event", "an event");
  let event: StreamdEvent;
  try {
    event = parseEvent(JSON.stringify(raw.event));
  } catch (err) {
    throw new StreamdError(`Cannot parse alert: "event": ${err instanceof Error ? err.message : String(err)}`, err);
  }
  return { id, rule, windowStart, windowEnd, threshold: t, value, ...(band !== undefined && { anomaly: band }), event };
}
