import { sum } from "@streamd/core";
import type { AlertAnomalyRule, AlertThreshold, EventDescriptor } from "@streamd/core";
import { K_PATTERN } from "./anomaly.js";
import { descriptor } from "./args.js";
import { parseDecode, parseFilter } from "./filter.js";
import type { CliFilter } from "./filter.js";

/** A band threshold over the rule's previous closed windows; `k` is a normalised decimal string. */
export type Anomaly = AlertAnomalyRule;

/** The alert payload echoes the threshold, so the rule's type is the payload's. */
export type Threshold = AlertThreshold;

/** A record source in place of a descriptor; names and values are checked by the client. */
export type AlertSource =
  | { kind: "transfers"; asset?: string; account?: string }
  | { kind: "swaps"; protocol?: string[]; asset?: string }
  | { kind: "sales"; marketplace?: string[]; token?: string };

export interface AlertRule {
  name: string;
  /** Exactly one of `descriptor` and `source` is set. */
  descriptor?: EventDescriptor;
  source?: AlertSource;
  filter?: CliFilter;
  decode?: string[];
  /** Window length in microseconds of chain time. */
  window: bigint;
  threshold: Threshold;
  /** Identifies what the rule counts, so a saved window is never resumed under a different rule. */
  fingerprint: string;
}

const ANOMALY_KEYS = new Set(["metric", "sum", "k", "baseline", "minBaseline", "direction"]);
const DIRECTIONS = new Set(["above", "below", "both"]);
const RULE_KEYS = new Set(["name", "descriptor", "source", "where", "sender", "function", "decode", "window", "threshold"]);
const DESCRIPTOR_KEYS = new Set(["eventType", "account", "resource"]);
const HANDLE_KEYS = new Set(["handle", "field", "creationNumber"]);
/** Each source's narrowing fields, and whether the field takes a list; then the record fields a sum may read. */
const SOURCES: Record<AlertSource["kind"], { fields: Record<string, boolean>; sums: string[] }> = {
  transfers: { fields: { asset: false, account: false }, sums: ["amount"] },
  swaps: { fields: { protocol: true, asset: false }, sums: ["amountIn", "amountOut"] },
  sales: { fields: { marketplace: true, token: false }, sums: ["price", "royalty", "fee"] },
};
const WINDOW = /^([1-9]\d*)([smhd])$/;
const US_PER: Record<string, bigint> = { s: 1_000_000n, m: 60_000_000n, h: 3_600_000_000n, d: 86_400_000_000n };

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function strings(value: unknown, field: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || !value.every((v) => typeof v === "string")) {
    throw new Error(`${field}: must be an array of strings`);
  }
  return value;
}

const reason = (err: unknown) => (err instanceof Error ? err.message : String(err));

function ruleDescriptor(raw: unknown, sui: boolean): EventDescriptor {
  if (!isObject(raw)) throw new Error("descriptor: must be an object");
  for (const key of Object.keys(raw)) {
    if (HANDLE_KEYS.has(key)) {
      throw new Error("descriptor: handle descriptors are not supported, since their events carry no timestamp");
    }
    if (!DESCRIPTOR_KEYS.has(key)) throw new Error(`descriptor: unknown field ${JSON.stringify(key)}`);
  }
  const { eventType, account, resource } = raw;
  const types = typeof eventType === "string" ? [eventType] : eventType;
  if (types !== undefined && (!Array.isArray(types) || types.length === 0 || !types.every((t) => typeof t === "string"))) {
    throw new Error("descriptor: eventType must be a string or a non-empty array of strings");
  }
  for (const [key, value] of [["account", account], ["resource", resource]] as const) {
    if (value !== undefined && typeof value !== "string") throw new Error(`descriptor: ${key} must be a string`);
  }
  if (resource !== undefined && (account === undefined || types !== undefined)) {
    throw new Error("descriptor: resource needs account, and no eventType");
  }
  if (types === undefined && account === undefined) throw new Error("descriptor: needs eventType or account");
  try {
    return descriptor("a rule", { account: account as string | undefined, resource: resource as string | undefined }, types ?? [], sui);
  } catch (err) {
    throw new Error(`descriptor: ${reason(err).replace(/--(\w)/g, "$1")}`);
  }
}

function ruleSource(raw: unknown, sui: boolean): AlertSource {
  if (sui) throw new Error("source: record sources are Aptos only, so not available with --chain sui");
  if (!isObject(raw)) throw new Error("source: must be an object");
  const { kind } = raw;
  if (typeof kind !== "string" || !Object.hasOwn(SOURCES, kind)) throw new Error('source: kind must be "transfers", "swaps" or "sales"');
  const { fields } = SOURCES[kind as AlertSource["kind"]];
  const source: Record<string, unknown> = { kind };
  for (const [key, value] of Object.entries(raw)) {
    if (key === "kind") continue;
    if (!Object.hasOwn(fields, key)) throw new Error(`source: unknown field ${JSON.stringify(key)} for ${kind}`);
    const list = fields[key]! && Array.isArray(value) ? value : [value];
    if (list.length === 0 || !list.every((v) => typeof v === "string" && v !== "")) {
      throw new Error(`source: ${key} must be a non-empty string${fields[key] ? " or a non-empty array of them" : ""}`);
    }
    source[key] = fields[key] ? list : list[0];
  }
  return source as AlertSource;
}

function window(raw: unknown): bigint {
  const match = typeof raw === "string" ? WINDOW.exec(raw) : null;
  if (match === null) throw new Error("window: must be a duration such as 30s, 5m, 1h or 1d");
  return BigInt(match[1]!) * US_PER[match[2]!]!;
}

function integer(raw: unknown): bigint | undefined {
  if (typeof raw === "number") return Number.isSafeInteger(raw) ? BigInt(raw) : undefined;
  return typeof raw === "string" && /^-?\d+$/.test(raw) ? BigInt(raw) : undefined;
}

function sumPath(raw: unknown, at: string): string {
  if (typeof raw !== "string") throw new Error(`${at}sum must be a field path`);
  try {
    sum(raw);
  } catch (err) {
    throw new Error(`${at}sum: ${reason(err)}`);
  }
  return raw;
}

function bandK(raw: unknown): string {
  const text = raw === undefined ? "3" : typeof raw === "number" ? String(raw) : raw;
  if (typeof text !== "string" || !K_PATTERN.test(text) || !/[1-9]/.test(text)) {
    throw new Error("threshold: anomaly: k must be a positive decimal with at most 4 fractional digits");
  }
  return text.includes(".") ? text.replace(/\.?0+$/, "") : text;
}

function windows(raw: unknown, fallback: number, min: number, max: number, field: string, range: string): number {
  const value = raw === undefined ? fallback : raw;
  if (typeof value !== "number" || !Number.isInteger(value) || value < min || value > max) {
    throw new Error(`threshold: anomaly: ${field} must be an integer from ${range}`);
  }
  return value;
}

function anomaly(raw: unknown): Anomaly {
  const at = "threshold: anomaly: ";
  if (!isObject(raw)) throw new Error(`${at}must be an object`);
  for (const key of Object.keys(raw)) {
    if (!ANOMALY_KEYS.has(key)) throw new Error(`${at}unknown field ${JSON.stringify(key)}`);
  }
  const { metric, direction = "both" } = raw;
  if (metric !== "count" && metric !== "sum") throw new Error(`${at}metric must be "count" or "sum"`);
  if (metric === "count" && raw.sum !== undefined) throw new Error(`${at}sum is only for metric "sum"`);
  const path = metric === "sum" ? sumPath(raw.sum, at) : undefined;
  const k = bandK(raw.k);
  const baseline = windows(raw.baseline, 30, 3, 1000, "baseline", "3 to 1000");
  const minBaseline = windows(raw.minBaseline, Math.min(10, baseline), 1, baseline, "minBaseline", `1 to baseline (${baseline})`);
  if (typeof direction !== "string" || !DIRECTIONS.has(direction)) throw new Error(`${at}direction must be "above", "below" or "both"`);
  return {
    metric,
    ...(path !== undefined && { sum: path }),
    k,
    baseline,
    minBaseline,
    direction: direction as Anomaly["direction"],
  };
}

function threshold(raw: unknown): Threshold {
  if (!isObject(raw)) throw new Error("threshold: must be an object");
  const keys = Object.keys(raw).sort().join(",");
  if (keys === "count") {
    const count = typeof raw.count === "number" ? integer(raw.count) : undefined;
    if (count === undefined || count < 1n) throw new Error("threshold: count must be a positive integer");
    return { count };
  }
  if (keys === "anomaly") return { anomaly: anomaly(raw.anomaly) };
  if (keys === "gte,sum") {
    const path = sumPath(raw.sum, "threshold: ");
    const gte = integer(raw.gte);
    if (gte === undefined) throw new Error("threshold: gte must be an integer, as a decimal string or a safe integer");
    return { sum: path, gte };
  }
  throw new Error('threshold: must be { "count": n }, { "sum": "<path>", "gte": "<integer>" } or { "anomaly": { ... } }');
}

function rule(raw: unknown, sui: boolean): AlertRule {
  if (!isObject(raw)) throw new Error("must be an object");
  for (const key of Object.keys(raw)) {
    if (!RULE_KEYS.has(key)) throw new Error(`unknown field ${JSON.stringify(key)}`);
  }
  if ((raw.descriptor === undefined) === (raw.source === undefined)) throw new Error("needs exactly one of descriptor and source");
  for (const key of ["window", "threshold"]) {
    if (raw[key] === undefined) throw new Error(`${key}: is required`);
  }
  // `source` takes the descriptor's place, so a descriptor rule's fingerprint is unchanged.
  const parsed: Omit<AlertRule, "fingerprint"> = {
    name: raw.name as string,
    ...(raw.source === undefined ? { descriptor: ruleDescriptor(raw.descriptor, sui) } : { source: ruleSource(raw.source, sui) }),
    window: window(raw.window),
    threshold: threshold(raw.threshold),
  };
  if (parsed.source !== undefined) {
    const { threshold: t } = parsed;
    const summed = "anomaly" in t ? t.anomaly.sum : "sum" in t ? t.sum : undefined;
    const { sums } = SOURCES[parsed.source.kind];
    if (summed !== undefined && !sums.includes(summed)) {
      const at = "anomaly" in t ? "threshold: anomaly: sum" : "threshold: sum";
      throw new Error(`${at}: ${parsed.source.kind} records sum ${sums.map((f) => JSON.stringify(f)).join(" or ")}, not ${JSON.stringify(summed)}`);
    }
    if (raw.decode !== undefined) throw new Error("decode: not for a source, whose record amounts are already integers");
  }
  const flags = { sender: strings(raw.sender, "sender"), function: strings(raw.function, "function"), where: strings(raw.where, "where") };
  const decodeRaw = strings(raw.decode, "decode");
  try {
    const filter = parseFilter(flags);
    if (filter !== undefined) parsed.filter = filter;
  } catch (err) {
    // The shared parsers name the flag; here the field has the flag's name without the dashes.
    throw new Error(reason(err).replace(/^--/, ""));
  }
  try {
    const decode = parseDecode(decodeRaw);
    if (decode !== undefined) parsed.decode = decode;
  } catch (err) {
    throw new Error(reason(err).replace(/^--/, ""));
  }
  // The name already keys the saved entry; JSON drops it as undefined.
  const fingerprint = JSON.stringify({ ...parsed, name: undefined }, (_key, value: unknown) => (typeof value === "bigint" ? value.toString() : value));
  return { ...parsed, fingerprint };
}

/** The rules in `text`, the contents of `file`; throws a message naming the file, the rule and the field. */
export function parseRules(text: string, file: string, sui = false): AlertRule[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`${file}: not valid JSON`);
  }
  if (!isObject(parsed)) throw new Error(`${file}: must be a JSON object with "rules"`);
  for (const key of Object.keys(parsed)) {
    if (key !== "rules") throw new Error(`${file}: unknown field ${JSON.stringify(key)}`);
  }
  const { rules } = parsed;
  if (!Array.isArray(rules) || rules.length === 0) throw new Error(`${file}: "rules" must be a non-empty array`);
  const names = new Set<string>();
  return rules.map((raw: unknown, i) => {
    const name = isObject(raw) ? raw.name : undefined;
    if (typeof name !== "string" || name === "") throw new Error(`${file}: rule #${i + 1}: name: must be a non-empty string`);
    if (names.has(name)) throw new Error(`${file}: rule #${i + 1}: name: ${JSON.stringify(name)} is used by an earlier rule`);
    names.add(name);
    try {
      return rule(raw, sui);
    } catch (err) {
      throw new Error(`${file}: rule ${JSON.stringify(name)} (#${i + 1}): ${reason(err)}`);
    }
  });
}
