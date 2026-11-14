import { sum } from "@streamd/core";
import type { EventDescriptor } from "@streamd/core";
import { descriptor } from "./args.js";
import { parseDecode, parseFilter } from "./filter.js";
import type { CliFilter } from "./filter.js";

export type Threshold = { count: bigint } | { sum: string; gte: bigint };

export interface AlertRule {
  name: string;
  descriptor: EventDescriptor;
  filter?: CliFilter;
  decode?: string[];
  /** Window length in microseconds of chain time. */
  window: bigint;
  threshold: Threshold;
  /** Identifies what the rule counts, so a saved window is never resumed under a different rule. */
  fingerprint: string;
}

const RULE_KEYS = new Set(["name", "descriptor", "where", "sender", "function", "decode", "window", "threshold"]);
const DESCRIPTOR_KEYS = new Set(["eventType", "account", "resource"]);
const HANDLE_KEYS = new Set(["handle", "field", "creationNumber"]);
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
    throw new Error(`descriptor: ${reason(err)}`);
  }
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

function threshold(raw: unknown): Threshold {
  if (!isObject(raw)) throw new Error("threshold: must be an object");
  const keys = Object.keys(raw).sort().join(",");
  if (keys === "count") {
    const count = typeof raw.count === "number" ? integer(raw.count) : undefined;
    if (count === undefined || count < 1n) throw new Error("threshold: count must be a positive integer");
    return { count };
  }
  if (keys === "gte,sum") {
    if (typeof raw.sum !== "string") throw new Error("threshold: sum must be a field path");
    try {
      sum(raw.sum);
    } catch (err) {
      throw new Error(`threshold: sum: ${reason(err)}`);
    }
    const gte = integer(raw.gte);
    if (gte === undefined) throw new Error("threshold: gte must be an integer, as a decimal string or a safe integer");
    return { sum: raw.sum, gte };
  }
  throw new Error('threshold: must be { "count": n } or { "sum": "<path>", "gte": "<integer>" }');
}

function rule(raw: unknown, sui: boolean): AlertRule {
  if (!isObject(raw)) throw new Error("must be an object");
  for (const key of Object.keys(raw)) {
    if (!RULE_KEYS.has(key)) throw new Error(`unknown field ${JSON.stringify(key)}`);
  }
  for (const key of ["descriptor", "window", "threshold"]) {
    if (raw[key] === undefined) throw new Error(`${key}: is required`);
  }
  const parsed: Omit<AlertRule, "fingerprint"> = {
    name: raw.name as string,
    descriptor: ruleDescriptor(raw.descriptor, sui),
    window: window(raw.window),
    threshold: threshold(raw.threshold),
  };
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
