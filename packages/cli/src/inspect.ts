import type { StreamdEvent } from "@streamd/core";
import { clock, printable } from "./format.js";

/** One leaf that differs; a side is missing when the path does not exist there. */
export interface FieldChange {
  path: string;
  before?: unknown;
  after?: unknown;
}

/** One write or delete of the resource, as `inspect` prints it. */
export type ChangeRecord = {
  version: bigint;
  transactionHash?: string;
  timestamp?: bigint;
  sender?: string;
} & (
  | { change: "updated"; changes: FieldChange[] }
  /** `written`: the value before is unknown, so the new one is given whole. */
  | { change: "created" | "written"; value: unknown }
  | { change: "deleted" }
);

const json = (value: unknown, space?: number) =>
  JSON.stringify(value, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v), space) ?? "null";

function children(value: unknown): Map<string, unknown> | undefined {
  if (Array.isArray(value)) return new Map(value.map((v, i) => [String(i), v]));
  if (typeof value === "object" && value !== null) return new Map(Object.entries(value));
  return undefined;
}

/**
 * Every leaf that differs between two JSON values, by dotted path: objects by
 * key in sorted order, arrays by index. A change of shape is one change at
 * its path, with both values whole.
 */
export function diffValues(before: unknown, after: unknown, path = ""): FieldChange[] {
  const a = children(before);
  const b = children(after);
  if (a === undefined || b === undefined || Array.isArray(before) !== Array.isArray(after)) {
    return before === after ? [] : [{ path, before, after }];
  }
  const keys = [...new Set([...a.keys(), ...b.keys()])];
  if (!Array.isArray(before)) keys.sort();
  return keys.flatMap((key) => {
    const at = path === "" ? key : `${path}.${key}`;
    if (!a.has(key)) return [{ path: at, after: b.get(key) }];
    if (!b.has(key)) return [{ path: at, before: a.get(key) }];
    return diffValues(a.get(key), b.get(key), at);
  });
}

/**
 * The record for one change, given the value before it: `null` when the
 * resource did not exist, `undefined` when that is unknown.
 */
export function changeRecord(event: StreamdEvent, before: unknown): ChangeRecord {
  const { version, transactionHash, timestamp, sender } = event;
  const head = {
    version,
    ...(transactionHash !== undefined && { transactionHash }),
    ...(timestamp !== undefined && { timestamp }),
    ...(sender !== undefined && { sender }),
  };
  if (event.data === null) return { ...head, change: "deleted" };
  if (before === undefined) return { ...head, change: "written", value: event.data };
  if (before === null) return { ...head, change: "created", value: event.data };
  return { ...head, change: "updated", changes: diffValues(before, event.data) };
}

export function changeLine(record: ChangeRecord): string {
  return `${json(record)}\n`;
}

const side = (change: FieldChange, key: "before" | "after") => (key in change ? json(change[key]) : "(absent)");

/** A header line, then one indented line per changed path, or the whole value. */
export function prettyChange(record: ChangeRecord): string {
  const lines = [`${record.version}  ${clock(record.timestamp)}  ${record.change}`];
  if (record.change === "updated") {
    if (record.changes.length === 0) lines.push("  (no field changed)");
    for (const change of record.changes) {
      lines.push(`  ${change.path === "" ? "(value)" : change.path}: ${side(change, "before")} -> ${side(change, "after")}`);
    }
  } else if (record.change !== "deleted") {
    lines.push(`  ${json(record.value)}`);
  }
  return lines.map((line) => `${printable(line)}\n`).join("");
}

/** The `--at` output: `value` is `null` when the resource did not exist at `version`. */
export function valueLine(version: bigint, value: unknown, pretty: boolean): string {
  if (!pretty) return `${json({ version, value })}\n`;
  return `${json(value, 2).split("\n").map(printable).join("\n")}\n`;
}
