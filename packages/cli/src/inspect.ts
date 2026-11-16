import { StreamdNetworkError } from "@streamd/core";
import type { EventFilter, MoveEventType, ResourceDescriptor, StreamdClient, StreamdEvent } from "@streamd/core";
import type { InspectOptions } from "./args.js";
import { clock, message, printable } from "./format.js";

export interface InspectIO {
  client: Pick<StreamdClient, "iterateEvents">;
  /**
   * The resource's value at `version`, `null` when it did not exist then;
   * rejects with a 410 `StreamdNetworkError` when the version is pruned.
   */
  read(version: bigint): Promise<unknown>;
  /** Writes output; a returned promise is awaited, for backpressure. */
  write(text: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

const RETRIES = 5;

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

/** The value before the range, or `undefined` when the node has pruned it. */
async function baseline(options: InspectOptions, io: InspectIO, fromVersion: bigint): Promise<unknown> {
  if (fromVersion === 0n) return null;
  try {
    return await io.read(fromVersion - 1n);
  } catch (err) {
    if (!(err instanceof StreamdNetworkError && err.statusCode === 410)) throw err;
    const where = options.archive === undefined ? "this node; pass --archive to diff it" : "this node and the archive";
    io.error(`streamd: version ${fromVersion - 1n} is pruned on ${where}, so the first change is printed whole`);
    return undefined;
  }
}

/**
 * Prints the resource's value at `--at`, or each change over the range diffed
 * against the one before, and resolves to the exit code: 0 when finished or
 * when the signal aborts, 1 when a read or a write fails.
 */
export async function runInspect(options: InspectOptions, io: InspectIO): Promise<number> {
  const pretty = options.format === "pretty";
  try {
    if (options.at !== undefined) {
      await io.write(valueLine(options.at, await io.read(options.at), pretty));
      return 0;
    }
    const fromVersion = options.fromVersion ?? 0n;
    let before = await baseline(options, io, fromVersion);
    const descriptor: ResourceDescriptor = {
      kind: "resource",
      accountAddress: options.address,
      resourceType: options.resourceType as MoveEventType,
    };
    const filter: EventFilter = {
      fromVersion,
      toVersion: options.toVersion ?? "head",
      retries: RETRIES,
      signal: io.signal,
      ...(options.limit !== undefined && { limit: options.limit }),
    };
    for await (const event of io.client.iterateEvents(descriptor, filter)) {
      const record = changeRecord(event, before);
      await io.write(pretty ? prettyChange(record) : changeLine(record));
      before = event.data;
    }
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}
