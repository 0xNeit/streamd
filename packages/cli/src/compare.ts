import { compareAdapters, serializeEvent } from "@streamd/core";
import type { AdapterComparison, EventAdapter, StreamdEvent } from "@streamd/core";
import type { CompareOptions } from "./args.js";
import { message } from "./format.js";

export interface CompareIO {
  /** The command's own node, then the one given by `--against`. */
  a: EventAdapter;
  b: EventAdapter;
  write(line: string): void;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
}

const event = (e: StreamdEvent): unknown => JSON.parse(serializeEvent(e));
const versions = (value: Record<string, bigint>) =>
  Object.fromEntries(Object.entries(value).map(([k, v]) => [k, v.toString()]));

function summary(report: AdapterComparison): unknown {
  return {
    kind: "summary",
    ok: report.ok,
    window: report.window === undefined ? null : versions(report.window),
    coverage: versions(report.coverage),
    matched: report.matched,
    pagesRead: report.pagesRead,
    errors: report.errors,
    truncated: report.truncated,
  };
}

/**
 * Compares the two nodes over the range, prints one JSON line per finding and
 * a summary, and resolves to the exit code: 0 when they agree over a shared
 * window, 1 otherwise.
 */
export async function runCompare(options: CompareOptions, io: CompareIO): Promise<number> {
  let report: AdapterComparison;
  try {
    report = await compareAdapters(io.a, io.b, options.descriptor, {
      fromVersion: options.fromVersion,
      toVersion: options.toVersion,
      ...(options.maxPages !== undefined && { maxPages: options.maxPages }),
      ...(options.limit !== undefined && { limit: options.limit }),
    });
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  const line = (value: unknown) => io.write(`${JSON.stringify(value)}\n`);
  for (const e of report.missing) line({ kind: "missing", event: event(e) });
  for (const e of report.extra) line({ kind: "extra", event: event(e) });
  for (const d of report.different) line({ kind: "different", id: d.id, fields: d.fields, a: event(d.a), b: event(d.b) });
  line(summary(report));
  return report.ok ? 0 : 1;
}
