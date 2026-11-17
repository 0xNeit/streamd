import type { StreamdClient, StreamdEvent } from "@streamd/core";
import type { TailOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
import { filterPredicate } from "./filter.js";
import { formatEvent, message, prettyEvent } from "./format.js";

export interface TailIO {
  client: Pick<StreamdClient, "stream">;
  /** Writes one output line; a returned promise is awaited, for backpressure. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the tail with exit code 0. */
  signal?: AbortSignal;
}

const TAIL_CURSOR_PREFIX = "streamd-tail:";

interface TailCheckpoint {
  version: bigint;
  eventIndex: number;
  sequenceNumber: bigint;
  type: string;
  guid?: { accountAddress: string; creationNumber: bigint };
}

interface ParsedTailCursor {
  streamCursor?: string;
  skipUntil?: TailCheckpoint;
}

function parseTailCursor(raw: string | undefined): ParsedTailCursor {
  if (raw === undefined) return {};
  if (!raw.startsWith(TAIL_CURSOR_PREFIX)) return { streamCursor: raw };
  const parsed = JSON.parse(raw.slice(TAIL_CURSOR_PREFIX.length)) as Record<string, unknown>;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("invalid tail cursor payload");
  }
  const { c, v, i, s, t, g } = parsed;
  if (c !== undefined && (typeof c !== "string" || c.length === 0)) throw new Error("invalid tail cursor");
  if (typeof v !== "string" || !/^\d+$/.test(v) || typeof s !== "string" || !/^\d+$/.test(s)) {
    throw new Error("invalid tail cursor version");
  }
  if (typeof i !== "number" || !Number.isInteger(i) || i < 0 || typeof t !== "string" || t.length === 0) {
    throw new Error("invalid tail cursor event");
  }
  let guid: TailCheckpoint["guid"];
  if (g !== undefined) {
    const obj = g as Record<string, unknown>;
    if (typeof g !== "object" || g === null || typeof obj.a !== "string" || typeof obj.n !== "string" || !/^\d+$/.test(obj.n)) {
      throw new Error("invalid tail cursor guid");
    }
    guid = { accountAddress: obj.a, creationNumber: BigInt(obj.n) };
  }
  return {
    ...(c !== undefined && { streamCursor: c }),
    skipUntil: { version: BigInt(v), eventIndex: i, sequenceNumber: BigInt(s), type: t, ...(guid !== undefined && { guid }) },
  };
}

function encodeTailCursor(streamCursor: string | undefined, event: StreamdEvent): string {
  return (
    TAIL_CURSOR_PREFIX +
    JSON.stringify({
      ...(streamCursor !== undefined && { c: streamCursor }),
      v: String(event.version),
      i: event.eventIndex,
      s: String(event.sequenceNumber),
      t: event.type,
      ...(event.guid !== undefined && {
        g: { a: event.guid.accountAddress, n: String(event.guid.creationNumber) },
      }),
    })
  );
}

function sameGuid(a: StreamdEvent["guid"], b: TailCheckpoint["guid"]): boolean {
  if (a === undefined && b === undefined) return true;
  if (a === undefined || b === undefined) return false;
  return a.accountAddress === b.accountAddress && a.creationNumber === b.creationNumber;
}

function stepSkip(event: StreamdEvent, target: TailCheckpoint): "skip" | "match" | "past" {
  if (event.version > target.version) return "past";
  if (
    event.version === target.version &&
    event.eventIndex === target.eventIndex &&
    event.sequenceNumber === target.sequenceNumber &&
    event.type === target.type &&
    sameGuid(event.guid, target.guid)
  ) {
    return "match";
  }
  if (event.version === target.version && event.eventIndex > target.eventIndex && sameGuid(event.guid, target.guid)) {
    return "past";
  }
  return "skip";
}

function loadTailCursor(path: string | undefined, report: (err: unknown) => void): ParsedTailCursor | null {
  if (path === undefined) return {};
  try {
    return parseTailCursor(readCursor(path));
  } catch (err) {
    report(`cannot read cursor file: ${message(err)}`);
    return null;
  }
}

function tryWriteCursor(path: string, value: string, report: (err: unknown) => void): boolean {
  try {
    writeCursor(path, value);
    return true;
  } catch (err) {
    report(`cannot write cursor file: ${message(err)}`);
    return false;
  }
}

function openTailStream(options: TailOptions, parsed: ParsedTailCursor, io: TailIO, report: (err: unknown) => void) {
  const { fromVersion, fromTime, pollInterval, filter, decode } = options;
  const start =
    parsed.streamCursor !== undefined
      ? {
          cursor: parsed.streamCursor,
          ...(fromVersion !== undefined && { fromVersion }),
          ...(fromTime !== undefined && { fromTime }),
        }
      : parsed.skipUntil !== undefined
        ? { fromVersion: parsed.skipUntil.version }
        : {
            ...(fromVersion !== undefined && { fromVersion }),
            ...(fromTime !== undefined && { fromTime }),
          };
  return io.client.stream(options.descriptor, {
    ...start,
    ...(pollInterval !== undefined && { pollInterval }),
    ...(decode !== undefined && { decode }),
    ...(filter !== undefined && { where: filterPredicate(filter) }),
    signal: io.signal,
    onError: report,
  });
}

async function consumeTail(
  options: TailOptions,
  io: TailIO,
  parsed: ParsedTailCursor,
  report: (err: unknown) => void,
  getLastReported: () => unknown,
): Promise<number> {
  const stream = openTailStream(options, parsed, io, report);
  const render = options.format === "pretty" ? prettyEvent : formatEvent;
  let skipUntil = parsed.skipUntil;
  let printed = 0;
  let lastStreamCursor: string | undefined;
  let wrote = false;
  try {
    for await (const event of stream) {
      if (skipUntil !== undefined) {
        const step = stepSkip(event, skipUntil);
        skipUntil = step === "skip" ? skipUntil : undefined;
        if (step !== "past") continue;
      }
      await io.write(`${render(event)}\n`);
      lastStreamCursor = stream.cursor;
      wrote = true;
      if (options.cursorFile !== undefined && !tryWriteCursor(options.cursorFile, encodeTailCursor(lastStreamCursor, event), report)) {
        return 1;
      }
      if (options.limit !== undefined && ++printed >= options.limit) break;
    }
  } catch (err) {
    if (!io.signal?.aborted) {
      // The error that stopped the subscription was the last one through onError.
      if (err !== getLastReported()) report(err);
      return 1;
    }
  }
  const advanced = stream.cursor !== undefined && (wrote ? stream.cursor !== lastStreamCursor : stream.cursor !== parsed.streamCursor);
  if (options.cursorFile !== undefined && advanced && !tryWriteCursor(options.cursorFile, stream.cursor!, report)) return 1;
  return 0;
}

/**
 * Follows `options.descriptor`, writing one line per event, and
 * resolves to the process exit code: 0 when the limit is reached or the
 * signal aborts, 1 when the subscription stopped on an error or a write failed.
 */
export async function runTail(options: TailOptions, io: TailIO): Promise<number> {
  let lastReported: unknown;
  const report = (err: unknown) => {
    lastReported = err;
    io.error(`streamd: ${typeof err === "string" ? err : message(err)}`);
  };
  const parsed = loadTailCursor(options.cursorFile, report);
  if (parsed === null) return 1;
  return consumeTail(options, io, parsed, report, () => lastReported);
}
