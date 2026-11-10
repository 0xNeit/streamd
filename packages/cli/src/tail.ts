import type { StreamdClient } from "@streamd/core";
import type { TailOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
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
  let saved: string | undefined;
  if (options.cursorFile !== undefined) {
    try {
      saved = readCursor(options.cursorFile);
    } catch (err) {
      report(`cannot read cursor file: ${message(err)}`);
      return 1;
    }
  }
  // Unset options are left out rather than passed as undefined, which would
  // override the client's own defaults when spread.
  const { fromVersion, fromTime, pollInterval } = options;
  const stream = io.client.stream(options.descriptor, {
    ...(saved !== undefined && { cursor: saved }),
    ...(fromVersion !== undefined && { fromVersion }),
    ...(fromTime !== undefined && { fromTime }),
    ...(pollInterval !== undefined && { pollInterval }),
    signal: io.signal,
    onError: report,
  });

  /** Saves the stream's position if it moved; false when the file cannot be written. */
  const checkpoint = (): boolean => {
    const cursor = stream.cursor;
    if (options.cursorFile === undefined || cursor === undefined || cursor === saved) return true;
    try {
      writeCursor(options.cursorFile, cursor);
      saved = cursor;
      return true;
    } catch (err) {
      report(`cannot write cursor file: ${message(err)}`);
      return false;
    }
  };

  const render = options.format === "pretty" ? prettyEvent : formatEvent;
  let printed = 0;
  let code = 0;
  try {
    for await (const event of stream) {
      await io.write(`${render(event)}\n`);
      if (!checkpoint()) return 1;
      if (options.limit !== undefined && ++printed >= options.limit) break;
    }
  } catch (err) {
    if (!io.signal?.aborted) {
      // The error that stopped the subscription was the last one through onError.
      if (err !== lastReported) report(err);
      code = 1;
    }
  }
  return checkpoint() ? code : 1;
}
