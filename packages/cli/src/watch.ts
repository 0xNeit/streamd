import type { StreamdClient, StreamdEvent, Watch } from "@streamd/core";
import type { WatchCommandOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
import { filterPredicate } from "./filter.js";
import { formatEvent, message } from "./format.js";

export interface WatchIO {
  client: Pick<StreamdClient, "watch">;
  /** Writes one output line; a returned promise is awaited, for backpressure. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

/** The serialized event with the watched address first; `parseEvent` ignores the extra field. */
export function watchLine(event: StreamdEvent, address: string): string {
  return `{"address":${JSON.stringify(address)},${formatEvent(event).slice(1)}\n`;
}

function readPositions(path: string): Record<string, string> | undefined {
  const text = readCursor(path);
  if (text === undefined) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`not a JSON object of address to cursor: ${path}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`not a JSON object of address to cursor: ${path}`);
  }
  return parsed as Record<string, string>;
}

/**
 * Follows every address with its own walk, writing one line per event, and
 * resolves to the process exit code: 0 at the limit or on the signal; 1 when
 * every walk gave up, or a write or the cursor file failed.
 */
export function runWatch(options: WatchCommandOptions, io: WatchIO): Promise<number> {
  const report = (err: unknown) => io.error(`streamd: ${typeof err === "string" ? err : message(err)}`);
  const { cursorFile, fromVersion, fromTime, pollInterval, filter, decode, limit } = options;
  let positions: Record<string, string> | undefined;
  if (cursorFile !== undefined) {
    try {
      positions = readPositions(cursorFile);
    } catch (err) {
      report(`cannot read cursor file: ${message(err)}`);
      return Promise.resolve(1);
    }
  }
  if (io.signal.aborted) return Promise.resolve(0);

  let watch: Watch | undefined;
  let saved = positions === undefined ? undefined : JSON.stringify(positions);
  /** Saves every position if any moved; false when the file cannot be written. */
  const checkpoint = (): boolean => {
    if (cursorFile === undefined || watch === undefined) return true;
    const current = JSON.stringify(watch.positions);
    if (current === saved) return true;
    try {
      writeCursor(cursorFile, current);
      saved = current;
      return true;
    } catch (err) {
      report(`cannot write cursor file: ${message(err)}`);
      return false;
    }
  };

  return new Promise<number>((resolve) => {
    let done = false;
    const finish = (code: number) => {
      if (done) return;
      done = true;
      io.signal.removeEventListener("abort", aborted);
      watch?.unsubscribe();
      resolve(checkpoint() ? code : 1);
    };
    const aborted = () => finish(0);
    io.signal.addEventListener("abort", aborted);

    let printed = 0;
    // The walk awaits each handler, so awaiting the write holds back its next poll.
    const print = async (event: StreamdEvent, address: string) => {
      if (done) return;
      try {
        await io.write(watchLine(event, address));
      } catch (err) {
        if (!io.signal.aborted) report(err);
        return finish(io.signal.aborted ? 0 : 1);
      }
      if (!checkpoint()) return finish(1);
      if (limit !== undefined && ++printed >= limit) finish(0);
    };

    let following = new Set(options.addresses);
    try {
      watch = io.client.watch(options.addresses, print, {
        ...(positions !== undefined && { positions }),
        ...(fromVersion !== undefined && { fromVersion }),
        ...(fromTime !== undefined && { fromTime }),
        ...(pollInterval !== undefined && { pollInterval }),
        ...(decode !== undefined && { decode }),
        ...(filter !== undefined && { where: filterPredicate(filter) }),
        onError: (err) => {
          report(err);
          // watch drops a walk that gave up before reporting its error.
          const left = new Set(watch?.addresses ?? options.addresses);
          for (const address of following) {
            if (!left.has(address)) report(`stopped following ${address}`);
          }
          following = left;
          if (left.size === 0) finish(1);
        },
      });
    } catch (err) {
      report(err);
      finish(1);
    }
  });
}
