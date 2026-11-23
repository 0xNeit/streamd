import type { EventFilter, StreamdEvent, Subscription, SubscriptionOptions } from "@streamd/core";
import type { RecordWalkOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
import { formatEvent, message } from "./format.js";

export interface RecordsIO<C> {
  client: C;
  /** Writes one output line; a returned promise is awaited, for backpressure. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

/** How one command follows and reads its records; each adds its own narrowing. */
export interface RecordSource<T> {
  follow(callback: (record: StreamdEvent<T>) => Promise<void>, options: SubscriptionOptions): Subscription;
  range(filter: EventFilter): AsyncIterable<StreamdEvent<T>>;
}

const RETRIES = 5;

/**
 * Prints records as JSON lines and resolves to the process exit code:
 * followed until the limit or the signal (0), or read over a closed range
 * with `toVersion` (0 when finished); 1 when reading, a write or the cursor
 * file fails.
 */
export function runRecords<T>(options: RecordWalkOptions, source: RecordSource<T>, io: RecordsIO<unknown>): Promise<number> {
  return options.toVersion === undefined ? follow(options, source, io) : range(options, options.toVersion, source, io);
}

/** An array read as an async iterable, for clients with no iterator over a range. */
export async function* fromArray<T>(records: Promise<T[]>): AsyncGenerator<T, void, unknown> {
  yield* await records;
}

// Unset options are left out rather than passed as undefined, which would
// override the client's own defaults when spread.
function start({ fromVersion, fromTime }: RecordWalkOptions) {
  return {
    ...(fromVersion !== undefined && { fromVersion }),
    ...(fromTime !== undefined && { fromTime }),
  };
}

async function range<T>(options: RecordWalkOptions, toVersion: bigint | "head", source: RecordSource<T>, io: RecordsIO<unknown>): Promise<number> {
  const filter: EventFilter = {
    ...start(options),
    ...(options.limit !== undefined && { limit: options.limit }),
    toVersion,
    orderBy: options.order ?? "asc",
    retries: RETRIES,
    signal: io.signal,
  };
  try {
    for await (const record of source.range(filter)) await io.write(`${formatEvent(record)}\n`);
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}

async function follow<T>(options: RecordWalkOptions, source: RecordSource<T>, io: RecordsIO<unknown>): Promise<number> {
  const report = (err: unknown) => io.error(`streamd: ${typeof err === "string" ? err : message(err)}`);
  const { cursorFile, pollInterval, batchSize, limit } = options;
  let saved: string | undefined;
  if (cursorFile !== undefined) {
    try {
      saved = readCursor(cursorFile);
    } catch (err) {
      report(`cannot read cursor file: ${message(err)}`);
      return 1;
    }
  }
  if (io.signal.aborted) return 0;

  let sub: Subscription | undefined;
  /** Saves the subscription's position if it moved; false when the file cannot be written. */
  const checkpoint = (): boolean => {
    const cursor = sub?.cursor;
    if (cursorFile === undefined || cursor === undefined || cursor === saved) return true;
    try {
      writeCursor(cursorFile, cursor);
      saved = cursor;
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
      sub?.unsubscribe();
      resolve(checkpoint() ? code : 1);
    };
    const aborted = () => finish(0);
    io.signal.addEventListener("abort", aborted);
    let printed = 0;
    // The poller awaits each handler, so awaiting the write holds back the next poll.
    const print = async (record: StreamdEvent<T>) => {
      if (done) return;
      try {
        await io.write(`${formatEvent(record)}\n`);
      } catch (err) {
        if (!io.signal.aborted) report(err);
        return finish(io.signal.aborted ? 0 : 1);
      }
      if (!checkpoint()) return finish(1);
      if (limit !== undefined && ++printed >= limit) finish(0);
    };
    try {
      sub = source.follow(print, {
        ...start(options),
        ...(saved !== undefined && { cursor: saved }),
        ...(pollInterval !== undefined && { pollInterval }),
        ...(batchSize !== undefined && { batchSize }),
        signal: io.signal,
        onError: (err) => {
          report(err);
          // Only a subscription that has given up ends the command.
          if (sub !== undefined && !sub.active) finish(1);
        },
      });
    } catch (err) {
      report(err);
      finish(1);
    }
  });
}
