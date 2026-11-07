import type { StreamdClient } from "../client.js";
import type { TailOptions } from "./args.js";
import { formatEvent, message } from "./format.js";

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
 * Follows `options.descriptor`, writing each event as a JSON line, and
 * resolves to the process exit code: 0 when the limit is reached or the
 * signal aborts, 1 when the subscription stopped on an error or a write failed.
 */
export async function runTail(options: TailOptions, io: TailIO): Promise<number> {
  let lastReported: unknown;
  const report = (err: unknown) => {
    lastReported = err;
    io.error(`streamd: ${message(err)}`);
  };
  // Unset options are left out rather than passed as undefined, which would
  // override the client's own defaults when spread.
  const { fromVersion, pollInterval } = options;
  const stream = io.client.stream(options.descriptor, {
    ...(fromVersion !== undefined && { fromVersion }),
    ...(pollInterval !== undefined && { pollInterval }),
    signal: io.signal,
    onError: report,
  });

  let printed = 0;
  try {
    for await (const event of stream) {
      await io.write(`${formatEvent(event)}\n`);
      if (options.limit !== undefined && ++printed >= options.limit) break;
    }
    return 0;
  } catch (err) {
    if (io.signal?.aborted) return 0;
    // The error that stopped the subscription was the last one through onError.
    if (err !== lastReported) report(err);
    return 1;
  }
}
