import type { EventFilter, StreamdClient } from "@streamd/core";
import type { QueryOptions } from "./args.js";
import { formatEvent, message } from "./format.js";

export interface QueryIO {
  client: Pick<StreamdClient, "iterateEvents">;
  /** Writes one output line; a returned promise is awaited, for backpressure. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the query with exit code 0. */
  signal: AbortSignal;
}

const RETRIES = 5;

/**
 * Writes the range as JSON lines and resolves to the process exit code: 0 when
 * finished or when the signal aborts, 1 when the query or a write fails.
 */
export async function runQuery(options: QueryOptions, io: QueryIO): Promise<number> {
  const { fromVersion, fromTime, toVersion, limit, order } = options;
  const filter: EventFilter = {
    ...(fromVersion !== undefined && { fromVersion }),
    ...(fromTime !== undefined && { fromTime }),
    toVersion,
    orderBy: order,
    retries: RETRIES,
    signal: io.signal,
    ...(limit !== undefined && { limit }),
  };
  try {
    for await (const event of io.client.iterateEvents(options.descriptor, filter)) {
      await io.write(`${formatEvent(event)}\n`);
    }
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}
