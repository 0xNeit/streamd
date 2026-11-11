import { StreamdTransactionError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import type { AptosClient } from "@streamd/aptos";
import type { TxOptions } from "./args.js";
import { formatEvent, message } from "./format.js";

export interface TxIO {
  client: Pick<AptosClient, "eventsOf">;
  /** Writes one output line; a returned promise is awaited, for backpressure. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  signal: AbortSignal;
}

/**
 * Prints a transaction's events and resolves to the process exit code: 0 when
 * it succeeded (or the output closed), 1 when it failed, was not found or a
 * write failed, 130 when interrupted while waiting.
 */
export async function runTx(options: TxOptions, io: TxIO): Promise<number> {
  const { timeout } = options;
  let events: StreamdEvent[];
  try {
    events = await io.client.eventsOf(options.hash, undefined, {
      signal: io.signal,
      ...(timeout !== undefined && { timeout }),
    });
  } catch (err) {
    if (io.signal.aborted) return 130;
    if (err instanceof StreamdTransactionError) {
      io.error(`streamd: transaction failed at version ${err.version}: ${err.vmStatus}`);
    } else {
      io.error(`streamd: ${message(err)}`);
    }
    return 1;
  }
  try {
    for (const event of events) await io.write(`${formatEvent(event)}\n`);
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}
