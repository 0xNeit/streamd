import type { AptosClient } from "@streamd/aptos";
import type { Balances } from "@streamd/core";
import type { BalancesCommandOptions } from "./args.js";
import { message } from "./format.js";

export interface BalancesIO {
  /** An `AptosClient` or a `SuiClient`, which take the same options here. */
  client: Pick<AptosClient, "balances">;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

function line(asset: string, balance: bigint, previous?: bigint, version?: bigint): string {
  return `${JSON.stringify({
    asset,
    balance: String(balance),
    ...(previous !== undefined && { previous: String(previous) }),
    ...(version !== undefined && { version: String(version) }),
  })}\n`;
}

/**
 * Prints each asset's balance once the first read finishes, then one JSON
 * line per change, and resolves to the process exit code: 0 at the limit or
 * on the signal; 1 when the first read fails, polling gives up, or a write
 * fails.
 */
export function runBalances(options: BalancesCommandOptions, io: BalancesIO): Promise<number> {
  const report = (err: unknown) => io.error(`streamd: ${message(err)}`);
  if (io.signal.aborted) return Promise.resolve(0);
  return new Promise<number>((resolve) => {
    let done = false;
    let running: Balances | undefined;
    const finish = (code: number) => {
      if (done) return;
      done = true;
      io.signal.removeEventListener("abort", aborted);
      running?.unsubscribe();
      resolve(code);
    };
    const aborted = () => finish(0);
    io.signal.addEventListener("abort", aborted);

    // onChange is not awaited by the client, so writes queue here in order.
    let queue = Promise.resolve();
    const print = (text: string, then?: () => void) => {
      queue = queue.then(async () => {
        if (done) return;
        try {
          await io.write(text);
        } catch (err) {
          if (!io.signal.aborted) report(err);
          return finish(io.signal.aborted ? 0 : 1);
        }
        then?.();
      });
    };

    const last = new Map<string, bigint>();
    let changes = 0;
    try {
      running = io.client.balances(options.address, {
        ...(options.asset !== undefined && { asset: options.asset }),
        ...(options.pollInterval !== undefined && { pollInterval: options.pollInterval }),
        signal: io.signal,
        onChange: (_all, change) => {
          const { asset, balance } = change.data;
          print(line(asset, balance, last.get(asset), change.version), () => {
            if (options.limit !== undefined && ++changes >= options.limit) finish(0);
          });
          last.set(asset, balance);
        },
        onError: (err) => {
          report(err);
          // Only balances that have stopped end the command; a failed poll is retried.
          if (running !== undefined && !running.active) finish(1);
        },
      });
    } catch (err) {
      report(err);
      return finish(1);
    }
    // The first poll is a timer, so this runs before any change.
    running.ready.then(
      (first) => {
        for (const [asset, balance] of first) {
          last.set(asset, balance);
          print(line(asset, balance));
        }
      },
      // A failed first read was reported through onError already.
      () => finish(io.signal.aborted ? 0 : 1),
    );
  });
}
