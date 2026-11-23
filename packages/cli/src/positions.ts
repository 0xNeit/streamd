import { deliver, webhookSink } from "@streamd/core";
import type { AptosClient, Fraction, LendingPosition, Positions } from "@streamd/aptos";
import type { PositionsCommandOptions } from "./args.js";
import { message } from "./format.js";

export interface PositionsIO {
  client: Pick<AptosClient, "positions">;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting abandons any in-flight POST and ends the command with exit code 0. */
  signal: AbortSignal;
  /** First retry wait in ms, doubling per attempt (default 1000). */
  retryDelay?: number;
}

const isFraction = (v: unknown): v is Fraction =>
  typeof v === "object" && v !== null && typeof (v as Fraction).numerator === "bigint" && typeof (v as Fraction).denominator === "bigint";

const toJson = (value: unknown) =>
  JSON.stringify(value, (_key, v: unknown) =>
    typeof v === "bigint" ? v.toString() : isFraction(v) ? `${v.numerator}/${v.denominator}` : v,
  );

/** One position as a JSON line, bigints as decimal strings and fractions as `"n/d"`. */
export function positionLine(position: LendingPosition): string {
  return `${toJson(position)}\n`;
}

/** The JSON body POSTed for a crossing; `id` is the same on every redelivery. */
export function alertBody(position: LendingPosition, threshold: Fraction): string {
  const side = position.below ? "below" : "above";
  return toJson({
    id: `${position.account}:${side}:${position.version}`,
    protocol: position.protocol,
    account: position.account,
    side,
    threshold,
    version: position.version,
    position,
  });
}

/**
 * Prints every tracked position once the start read finishes, then one line
 * per change, POSTing an alert per crossing when `to` is set, and resolves to
 * the exit code: 0 on the signal; 1 when the start read fails, the monitor
 * stops, a write fails, or an alert is not delivered past its retries.
 */
export function runPositions(options: PositionsCommandOptions, io: PositionsIO): Promise<number> {
  if (io.signal.aborted) return Promise.resolve(0);
  const { to, account, threshold, refreshInterval, fromVersion } = options;
  const sink = to === undefined ? undefined : webhookSink({ ...options, to });
  return new Promise<number>((resolve) => {
    let done = false;
    let running: Positions | undefined;
    let reported: unknown;
    const report = (err: unknown) => {
      // A start read that fails can reach onError and reject ready with the same error.
      if (err === reported) return;
      reported = err;
      io.error(`streamd: ${message(err)}`);
    };
    const finish = (code: number) => {
      if (done) return;
      done = true;
      io.signal.removeEventListener("abort", aborted);
      running?.unsubscribe();
      resolve(code);
    };
    const aborted = () => finish(0);
    io.signal.addEventListener("abort", aborted);

    // The client does not await its callbacks, so lines and alerts queue here in order.
    let queue = Promise.resolve();
    const enqueue = (task: () => Promise<void>) => {
      queue = queue.then(async () => {
        if (done) return;
        try {
          await task();
        } catch (err) {
          if (!io.signal.aborted) report(err);
          finish(io.signal.aborted ? 0 : 1);
        }
      });
    };
    const print = (position: LendingPosition) => enqueue(async () => io.write(positionLine(position)));
    const alert = (position: LendingPosition) =>
      enqueue(async () => {
        const body = alertBody(position, threshold ?? { numerator: 1n, denominator: 1n });
        if (await deliver(body, sink!, options.retries, io)) return;
        if (!io.signal.aborted) io.error(`streamd: the alert for ${position.account} at ${position.version} was not delivered`);
        finish(io.signal.aborted ? 0 : 1);
      });

    try {
      running = io.client.positions(options.protocol, {
        ...(account !== undefined && { account }),
        ...(threshold !== undefined && { threshold }),
        ...(refreshInterval !== undefined && { refreshInterval }),
        ...(fromVersion !== undefined && { fromVersion }),
        signal: io.signal,
        onChange: print,
        ...(sink !== undefined && { onCross: alert }),
        onError: (err) => {
          report(err);
          if (running !== undefined && !running.active) finish(1);
        },
      });
    } catch (err) {
      report(err);
      return finish(1);
    }
    running.ready.then(
      (first) => {
        for (const position of first.values()) print(position);
      },
      (err) => {
        if (!io.signal.aborted) report(err);
        finish(io.signal.aborted ? 0 : 1);
      },
    );
  });
}
