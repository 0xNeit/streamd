import type { AptosClient, MultisigState, Proposal, StakePool } from "@streamd/aptos";
import type { MonitorCommandOptions } from "./args.js";
import { message } from "./format.js";

export type MonitorRecord = Proposal | StakePool | MultisigState;

export interface MonitorIO {
  client: Pick<AptosClient, "proposals" | "stakePools" | "multisig">;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

/**
 * One record as a JSON line, bigints as decimal strings and maps (a pool's
 * `delegators`, a multisig's `pending`) as arrays of their values, each of
 * which carries its own key.
 */
export function monitorLine(record: MonitorRecord): string {
  const json = JSON.stringify(record, (_key, v: unknown) =>
    typeof v === "bigint" ? v.toString() : v instanceof Map ? [...v.values()] : v,
  );
  return `${json}\n`;
}

interface Running {
  readonly ready: Promise<unknown>;
  readonly active: boolean;
  unsubscribe(): void;
}

type Callbacks = {
  signal: AbortSignal;
  onChange: (record: MonitorRecord) => void;
  onError: (error: Error) => void;
};

function start(options: MonitorCommandOptions, client: MonitorIO["client"], callbacks: Callbacks): Running {
  const from = options.fromVersion === undefined ? {} : { fromVersion: options.fromVersion };
  if (options.command === "proposals") return client.proposals({ ...from, ...callbacks });
  if (options.command === "stake-pools") return client.stakePools({ ...from, ...callbacks });
  return client.multisig(options.address, { ...from, ...callbacks });
}

/** `ready`'s value as the opening lines: proposals by id, pools in map order, or the one state. */
function opening(command: MonitorCommandOptions["command"], value: unknown): MonitorRecord[] {
  if (command === "multisig") return [value as MultisigState];
  const records = [...(value as ReadonlyMap<unknown, Proposal | StakePool>).values()];
  if (command === "proposals") (records as Proposal[]).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return records;
}

/**
 * Prints the monitor's state once it is ready, then one line per change, and
 * resolves to the exit code: 0 on the signal or once `limit` changes are
 * printed; 1 when the start read fails, the monitor stops, or a write fails.
 */
export function runMonitorState(options: MonitorCommandOptions, io: MonitorIO): Promise<number> {
  if (io.signal.aborted) return Promise.resolve(0);
  return new Promise<number>((resolve) => {
    let done = false;
    let running: Running | undefined;
    let reported: unknown;
    let changes = 0;
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

    // The client does not await its callbacks, so lines queue here in order.
    let queue = Promise.resolve();
    const print = (record: MonitorRecord, change: boolean) => {
      // Records are updated in place, so the line is taken now, not when its turn comes.
      const line = monitorLine(record);
      queue = queue.then(async () => {
        if (done) return;
        try {
          await io.write(line);
        } catch (err) {
          if (!io.signal.aborted) report(err);
          return finish(io.signal.aborted ? 0 : 1);
        }
        if (change && options.limit !== undefined && ++changes >= options.limit) finish(0);
      });
    };

    try {
      running = start(options, io.client, {
        signal: io.signal,
        onChange: (record) => print(record, true),
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
      (value) => {
        for (const record of opening(options.command, value)) print(record, false);
      },
      (err) => {
        if (!io.signal.aborted) report(err);
        finish(io.signal.aborted ? 0 : 1);
      },
    );
  });
}
