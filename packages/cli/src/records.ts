import type { AptosClient, PackageUpgrade, SaleMarketplace, SwapProtocol } from "@streamd/aptos";
import type { EventFilter, StreamdEvent, Subscription, SubscriptionOptions } from "@streamd/core";
import type { ActivityCommandOptions, RecordWalkOptions, SalesCommandOptions, SwapsCommandOptions, UpgradesCommandOptions } from "./args.js";
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
  /** Names what a record broke, which ends the command with exit code 1 once it is printed. */
  breaking?(record: StreamdEvent<T>): string | undefined;
}

const RETRIES = 5;

/**
 * Prints records as JSON lines and resolves to the process exit code:
 * followed until the limit or the signal (0), or read over a closed range
 * with `toVersion` (0 when finished); 1 when reading, a write or the cursor
 * file fails, or when a printed record is `breaking` (followed: at once; over
 * a range: once the range is printed).
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
  let broken = false;
  try {
    for await (const record of source.range(filter)) {
      await io.write(`${formatEvent(record)}\n`);
      const why = source.breaking?.(record);
      if (why !== undefined) {
        io.error(`streamd: ${why}`);
        broken = true;
      }
    }
    return broken ? 1 : 0;
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
      const why = source.breaking?.(record);
      if (why !== undefined) {
        report(why);
        return finish(1);
      }
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

/**
 * Prints DEX swaps through `swaps` or `getSwaps`; the protocol names are the
 * client's to check.
 */
export function runSwaps(options: SwapsCommandOptions, io: RecordsIO<Pick<AptosClient, "swaps" | "getSwaps">>): Promise<number> {
  const { protocol, asset } = options;
  const narrowing = {
    ...(protocol !== undefined && { protocol: protocol as SwapProtocol[] }),
    ...(asset !== undefined && { asset }),
  };
  const { client } = io;
  return runRecords(options, {
    follow: (callback, listener) => client.swaps(callback, { ...narrowing, ...listener }),
    range: (filter) => fromArray(client.getSwaps({ ...narrowing, ...filter })),
  }, io);
}

/**
 * Prints NFT sales through `sales` or `getSales`; the marketplace names are
 * the client's to check.
 */
export function runSales(options: SalesCommandOptions, io: RecordsIO<Pick<AptosClient, "sales" | "getSales">>): Promise<number> {
  const { marketplace, token } = options;
  const narrowing = {
    ...(marketplace !== undefined && { marketplace: marketplace as SaleMarketplace[] }),
    ...(token !== undefined && { token }),
  };
  const { client } = io;
  return runRecords(options, {
    follow: (callback, listener) => client.sales(callback, { ...narrowing, ...listener }),
    range: (filter) => fromArray(client.getSales({ ...narrowing, ...filter })),
  }, io);
}

/** Prints an account's activity through `activity` or `getActivity`. */
export function runActivity(options: ActivityCommandOptions, io: RecordsIO<Pick<AptosClient, "activity" | "getActivity">>): Promise<number> {
  const { address, withAssetInfo } = options;
  const narrowing = withAssetInfo ? { withAssetInfo } : {};
  const { client } = io;
  return runRecords(options, {
    follow: (callback, listener) => client.activity(address, callback, { ...narrowing, ...listener }),
    range: (filter) => fromArray(client.getActivity(address, { ...narrowing, ...filter })),
  }, io);
}

/**
 * What a publish removed or changed, by full name, or `undefined` when it only
 * added; names are JSON-encoded, since they are on-chain text.
 */
export function breakingChanges({ version, data }: StreamdEvent<PackageUpgrade>): string | undefined {
  const kinds = [data.events, data.entryFunctions, data.views];
  const removed = kinds.flatMap((kind) => kind.removed.map(({ name }) => name));
  const changed = kinds.flatMap((kind) => kind.changed.map(({ before }) => before.name));
  if (removed.length === 0 && changed.length === 0) return undefined;
  return `breaking upgrade at version ${version}: ${JSON.stringify({ address: data.address, removed, changed })}`;
}

/** Prints package publishes through `upgrades` or `getUpgrades`, exiting 1 on a breaking one. */
export function runUpgrades(options: UpgradesCommandOptions, io: RecordsIO<Pick<AptosClient, "upgrades" | "getUpgrades">>): Promise<number> {
  const { address } = options;
  const { client } = io;
  return runRecords(options, {
    follow: (callback, listener) => client.upgrades(address, callback, listener),
    range: (filter) => fromArray(client.getUpgrades(address, filter)),
    breaking: breakingChanges,
  }, io);
}
