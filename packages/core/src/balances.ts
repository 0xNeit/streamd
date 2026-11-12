import { StreamdError } from "./errors.js";
import { onAbort } from "./utils.js";
import type {
  BalanceChange,
  Balances,
  BalancesOptions,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  ListenerOptions,
  StreamdEvent,
  Subscription,
} from "./types.js";

/**
 * `amount` in whole units as a decimal string, every digit kept and trailing
 * zeros dropped. It never passes through a float, so a `u128` stays exact.
 */
export function formatAmount(amount: bigint, decimals: number): string {
  if (typeof amount !== "bigint") throw new StreamdError(`Not a valid amount: ${String(amount)}`);
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 255) {
    throw new StreamdError(`Not a valid decimals: ${String(decimals)} (expected 0..255)`);
  }
  const digits = (amount < 0n ? -amount : amount).toString().padStart(decimals + 1, "0");
  const split = digits.length - decimals;
  const fraction = digits.slice(split).replace(/0+$/, "");
  return `${amount < 0n ? "-" : ""}${digits.slice(0, split)}${fraction === "" ? "" : `.${fraction}`}`;
}

/** What a chain supplies to `startBalances`: how it names assets and reads one balance. */
export interface BalanceReader {
  /** Throws unless `adapter`, the client's, can serve `read` and `getHeadVersion`. */
  check(adapter: EventAdapter): void;
  /** The assets `named` (undefined for the chain's default), canonical and deduplicated; throws on a bad one. */
  assets(named: string | readonly string[] | undefined): readonly string[];
  /** `asset` in any form as its map key; throws on a bad one. */
  canonical(asset: string): string;
  /** The `type` of a change event for `asset`. */
  type(asset: string): string;
  /** `owner`'s balance of `asset` at `version`, through `adapter`. A non-network error stops the poller. */
  read(adapter: EventAdapter, owner: string, asset: string, version: bigint): Promise<bigint>;
}

/** What `startBalances` needs from its client. */
export interface BalancesHost {
  adapter: EventAdapter;
  /** The descriptor head reads and the poller run under. */
  descriptor: EventDescriptor;
  onError: ((error: Error) => void) | undefined;
  subscribe(
    callback: EventCallback,
    options: ListenerOptions,
    wrap: (source: EventAdapter) => EventAdapter,
  ): Subscription;
  track(stop: () => void): () => void;
}

/**
 * `adapter` as a source of balance changes: each fetch reads every asset at
 * one head version and returns those that differ from `known`, so
 * `EventPoller` supplies the interval, backoff and error budget. Reading
 * rather than following events is what keeps it exact.
 */
function balancePoll(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  reader: BalanceReader,
  who: string,
  known: ReadonlyMap<string, bigint>,
  assets: readonly string[],
  from: bigint,
): EventAdapter {
  let latest = from;
  return {
    fetchEvents: async () => {
      const head = await adapter.getHeadVersion!(descriptor);
      // A load balancer can answer from a node a few versions behind the last one.
      if (head < latest) return { events: [], nextCursor: String(latest), hasMore: false, throughVersion: latest };
      latest = head;
      const events: StreamdEvent[] = [];
      for (const [eventIndex, asset] of assets.entries()) {
        const balance = await reader.read(adapter, who, asset, head);
        if (known.get(asset) !== balance) {
          events.push({
            type: reader.type(asset),
            data: { asset, balance } satisfies BalanceChange,
            version: head,
            sequenceNumber: 0n,
            eventIndex,
          });
        }
      }
      return { events, nextCursor: String(head), hasMore: false, throughVersion: head };
    },
  };
}

const ALLOWED: readonly string[] = [
  "asset",
  "onChange",
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "dedupWindow",
  "onError",
  "onCaughtUp",
  "signal",
] satisfies readonly (keyof BalancesOptions)[];

/** A chain client's `balances()`: read each asset at the head, then keep reading it. */
export function startBalances(
  host: BalancesHost,
  account: () => Promise<string>,
  reader: BalanceReader,
  options: BalancesOptions = {},
): Balances {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported balances option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  reader.check(host.adapter);
  if (typeof host.adapter.getHeadVersion !== "function") {
    throw new StreamdError("balances needs an adapter that implements getHeadVersion");
  }
  const { asset: named, onChange, ...listener } = options;
  if (Array.isArray(named) && named.length === 0) throw new StreamdError("asset must not be an empty list");
  const assets = reader.assets(named);

  const amounts = new Map<string, bigint>();
  let polling: Subscription | undefined;
  let stopped = false;
  const abandon = new AbortController();
  const report = (err: Error) => (options.onError ?? host.onError)?.(err);
  const stop = (reason: unknown = new StreamdError("Balances stopped before the first read finished")) => {
    if (stopped) return;
    stopped = true;
    untrack();
    detachSignal();
    abandon.abort(reason);
    polling?.unsubscribe();
  };
  const untrack = host.track(stop);
  const detachSignal = onAbort(options.signal, () => stop(options.signal!.reason));
  if (options.signal?.aborted) stop(options.signal.reason);

  const throwIfStopped = () => {
    if (stopped) throw abandon.signal.reason;
  };

  const run = async (): Promise<ReadonlyMap<string, bigint>> => {
    throwIfStopped();
    const who = await account();
    const head = await host.adapter.getHeadVersion!(host.descriptor);
    const first: [string, bigint][] = [];
    for (const asset of assets) first.push([asset, await reader.read(host.adapter, who, asset, head)]);
    throwIfStopped();
    for (const [asset, balance] of first) amounts.set(asset, balance);

    polling = host.subscribe(
      (event) => {
        const { asset, balance: next } = event.data as BalanceChange;
        amounts.set(asset, next);
        onChange?.(new Map(amounts), event as StreamdEvent<BalanceChange>);
      },
      {
        ...listener,
        fromVersion: 0n,
        onError: (err) => {
          if (!polling?.active && !polling?.paused) stop();
          report(err);
        },
      },
      (source) => balancePoll(source, host.descriptor, reader, who, amounts, assets, head),
    );
    return new Map(amounts);
  };

  const ready = run().catch((err: unknown) => {
    const wasStopped = stopped;
    stop(err);
    if (!wasStopped) report(err as Error);
    throw err;
  });
  ready.catch(() => {});

  return {
    get value() {
      return new Map(amounts);
    },
    get(asset: string) {
      return amounts.get(reader.canonical(asset));
    },
    ready,
    get active() {
      return !stopped && polling?.active !== false;
    },
    unsubscribe: () => stop(),
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: () => stop(),
  } as Balances;
}
