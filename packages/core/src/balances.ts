import { StreamdError, StreamdNetworkError } from "./errors.js";
import { UNSIGNED_INTEGER } from "./decode.js";
import { TRANSFER_DESCRIPTOR, assetKey, requireAsset, transferSource } from "./transfers.js";
import { onAbort } from "./utils.js";
import type {
  BalanceChange,
  Balances,
  BalancesOptions,
  EventAdapter,
  EventCallback,
  ListenerOptions,
  StreamdEvent,
  Subscription,
  Transfer,
  ViewRequest,
} from "./types.js";

const APT = `0x${"a".padStart(64, "0")}`;
const APT_COIN = "0x1::aptos_coin::AptosCoin";
const APT_COIN_KEY = assetKey(APT_COIN);
const LEGACY_TOKEN = /^0x[0-9a-fA-F]+::.+::.+::\d+$/s;

/** The asset's map key: long-form addresses, with APT one entry however it was named. */
function canonical(asset: string): string {
  const key = assetKey(asset);
  return key === APT_COIN_KEY ? APT : key;
}

function balanceRequest(owner: string, asset: string): ViewRequest {
  if (asset === APT || asset.includes("::")) {
    return {
      function: "0x1::coin::balance",
      typeArguments: [asset === APT ? APT_COIN : asset],
      functionArguments: [owner],
    };
  }
  return {
    function: "0x1::primary_fungible_store::balance",
    typeArguments: ["0x1::fungible_asset::Metadata"],
    functionArguments: [owner, asset],
  };
}

/** `undefined` when the view aborts, which is how a token object answers. */
async function balanceAt(
  adapter: EventAdapter,
  owner: string,
  asset: string,
  version: bigint,
): Promise<bigint | undefined> {
  const request = balanceRequest(owner, asset);
  let result: unknown[];
  try {
    result = await adapter.view!(request, version, TRANSFER_DESCRIPTOR);
  } catch (err) {
    if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) return undefined;
    throw err;
  }
  const amount = Array.isArray(result) ? result[0] : undefined;
  if (typeof amount !== "string" || !UNSIGNED_INTEGER.test(amount)) {
    throw new StreamdError(`${request.function} of ${owner} at version ${version} returned no amount`);
  }
  return BigInt(amount);
}

/**
 * `adapter` as a source of balance changes: each fetch reads every tracked
 * asset at one head version and returns those that differ from `known`, so
 * `EventPoller` supplies the interval, backoff and error budget. Reading
 * rather than following events is what keeps it exact: gas moves APT with no
 * withdraw, a coin can move as its paired fungible asset, and a scan of the
 * whole chain cannot keep up with mainnet.
 */
function balancePoll(
  adapter: EventAdapter,
  who: string,
  known: ReadonlyMap<string, bigint>,
  tracked: Set<string>,
  notFungible: Set<string>,
  from: bigint,
): EventAdapter {
  let latest = from;
  return {
    fetchEvents: async () => {
      const head = await adapter.getHeadVersion!(TRANSFER_DESCRIPTOR);
      // A load balancer can answer from a node a few versions behind the last one.
      if (head < latest) return { events: [], nextCursor: String(latest), hasMore: false, throughVersion: latest };
      latest = head;
      const events: StreamdEvent[] = [];
      let position = 0;
      for (const asset of [...tracked]) {
        const eventIndex = position++;
        const balance = await balanceAt(adapter, who, asset, head);
        if (balance === undefined) {
          tracked.delete(asset);
          notFungible.add(asset);
        } else if (known.get(asset) !== balance) {
          events.push({
            type: balanceRequest(who, asset).function,
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

/** What `startBalances` needs from its client. */
export interface BalancesHost {
  adapter: EventAdapter;
  onError: ((error: Error) => void) | undefined;
  subscribe(
    callback: EventCallback,
    options: ListenerOptions,
    wrap: (source: EventAdapter) => EventAdapter,
  ): Subscription;
  track(stop: () => void): () => void;
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

/** `client.balances()`: read what is tracked at the head, then keep reading it. */
export function startBalances(
  host: BalancesHost,
  account: () => Promise<string>,
  options: BalancesOptions = {},
): Balances {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported balances option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  if (typeof host.adapter.view !== "function" || typeof host.adapter.getHeadVersion !== "function") {
    throw new StreamdError("balances needs an adapter that implements view and getHeadVersion");
  }
  const { asset: named, onChange, ...listener } = options;
  const wanted = named === undefined ? undefined : canonical(requireAsset(named));
  if (wanted !== undefined && LEGACY_TOKEN.test(wanted)) {
    throw new StreamdError(`A legacy token has no balance to follow: ${named}`);
  }

  const amounts = new Map<string, bigint>();
  const tracked = new Set([wanted ?? APT]);
  const notFungible = new Set<string>();
  let polling: Subscription | undefined;
  let discovery: Subscription | undefined;
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
    discovery?.unsubscribe();
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
    const head = await host.adapter.getHeadVersion!(TRANSFER_DESCRIPTOR);
    const first = wanted ?? APT;
    const balance = await balanceAt(host.adapter, who, first, head);
    if (balance === undefined) throw new StreamdError(`Not a fungible asset or coin type: ${named ?? first}`);
    throwIfStopped();
    amounts.set(first, balance);

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
      (source) => balancePoll(source, who, amounts, tracked, notFungible, head),
    );
    if (wanted === undefined) {
      const { onCaughtUp: _unused, ...scan } = listener;
      discovery = host.subscribe(
        (event) => {
          const asset = canonical((event as StreamdEvent<Transfer>).data.asset);
          if (!LEGACY_TOKEN.test(asset) && !notFungible.has(asset)) tracked.add(asset);
        },
        // A scan that gives up only stops new assets appearing, so polling carries on.
        { ...scan, fromVersion: head, onError: report },
        (source) => transferSource(source, undefined, account),
      );
    }
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
      return amounts.get(canonical(requireAsset(asset)));
    },
    ready,
    get active() {
      return !stopped && polling?.active !== false;
    },
    unsubscribe: () => stop(),
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: () => stop(),
  } as Balances;
}
