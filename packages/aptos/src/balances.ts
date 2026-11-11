import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { UNSIGNED_INTEGER } from "@streamd/core";
import { TRANSFER_DESCRIPTOR, assetKey, requireAsset } from "./transfers.js";
import { onAbort } from "@streamd/core";
import type { EventAdapter, EventCallback, ListenerOptions, StreamdEvent, Subscription, ViewRequest } from "@streamd/core";
import type { BalanceChange, Balances, BalancesOptions } from "./types.js";

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

/** Rejects with a non-network error, which stops a poller, when the view aborts, as for a token object. */
async function requireBalance(
  adapter: EventAdapter,
  owner: string,
  asset: string,
  version: bigint,
): Promise<bigint> {
  const request = balanceRequest(owner, asset);
  let result: unknown[];
  try {
    result = await adapter.view!(request, version, TRANSFER_DESCRIPTOR);
  } catch (err) {
    if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) {
      throw new StreamdError(`Not a fungible asset or coin type: ${asset}`, err);
    }
    throw err;
  }
  const amount = Array.isArray(result) ? result[0] : undefined;
  if (typeof amount !== "string" || !UNSIGNED_INTEGER.test(amount)) {
    throw new StreamdError(`${request.function} of ${owner} at version ${version} returned no amount`);
  }
  return BigInt(amount);
}

/**
 * `adapter` as a source of balance changes: each fetch reads every asset at
 * one head version and returns those that differ from `known`, so
 * `EventPoller` supplies the interval, backoff and error budget. Reading
 * rather than following events is what keeps it exact: gas moves APT with no
 * withdraw, a coin can move as its paired fungible asset, and a scan of the
 * whole chain cannot keep up with mainnet.
 */
function balancePoll(
  adapter: EventAdapter,
  who: string,
  known: ReadonlyMap<string, bigint>,
  assets: readonly string[],
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
      for (const [eventIndex, asset] of assets.entries()) {
        const balance = await requireBalance(adapter, who, asset, head);
        if (known.get(asset) !== balance) {
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

/** `client.balances()`: read each asset at the head, then keep reading it. */
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
  if (Array.isArray(named) && named.length === 0) throw new StreamdError("asset must not be an empty list");
  const names: readonly string[] = named === undefined ? [APT] : typeof named === "string" ? [named] : named;
  const assets = [...new Set(names.map((name) => canonical(requireAsset(name))))];
  const token = assets.find((asset) => LEGACY_TOKEN.test(asset));
  if (token !== undefined) throw new StreamdError(`A legacy token has no balance to follow: ${token}`);

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
    const head = await host.adapter.getHeadVersion!(TRANSFER_DESCRIPTOR);
    const first: [string, bigint][] = [];
    for (const asset of assets) first.push([asset, await requireBalance(host.adapter, who, asset, head)]);
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
      (source) => balancePoll(source, who, amounts, assets, head),
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
