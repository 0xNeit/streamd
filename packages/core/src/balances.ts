import { StreamdError, StreamdNetworkError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
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
  ModuleEventDescriptor,
  MoveEventType,
  StreamdEvent,
  Subscription,
  Transfer,
  ViewRequest,
} from "./types.js";

const FEE_STATEMENT = "0x1::transaction_fee::FeeStatement";
const APT = `0x${"a".padStart(64, "0")}`;
const APT_COIN = "0x1::aptos_coin::AptosCoin";
const APT_COIN_KEY = assetKey(APT_COIN);
const COIN = /::/;
const LEGACY_TOKEN = /^0x[0-9a-fA-F]+::.+::.+::\d+$/s;
const NOT_FUNGIBLE_CACHE = 10_000;

/** Every event that can change a balance, gas included, served by one scan. */
export const BALANCE_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [...(TRANSFER_DESCRIPTOR.eventType as readonly MoveEventType[]), FEE_STATEMENT],
};

/** The asset's map key: long-form addresses, with APT one entry however it was named. */
function canonical(asset: string): string {
  const key = assetKey(asset);
  return key === APT_COIN_KEY ? APT : key;
}

function balanceRequest(owner: string, asset: string): ViewRequest {
  if (asset === APT || COIN.test(asset)) {
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
    result = await adapter.view!(request, version, BALANCE_DESCRIPTOR);
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
 * `adapter` as a source of balance changes for `account`: one record per asset
 * per version a transaction changed, holding the balance as that version left
 * it. Reads happen inside the page fetch, so a failed view fails the page and
 * a listener retries it. Records keep the identity of the event that signalled
 * them.
 *
 * Balances are read, never summed from transfers: gas leaves APT with no
 * withdraw event, so a running total would drift.
 */
export function balanceSource(
  adapter: EventAdapter,
  account: () => Promise<string>,
  wanted?: string,
): EventAdapter {
  const notFungible = new Set<string>();
  const isFee = (event: StreamdEvent) => event.type === FEE_STATEMENT;
  const transfers = transferSource(
    adapter,
    // Coin and fungible legs of APT report different assets, so APT is narrowed below.
    wanted === APT ? undefined : wanted,
    account,
    (event, who) =>
      isFee(event) && typeof event.sender === "string" && normalizeAddress(event.sender) === who,
  );

  const source: EventAdapter = {
    ...transfers,
    fetchEvents: async (descriptor, options) => {
      const page = await transfers.fetchEvents(descriptor, options);
      const who = await account();
      const done = new Set<string>();
      const events: StreamdEvent[] = [];
      for (const event of page.events) {
        const asset = isFee(event) ? APT : canonical((event as StreamdEvent<Transfer>).data.asset);
        const key = `${event.version}:${asset}`;
        if (done.has(key) || notFungible.has(asset)) continue;
        if ((wanted !== undefined && asset !== wanted) || LEGACY_TOKEN.test(asset)) continue;
        done.add(key);
        const balance = await balanceAt(adapter, who, asset, event.version);
        if (balance === undefined) {
          if (notFungible.size >= NOT_FUNGIBLE_CACHE) notFungible.clear();
          notFungible.add(asset);
          continue;
        }
        events.push({ ...event, data: { asset, balance } satisfies BalanceChange });
      }
      return { ...page, events };
    },
  };
  return source;
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

/** `client.balances()`: read what is known at the head, then follow what changes. */
export function startBalances(
  host: BalancesHost,
  account: () => Promise<string>,
  options: BalancesOptions = {},
): Balances {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported balances option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  if (typeof host.adapter.view !== "function") {
    throw new StreamdError("balances needs an adapter that implements view");
  }
  const { asset: named, onChange, ...listener } = options;
  const wanted = named === undefined ? undefined : canonical(requireAsset(named));
  if (wanted !== undefined && LEGACY_TOKEN.test(wanted)) {
    throw new StreamdError(`A legacy token has no balance to follow: ${named}`);
  }

  const amounts = new Map<string, bigint>();
  let live: Subscription | undefined;
  let stopped = false;
  const abandon = new AbortController();
  const stop = (reason: unknown = new StreamdError("Balances stopped before the first read finished")) => {
    if (stopped) return;
    stopped = true;
    untrack();
    detachSignal();
    abandon.abort(reason);
    live?.unsubscribe();
  };
  const untrack = host.track(stop);
  const detachSignal = onAbort(options.signal, () => stop(options.signal!.reason));
  if (options.signal?.aborted) stop(options.signal.reason);

  const throwIfStopped = () => {
    if (stopped) throw abandon.signal.reason;
  };

  const run = async (): Promise<ReadonlyMap<string, bigint>> => {
    throwIfStopped();
    let start: bigint | undefined;
    if (wanted !== undefined) {
      if (host.adapter.getHeadVersion === undefined) {
        throw new StreamdError("balances with asset needs an adapter that implements getHeadVersion");
      }
      const who = await account();
      start = await host.adapter.getHeadVersion(BALANCE_DESCRIPTOR);
      const balance = await balanceAt(host.adapter, who, wanted, start);
      if (balance === undefined) throw new StreamdError(`Not a fungible asset or coin type: ${named}`);
      throwIfStopped();
      amounts.set(wanted, balance);
    }
    live = host.subscribe(
      (event) => {
        const { asset, balance } = event.data as BalanceChange;
        if (amounts.get(asset) === balance) return;
        amounts.set(asset, balance);
        onChange?.(new Map(amounts), event as StreamdEvent<BalanceChange>);
      },
      {
        ...listener,
        ...(start === undefined ? {} : { fromVersion: start }),
        onError: (err) => {
          if (!live?.active && !live?.paused) stop();
          (options.onError ?? host.onError)?.(err);
        },
      },
      (source) => balanceSource(source, account, wanted),
    );
    return new Map(amounts);
  };

  const ready = run().catch((err: unknown) => {
    const wasStopped = stopped;
    stop(err);
    if (!wasStopped) (options.onError ?? host.onError)?.(err as Error);
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
      return !stopped && live?.active !== false;
    },
    unsubscribe: () => stop(),
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: () => stop(),
  } as Balances;
}
