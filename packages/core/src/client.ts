import { Aptos } from "@aptos-labs/ts-sdk";
import type { SimpleTransaction } from "@aptos-labs/ts-sdk";
import type {
  Balances,
  BalancesOptions,
  BatchCallback,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventStream,
  EventFilter,
  EventExpectation,
  EventsOfOptions,
  ExpectEventsOptions,
  ListenerDefaults,
  ListenerOptions,
  OnceOptions,
  PreviewEventsOptions,
  ReduceOptions,
  Reducer,
  Reduction,
  Replay,
  ReplayOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  Transfer,
  TransferFilter,
  TransferListenerOptions,
  TransactionCallback,
  Watch,
  WatchCallback,
  WatchOptions,
} from "./types.js";
import { FullnodeScanAdapter } from "./adapters/fullnode.js";
import { EventPoller, resolveMaxBackoff, type Delivery } from "./poller.js";
import { EventQuery } from "./query.js";
import {
  StreamdError,
  StreamdTimeoutError,
} from "./errors.js";
import { descriptorKey, generateId, onAbort } from "./utils.js";
import { eventStream } from "./stream.js";
import { RequestLimiter } from "./limiter.js";
import { CursorManager } from "./cursor.js";
import { fieldDecoder } from "./decode.js";
import { SharedFetches } from "./shared.js";
import { startReduction, type ReduceHost } from "./reduce.js";
import { startReplay } from "./replay.js";
import { startTransactions, type TransactionHost } from "./grouping.js";
import { startWatch } from "./watch.js";
import { eventsOf, previewEvents } from "./transaction.js";
import { expectEvents } from "./expect.js";
import { requireTransactionHash } from "./adapters/validate.js";
import { addressOrName, resolveName } from "./names.js";
import { startBalances } from "./balances.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";

const MAX_TIMEOUT = 2_147_483_647;

// Runtimes without explicit resource management lack `Symbol.dispose`. The
// registry key is the one core-js polyfills it with, so `using` still finds
// the method when such a polyfill loads after this module.
const DISPOSE: typeof Symbol.dispose =
  Symbol.dispose ?? (Symbol.for("Symbol.dispose") as typeof Symbol.dispose);

function checkTimeout(ms: number | undefined): StreamdError | undefined {
  if (ms === undefined || (typeof ms === "number" && ms >= 0 && ms <= MAX_TIMEOUT)) {
    return undefined;
  }
  return new StreamdError(`Invalid timeout: ${String(ms)} (expected 0..${MAX_TIMEOUT}ms)`);
}

function resolveLimiter(max: number | undefined, perSecond: number | undefined): RequestLimiter {
  if (max !== undefined && (!Number.isInteger(max) || max < 1)) {
    throw new StreamdError(
      `Invalid maxConcurrentRequests: ${String(max)} (expected a positive integer)`,
    );
  }
  if (perSecond !== undefined && (!Number.isFinite(perSecond) || perSecond <= 0)) {
    throw new StreamdError(
      `Invalid maxRequestsPerSecond: ${String(perSecond)} (expected a positive number)`,
    );
  }
  return new RequestLimiter(max, perSecond);
}

const DEFAULT_KEYS = [
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "dedupWindow",
  "decode",
] as const satisfies readonly (keyof ListenerDefaults)[];

/** `config.defaults` checked as a call's options would be, with `defaultPollInterval` folded in. */
function resolveDefaults(config: StreamdConfig): ListenerDefaults {
  const defaults: unknown = config.defaults === undefined ? {} : config.defaults;
  if (typeof defaults !== "object" || defaults === null || Array.isArray(defaults)) {
    throw new StreamdError("defaults must be an object of listener options");
  }
  const refused = Object.keys(defaults).find(
    (k) => !(DEFAULT_KEYS as readonly string[]).includes(k),
  );
  if (refused !== undefined) {
    throw new StreamdError(
      `Unsupported listener default: ${refused} (allowed: ${DEFAULT_KEYS.join(", ")})`,
    );
  }
  const resolved: ListenerDefaults = { ...defaults };
  resolveMaxBackoff(resolved.maxBackoff);
  fieldDecoder(resolved.decode);
  new CursorManager(undefined, resolved.dedupWindow);
  resolved.pollInterval ??= config.defaultPollInterval ?? 1000;
  return resolved;
}

/** `options` with each field it leaves `undefined` taken from `defaults`. */
function withDefaults(
  options: ListenerOptions | undefined,
  defaults: ListenerDefaults,
): ListenerOptions {
  const merged: Record<string, unknown> = { ...options };
  for (const key of DEFAULT_KEYS) merged[key] ??= defaults[key];
  return merged as ListenerOptions;
}

/** An `on()` or `onBatch()` subscription, as `off()` and friends see it. */
interface Listener {
  key: string | undefined;
  callback: unknown;
  stop: () => void;
}

function resolveAptos(config: StreamdConfig): Aptos | undefined {
  if (config.aptos === undefined) return undefined;
  return config.aptos instanceof Aptos ? config.aptos : new Aptos(config.aptos);
}

function resolveAdapter(config: StreamdConfig, aptos: Aptos | undefined): EventAdapter {
  if (config.adapter !== undefined) return config.adapter;
  if (aptos === undefined) {
    throw new StreamdError(
      "StreamdClient requires either `aptos` or `adapter` in config",
    );
  }
  return new FullnodeScanAdapter(aptos);
}

export class StreamdClient {
  private readonly defaults: ListenerDefaults;
  private readonly onError?: (error: Error) => void;
  private readonly aptos?: Aptos;
  private readonly adapter: EventAdapter;
  private readonly query: EventQuery;
  /** Shared by every poller and query, so the cap holds client-wide. */
  private readonly limiter: RequestLimiter;
  /** Lets identical subscriptions share each poll's adapter request. */
  private readonly shared: SharedFetches;
  private readonly subscriptions = new Map<string, { poller: EventPoller; stop: () => void }>();
  /** The `on()`/`onBatch()` subset of `subscriptions`; `once()` and streams own theirs. */
  private readonly listeners = new Map<string, Listener>();
  /** Open streams, so destroy() can end their loops rather than strand them. */
  private readonly streams = new Set<() => void>();
  /** What `reduce()` needs, built once. */
  private readonly reduceHost: ReduceHost;
  /** What `onTransaction()` needs, built once. */
  private readonly transactionHost: TransactionHost;

  constructor(config: StreamdConfig) {
    this.defaults = resolveDefaults(config);
    this.onError = config.onError;
    this.limiter = resolveLimiter(config.maxConcurrentRequests, config.maxRequestsPerSecond);
    this.aptos = resolveAptos(config);
    this.adapter = resolveAdapter(config, this.aptos);
    this.shared = new SharedFetches(this.adapter);
    this.query = new EventQuery(this.adapter, this.limiter);
    this.reduceHost = {
      query: this.query,
      defaults: this.defaults,
      onError: this.onError,
      subscribe: (d, callback, options) => this.subscribe(d, callback, options),
      track: (stop) => (this.streams.add(stop), () => void this.streams.delete(stop)),
    };
    this.transactionHost = {
      defaults: this.defaults,
      onError: this.onError,
      unshared: this.shared.view(0),
      subscribe: (d, delivery, options, id, wrap) =>
        this.subscribe(d, delivery, options, undefined, id, wrap),
    };
  }

  /**
   * Subscribe to real-time events. Polls under the hood.
   *
   * Subscriptions on this client that would send the adapter the identical
   * request (same descriptor value, cursor or absolute start, `batchSize` and
   * `pollInterval`) share one request per `pollInterval`; everything else about
   * each one, its cursor, pause and error budget included, stays its own. A
   * start at the head is never shared, so subscriptions started at different
   * times share only once their cursors coincide. Shared subscriptions receive
   * the same event objects, so treat them as read-only. Pass an array of
   * `eventType`s on one `module` descriptor to serve several Move types from a
   * single scan.
   */
  on<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    callback: EventCallback<TData>,
    options?: SubscriptionOptions,
  ): Subscription {
    return this.subscribe(descriptor, callback as EventCallback, options, callback, options?.id);
  }

  /**
   * Like `on()`, but each poll's new events arrive together in one
   * `callback(events)` call, at most `batchSize` of them.
   */
  onBatch<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    callback: BatchCallback<TData>,
    options?: SubscriptionOptions,
  ): Subscription {
    return this.subscribe(
      descriptor,
      { batch: callback as BatchCallback },
      options,
      callback,
      options?.id,
    );
  }

  /**
   * Like `onBatch()`, but each call holds one transaction's matching events,
   * all of them: events are held by version until the adapter's
   * `EventPage.throughVersion` passes it (without one, until a later version
   * arrives or the adapter reports nothing more), then released in version
   * order. `decode`, `where` and `withState` apply per event first, so a
   * transaction with no match delivers nothing.
   *
   * Several descriptors are merged: one walk each, a transaction released once
   * every walk has passed it, each event once. Their `cursor` is `undefined`
   * and the `cursor` option is refused. With one descriptor, `cursor` moves only
   * past pages that left nothing held. Like `reduce()`, it is not a listener
   * for `off()` or `listenerCount()`; `unsubscribe()` or `destroy()` ends it.
   */
  onTransaction<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    callback: TransactionCallback<TData>,
    options?: SubscriptionOptions,
  ): Subscription {
    return startTransactions(
      this.transactionHost,
      descriptor,
      callback as TransactionCallback,
      options,
    );
  }

  /**
   * Follow many addresses at once: one `account` walk per address, each event
   * delivered as `callback(event, address)`. What a walk sees is what the
   * adapter yields for an `account` descriptor: on `FullnodeScanAdapter`,
   * transactions the address sent; on `IndexerAccountAdapter`, every one
   * touching it. `add()` and `remove()` change the list at runtime; a walk that
   * gives up drops its address and the rest carry on. Like `onTransaction()`,
   * it is not a listener for `off()` or `listenerCount()`; `unsubscribe()` or
   * `destroy()` ends it.
   */
  watch<TData = unknown>(
    addresses: readonly string[],
    callback: WatchCallback<TData>,
    options?: WatchOptions,
  ): Watch {
    return startWatch(this.reduceHost, addresses, callback as WatchCallback, options);
  }

  private subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options?: ListenerOptions,
    listener?: unknown,
    callerId?: string,
    wrap?: (source: EventAdapter) => EventAdapter,
  ): Subscription {
    const id = callerId === undefined ? generateId() : this.claimId(callerId);
    let detach = () => {};
    const forget = () => {
      // A later subscription may hold this id by now; leave its entries alone.
      if (this.subscriptions.get(id)?.poller === poller) {
        this.subscriptions.delete(id);
        this.listeners.delete(id);
      }
      detach();
    };
    const resolved = withDefaults(options, this.defaults);
    const source = this.shared.view(resolved.pollInterval!);
    const poller = new EventPoller(
      wrap === undefined ? source : wrap(source),
      descriptor,
      delivery,
      {
        ...resolved,
        onError: (err) => {
          // Drop the subscription once the poller has given up and stopped
          // itself; a paused one reporting a late handler error stays.
          if (!poller.active && !poller.paused) forget();
          (options?.onError ?? this.onError)?.(err);
        },
      },
      this.limiter,
    );

    poller.start();
    // An already-aborted signal leaves the poller stopped: nothing to track.
    if (poller.active) {
      const stop = () => (poller.stop(), forget());
      this.subscriptions.set(id, { poller, stop });
      if (listener !== undefined) {
        this.listeners.set(id, { key: descriptorKey(descriptor), callback: listener, stop });
      }
      detach = onAbort(options?.signal, forget);
    }

    const unsubscribe = () => {
      poller.stop();
      forget();
    };
    return {
      id,
      get active() {
        return poller.active;
      },
      get paused() {
        return poller.paused;
      },
      get cursor() {
        return poller.cursor;
      },
      get status() {
        return poller.status;
      },
      pause: () => poller.pause(),
      resume: () => poller.resume(),
      unsubscribe,
      [DISPOSE]: unsubscribe,
    };
  }

  private claimId(id: unknown): string {
    if (typeof id !== "string" || id === "") {
      const shown = typeof id === "string" ? '""' : String(id);
      throw new StreamdError(`Invalid id: ${shown} (expected a non-empty string)`);
    }
    if (this.subscriptions.has(id)) {
      throw new StreamdError(`Subscription id already in use on this client: ${id}`);
    }
    return id;
  }

  /**
   * Wait for the next event matching the descriptor. Resolves with the first event received.
   *
   * Rejects with `signal.reason` on abort, with `StreamdTimeoutError` if
   * `timeout` elapses first, when polling gives up after `maxErrors`, at once
   * when the adapter refuses the descriptor or options as invalid input, or on
   * the first error `where` throws synchronously, even if a later event would
   * have matched. A rejected async `where` fails the poll and is retried, as
   * for `on()`. A throwing `onCaughtUp`, an event that fails `decode` or a
   * throwing `withState` builder goes to `onError` and is skipped, as for `on()`.
   */
  once<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    options?: OnceOptions,
  ): Promise<StreamdEvent<TData>> {
    const badTimeout = checkTimeout(options?.timeout);
    if (badTimeout !== undefined) return Promise.reject(badTimeout);
    if (options?.signal?.aborted) return Promise.reject(options.signal.reason);

    return new Promise<StreamdEvent<TData>>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let detach = () => {};
      const close = () => {
        if (timer !== undefined) clearTimeout(timer);
        detach();
        sub.unsubscribe();
      };
      const fail = (reason: unknown) => (close(), reject(reason));
      // A throwing predicate would skip every event and leave the promise
      // pending; other handler errors are per event, so waiting can still match.
      // Only a synchronous throw counts: a rejection fails the poll and is retried.
      let whereThrew = false;
      const where = options?.where;
      const sub = this.subscribe(
        descriptor,
        ((event: StreamdEvent<TData>) => (close(), resolve(event))) as EventCallback,
        {
          ...options,
          where:
            where === undefined
              ? undefined
              : (event) => {
                  try {
                    return where(event);
                  } catch (err) {
                    whereThrew = true;
                    throw err;
                  }
                },
          signal: undefined,
          onError: (err) => {
            // Settle before the caller's handler runs, so a throw from it
            // cannot leave the promise pending.
            if (!sub.active || whereThrew) fail(err);
            (options?.onError ?? this.onError)?.(err);
          },
        },
      );
      detach = onAbort(options?.signal, () => fail(options!.signal!.reason));
      if (options?.timeout !== undefined) {
        const ms = options.timeout;
        timer = setTimeout(
          () => fail(new StreamdTimeoutError(`Timed out after ${ms}ms waiting for event`, ms)),
          ms,
        );
      }
    });
  }

  /**
   * Live events as an async iterable: `for await (const e of client.stream(d))`.
   * Leaving the loop unsubscribes. Each event holds back the next poll until
   * the loop asks for the following one, and the loop throws if polling gives
   * up (after `maxErrors`, or at once on invalid input) or `options.signal`
   * aborts. Other errors go to `options.onError`. Save `cursor` to resume
   * later, as for `on()`.
   */
  stream<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    options?: ListenerOptions,
  ): EventStream<TData> {
    // Registered only once the stream subscribes, so an unused stream costs nothing.
    const close = () => void stream.return?.();
    const stream = eventStream<TData>(
      (callback, onError) => {
        const sub = this.subscribe(descriptor, callback as EventCallback, {
          ...options,
          signal: undefined,
          onError: (err) => {
            onError(err);
            (options?.onError ?? this.onError)?.(err);
          },
        });
        this.streams.add(close);
        return sub;
      },
      () => this.streams.delete(close),
      options?.signal,
      options?.cursor,
    );
    return stream;
  }

  /**
   * Keep a value reduced over a descriptor's events: fold the history from the
   * start position (`fromVersion`, `fromTime` or `fromBlock`; as for
   * `getEvents` when omitted) into `initial`, then fold each live event as it arrives, each
   * exactly once. Read the current state as `value`; `ready` resolves once
   * the history is folded. A reducer throw stops the reduction, as folding
   * past a lost event would leave `value` wrong. Like `once()` and `stream()`,
   * it is not a listener for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  reduce<S, TData = unknown>(
    descriptor: EventDescriptor<TData>,
    reducer: Reducer<S, TData>,
    initial: S,
    options?: ReduceOptions<S, TData>,
  ): Reduction<S> {
    return startReduction(this.reduceHost, descriptor, reducer, initial, options);
  }

  /**
   * Feed a past range of events through an `on()` callback, in order, so a
   * live handler can be backtested unchanged: each call is awaited before the
   * next, and a throw or rejection goes to `onError` while delivery carries
   * on. The range and narrowing options are `getEvents`'s, applied as it
   * applies them; `speed` paces delivery by the events' `timestamp`s. `done`
   * settles when the range is exhausted or a query fails. Like `reduce()`, it
   * is not a listener for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  replay<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    callback: EventCallback<TData>,
    options?: ReplayOptions,
  ): Replay {
    return startReplay(this.reduceHost, descriptor, callback, options);
  }

  /**
   * Follow coin, fungible-asset and token transfers as they land, each
   * delivered as an event whose `data` is a `Transfer`, from one scan over
   * `0x1::fungible_asset::Withdraw`/`Deposit`, `0x1::coin::CoinWithdraw`/`CoinDeposit`,
   * `0x3::token::TokenWithdraw`/`TokenDeposit` and `0x1::object::Transfer`.
   *
   * Within a transaction, in event order, a deposit pairs with the most recent
   * unpaired withdraw of the same asset and amount; a deposit with none has no
   * `from`, and a withdraw left over has no `to`. A fungible-asset store's
   * asset and owner are read through `EventAdapter.view` at the event's
   * version (before it, when the transaction deleted the store), at most 4 at
   * a time per subscription and outside `maxConcurrentRequests`; a leg whose store existed neither after nor before its
   * transaction is skipped. A failed read fails the poll, which retries as
   * for `on()`. The record keeps the deposit's identity, or the withdraw's
   * when it stands alone. An `object::Transfer` is reported, unpaired and
   * with `amount` 1, only when the object holds a `0x4::token::Token`,
   * checked once per object by view. `asset` narrows to one fungible asset,
   * coin type or token before any owner is read; `account` keeps what one
   * address sent or received. Like `reduce()`, it is not a listener for
   * `off()` or `listenerCount()`; `unsubscribe()` or `destroy()` ends it.
   */
  transfers(
    callback: EventCallback<Transfer>,
    options: TransferListenerOptions = {},
  ): Subscription {
    const { asset, account, ...rest } = options;
    const who = this.accountOf(account);
    return this.subscribe(
      TRANSFER_DESCRIPTOR,
      callback as EventCallback,
      rest,
      undefined,
      rest.id,
      (source) => transferSource(source, asset, who),
    );
  }

  /**
   * Keep one wallet's balances current, by reading each at the head every
   * `pollInterval`, so it cannot drift or fall behind. `asset` names what to
   * follow, one or a list, since no view lists a wallet's assets; without
   * it, APT is followed. `address` may be an Aptos name. Like `reduce()`, it
   * is not a listener for `off()`
   * or `listenerCount()`; `destroy()` ends it.
   */
  balances(address: string, options?: BalancesOptions): Balances {
    if (address === undefined) throw new StreamdError("balances needs an address or Aptos name");
    return startBalances(
      {
        adapter: this.adapter,
        onError: this.onError,
        subscribe: (callback, listener, wrap) =>
          this.subscribe(TRANSFER_DESCRIPTOR, callback, listener, undefined, undefined, wrap),
        track: this.reduceHost.track,
      },
      this.accountOf(address)!,
      options,
    );
  }

  /**
   * `account` as a lazy address: a name resolves once, on first use, and a
   * failed lookup is tried again by the next page.
   */
  private accountOf(account: string | undefined): (() => Promise<string>) | undefined {
    if (account === undefined) return undefined;
    const address = addressOrName(account);
    if (address !== undefined) return () => Promise.resolve(address);
    const aptos = this.aptos;
    if (aptos === undefined) {
      throw new StreamdError(`Resolving the Aptos name ${account} needs \`aptos\` in the client config`);
    }
    let resolved: Promise<string> | undefined;
    return () =>
      (resolved ??= resolveName(aptos, account).catch((err: unknown) => {
        resolved = undefined;
        throw err;
      }));
  }

  /**
   * Past transfers, built as `transfers()` builds them, over the range and
   * options `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getTransfers(filter: TransferFilter = {}): Promise<StreamdEvent<Transfer>[]> {
    const { asset, account, ...rest } = filter;
    const query = new EventQuery(transferSource(this.adapter, asset, this.accountOf(account)), this.limiter);
    return query.getEvents<Transfer>(TRANSFER_DESCRIPTOR, rest);
  }

  /**
   * Query historical events matching a descriptor and filter. Pass an array of
   * descriptors, of any kinds, to get their events merged into one history in
   * `(version, eventIndex)` order, each event once. Each descriptor is walked
   * separately (`maxPages` caps each walk); several Move types on one `module`
   * descriptor stay the cheaper single scan, and the one that works with
   * `onCursor`, which a merge rejects, as it does `offset`.
   */
  async getEvents<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: EventFilter,
  ): Promise<StreamdEvent<TData>[]> {
    return this.query.getEvents<TData>(descriptor, filter);
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getEventPages<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    return this.query.getEventPages<TData>(descriptor, filter, pageSize);
  }

  /**
   * Historical events one at a time: `getEventPages` flattened, with the same
   * options. Leaving the loop early fetches nothing further.
   */
  iterateEvents<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<StreamdEvent<TData>, void, unknown> {
    return this.query.iterateEvents<TData>(descriptor, filter, pageSize);
  }

  /**
   * Wait for the transaction with this hash to be committed and resolve with
   * its events in transaction order, narrowed by `descriptor` when given, then
   * `decode` and `where`. A `handle` descriptor must name its handle by
   * `creationNumber`. Looks the transaction up every `pollInterval` (the
   * client-wide default, at least 100ms here) through
   * `EventAdapter.getTransactionEvents`.
   *
   * Rejects with `StreamdTransactionError` (carrying `vmStatus`) when the
   * transaction failed, as its events were discarded on chain; with
   * `StreamdTimeoutError` when it is not committed within `timeout`; with
   * `signal.reason` on abort; and at once with any lookup error.
   */
  async eventsOf<TData = unknown>(
    transactionHash: string,
    descriptor?: EventDescriptor<TData>,
    options: EventsOfOptions = {},
  ): Promise<StreamdEvent<TData>[]> {
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    const events = await eventsOf(
      this.adapter,
      this.limiter,
      this.defaults.pollInterval!,
      transactionHash,
      descriptor,
      options,
    );
    return events as StreamdEvent<TData>[];
  }

  /**
   * Assertions on the events of the transaction with this hash, for
   * integration tests under any runner: `toEmit(descriptor, where?)` resolves
   * with the matching events and `notToEmit(descriptor, where?)` with nothing,
   * and either rejects with `StreamdAssertionError`, listing every event the
   * transaction emitted, when it does not hold. Nothing is looked up until an
   * assertion runs; each waits as `eventsOf` does and rejects as it does.
   * Throws at once on a malformed hash or `timeout`.
   */
  expectEvents(transactionHash: string, options: ExpectEventsOptions = {}): EventExpectation {
    requireTransactionHash(transactionHash);
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    return expectEvents(
      this.adapter,
      this.limiter,
      this.defaults.pollInterval!,
      transactionHash,
      options,
    );
  }

  /**
   * Simulate a built, unsigned `transaction` (from
   * `aptos.transaction.build.simple`) and resolve with the events it would
   * emit if it ran now, narrowed like `eventsOf`. Nothing is signed or
   * submitted. The events carry the simulated `version` and no
   * `transactionHash`: neither is the one the transaction will commit under.
   * Goes through `EventAdapter.simulateTransactionEvents`.
   *
   * Rejects with `StreamdTransactionError` (carrying `vmStatus`) when the
   * simulation fails, and with any simulation error as it is.
   */
  async previewEvents<TData = unknown>(
    transaction: SimpleTransaction,
    descriptor?: EventDescriptor<TData>,
    options: PreviewEventsOptions = {},
  ): Promise<StreamdEvent<TData>[]> {
    const events = await previewEvents(this.adapter, this.limiter, transaction, descriptor, options);
    return events as StreamdEvent<TData>[];
  }

  /**
   * Stop the earliest `on()`/`onBatch()` subscription on `descriptor` whose
   * callback is `callback`, or all of them on `descriptor` when it is omitted.
   */
  off<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    callback?: EventCallback<TData> | BatchCallback<TData>,
  ): this {
    // A missing descriptor must not fall through to removing everything.
    if (descriptorKey(descriptor) === undefined) return this;
    if (callback === undefined) return this.removeAllListeners(descriptor);
    this.matching(descriptor).find((l) => l.callback === callback)?.stop();
    return this;
  }

  /**
   * Stop every `on()`/`onBatch()` subscription, or those on `descriptor`.
   * `once()` and `stream()` keep running; `destroy()` ends those too.
   */
  removeAllListeners(descriptor?: EventDescriptor): this {
    for (const l of this.matching(descriptor)) l.stop();
    return this;
  }

  /**
   * How many `on()`/`onBatch()` subscriptions are live (paused included), on
   * `descriptor` or in total. `once()` and `stream()` are not counted.
   */
  listenerCount(descriptor?: EventDescriptor): number {
    return this.matching(descriptor).length;
  }

  private matching(descriptor?: EventDescriptor): Listener[] {
    const all = [...this.listeners.values()];
    if (descriptor === undefined) return all;
    const key = descriptorKey(descriptor);
    return key === undefined ? [] : all.filter((l) => l.key === key);
  }

  /**
   * Stop all active subscriptions and end open streams. It ends what is
   * running rather than retiring the client: `on()` or a stream first iterated
   * afterwards still starts polling.
   */
  destroy(): void {
    for (const close of [...this.streams]) close();
    for (const { stop } of [...this.subscriptions.values()]) stop();
    this.shared.clear();
    this.limiter.clear();
  }

  /** `destroy()`, so `using client = new StreamdClient(...)` cleans up at scope exit. */
  [DISPOSE](): void {
    this.destroy();
  }
}
