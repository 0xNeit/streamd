import type {
  CloseOptions,
  DescriptorData,
  DescriptorEvent,
  BatchCallback,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventStream,
  EventFilter,
  ListenerDefaults,
  ListenerOptions,
  OnceOptions,
  ReduceOptions,
  Reducer,
  Reduction,
  Replay,
  ReplayOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
  SubscriptionError,
  SubscriptionInfo,
  SubscriptionOptions,
  TransactionCallback,
  TransactionFilter,
  TransactionGroup,
  Watch,
  WatchCallback,
  WatchOptions,
} from "./types.js";
import { EventPoller, resolveMaxBackoff, resolveMaxPollInterval, type Delivery } from "./poller.js";
import { EventQuery } from "./query.js";
import {
  StreamdError,
  StreamdTimeoutError,
} from "./errors.js";
import { ASYNC_DISPOSE, descriptorKey, generateId, latch, onAbort, redactSecrets } from "./utils.js";
import { eventStream } from "./stream.js";
import { RequestLimiter } from "./limiter.js";
import { CursorManager } from "./cursor.js";
import { fieldDecoder } from "./decode.js";
import { SharedFetches } from "./shared.js";
import { startReduction, type ReduceHost } from "./reduce.js";
import { startReplay } from "./replay.js";
import { startTransactions, type TransactionHost } from "./grouping.js";
import { isThenable } from "./filter.js";
import { startWatch } from "./watch.js";
import { SenderNames } from "./names.js";

const MAX_TIMEOUT = 2_147_483_647;

// Runtimes without explicit resource management lack `Symbol.dispose`. The
// registry key is the one core-js polyfills it with, so `using` still finds
// the method when such a polyfill loads after this module.
const DISPOSE: typeof Symbol.dispose =
  Symbol.dispose ?? (Symbol.for("Symbol.dispose") as typeof Symbol.dispose);

const DEFAULT_CLOSE_TIMEOUT = 5000;

// Enough to see why recent walks ended, without a long-lived client growing.
const KEEP_ENDED = 20;

/** Whether `work` settled within `ms`. */
async function settlesWithin(work: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<boolean>((resolve) => {
    timer = setTimeout(() => resolve(false), ms);
  });
  try {
    return await Promise.race([work.then(() => true), expired]);
  } finally {
    clearTimeout(timer);
  }
}

function guardedWhere(
  where: ListenerOptions["where"],
  onThrow: () => void,
): ListenerOptions["where"] {
  if (where === undefined) return undefined;
  return (event) => {
    try {
      return where(event);
    } catch (err) {
      onThrow();
      throw err;
    }
  };
}

/** A `timeout` option checked, or the error to throw for it. */
export function checkTimeout(ms: number | undefined): StreamdError | undefined {
  if (ms === undefined || (typeof ms === "number" && ms >= 0 && ms <= MAX_TIMEOUT)) {
    return undefined;
  }
  return new StreamdError(`Invalid timeout: ${String(ms)} (expected 0..${MAX_TIMEOUT}ms)`);
}

function resolveLimiter(config: StreamdConfig): RequestLimiter {
  const { maxConcurrentRequests: max, maxRequestsPerSecond: perSecond, requestTimeout } = config;
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
  if (
    requestTimeout !== undefined &&
    (!Number.isFinite(requestTimeout) || requestTimeout <= 0 || requestTimeout > MAX_TIMEOUT)
  ) {
    throw new StreamdError(
      `Invalid requestTimeout: ${String(requestTimeout)} (expected 1..${MAX_TIMEOUT}ms)`,
    );
  }
  return new RequestLimiter(max, perSecond, requestTimeout);
}

const DEFAULT_KEYS = [
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "maxPollInterval",
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
  resolveMaxPollInterval(resolved.maxPollInterval);
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

/** A running poller, with what `listSubscriptions()` reports beside its status. */
interface Running {
  poller: EventPoller;
  stop: () => void;
  descriptor: EventDescriptor;
  listener: boolean;
  lastError?: SubscriptionError;
}

/** A copy nothing can change, so a snapshot never hands out the caller's object. */
function frozenDescriptor(descriptor: EventDescriptor): EventDescriptor {
  const copy = Object.fromEntries(
    Object.entries(descriptor).map(([k, v]) => [k, Array.isArray(v) ? Object.freeze([...v]) : v]),
  );
  return Object.freeze(copy) as EventDescriptor;
}

function snapshot(id: string, { poller, descriptor, listener, lastError }: Running, ended: boolean): SubscriptionInfo {
  return Object.freeze({
    id,
    descriptor,
    listener,
    ended,
    paused: poller.paused,
    ...poller.status,
    ...(lastError === undefined ? {} : { lastError }),
  });
}

function errorInfo(error: Error): SubscriptionError {
  const cause = (error as { cause?: unknown }).cause;
  return Object.freeze({
    name: error.name,
    message: redactSecrets(error.message),
    ...(cause instanceof Error ? { cause: redactSecrets(cause.message) } : {}),
    at: Date.now(),
  });
}


function resolveAdapter(config: StreamdConfig): EventAdapter {
  // Still checked at runtime for JavaScript callers, whom the type does not reach.
  if (config?.adapter === undefined) {
    throw new StreamdError("StreamdClient requires an `adapter`, such as FullnodeScanAdapter from @streamd/aptos");
  }
  return config.adapter;
}

export class StreamdClient {
  protected readonly defaults: ListenerDefaults;
  protected readonly onError?: (error: Error) => void;
  protected readonly adapter: EventAdapter;
  private readonly query: EventQuery;
  /** Shared by every poller and query, so the cap holds client-wide. */
  protected readonly limiter: RequestLimiter;
  protected readonly requestTimeout?: number;
  /** The `withNames` cache, shared by every call; a chain client serves its lookup into it. */
  protected readonly names: SenderNames;
  /** Lets identical subscriptions share each poll's adapter request. */
  private readonly shared: SharedFetches;
  private readonly subscriptions = new Map<string, Running>();
  /** The `on()`/`onBatch()` subset of `subscriptions`; `once()` and streams own theirs. */
  private readonly listeners = new Map<string, Listener>();
  /** Final snapshots of the most recently ended subscriptions, newest first. */
  private readonly ended: SubscriptionInfo[] = [];
  /** Open streams, so destroy() can end their loops rather than strand them. */
  private readonly streams = new Set<() => void>();
  /** In-flight handler promises that outlived their subscription's removal or belong to replay(). */
  private readonly inflight = new Set<Promise<void>>();
  /** What `reduce()` needs, built once. */
  protected readonly reduceHost: ReduceHost;
  /** What `onTransaction()` needs, built once. */
  private readonly transactionHost: TransactionHost;

  private trackHandler(work: Promise<unknown>): void {
    const settled = Promise.resolve(work).then(
      () => {},
      () => {},
    );
    this.inflight.add(settled);
    void settled.finally(() => this.inflight.delete(settled));
  }

  constructor(config: StreamdConfig) {
    this.defaults = resolveDefaults(config);
    this.onError = config.onError;
    this.limiter = resolveLimiter(config);
    this.requestTimeout = config.requestTimeout;
    this.adapter = resolveAdapter(config);
    this.shared = new SharedFetches(this.adapter, this.requestTimeout);
    this.names = new SenderNames(this.limiter);
    this.query = new EventQuery(this.adapter, this.limiter, this.names);
    this.reduceHost = {
      query: this.query,
      defaults: this.defaults,
      onError: this.onError,
      subscribe: (d, callback, options) => this.follow(d, callback, options),
      head: async (d) => {
        const adapter = this.limiter.wrap(this.adapter);
        if (adapter.getHeadVersion === undefined) {
          throw new StreamdError("A merged reduction needs an adapter that implements getHeadVersion");
        }
        return adapter.getHeadVersion(d);
      },
      track: (stop) => (this.streams.add(stop), () => void this.streams.delete(stop)),
      trackHandler: (work) => this.trackHandler(work),
    };
    this.transactionHost = {
      defaults: this.defaults,
      onError: this.onError,
      unshared: this.shared.view(0),
      subscribe: (d, delivery, options, id, wrap) =>
        this.subscribe(d, delivery, options, undefined, id, wrap),
      track: (stop) => (this.streams.add(stop), () => void this.streams.delete(stop)),
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
  on<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    callback: EventCallback<TData, TEvent>,
    options?: SubscriptionOptions,
  ): Subscription;
  on<const TList extends readonly EventDescriptor<any, any>[]>(
    descriptors: TList,
    callback: EventCallback<DescriptorData<TList[number]>, DescriptorEvent<TList[number]>>,
    options?: SubscriptionOptions,
  ): Subscription;
  on(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    callback: EventCallback<any, any>,
    options?: SubscriptionOptions,
  ): Subscription {
    return this.follow(descriptor, callback as EventCallback, options, callback, options?.id);
  }

  /**
   * Like `on()`, but each poll's new events arrive together in one
   * `callback(events)` call, at most `batchSize` of them.
   */
  onBatch<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    callback: BatchCallback<TData, TEvent>,
    options?: SubscriptionOptions,
  ): Subscription;
  onBatch<const TList extends readonly EventDescriptor<any, any>[]>(
    descriptors: TList,
    callback: BatchCallback<DescriptorData<TList[number]>, DescriptorEvent<TList[number]>>,
    options?: SubscriptionOptions,
  ): Subscription;
  onBatch(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    callback: BatchCallback<any, any>,
    options?: SubscriptionOptions,
  ): Subscription {
    const merged = mergeable(descriptor);
    if (Array.isArray(merged)) {
      return startTransactions(
        this.transactionHost,
        merged,
        { batch: (groups) => (callback as BatchCallback)(groups.flatMap((g) => g.events)) },
        options,
      );
    }
    return this.subscribe(
      merged as EventDescriptor,
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
  onTransaction<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    callback: TransactionCallback<TData, TEvent>,
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
   * sequence-numbered transactions the address sent (omitting orderless
   * transactions and received transfers); on `IndexerAccountAdapter`, every one
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

  /**
   * One descriptor follows as one walk; a list of them is merged through the
   * transaction buffer, so events arrive in version order, each once.
   */
  private follow(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    callback: EventCallback,
    options?: ListenerOptions,
    listener?: unknown,
    id?: string,
  ): Subscription {
    const merged = mergeable(descriptor);
    if (!Array.isArray(merged)) return this.subscribe(merged as EventDescriptor, callback, options, listener, id);
    const report = (error: Error) => {
      try {
        (options?.onError ?? this.onError)?.(error);
      } catch (thrown) {
        queueMicrotask(() => {
          throw thrown;
        });
      }
    };
    return startTransactions(
      this.transactionHost,
      merged,
      async (group) => {
        for (const event of group.events) {
          try {
            const result: unknown = callback(event);
            if (isThenable(result)) await result;
          } catch (err) {
            report(new StreamdError(`Event callback threw for ${event.version}:${event.eventIndex}`, err));
          }
        }
      },
      options,
    );
  }

  protected subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options?: ListenerOptions,
    listener?: unknown,
    callerId?: string,
    wrap?: (source: EventAdapter) => EventAdapter,
  ): Subscription {
    const id = callerId === undefined ? generateId() : this.claimId(callerId);
    let detach = () => {};
    const reached = latch();
    const forget = () => {
      reached.reject(new StreamdError("Subscription ended before catching up"));
      this.trackHandler(poller.handling);
      // A later subscription may hold this id by now; leave its entries alone.
      const running = this.subscriptions.get(id);
      if (running?.poller === poller) {
        this.ended.unshift(snapshot(id, running, true));
        this.ended.length = Math.min(this.ended.length, KEEP_ENDED);
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
        onCaughtUp: () => {
          reached.resolve();
          resolved.onCaughtUp?.();
        },
        onError: (err) => {
          const running = this.subscriptions.get(id);
          if (running?.poller === poller) running.lastError = errorInfo(err);
          // Drop the subscription once the poller has given up and stopped
          // itself; a paused one reporting a late handler error stays.
          if (!poller.active && !poller.paused) {
            reached.reject(err);
            forget();
          }
          (options?.onError ?? this.onError)?.(err);
        },
      },
      this.limiter,
      this.names,
    );

    poller.start();
    // An already-aborted signal leaves the poller stopped: nothing to track.
    if (poller.active) {
      const stop = () => (poller.stop(), forget());
      this.subscriptions.set(id, {
        poller,
        stop,
        descriptor: frozenDescriptor(descriptor),
        listener: listener !== undefined,
      });
      if (listener !== undefined) {
        this.listeners.set(id, { key: descriptorKey(descriptor), callback: listener, stop });
      }
      detach = onAbort(options?.signal, () => {
        reached.reject(options!.signal!.reason);
        forget();
      });
    } else {
      reached.reject(options?.signal?.reason);
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
      caughtUp: reached.promise,
      pause: () => poller.pause(),
      resume: () => poller.resume(),
      unsubscribe,
      [DISPOSE]: unsubscribe,
      [ASYNC_DISPOSE]: async () => {
        unsubscribe();
        await settlesWithin(poller.handling, DEFAULT_CLOSE_TIMEOUT);
      },
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
  once<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    options?: OnceOptions,
  ): Promise<TEvent>;
  once<const TList extends readonly EventDescriptor<any, any>[]>(
    descriptors: TList,
    options?: OnceOptions,
  ): Promise<DescriptorEvent<TList[number]>>;
  once(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    options?: OnceOptions,
  ): Promise<any> {
    const badTimeout = checkTimeout(options?.timeout);
    if (badTimeout !== undefined) return Promise.reject(badTimeout);
    if (options?.signal?.aborted) return Promise.reject(options.signal.reason);

    return new Promise<any>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let detach = () => {};
      let sub: Subscription | undefined;
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        if (timer !== undefined) clearTimeout(timer);
        detach();
        sub?.unsubscribe();
      };
      const fail = (reason: unknown) => (close(), reject(reason));
      // Only a synchronous throw counts: a rejection fails the poll and is retried.
      let whereThrew = false;
      sub = this.follow(
        descriptor,
        ((event: unknown) => (close(), resolve(event))) as unknown as EventCallback,
        {
          ...options,
          where: guardedWhere(options?.where, () => (whereThrew = true)),
          signal: undefined,
          onError: (err) => {
            // Settle before the caller's handler runs, so a throw cannot strand the promise.
            if (sub === undefined || !sub.active || whereThrew) fail(err);
            (options?.onError ?? this.onError)?.(err);
          },
        },
      );
      if (closed) return sub.unsubscribe();
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
  stream<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    options?: ListenerOptions,
  ): EventStream<TData, TEvent>;
  stream<const TList extends readonly EventDescriptor<any, any>[]>(
    descriptors: TList,
    options?: ListenerOptions,
  ): EventStream<DescriptorData<TList[number]>, DescriptorEvent<TList[number]>>;
  stream(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    options?: ListenerOptions,
  ): EventStream<any, any> {
    // Registered only once the stream subscribes, so an unused stream costs nothing.
    const close = () => void stream.return?.();
    const stream = eventStream<any, any>(
      (callback, onError) => {
        const sub = this.follow(descriptor, callback as EventCallback, {
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
  reduce<S, TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    reducer: Reducer<S, TData, TEvent>,
    initial: S,
    options?: ReduceOptions<S, TData, TEvent>,
  ): Reduction<S>;
  reduce<S, const TList extends readonly EventDescriptor<any, any>[]>(
    descriptors: TList,
    reducer: Reducer<S, DescriptorData<TList[number]>, DescriptorEvent<TList[number]>>,
    initial: S,
    options?: ReduceOptions<S, DescriptorData<TList[number]>, DescriptorEvent<TList[number]>>,
  ): Reduction<S>;
  reduce<S>(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    reducer: Reducer<S, any, any>,
    initial: S,
    options?: ReduceOptions<S, any, any>,
  ): Reduction<S> {
    return startReduction<S, any, any>(this.reduceHost, descriptor, reducer, initial, options);
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
  replay<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    callback: EventCallback<TData, TEvent>,
    options?: ReplayOptions,
  ): Replay {
    return startReplay<any, any>(this.reduceHost, descriptor, callback, options);
  }

  /**
   * Query historical events matching a descriptor and filter. Pass an array of
   * descriptors, of any kinds, to get their events merged into one history in
   * `(version, eventIndex)` order, each event once. Each descriptor is walked
   * separately (`maxPages` caps each walk); several Move types on one `module`
   * descriptor stay the cheaper single scan, and the one that works with
   * `onCursor`, which a merge rejects, as it does `offset`.
   */
  async getEvents<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    filter?: EventFilter,
  ): Promise<TEvent[]> {
    return this.query.getEvents(descriptor, filter) as unknown as Promise<TEvent[]>;
  }

  /**
   * A closed range as whole transactions, the query form of `onTransaction()`:
   * each group holds a transaction's matching events in `eventIndex` order,
   * its fields taken as `onTransaction()` takes them. `decode` and `where`
   * apply per event first, so a transaction with no match yields no group.
   * `limit` counts groups, and the walk reads on until a later version or the
   * range's end so the last one is whole; `orderBy: "desc"` returns the last
   * `limit` newest first; `withState` reads only the returned events. A list
   * of descriptors is merged as `getEvents` merges it, each event once.
   * `maxPages` caps the work, so a group at the cap may lack events past it.
   */
  async getTransactions<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    filter?: TransactionFilter,
  ): Promise<TransactionGroup<TData, TEvent>[]> {
    return this.query.getTransactions(descriptor, filter) as unknown as Promise<
      TransactionGroup<TData, TEvent>[]
    >;
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getEventPages<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<TEvent[], void, unknown> {
    return this.query.getEventPages(descriptor, filter, pageSize) as unknown as AsyncGenerator<
      TEvent[],
      void,
      unknown
    >;
  }

  /**
   * Historical events one at a time: `getEventPages` flattened, with the same
   * options. Leaving the loop early fetches nothing further.
   */
  iterateEvents<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent> | readonly EventDescriptor<TData, TEvent>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<TEvent, void, unknown> {
    return this.query.iterateEvents(descriptor, filter, pageSize) as unknown as AsyncGenerator<
      TEvent,
      void,
      unknown
    >;
  }

  /**
   * Stop the earliest `on()`/`onBatch()` subscription on `descriptor` whose
   * callback is `callback`, or all of them on `descriptor` when it is omitted.
   */
  off<TData = unknown, TEvent = StreamdEvent<TData>>(
    descriptor: EventDescriptor<TData, TEvent>,
    callback?: EventCallback<TData, TEvent> | BatchCallback<TData, TEvent>,
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

  /**
   * A snapshot of every subscription this client is running, in start order:
   * `on()`, `onBatch()`, `once()` and `stream()`, each walk of a merged list,
   * and the walks behind `reduce()`, `onTransaction()`, `watch()` and chain
   * helpers. With `ended: true`, the final snapshots of the last 20 that ended
   * follow, newest first, so a walk that gave up still shows its error.
   * Read-only, for devtools and logs: each entry is a frozen copy, and nothing
   * in it pauses, resumes or moves a subscription. Error text has URLs and
   * credentials taken out.
   */
  listSubscriptions(options: { ended?: boolean } = {}): SubscriptionInfo[] {
    const live = [...this.subscriptions].map(([id, running]) => snapshot(id, running, false));
    return options.ended === true ? [...live, ...this.ended] : live;
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

  /**
   * `destroy()`, then waits for running handlers (including ended
   * subscriptions and `replay()`), stopping any subscriptions started while
   * draining. Resolves `true` once settled, or `false` at `timeout`.
   */
  async close(options: CloseOptions = {}): Promise<boolean> {
    const timeout = options.timeout ?? DEFAULT_CLOSE_TIMEOUT;
    const invalid = checkTimeout(timeout);
    if (invalid !== undefined) throw invalid;
    const deadline = performance.now() + timeout;
    for (;;) {
      this.destroy();
      const pending = [...this.inflight];
      if (pending.length === 0) return true;
      const remaining = Math.max(0, deadline - performance.now());
      if (!(await settlesWithin(Promise.all(pending), remaining))) {
        this.destroy();
        return false;
      }
      for (const p of pending) this.inflight.delete(p);
      if (this.subscriptions.size === 0 && this.streams.size === 0 && this.inflight.size === 0) {
        return true;
      }
    }
  }

  /** `close()`, so `await using client = new StreamdClient(...)` drains at scope exit. */
  async [ASYNC_DISPOSE](): Promise<void> {
    await this.close();
  }
}

/** A lone descriptor, or a list of two or more to merge; one in a list is followed alone. */
function mergeable(
  descriptor: EventDescriptor | readonly EventDescriptor[],
): EventDescriptor | readonly EventDescriptor[] {
  if (!Array.isArray(descriptor)) return descriptor as EventDescriptor;
  if (descriptor.length === 0) throw new StreamdError("At least one descriptor is needed");
  return descriptor.length === 1 ? descriptor[0]! : descriptor;
}
