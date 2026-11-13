import type {
  CloseOptions,
  DescriptorData,
  EventOf,
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
import { ASYNC_DISPOSE, descriptorKey, generateId, latch, onAbort } from "./utils.js";
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

const MAX_TIMEOUT = 2_147_483_647;

// Runtimes without explicit resource management lack `Symbol.dispose`. The
// registry key is the one core-js polyfills it with, so `using` still finds
// the method when such a polyfill loads after this module.
const DISPOSE: typeof Symbol.dispose =
  Symbol.dispose ?? (Symbol.for("Symbol.dispose") as typeof Symbol.dispose);

const DEFAULT_CLOSE_TIMEOUT = 5000;

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
  /** Lets identical subscriptions share each poll's adapter request. */
  private readonly shared: SharedFetches;
  private readonly subscriptions = new Map<string, { poller: EventPoller; stop: () => void }>();
  /** The `on()`/`onBatch()` subset of `subscriptions`; `once()` and streams own theirs. */
  private readonly listeners = new Map<string, Listener>();
  /** Open streams, so destroy() can end their loops rather than strand them. */
  private readonly streams = new Set<() => void>();
  /** What `reduce()` needs, built once. */
  protected readonly reduceHost: ReduceHost;
  /** What `onTransaction()` needs, built once. */
  private readonly transactionHost: TransactionHost;

  constructor(config: StreamdConfig) {
    this.defaults = resolveDefaults(config);
    this.onError = config.onError;
    this.limiter = resolveLimiter(config);
    this.requestTimeout = config.requestTimeout;
    this.adapter = resolveAdapter(config);
    this.shared = new SharedFetches(this.adapter, this.requestTimeout);
    this.query = new EventQuery(this.adapter, this.limiter);
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
  ): Subscription;
  on<const TList extends readonly EventDescriptor<any>[]>(
    descriptors: TList,
    callback: EventCallback<DescriptorData<TList[number]>>,
    options?: SubscriptionOptions,
  ): Subscription;
  on(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    callback: EventCallback<any>,
    options?: SubscriptionOptions,
  ): Subscription {
    return this.follow(descriptor, callback as EventCallback, options, callback, options?.id);
  }

  /**
   * Like `on()`, but each poll's new events arrive together in one
   * `callback(events)` call, at most `batchSize` of them.
   */
  onBatch<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    callback: BatchCallback<TData>,
    options?: SubscriptionOptions,
  ): Subscription;
  onBatch<const TList extends readonly EventDescriptor<any>[]>(
    descriptors: TList,
    callback: BatchCallback<DescriptorData<TList[number]>>,
    options?: SubscriptionOptions,
  ): Subscription;
  onBatch(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    callback: BatchCallback<any>,
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
        onCaughtUp: () => {
          reached.resolve();
          resolved.onCaughtUp?.();
        },
        onError: (err) => {
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
    );

    poller.start();
    // An already-aborted signal leaves the poller stopped: nothing to track.
    if (poller.active) {
      const stop = () => (poller.stop(), forget());
      this.subscriptions.set(id, { poller, stop });
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
  once<TData = unknown>(descriptor: EventDescriptor<TData>, options?: OnceOptions): Promise<EventOf<TData>>;
  once<const TList extends readonly EventDescriptor<any>[]>(
    descriptors: TList,
    options?: OnceOptions,
  ): Promise<EventOf<DescriptorData<TList[number]>>>;
  once<TData = unknown>(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    options?: OnceOptions,
  ): Promise<EventOf<TData>> {
    const badTimeout = checkTimeout(options?.timeout);
    if (badTimeout !== undefined) return Promise.reject(badTimeout);
    if (options?.signal?.aborted) return Promise.reject(options.signal.reason);

    return new Promise<EventOf<TData>>((resolve, reject) => {
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
      const sub = this.follow(
        descriptor,
        ((event: EventOf<TData>) => (close(), resolve(event))) as unknown as EventCallback,
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
  stream<TData = unknown>(descriptor: EventDescriptor<TData>, options?: ListenerOptions): EventStream<TData>;
  stream<const TList extends readonly EventDescriptor<any>[]>(
    descriptors: TList,
    options?: ListenerOptions,
  ): EventStream<DescriptorData<TList[number]>>;
  stream<TData = unknown>(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    options?: ListenerOptions,
  ): EventStream<TData> {
    // Registered only once the stream subscribes, so an unused stream costs nothing.
    const close = () => void stream.return?.();
    const stream = eventStream<TData>(
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
  reduce<S, TData = unknown>(
    descriptor: EventDescriptor<TData>,
    reducer: Reducer<S, TData>,
    initial: S,
    options?: ReduceOptions<S, TData>,
  ): Reduction<S>;
  reduce<S, const TList extends readonly EventDescriptor<any>[]>(
    descriptors: TList,
    reducer: Reducer<S, DescriptorData<TList[number]>>,
    initial: S,
    options?: ReduceOptions<S, DescriptorData<TList[number]>>,
  ): Reduction<S>;
  reduce<S>(
    descriptor: EventDescriptor | readonly EventDescriptor[],
    reducer: Reducer<S, any>,
    initial: S,
    options?: ReduceOptions<S, any>,
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
  ): Promise<EventOf<TData>[]> {
    return this.query.getEvents<TData>(descriptor, filter) as unknown as Promise<EventOf<TData>[]>;
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
  async getTransactions<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: TransactionFilter,
  ): Promise<TransactionGroup<TData>[]> {
    return this.query.getTransactions<TData>(descriptor, filter);
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getEventPages<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<EventOf<TData>[], void, unknown> {
    return this.query.getEventPages<TData>(descriptor, filter, pageSize) as unknown as AsyncGenerator<
      EventOf<TData>[],
      void,
      unknown
    >;
  }

  /**
   * Historical events one at a time: `getEventPages` flattened, with the same
   * options. Leaving the loop early fetches nothing further.
   */
  iterateEvents<TData = unknown>(
    descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<EventOf<TData>, void, unknown> {
    return this.query.iterateEvents<TData>(descriptor, filter, pageSize) as unknown as AsyncGenerator<
      EventOf<TData>,
      void,
      unknown
    >;
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

  /**
   * `destroy()`, then waits for the handlers those subscriptions have running.
   * Resolves `true` once they have finished, or `false` at `timeout`.
   */
  async close(options: CloseOptions = {}): Promise<boolean> {
    const timeout = options.timeout ?? DEFAULT_CLOSE_TIMEOUT;
    const invalid = checkTimeout(timeout);
    if (invalid !== undefined) throw invalid;
    const running = [...this.subscriptions.values()].map(({ poller }) => poller.handling);
    this.destroy();
    return settlesWithin(Promise.all(running), timeout);
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
