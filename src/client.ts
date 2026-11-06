import { Aptos } from "@aptos-labs/ts-sdk";
import type {
  BatchCallback,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  ListenerDefaults,
  ListenerOptions,
  OnceOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "./types.js";
import { FullnodeScanAdapter } from "./adapters/fullnode.js";
import { EventPoller, resolveMaxBackoff, type Delivery } from "./poller.js";
import { EventQuery } from "./query.js";
import {
  StreamdError,
  StreamdPollingError,
  StreamdTimeoutError,
} from "./errors.js";
import { descriptorKey, generateId, onAbort } from "./utils.js";
import { eventStream } from "./stream.js";
import { RequestLimiter } from "./limiter.js";
import { CursorManager } from "./cursor.js";
import { fieldDecoder } from "./decode.js";
import { SharedFetches } from "./shared.js";

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

function resolveLimiter(max: number | undefined): RequestLimiter {
  if (max === undefined) return new RequestLimiter();
  if (!Number.isInteger(max) || max < 1) {
    throw new StreamdError(
      `Invalid maxConcurrentRequests: ${String(max)} (expected a positive integer)`,
    );
  }
  return new RequestLimiter(max);
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

function resolveAdapter(config: StreamdConfig): EventAdapter {
  if (config.adapter !== undefined) return config.adapter;
  if (config.aptos === undefined) {
    throw new StreamdError(
      "StreamdClient requires either `aptos` or `adapter` in config",
    );
  }
  const aptos =
    config.aptos instanceof Aptos ? config.aptos : new Aptos(config.aptos);
  return new FullnodeScanAdapter(aptos);
}

export class StreamdClient {
  private readonly defaults: ListenerDefaults;
  private readonly onError?: (error: Error) => void;
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

  constructor(config: StreamdConfig) {
    this.defaults = resolveDefaults(config);
    this.onError = config.onError;
    this.limiter = resolveLimiter(config.maxConcurrentRequests);
    this.adapter = resolveAdapter(config);
    this.shared = new SharedFetches(this.adapter);
    this.query = new EventQuery(this.adapter, this.limiter);
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
    options?: ListenerOptions,
  ): Subscription {
    return this.subscribe(descriptor, callback as EventCallback, options, callback);
  }

  /**
   * Like `on()`, but each poll's new events arrive together in one
   * `callback(events)` call, at most `batchSize` of them.
   */
  onBatch<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    callback: BatchCallback<TData>,
    options?: ListenerOptions,
  ): Subscription {
    return this.subscribe(
      descriptor,
      { batch: callback as BatchCallback },
      options,
      callback,
    );
  }

  private subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options?: ListenerOptions,
    listener?: unknown,
  ): Subscription {
    const id = generateId();
    let detach = () => {};
    const forget = () => {
      this.subscriptions.delete(id);
      this.listeners.delete(id);
      detach();
    };
    const resolved = withDefaults(options, this.defaults);
    const poller = new EventPoller(
      this.shared.view(resolved.pollInterval!),
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

  /**
   * Wait for the next event matching the descriptor. Resolves with the first event received.
   *
   * Rejects with `signal.reason` on abort, with `StreamdTimeoutError` if
   * `timeout` elapses first, when polling gives up after `maxErrors`, at once
   * when the adapter refuses the descriptor or options as invalid input, or on
   * the first error thrown by `where`, even if a later event would have matched.
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
      const sub = this.subscribe(
        descriptor,
        ((event: StreamdEvent<TData>) => (close(), resolve(event))) as EventCallback,
        {
          ...options,
          signal: undefined,
          onError: (err) => {
            // Settle before the caller's handler runs, so a throw from it
            // cannot leave the promise pending.
            if (!sub.active || !(err instanceof StreamdPollingError)) fail(err);
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
   * aborts. Other errors go to `options.onError`.
   */
  stream<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    options?: ListenerOptions,
  ): AsyncIterableIterator<StreamdEvent<TData>> {
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
    );
    return stream;
  }

  /**
   * Query historical events matching a descriptor and filter.
   */
  async getEvents<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    filter?: EventFilter,
  ): Promise<StreamdEvent<TData>[]> {
    return this.query.getEvents<TData>(descriptor, filter);
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getEventPages<TData = unknown>(
    descriptor: EventDescriptor<TData>,
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
    descriptor: EventDescriptor<TData>,
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<StreamdEvent<TData>, void, unknown> {
    return this.query.iterateEvents<TData>(descriptor, filter, pageSize);
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
  }

  /** `destroy()`, so `using client = new StreamdClient(...)` cleans up at scope exit. */
  [DISPOSE](): void {
    this.destroy();
  }
}
