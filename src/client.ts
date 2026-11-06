import { Aptos } from "@aptos-labs/ts-sdk";
import type {
  BatchCallback,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  ListenerOptions,
  OnceOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "./types.js";
import { FullnodeScanAdapter } from "./adapters/fullnode.js";
import { EventPoller, type Delivery } from "./poller.js";
import { EventQuery } from "./query.js";
import {
  StreamdError,
  StreamdPollingError,
  StreamdTimeoutError,
} from "./errors.js";
import { generateId, onAbort } from "./utils.js";
import { eventStream } from "./stream.js";
import { RequestLimiter } from "./limiter.js";

const MAX_TIMEOUT = 2_147_483_647;

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
  private readonly defaultPollInterval: number;
  private readonly onError?: (error: Error) => void;
  private readonly adapter: EventAdapter;
  private readonly query: EventQuery;
  /** Shared by every poller and query, so the cap holds client-wide. */
  private readonly limiter: RequestLimiter;
  private readonly subscriptions = new Map<string, EventPoller>();
  /** Open streams, so destroy() can end their loops rather than strand them. */
  private readonly streams = new Set<() => void>();

  constructor(config: StreamdConfig) {
    this.defaultPollInterval = config.defaultPollInterval ?? 1000;
    this.onError = config.onError;
    this.limiter = resolveLimiter(config.maxConcurrentRequests);
    this.adapter = resolveAdapter(config);
    this.query = new EventQuery(this.adapter, this.limiter);
  }

  /**
   * Subscribe to real-time events. Polls under the hood.
   *
   * Every subscription polls on its own. For a `module` descriptor on the
   * default adapter that means its own ledger read and transaction scan per
   * poll, so N module subscriptions scan the same transactions N times. Pass an
   * array of `eventType`s on one `module` descriptor to serve several Move
   * types from a single scan.
   */
  on<TData = unknown>(
    descriptor: EventDescriptor,
    callback: EventCallback<TData>,
    options?: ListenerOptions,
  ): Subscription {
    return this.subscribe(descriptor, callback as EventCallback, options);
  }

  /**
   * Like `on()`, but each poll's new events arrive together in one
   * `callback(events)` call, at most `batchSize` of them.
   */
  onBatch<TData = unknown>(
    descriptor: EventDescriptor,
    callback: BatchCallback<TData>,
    options?: ListenerOptions,
  ): Subscription {
    return this.subscribe(descriptor, { batch: callback as BatchCallback }, options);
  }

  private subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options?: ListenerOptions,
  ): Subscription {
    const id = generateId();
    let detach = () => {};
    const forget = () => {
      this.subscriptions.delete(id);
      detach();
    };
    const poller = new EventPoller(
      this.adapter,
      descriptor,
      delivery,
      {
        pollInterval: this.defaultPollInterval,
        ...options,
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
      this.subscriptions.set(id, poller);
      detach = onAbort(options?.signal, forget);
    }

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
      unsubscribe: () => {
        poller.stop();
        forget();
      },
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
    descriptor: EventDescriptor,
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
      const sub = this.on<TData>(
        descriptor,
        (event) => (close(), resolve(event)),
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
    descriptor: EventDescriptor,
    options?: ListenerOptions,
  ): AsyncIterableIterator<StreamdEvent<TData>> {
    // Registered only once the stream subscribes, so an unused stream costs nothing.
    const close = () => void stream.return?.();
    const stream = eventStream<TData>(
      (callback, onError) => {
        const sub = this.on<TData>(descriptor, callback, {
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
    descriptor: EventDescriptor,
    filter?: EventFilter,
  ): Promise<StreamdEvent<TData>[]> {
    return this.query.getEvents<TData>(descriptor, filter);
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getEventPages<TData = unknown>(
    descriptor: EventDescriptor,
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    return this.query.getEventPages<TData>(descriptor, filter, pageSize);
  }

  /**
   * Stop all active subscriptions and end open streams. It ends what is
   * running rather than retiring the client: `on()` or a stream first iterated
   * afterwards still starts polling.
   */
  destroy(): void {
    for (const close of [...this.streams]) close();
    for (const [id, poller] of this.subscriptions) {
      poller.stop();
      this.subscriptions.delete(id);
    }
  }
}
