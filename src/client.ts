import { Aptos } from "@aptos-labs/ts-sdk";
import type {
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
import { EventPoller } from "./poller.js";
import { EventQuery } from "./query.js";
import {
  StreamdError,
  StreamdPollingError,
  StreamdTimeoutError,
} from "./errors.js";
import { generateId, onAbort } from "./utils.js";
import { eventStream } from "./stream.js";

const MAX_TIMEOUT = 2_147_483_647;

function checkTimeout(ms: number | undefined): StreamdError | undefined {
  if (ms === undefined || (typeof ms === "number" && ms >= 0 && ms <= MAX_TIMEOUT)) {
    return undefined;
  }
  return new StreamdError(`Invalid timeout: ${String(ms)} (expected 0..${MAX_TIMEOUT}ms)`);
}

export class StreamdClient {
  private readonly aptos: Aptos;
  private readonly defaultPollInterval: number;
  private readonly onError?: (error: Error) => void;
  private readonly adapter: EventAdapter;
  private readonly query: EventQuery;
  private readonly subscriptions = new Map<string, EventPoller>();
  /** Open streams, so destroy() can end their loops rather than strand them. */
  private readonly streams = new Set<() => void>();

  constructor(config: StreamdConfig) {
    this.aptos =
      config.aptos instanceof Aptos
        ? config.aptos
        : new Aptos(config.aptos);
    this.defaultPollInterval = config.defaultPollInterval ?? 1000;
    this.onError = config.onError;
    this.adapter = config.adapter ?? new FullnodeScanAdapter(this.aptos);
    this.query = new EventQuery(this.adapter);
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
    const id = generateId();
    let detach = () => {};
    const forget = () => {
      this.subscriptions.delete(id);
      detach();
    };
    const poller = new EventPoller(
      this.adapter,
      descriptor,
      callback as EventCallback,
      {
        pollInterval: this.defaultPollInterval,
        ...options,
        onError: (err) => {
          // The poller stops itself before reporting its last failure, so an
          // inactive poller here has given up and must not stay in the map.
          if (!poller.active) forget();
          (options?.onError ?? this.onError)?.(err);
        },
      },
    );

    poller.start();
    // An already-aborted signal leaves the poller stopped: nothing to track.
    if (poller.active) {
      this.subscriptions.set(id, poller);
      // The poller stops itself on abort; the client only has to forget it.
      detach = onAbort(options?.signal, forget);
    }

    return {
      id,
      get active() {
        return poller.active;
      },
      get cursor() {
        return poller.cursor;
      },
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
        this.streams.add(close);
        return this.on<TData>(descriptor, callback, {
          ...options,
          signal: undefined,
          onError: (err) => {
            onError(err);
            (options?.onError ?? this.onError)?.(err);
          },
        });
      },
      () => this.streams.delete(close),
      options?.signal,
    );
    return stream;
  }

  /**
   * Query historical events (like eth_getLogs).
   */
  async getLogs<TData = unknown>(
    descriptor: EventDescriptor,
    filter?: EventFilter,
  ): Promise<StreamdEvent<TData>[]> {
    return this.query.getLogs<TData>(descriptor, filter);
  }

  /**
   * Async iterator for paginating through large historical result sets.
   */
  getLogsIterator<TData = unknown>(
    descriptor: EventDescriptor,
    filter?: EventFilter,
    pageSize?: number,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    return this.query.getLogsIterator<TData>(descriptor, filter, pageSize);
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
