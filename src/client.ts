import { Aptos } from "@aptos-labs/ts-sdk";
import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  ListenerOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "./types.js";
import { FullnodeScanAdapter } from "./adapters/fullnode.js";
import { EventPoller } from "./poller.js";
import { EventQuery } from "./query.js";
import { StreamdPollingError } from "./errors.js";
import { generateId } from "./utils.js";
import { eventStream } from "./stream.js";

export class StreamdClient {
  private readonly aptos: Aptos;
  private readonly defaultPollInterval: number;
  private readonly adapter: EventAdapter;
  private readonly query: EventQuery;
  private readonly subscriptions = new Map<string, EventPoller>();

  constructor(config: StreamdConfig) {
    this.aptos =
      config.aptos instanceof Aptos
        ? config.aptos
        : new Aptos(config.aptos);
    this.defaultPollInterval = config.defaultPollInterval ?? 1000;
    this.adapter = config.adapter ?? new FullnodeScanAdapter(this.aptos);
    this.query = new EventQuery(this.adapter);
  }

  /**
   * Subscribe to real-time events. Polls under the hood.
   *
   * Every subscription polls on its own. For a `module` descriptor on the
   * default adapter that means its own ledger read and transaction scan per
   * poll, so N module subscriptions scan the same transactions N times. Keep
   * them few on a shared or rate-limited node.
   */
  on<TData = unknown>(
    descriptor: EventDescriptor,
    callback: EventCallback<TData>,
    options?: ListenerOptions,
  ): Subscription {
    const id = generateId();
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
          if (!poller.active) this.subscriptions.delete(id);
          options?.onError?.(err);
        },
      },
    );

    this.subscriptions.set(id, poller);
    poller.start();

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
        this.subscriptions.delete(id);
      },
    };
  }

  /**
   * Wait for the next event matching the descriptor. Resolves with the first event received.
   *
   * Rejects when polling gives up after `maxErrors`, at once when the adapter
   * refuses the descriptor or options as invalid input, or on the first error
   * thrown by `where`, even if a later event would have matched.
   */
  once<TData = unknown>(
    descriptor: EventDescriptor,
    options?: ListenerOptions,
  ): Promise<StreamdEvent<TData>> {
    return new Promise<StreamdEvent<TData>>((resolve, reject) => {
      const sub = this.on<TData>(
        descriptor,
        (event) => {
          sub.unsubscribe();
          resolve(event);
        },
        {
          ...options,
          onError: (err) => {
            // Settle before the caller's handler runs, so a throw from it
            // cannot leave the promise pending.
            if (!sub.active) {
              // Polling stopped itself: maxErrors, or invalid input.
              reject(err);
            } else if (!(err instanceof StreamdPollingError)) {
              // Filter and callback errors never stop the poller, so a
              // predicate that always throws would otherwise wait forever.
              sub.unsubscribe();
              reject(err);
            }
            options?.onError?.(err);
          },
        },
      );
    });
  }

  /**
   * Live events as an async iterable: `for await (const e of client.stream(d))`.
   * Leaving the loop unsubscribes. Each event holds back the next poll until
   * the loop asks for the following one, and the loop throws if polling gives
   * up (after `maxErrors`, or at once on invalid input). Other errors go to
   * `options.onError`.
   */
  stream<TData = unknown>(
    descriptor: EventDescriptor,
    options?: ListenerOptions,
  ): AsyncIterableIterator<StreamdEvent<TData>> {
    return eventStream<TData>((callback, onError) =>
      this.on<TData>(descriptor, callback, {
        ...options,
        onError: (err) => {
          onError(err);
          options?.onError?.(err);
        },
      }),
    );
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
   * Stop all active subscriptions and clean up.
   */
  destroy(): void {
    for (const [id, poller] of this.subscriptions) {
      poller.stop();
      this.subscriptions.delete(id);
    }
  }
}
