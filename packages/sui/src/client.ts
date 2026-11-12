import { SuiGrpcClient, type SuiGrpcClientOptions } from "@mysten/sui/grpc";
import { StreamdClient, StreamdError, checkTimeout, eventsOf, expectEvents } from "@streamd/core";
import type {
  EventAdapter,
  EventDescriptor,
  EventExpectation,
  EventOf,
  EventsOfOptions,
  ExpectEventsOptions,
  StreamdConfig,
} from "@streamd/core";
import { SuiAdapter, requireDigest, suiDescriptor } from "./adapter.js";

/** `StreamdClient`'s config, with the Sui gRPC client the Sui methods read through; `adapter` defaults to a `SuiAdapter` over it. */
export type SuiClientConfig = Omit<StreamdConfig, "adapter"> & {
  /** A `SuiGrpcClient`, or the options to build one, such as a preset from `networks`. */
  sui: SuiGrpcClient | SuiGrpcClientOptions;
  adapter?: EventAdapter;
};

/**
 * A `StreamdClient` for Sui: everything the chain-neutral client does, over a
 * `SuiAdapter` by default, plus transaction lookups and the assertions on them.
 */
export class SuiClient extends StreamdClient {
  protected readonly sui: SuiGrpcClient;

  constructor(config: SuiClientConfig) {
    const given: unknown = config?.sui;
    if (typeof given !== "object" || given === null) {
      throw new StreamdError("SuiClient requires `sui`, a SuiGrpcClient or its options");
    }
    // Structural, so a wrapped or fake client is taken as a client rather than as options.
    const isClient = typeof (given as SuiGrpcClient).ledgerService?.listEvents === "function";
    const sui = isClient ? (given as SuiGrpcClient) : new SuiGrpcClient(given as SuiGrpcClientOptions);
    const { sui: _given, ...rest } = config;
    super({ ...rest, adapter: config.adapter ?? new SuiAdapter(sui) });
    this.sui = sui;
  }

  /**
   * Wait for the transaction with this base58 digest to be checkpointed and
   * resolve with its events in transaction order, narrowed by `descriptor`
   * when given (`module` or `account`; types and addresses in any form), then
   * `decode` and `where`. Looks the transaction up every `pollInterval` (the
   * client-wide default, at least 100ms here) through
   * `EventAdapter.getTransactionEvents`.
   *
   * Rejects with `StreamdTransactionError` (carrying the execution error as
   * `vmStatus`) when the transaction failed; with `StreamdTimeoutError` when
   * it is not checkpointed within `timeout`; with `signal.reason` on abort;
   * and at once with any lookup error.
   */
  async eventsOf<TData = unknown>(
    digest: string,
    descriptor?: EventDescriptor<TData>,
    options: EventsOfOptions = {},
  ): Promise<EventOf<TData>[]> {
    requireDigest(digest);
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    const normal = descriptor === undefined ? undefined : suiDescriptor(descriptor);
    const events = await eventsOf(this.adapter, this.limiter, this.defaults.pollInterval!, digest, normal, options);
    return events as unknown as EventOf<TData>[];
  }

  /**
   * Assertions on the events of the transaction with this digest:
   * `toEmit(descriptor, where?)` resolves with the matching events and
   * `notToEmit(descriptor, where?)` with nothing, and either rejects with
   * `StreamdAssertionError`, listing every event the transaction emitted,
   * when it does not hold. Nothing is looked up until an assertion runs; each
   * waits as `eventsOf` does and rejects as it does. Throws at once on a
   * malformed digest or `timeout`.
   */
  expectEvents(digest: string, options: ExpectEventsOptions = {}): EventExpectation {
    requireDigest(digest);
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    const inner = expectEvents(this.adapter, this.limiter, this.defaults.pollInterval!, digest, options);
    return {
      toEmit: async (descriptor, where) => inner.toEmit(suiDescriptor(descriptor) as typeof descriptor, where),
      notToEmit: async (descriptor, where) => inner.notToEmit(suiDescriptor(descriptor), where),
    };
  }
}
