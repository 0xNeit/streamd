import { SuiGrpcClient, type SuiGrpcClientOptions } from "@mysten/sui/grpc";
import { EventQuery, StreamdClient, StreamdError, checkTimeout, eventsOf, expectEvents, expectPreview, previewEvents, startBalances } from "@streamd/core";
import type {
  BalanceReader,
  Balances,
  BalancesOptions,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventExpectation,
  EventOf,
  EventsOfOptions,
  ExpectEventsOptions,
  StreamdConfig,
  StreamdEvent,
  Subscription,
  Transfer,
} from "@streamd/core";
import { SuiAdapter, requireDigest, requireTransactionBytes, suiDescriptor } from "./adapter.js";
import { addressOrName, nameService, resolveName } from "./names.js";
import { TRANSFER_DESCRIPTOR, coinType, transferSource } from "./transfers.js";
import type { SuiTransferFilter, SuiTransferListenerOptions, SuiTransferOptions, TransferNarrowing } from "./transfers.js";

const SUI = coinType("0x2::sui::SUI");

/** Sui balances by `StateService.GetBalance`, which reads the latest state: there is no balance at a version. */
const SUI_BALANCES: BalanceReader = {
  check: () => {},
  assets: (named) => [...new Set((named === undefined ? [SUI] : typeof named === "string" ? [named] : named).map(coinType))],
  canonical: coinType,
  type: (asset) => `${coinType("0x2::balance::Balance")}<${asset}>`,
  read: (adapter, owner, asset) => (adapter as SuiAdapter).getBalance(owner, asset),
};

/**
 * `account` as a lazy address, checked at the call so a typo fails there: a
 * SuiNS name resolves once, on first use, and a failed lookup is tried again
 * by the next page.
 */
function accountOf(sui: SuiGrpcClient, account: string): () => Promise<string> {
  const address = addressOrName(account);
  if (address !== undefined) return () => Promise.resolve(address);
  nameService(sui);
  let resolved: Promise<string> | undefined;
  return () =>
    (resolved ??= resolveName(sui, account).catch((err: unknown) => {
      resolved = undefined;
      throw err;
    }));
}

/** `asset` and `account` checked at the call, so a typo fails there rather than on the first page. */
function transferFilter(sui: SuiGrpcClient, { asset, account }: SuiTransferOptions): TransferNarrowing {
  return {
    ...(asset !== undefined && { asset: coinType(asset) }),
    ...(account !== undefined && { account: accountOf(sui, account) }),
  };
}

/** `StreamdClient`'s config, with the Sui gRPC client the Sui methods read through; `adapter` defaults to a `SuiAdapter` over it. */
export type SuiClientConfig = Omit<StreamdConfig, "adapter"> & {
  /** A `SuiGrpcClient`, or the options to build one, such as a preset from `networks`. */
  sui: SuiGrpcClient | SuiGrpcClientOptions;
  adapter?: EventAdapter;
};

/**
 * A `StreamdClient` for Sui: everything the chain-neutral client does, over a
 * `SuiAdapter` by default, plus coin transfers and balances, transaction
 * lookups, simulations and the assertions on them.
 */
export class SuiClient extends StreamdClient {
  protected readonly sui: SuiGrpcClient;
  /** Reads balances and balance changes, which no `EventAdapter` method carries: the client's adapter when it is a `SuiAdapter`. */
  private readonly ledger: SuiAdapter;

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
    this.ledger = this.adapter instanceof SuiAdapter ? this.adapter : new SuiAdapter(sui);
  }

  /**
   * Follow coin transfers as they land, each delivered as an event whose
   * `data` is a `Transfer`, from the balance changes of every transaction,
   * walked with `ListTransactions`. Sui has no transfer events: a
   * transaction's effects carry each address's net change per coin type.
   *
   * The gas fee is taken out of the payer's SUI change first, as it is not a
   * transfer. Then, per coin type, each receiver takes from the earliest
   * sender with outflow left, as much as both have: inflow no sender covers
   * has no `from`, outflow no receiver took has no `to`. Each address's
   * records sum to its change, less gas. A record keeps the transaction's
   * `version`, digest, `timestamp` and `sender`, has `type` the coin type,
   * and `eventIndex` the transaction's event count plus its position among
   * the records, so it never shares an id with an event.
   *
   * `asset` narrows to one coin type; `account` to what one address (or
   * SuiNS name, resolved once) sent or received, narrowed on the node. Unnarrowed, a walk reads every
   * transaction on the chain, so following mainnet live needs a `batchSize`
   * near the node's 500-transaction page. Reads go through a `SuiAdapter`
   * over `sui`, the client's own when it is one. Like `reduce()`, it is not
   * a listener for `off()` or `listenerCount()`; `unsubscribe()` or
   * `destroy()` ends it.
   */
  transfers(callback: EventCallback<Transfer>, options: SuiTransferListenerOptions = {}): Subscription {
    const { asset, account, withAssetInfo, ...rest } = options;
    const source = transferSource(this.ledger, transferFilter(this.sui, { asset, account }), withAssetInfo);
    return this.subscribe(TRANSFER_DESCRIPTOR, callback as EventCallback, rest, undefined, rest.id, () => source);
  }

  /**
   * Past transfers, built as `transfers()` builds them, over the range and
   * options `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getTransfers(filter: SuiTransferFilter = {}): Promise<StreamdEvent<Transfer>[]> {
    const { asset, account, withAssetInfo, ...rest } = filter;
    const source = transferSource(this.ledger, transferFilter(this.sui, { asset, account }), withAssetInfo);
    return new EventQuery(source, this.limiter).getEvents<Transfer>(TRANSFER_DESCRIPTOR, rest);
  }

  /**
   * Keep one address's coin balances current, by reading each at the head
   * every `pollInterval`. `asset` names the coin types to follow, one or a
   * list; without it, SUI is followed. Balances are keyed by coin type in
   * long form. Sui reads a balance only at its latest state, so a change's
   * `version` is the head read just before it, and the balance is no older.
   * A coin type held nowhere reads 0. `address` may be a SuiNS name,
   * resolved once on the first read. Like `reduce()`, it is not a listener
   * for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  balances(address: string, options?: BalancesOptions): Balances {
    const owner = accountOf(this.sui, address);
    return startBalances(
      {
        adapter: this.ledger,
        descriptor: TRANSFER_DESCRIPTOR,
        onError: this.onError,
        subscribe: (callback, listener, wrap) =>
          this.subscribe(TRANSFER_DESCRIPTOR, callback, listener, undefined, undefined, () => wrap(this.ledger)),
        track: this.reduceHost.track,
      },
      owner,
      SUI_BALANCES,
      options,
    );
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

  /**
   * Simulate built, unsigned transaction `bytes` (from `await tx.build({ client })`)
   * and resolve with the events they would emit if run now, narrowed like
   * `eventsOf`. Nothing is signed or submitted. The events carry the next
   * version the chain would give a transaction and no `transactionHash`.
   * Goes through `EventAdapter.simulateTransactionEvents`.
   *
   * Rejects with `StreamdTransactionError` (carrying the execution error as
   * `vmStatus`) when the simulation fails, and with any simulation error as it is.
   */
  async previewEvents<TData = unknown>(
    bytes: Uint8Array,
    descriptor?: EventDescriptor<TData>,
    options: Pick<EventsOfOptions, "decode" | "where"> = {},
  ): Promise<EventOf<TData>[]> {
    const transaction = requireTransactionBytes(bytes);
    const normal = descriptor === undefined ? undefined : suiDescriptor(descriptor);
    const events = await previewEvents(this.adapter, this.limiter, { transaction }, normal, options);
    return events as unknown as EventOf<TData>[];
  }

  /**
   * Assert what built, unsigned transaction `bytes` would emit, with nothing
   * signed or submitted: `toEmit` and `notToEmit` as on `expectEvents`, each
   * answered by its own simulation. A failed simulation rejects with
   * `StreamdTransactionError`; a failed assertion with `StreamdAssertionError`,
   * whose `transactionHash` is `undefined`. Throws at once on bytes that are
   * not a built transaction.
   */
  expectPreview(bytes: Uint8Array, options: Pick<ExpectEventsOptions, "decode"> = {}): EventExpectation {
    const inner = expectPreview(this.adapter, this.limiter, { transaction: requireTransactionBytes(bytes) }, options);
    return {
      toEmit: async (descriptor, where) => inner.toEmit(suiDescriptor(descriptor) as typeof descriptor, where),
      notToEmit: async (descriptor, where) => inner.notToEmit(suiDescriptor(descriptor), where),
    };
  }
}
