import { Aptos, type AptosConfig, type MultiAgentTransaction, type SimpleTransaction } from "@aptos-labs/ts-sdk";
import { EventQuery, StreamdClient, StreamdError, checkTimeout, expectEvents, requireTransactionHash, startBalances } from "@streamd/core";
import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventOf,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "@streamd/core";
import { APTOS_BALANCES } from "./balances.js";
import { expectPreview } from "./expect.js";
import { FullnodeScanAdapter } from "./fullnode.js";
import { addressOrName, resolveName } from "./names.js";
import { eventsOf, previewEvents } from "./transaction.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import type { Balances, BalancesOptions, EventExpectation, EventsOfOptions, ExpectEventsOptions, ExpectPreviewOptions, PreviewEventsOptions, Transfer, TransferFilter, TransferListenerOptions } from "./types.js";

/** `StreamdClient`'s config, with the Aptos SDK the Aptos methods read through; `adapter` defaults to a fullnode over it. */
export type AptosClientConfig = Omit<StreamdConfig, "adapter"> & {
  aptos: Aptos | AptosConfig;
  adapter?: EventAdapter;
};

/**
 * A `StreamdClient` for Aptos: everything the chain-neutral client does, plus
 * asset transfers and balances, transaction lookups, simulations and the
 * assertions on them, and Aptos names wherever an account is taken.
 */
export class AptosClient extends StreamdClient {
  protected readonly aptos: Aptos;

  constructor(config: AptosClientConfig) {
    const aptos = config?.aptos instanceof Aptos ? config.aptos : config?.aptos === undefined ? undefined : new Aptos(config.aptos);
    if (aptos === undefined) throw new StreamdError("AptosClient requires `aptos`, an Aptos instance or AptosConfig");
    const { aptos: _given, ...rest } = config;
    super({ ...rest, adapter: config.adapter ?? new FullnodeScanAdapter(aptos) });
    this.aptos = aptos;
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
    const { asset, account, withAssetInfo, ...rest } = options;
    const who = this.accountOf(account);
    return this.subscribe(
      TRANSFER_DESCRIPTOR,
      callback as EventCallback,
      rest,
      undefined,
      rest.id,
      (source) => transferSource(source, asset, who, withAssetInfo, this.requestTimeout),
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
        descriptor: TRANSFER_DESCRIPTOR,
        onError: this.onError,
        subscribe: (callback, listener, wrap) =>
          this.subscribe(TRANSFER_DESCRIPTOR, callback, listener, undefined, undefined, wrap),
        track: this.reduceHost.track,
      },
      this.accountOf(address)!,
      APTOS_BALANCES,
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
    const { asset, account, withAssetInfo, ...rest } = filter;
    const source = transferSource(
      this.adapter,
      asset,
      this.accountOf(account),
      withAssetInfo,
      this.requestTimeout,
    );
    const query = new EventQuery(source, this.limiter);
    return query.getEvents<Transfer>(TRANSFER_DESCRIPTOR, rest);
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
  ): Promise<EventOf<TData>[]> {
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
    return events as unknown as EventOf<TData>[];
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
    transaction: SimpleTransaction | MultiAgentTransaction,
    descriptor?: EventDescriptor<TData>,
    options: PreviewEventsOptions = {},
  ): Promise<EventOf<TData>[]> {
    const events = await previewEvents(this.adapter, this.limiter, transaction, descriptor, options);
    return events as unknown as EventOf<TData>[];
  }

  /**
   * Assert what a built, unsigned `transaction` would emit, with nothing
   * signed or submitted: `toEmit` and `notToEmit` as on `expectEvents`, each
   * answered by its own simulation through
   * `EventAdapter.simulateTransactionEvents`. A failed simulation rejects with
   * `StreamdTransactionError`; a failed assertion with `StreamdAssertionError`,
   * whose `transactionHash` is `undefined`.
   */
  expectPreview(transaction: SimpleTransaction | MultiAgentTransaction, options: ExpectPreviewOptions = {}): EventExpectation {
    return expectPreview(this.adapter, this.limiter, transaction, options);
  }
}
