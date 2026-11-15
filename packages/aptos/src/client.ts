import { Aptos, Network, type AptosConfig, type MultiAgentTransaction, type SimpleTransaction } from "@aptos-labs/ts-sdk";
import { EventQuery, StreamdClient, StreamdError, checkTimeout, eventsAt, expectEvents, requireLedgerVersion, requireTransactionHash, startBalances } from "@streamd/core";
import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventOf,
  StreamdConfig,
  StreamdEvent,
  Subscription,
} from "@streamd/core";
import { activityDescriptor, activitySource } from "./activity.js";
import { APTOS_BALANCES } from "./balances.js";
import { expectPreview } from "./expect.js";
import { FullnodeScanAdapter } from "./fullnode.js";
import { startProposals } from "./governance.js";
import { addressOrName, primaryName, resolveName } from "./names.js";
import { eventsOf, previewEvents } from "./transaction.js";
import { saleDescriptor, saleSource } from "./sales.js";
import { swapDescriptor, swapSource } from "./swaps.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import type { Activity, ActivityFilter, ActivityListenerOptions, Balances, BalancesOptions, EventExpectation, EventsAtOptions, EventsOfOptions, ExpectEventsOptions, ExpectPreviewOptions, PreviewEventsOptions, Proposals, ProposalsOptions, Sale, SaleFilter, SaleListenerOptions, Swap, SwapFilter, SwapListenerOptions, Transfer, TransferFilter, TransferListenerOptions } from "./types.js";

/** `StreamdClient`'s config, with the Aptos SDK the Aptos methods read through; `adapter` defaults to a fullnode over it. */
export type AptosClientConfig = Omit<StreamdConfig, "adapter"> & {
  aptos: Aptos | AptosConfig;
  adapter?: EventAdapter;
};

/**
 * A `StreamdClient` for Aptos: everything the chain-neutral client does, plus
 * asset transfers and balances, transaction lookups, simulations and the
 * assertions on them, and Aptos names wherever an account is taken. On
 * mainnet and a localnet, `withNames` attaches each sender's primary name
 * through `primaryName`; elsewhere it is refused at the call.
 */
export class AptosClient extends StreamdClient {
  protected readonly aptos: Aptos;

  constructor(config: AptosClientConfig) {
    const aptos = config?.aptos instanceof Aptos ? config.aptos : config?.aptos === undefined ? undefined : new Aptos(config.aptos);
    if (aptos === undefined) throw new StreamdError("AptosClient requires `aptos`, an Aptos instance or AptosConfig");
    const { aptos: _given, ...rest } = config;
    super({ ...rest, adapter: config.adapter ?? new FullnodeScanAdapter(aptos) });
    this.aptos = aptos;
    // Testnet's ANS router is gone, so only mainnet and a localnet can answer.
    const network = aptos.config?.network;
    this.names.serve(
      network === Network.MAINNET || network === Network.LOCAL
        ? (address) => primaryName(aptos, address)
        : `withNames needs Aptos names, which network ${network} does not serve`,
    );
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
   * Follow every transaction `address` sent or received as it lands, one
   * record per transaction whose `data` is an `Activity`: its direction, the
   * transfers the address took part in, its net balance change per asset and
   * every event, the envelope carrying the transaction's hash, sender,
   * timestamp and entry function. Reads an `account` descriptor with
   * `received: true`, which the client's adapter must serve
   * (`IndexerAccountAdapter`, routed beside a fullnode for `view`); transfers
   * are built and paired as `transfers()` builds them. `address` may be an
   * Aptos name, resolved once. Under `withNames`, `names` holds the primary
   * name of the address, the sender and every counterparty. Like
   * `transfers()`, it is not a listener for `off()` or `listenerCount()`.
   */
  activity(
    address: string,
    callback: EventCallback<Activity>,
    options: ActivityListenerOptions = {},
  ): Subscription {
    const { withAssetInfo, ...rest } = options;
    const [descriptor, who, name] = this.activityOf(address, rest.withNames);
    return this.subscribe(descriptor, callback as EventCallback, rest, undefined, rest.id, (source) =>
      activitySource(source, who, withAssetInfo, this.requestTimeout, name),
    );
  }

  /**
   * Past activity of `address`, built as `activity()` builds it, over the
   * range and options `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getActivity(address: string, filter: ActivityFilter = {}): Promise<StreamdEvent<Activity>[]> {
    const { withAssetInfo, ...rest } = filter;
    const [descriptor, who, name] = this.activityOf(address, rest.withNames);
    const source = activitySource(this.adapter, who, withAssetInfo, this.requestTimeout, name);
    return new EventQuery(source, this.limiter, this.names).getEvents<Activity>(descriptor, rest);
  }

  /** The descriptor, lazy address and name lookup an activity call reads with, refusing bad input at the call. */
  private activityOf(address: string, withNames: unknown) {
    if (typeof address !== "string") throw new StreamdError("activity needs an address or Aptos name");
    const ttl = this.names.ttl(withNames);
    const name = ttl === undefined ? undefined : (a: string) => this.names.name(a, ttl);
    return [activityDescriptor(addressOrName(address) ?? address), this.accountOf(address)!, name] as const;
  }

  /**
   * Follow DEX swaps as they land, one record per pool hop, each delivered as
   * an event whose `data` is a `Swap`, from one scan over the swap events of
   * Hyperion (`pool_v3::SwapEventV3`), Thala CLAMM and ThalaSwap v2
   * (`pool::SwapEvent`) and Cellana (`liquidity_pool::SwapEvent`). Every
   * field comes from the event, `trader` from the transaction's sender, so
   * nothing is read by view. `protocol` narrows the scanned types; `asset`
   * keeps swaps with that asset on either side. Like `transfers()`, it is not
   * a listener for `off()` or `listenerCount()`.
   */
  swaps(callback: EventCallback<Swap>, options: SwapListenerOptions = {}): Subscription {
    const { protocol, asset, ...rest } = options;
    const descriptor = swapDescriptor(protocol);
    return this.subscribe(descriptor, callback as EventCallback, rest, undefined, rest.id, (source) =>
      swapSource(source, asset),
    );
  }

  /**
   * Past swaps, built as `swaps()` builds them, over the range and options
   * `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getSwaps(filter: SwapFilter = {}): Promise<StreamdEvent<Swap>[]> {
    const { protocol, asset, ...rest } = filter;
    const descriptor = swapDescriptor(protocol);
    const query = new EventQuery(swapSource(this.adapter, asset), this.limiter, this.names);
    return query.getEvents<Swap>(descriptor, rest);
  }

  /**
   * Follow NFT marketplace sales as they land, one record per token sold,
   * each delivered as an event whose `data` is a `Sale`, from one scan over
   * the fill events of Wapal (`events::ListingFilledEvent`,
   * `TokenOfferFilledEvent`, `CollectionOfferFilledEvent`) and Tradeport
   * (`listings_v2::BuyEvent`, `biddings_v2::AcceptTokenBidEvent`,
   * `AcceptCollectionBidEvent`, and the same in its legacy-token modules).
   * Every field comes from the event, so nothing is read by view.
   * `marketplace` narrows the scanned types; `token` keeps sales of one
   * token. Like `swaps()`, it is not a listener for `off()` or `listenerCount()`.
   */
  sales(callback: EventCallback<Sale>, options: SaleListenerOptions = {}): Subscription {
    const { marketplace, token, ...rest } = options;
    const descriptor = saleDescriptor(marketplace);
    return this.subscribe(descriptor, callback as EventCallback, rest, undefined, rest.id, (source) =>
      saleSource(source, token),
    );
  }

  /**
   * Past sales, built as `sales()` builds them, over the range and options
   * `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getSales(filter: SaleFilter = {}): Promise<StreamdEvent<Sale>[]> {
    const { marketplace, token, ...rest } = filter;
    const descriptor = saleDescriptor(marketplace);
    const query = new EventQuery(saleSource(this.adapter, token), this.limiter, this.names);
    return query.getEvents<Sale>(descriptor, rest);
  }

  /**
   * Keep every on-chain governance proposal current: its creation (proposer,
   * execution hash, metadata, expiry and thresholds), the stake for and
   * against, each vote with its voter and stake pool, and its resolution,
   * folded from the `voting` and `aptos_governance` events. Five walks run:
   * the four legacy handles at `0x1`, emitted until the module-event
   * migration, and one scan over the module events that replaced them, each
   * reading its history from `fromVersion` (as for `getEvents` when omitted:
   * a handle from its first event, a `FullnodeScanAdapter` scan from the
   * head) and then following live. `proposalState()` gives a proposal's
   * state at a time. Nothing is read by view. Like `reduce()`, it is not a
   * listener for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  proposals(options?: ProposalsOptions): Proposals {
    return startProposals((d, r, i, o) => this.reduce(d, r, i, o), this.onError, options);
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
   * One wallet's balance of one asset as it stood at `version`, by the view
   * `balances()` reads; `asset` `undefined` reads APT. A name resolves at the
   * head, so it reads the address the name points to now. Goes through the
   * client's limiter and `requestTimeout`. A pruned version rejects with the
   * adapter's 410 `StreamdNetworkError`, and one above the head with the
   * fullnode's 404.
   */
  async balanceAt(address: string, asset: string | undefined, version: bigint): Promise<bigint> {
    if (address === undefined) throw new StreamdError("balanceAt needs an address or Aptos name");
    if (asset !== undefined && typeof asset !== "string") {
      throw new StreamdError("balanceAt reads one asset: a fungible-asset metadata address or a coin type");
    }
    const [key] = APTOS_BALANCES.assets(asset);
    requireLedgerVersion(version);
    const adapter = this.limiter.wrap(this.adapter);
    if (typeof adapter.view !== "function") throw new StreamdError("balanceAt needs an adapter that implements view");
    const owner = this.accountOf(address)!;
    return APTOS_BALANCES.read(adapter, await owner(), key!, version);
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
    const query = new EventQuery(source, this.limiter, this.names);
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
   * Resolve with the events of the transaction committed at `version`, in
   * transaction order, narrowed as `eventsOf` narrows them. One lookup through
   * `EventAdapter.getTransactionEventsByVersion` and the client's limiter, with
   * no waiting.
   *
   * Rejects with `StreamdTransactionError` (carrying `vmStatus`) when the
   * transaction failed, as `eventsOf` does; with a `StreamdError` when there is
   * no transaction at `version` yet; with the adapter's 410
   * `StreamdNetworkError` for a pruned version (`ArchiveFallbackAdapter`
   * answers it from an archive); with `signal.reason` on abort; and at once
   * with any lookup error. Bad input is refused before the lookup.
   */
  async eventsAt<TData = unknown>(
    version: bigint,
    descriptor?: EventDescriptor<TData>,
    options: EventsAtOptions = {},
  ): Promise<EventOf<TData>[]> {
    const events = await eventsAt(this.adapter, this.limiter, version, descriptor, options);
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
