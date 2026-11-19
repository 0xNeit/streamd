import { Aptos, Network, SimpleTransaction, generateRawTransaction, type AptosConfig, type MultiAgentTransaction } from "@aptos-labs/ts-sdk";
import { EventQuery, StreamdClient, StreamdError, checkTimeout, eventsAt, expectEvents, lazyResolve, requireAddress, requireLedgerVersion, requireTransactionHash, startBalances, startReduction } from "@streamd/core";
import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  StreamdConfig,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  ViewRequest,
} from "@streamd/core";
import { activityDescriptor, activitySource } from "./activity.js";
import { APTOS_BALANCES } from "./balances.js";
import { expectPreview } from "./expect.js";
import { explain } from "./explain.js";
import { FullnodeScanAdapter } from "./fullnode.js";
import { holdersAt } from "./holders.js";
import { IndexerAccountAdapter } from "./indexer.js";
import { provenanceDescriptor, provenanceSource } from "./provenance.js";
import { governanceSource, startProposals } from "./governance.js";
import { MULTISIG_DESCRIPTOR, multisigSource, previewMultisig, startMultisig } from "./multisig.js";
import { startStakePools } from "./staking.js";
import { addressOrName, primaryName, resolveName } from "./names.js";
import { PYTH_DESCRIPTOR, priceSource, pythPriceReader, requireFeedId } from "./prices.js";
import { eventsOf, previewEvents } from "./transaction.js";
import { saleDescriptor, saleSource } from "./sales.js";
import { swapDescriptor, swapSource } from "./swaps.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "./transfers.js";
import { trace } from "./trace.js";
import { UPGRADE_DESCRIPTOR, moduleReader, upgradeSource } from "./upgrades.js";
import type { Activity, ActivityFilter, FundFlowGraph, TraceOptions, ActivityListenerOptions, Balances, BalancesOptions, EventExpectation, EventsAtOptions, EventsOfOptions, ExpectEventsOptions, ExplainOptions, ExpectPreviewOptions, FungibleAssetIndexer, HolderSnapshot, HoldersAtOptions, Multisig, MultisigOptions, MultisigPreviewOptions, PackageUpgrade, PreviewEventsOptions, Price, PriceListenerOptions, Provenance, ProvenanceFilter, ProvenanceListenerOptions, TransactionLister, Proposals, ProposalsOptions, Sale, SaleFilter, SaleListenerOptions, StakePools, StakePoolsOptions, Swap, SwapFilter, SwapListenerOptions, TransactionExplanation, Transfer, TransferFilter, TransferListenerOptions } from "./types.js";

/** `StreamdClient`'s config, with the Aptos SDK the Aptos methods read through; `adapter` defaults to a fullnode over it. */
export type AptosClientConfig = Omit<StreamdConfig, "adapter"> & {
  aptos: Aptos | AptosConfig;
  adapter?: EventAdapter;
};

/**
 * A `StreamdClient` for Aptos: everything the chain-neutral client does, plus
 * asset transfers and balances, transaction lookups, simulations and the
 * assertions on them, and Aptos names wherever an account is taken. On
 * mainnet, `withNames` attaches each sender's primary name through
 * `primaryName`; elsewhere it is refused at the call.
 */
export class AptosClient extends StreamdClient {
  protected readonly aptos: Aptos;
  private readonly pythPrice: (feedId: string, version: bigint) => Promise<Price | undefined>;

  constructor(config: AptosClientConfig) {
    const aptos = config?.aptos instanceof Aptos ? config.aptos : config?.aptos === undefined ? undefined : new Aptos(config.aptos);
    if (aptos === undefined) throw new StreamdError("AptosClient requires `aptos`, an Aptos instance or AptosConfig");
    const { aptos: _given, ...rest } = config;
    super({ ...rest, adapter: config.adapter ?? new FullnodeScanAdapter(aptos) });
    this.aptos = aptos;
    this.pythPrice = pythPriceReader(aptos, (task) => this.limiter.run(task));
    // Testnet's ANS router is gone, and a localnet has none published at the
    // SDK's local router address, so only mainnet can answer.
    const network = aptos.config?.network;
    this.names.serve(
      network === Network.MAINNET
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

  /**
   * Follow the funds around `address` (an address or Aptos name) for up to
   * `hops` hops and resolve a counterparty graph: each node's transfers read
   * as `getActivity` reads them, one node at a time over one range, so the
   * client's adapter must serve `account` with `received: true`
   * (`IndexerAccountAdapter` routed beside a fullnode). Edges sum transfers
   * per `(from, to, asset)` as bigints, a transfer seen from both ends once.
   * `maxNodes`, `maxRequests` and `target` end the walk early, named in
   * `stoppedBy`; `stop` keeps addresses unexpanded. `tracePath` reads a path
   * out of the result.
   */
  async trace(address: string, options: TraceOptions = {}): Promise<FundFlowGraph> {
    if (typeof address !== "string") throw new StreamdError("trace needs an address or Aptos name");
    return trace(this.adapter, this.limiter, this.accountOf(address)!, options, this.requestTimeout);
  }

  /** The descriptor, lazy address and name lookup an activity call reads with, refusing bad input at the call. */
  private activityOf(address: string, withNames: unknown) {
    if (typeof address !== "string") throw new StreamdError("activity needs an address or Aptos name");
    const ttl = this.names.ttl(withNames);
    const name = ttl === undefined ? undefined : (a: string) => this.names.nameWithinSlot(a, ttl);
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
   * state at a time. Any account can host a `0x1::voting` forum, so the
   * module walk keeps a `voting` creation only beside `aptos_governance`'s,
   * and a resolution only when `0x1`'s forum shows it, read through
   * `EventAdapter.view` at its version. Like `reduce()`, it is not a
   * listener for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  proposals(options?: ProposalsOptions): Proposals {
    const wrap = (source: EventAdapter) => governanceSource(source, this.requestTimeout);
    const verified = {
      ...this.reduceHost,
      query: new EventQuery(wrap(this.adapter), this.limiter, this.names),
      subscribe: (d: EventDescriptor | readonly EventDescriptor[], callback: EventCallback, options: object) =>
        this.subscribe(d as EventDescriptor, callback, options, undefined, undefined, wrap),
    };
    return startProposals(
      (d, r, i, o) => (d.kind === "handle" ? this.reduce(d, r, i, o) : startReduction(verified, d, r, i, o)),
      this.onError,
      options,
    );
  }

  /**
   * Keep one `0x1::multisig_account` current: owners, threshold, timelock and
   * every pending transaction with its approvals, rejections and decoded
   * call, read from the module's views at the head (or at the version before
   * `fromVersion`), then reread at the version of each of the account's
   * events from one scan over the module's events, narrowed by their
   * `multisig_account` field; the account never signs, so an `account` walk
   * would see nothing. Views go through `EventAdapter.view`, so the adapter
   * must implement it; a `fromVersion` older than the timelock views fails.
   * `address` may be an Aptos name, resolved once. `preview` simulates a
   * pending transaction with no key. Like `reduce()`, it is not a listener
   * for `off()` or `listenerCount()`; `destroy()` ends it.
   */
  multisig(address: string, options?: MultisigOptions): Multisig {
    if (typeof address !== "string") throw new StreamdError("multisig needs an address or Aptos name");
    const who = this.accountOf(address)!;
    const adapter = this.adapter;
    if (typeof adapter.view !== "function" || typeof adapter.getHeadVersion !== "function") {
      throw new StreamdError("multisig needs an adapter that implements view and getHeadVersion");
    }
    const host = {
      view: (request: ViewRequest, version: bigint) => this.limiter.run(() => adapter.view!(request, version)),
      head: () => this.limiter.run(() => adapter.getHeadVersion!(MULTISIG_DESCRIPTOR)),
    };
    const handle = startMultisig(
      (account, d, r, i, o) => {
        const wrap = (source: EventAdapter) => multisigSource(source, account, this.requestTimeout);
        const scoped = {
          ...this.reduceHost,
          query: new EventQuery(wrap(adapter), this.limiter, this.names),
          subscribe: (desc: EventDescriptor | readonly EventDescriptor[], callback: EventCallback, opts: object) =>
            this.subscribe(desc as EventDescriptor, callback, opts, undefined, undefined, wrap),
        };
        return startReduction(scoped, d, r, i, o);
      },
      host,
      who,
      options,
    );
    const previewHost = {
      ...host,
      build: async (sender: string, payload: Parameters<typeof generateRawTransaction>[0]["payload"]) =>
        new SimpleTransaction(await this.limiter.run(() => generateRawTransaction({ aptosConfig: this.aptos.config, sender, payload }))),
      simulate: (transaction: SimpleTransaction) => this.previewEvents(transaction),
    };
    return Object.assign(handle, {
      preview: async (sequenceNumber: bigint, opts?: MultisigPreviewOptions) => {
        await handle.ready;
        return previewMultisig(previewHost, handle.value, sequenceNumber, opts);
      },
    }) as Multisig;
  }

  /**
   * Keep every stake pool current from one scan over the staking module
   * events: the rewards and transaction fees each pool was paid at every
   * epoch change, its delegation pool commission changes, and each
   * delegator's adds (the first a join), unlocks, reactivations and
   * withdrawals. The history is read from `fromVersion` or `fromTime` (as for
   * `getEvents` when omitted: a `FullnodeScanAdapter` scan starts at the
   * head, and epochs change about every two hours), then followed live;
   * `pool` narrows to some pools. Like `reduce()`, it is not a listener for
   * `off()` or `listenerCount()`; `destroy()` ends it.
   */
  stakePools(options?: StakePoolsOptions): StakePools {
    return startStakePools((d, r, i, o) => this.reduce(d, r, i, o), options);
  }

  /**
   * Follow package publishes at `address` as they land, one record per
   * publishing transaction whose `data` is a `PackageUpgrade`: the event
   * structs, entry functions and view functions it added, changed or removed,
   * from one scan over `0x1::code::PublishPackage`. Each is found by reading
   * every module at the address at the version before the publish and at its
   * own, through the client's `aptos` (not the adapter, so an archive behind
   * it is not asked, and a version the node pruned fails the poll with its
   * 410), outside `maxConcurrentRequests`, within the page's `requestTimeout`. A
   * failed read fails the poll, which retries as for `on()`. `address` may be
   * an Aptos name, resolved once. Like `swaps()`, it is not a listener for
   * `off()` or `listenerCount()`.
   */
  upgrades(address: string, callback: EventCallback<PackageUpgrade>, options: SubscriptionOptions = {}): Subscription {
    return this.subscribe(UPGRADE_DESCRIPTOR, callback as EventCallback, options, undefined, options.id, this.upgradesOf(address));
  }

  /**
   * Past package publishes at `address`, built as `upgrades()` builds them,
   * over the range and options `getEvents` takes; `where` and `limit` apply
   * to the records.
   */
  async getUpgrades(address: string, filter: EventFilter = {}): Promise<StreamdEvent<PackageUpgrade>[]> {
    const source = this.upgradesOf(address)(this.adapter);
    return new EventQuery(source, this.limiter, this.names).getEvents<PackageUpgrade>(UPGRADE_DESCRIPTOR, filter);
  }

  private upgradesOf(address: string): (source: EventAdapter) => EventAdapter {
    if (typeof address !== "string") throw new StreamdError("upgrades needs an address or Aptos name");
    const who = this.accountOf(address)!;
    const read = moduleReader(this.aptos);
    return (source) => upgradeSource(source, who, read);
  }

  /**
   * Follow the transactions that touch one Aptos object (a `0x4` token or any
   * `0x1::object` object) as the Indexer processes them, one record per
   * transaction whose `data` is a `Provenance`: its mint, transfers, field
   * mutations, `PropertyMap` and burn, and every write-set change at the
   * object. Transactions are listed by `indexer`'s `account_transactions`,
   * which trails the fullnode by hundreds of versions, so the live head is
   * its processed version; each is read through the adapter's
   * `getTransactionEventsByVersion`, one at a time and outside
   * `maxConcurrentRequests`, with `requestTimeout` bounding each read. Under
   * `withNames`, `names` holds the sender's and every action address's
   * primary name. Like `activity()`, it is not a listener for `off()` or
   * `listenerCount()`.
   */
  provenance(object: string, callback: EventCallback<Provenance>, options: ProvenanceListenerOptions = {}): Subscription {
    const { indexer, ...rest } = options;
    const [descriptor, source] = this.provenanceOf(object, indexer, rest.withNames);
    return this.subscribe(descriptor, callback as EventCallback, rest, undefined, rest.id, () => source);
  }

  /**
   * The history of one object, built as `provenance()` builds it, from
   * version 0 unless the filter names a start, over the range and options
   * `getEvents` takes; `where` and `limit` apply to the records. A
   * transaction the fullnode pruned rejects with its 410 unless the adapter
   * is an `ArchiveFallbackAdapter`.
   */
  async getProvenance(object: string, filter: ProvenanceFilter = {}): Promise<StreamdEvent<Provenance>[]> {
    const { indexer, ...rest } = filter;
    const [descriptor, source] = this.provenanceOf(object, indexer, rest.withNames);
    const start = rest.fromVersion ?? rest.fromTime ?? rest.fromBlock;
    const range = start === undefined ? { ...rest, fromVersion: 0n } : rest;
    return new EventQuery(source, this.limiter, this.names).getEvents<Provenance>(descriptor, range);
  }

  private provenanceOf(object: string, indexer: TransactionLister | undefined, withNames: unknown) {
    if (typeof object !== "string") throw new StreamdError("provenance needs an object address");
    const address = requireAddress(object);
    if (indexer !== undefined && (typeof indexer.getHeadVersion !== "function" || typeof indexer.listTransactionVersions !== "function")) {
      throw new StreamdError("indexer must implement getHeadVersion and listTransactionVersions");
    }
    const ttl = this.names.ttl(withNames);
    const name = ttl === undefined ? undefined : (a: string) => this.names.nameWithinSlot(a, ttl);
    const source = provenanceSource(
      indexer ?? new IndexerAccountAdapter(this.aptos),
      this.adapter,
      address,
      name,
      this.requestTimeout,
    );
    return [provenanceDescriptor(address), source] as const;
  }

  /**
   * Keep one wallet's balances current, by reading each at the head every
   * `pollInterval` (one head read plus one view per followed asset per poll;
   * default 1000ms), so it cannot drift or fall behind. Even one asset at the
   * default interval makes 2 requests per second and needs an API key or a
   * higher `pollInterval` (such as 5000ms) on the anonymous fullnode. `asset`
   * names what to follow, one or a list, since no view lists a wallet's assets;
   * without it, APT is followed. `address` may be an Aptos name. Like
   * `reduce()`, it is not a listener for `off()` or `listenerCount()`;
   * `destroy()` ends it.
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
   * Every fungible store holding `asset` (a metadata address) at `version`,
   * with its owner and exact balance, folded backward from the Aptos Indexer:
   * the asset's current store balances, then each deposit and withdraw after
   * `version` undone. The total is checked against
   * `0x1::fungible_asset::supply` at `version` unless `verify` is `"none"`,
   * and `verify: "stores"` also reads every holder's balance there, so a
   * fold that does not hold rejects rather than resolving. APT and
   * coin-paired assets are refused, and a dispatchable asset needs
   * `verify: "stores"`. Costs about one Indexer request per 100 stores and
   * per 100 activity rows after `version`, plus the views; every request goes
   * through the client's limiter and `requestTimeout`, retried under
   * `retries` and capped by `maxRequests`. The views at `version` need it
   * inside the fullnode's window, or an `ArchiveFallbackAdapter`.
   */
  async holdersAt(asset: string, version: bigint, options: HoldersAtOptions = {}): Promise<HolderSnapshot> {
    const indexer = options?.indexer;
    const methods = ["getFungibleAssetVersion", "listFungibleStores", "getFungibleStores", "listFungibleActivities"] as const;
    if (indexer !== undefined && methods.some((method) => typeof (indexer as Partial<FungibleAssetIndexer>)?.[method] !== "function")) {
      throw new StreamdError(`indexer must implement ${methods.join(", ")}`);
    }
    const signal = options?.signal;
    return holdersAt(
      {
        indexer: indexer ?? new IndexerAccountAdapter(this.aptos),
        adapter: this.adapter,
        run: (task) => this.limiter.run(task, signal, signal),
      },
      asset,
      version,
      options,
    );
  }

  /**
   * Follow Pyth price updates for one feed or a list, each delivered as an
   * event whose `data` is a `Price`, its staleness measured against the
   * update's own block time. Every Pyth feed updates through one legacy
   * handle at Pyth's address, so the walk reads every update and keeps the
   * named feeds; `cursor` is that handle's sequence number, and with no
   * `cursor`, `fromVersion` or `fromTime` it starts one version below the head. A feed id is
   * Pyth's 32-byte price id. Like `swaps()`, it is not a listener for `off()`
   * or `listenerCount()`. Pyth stopped publishing on Aptos in September 2026,
   * so this replays history from a past position and a follow from the head
   * delivers nothing new.
   */
  prices(feedId: string | readonly string[], callback: EventCallback<Price>, options: PriceListenerOptions = {}): Subscription {
    const ids: readonly unknown[] = typeof feedId === "string" ? [feedId] : Array.isArray(feedId) ? feedId : [feedId];
    if (ids.length === 0) throw new StreamdError("prices needs a Pyth price feed id, or a non-empty list of them");
    const feeds = new Set(ids.map(requireFeedId));
    // A handle walk with no start reads from sequence 0 on FullnodeScanAdapter,
    // so starting at the head is asked for explicitly, by version, which it seeks.
    const unplaced = options.cursor === undefined && options.fromVersion === undefined && options.fromTime === undefined;
    const listener = unplaced ? { ...options, fromVersion: -1n } : options;
    return this.subscribe(PYTH_DESCRIPTOR, callback as EventCallback, listener, undefined, options.id, (source) =>
      priceSource(source, feeds),
    );
  }

  /**
   * The price Pyth held for `feedId` at `version`, from its latest-price
   * table, observed at the time of the block holding `version`, so a feed
   * nobody updated reads as stale; `undefined` when the table had no entry for
   * the feed then. Reads through the client's `aptos` (Pyth has no view), each
   * request through the limiter and `requestTimeout`, with no retry. A pruned
   * version rejects with the fullnode's 410 unless `aptos` is an archive node,
   * and one above the head with 404.
   */
  async priceAt(feedId: string, version: bigint): Promise<Price | undefined> {
    const id = requireFeedId(feedId);
    return this.pythPrice(id, requireLedgerVersion(version));
  }

  /**
   * `account` as a lazy address: a name resolves once, on first use, and a
   * failed lookup is tried again by the next page.
   */
  private accountOf(account: string | undefined): (() => Promise<string>) | undefined {
    if (account === undefined) return undefined;
    return lazyResolve(addressOrName(account), () => resolveName(this.aptos, account));
  }

  private transferQuery(
    filter: TransferFilter = {},
  ): [EventQuery, Omit<TransferFilter, "asset" | "account" | "withAssetInfo">] {
    const { asset, account, withAssetInfo, ...rest } = filter;
    const source = transferSource(
      this.adapter,
      asset,
      this.accountOf(account),
      withAssetInfo,
      this.requestTimeout,
    );
    return [new EventQuery(source, this.limiter, this.names), rest];
  }

  /**
   * Past transfers, built as `transfers()` builds them, over the range and
   * options `getEvents` takes; `where` and `limit` apply to the records.
   */
  async getTransfers(filter: TransferFilter = {}): Promise<StreamdEvent<Transfer>[]> {
    const [query, rest] = this.transferQuery(filter);
    return query.getEvents<Transfer>(TRANSFER_DESCRIPTOR, rest);
  }

  /**
   * Past transfers one at a time, built as `getTransfers()` builds them;
   * leaving the loop early fetches nothing further.
   */
  async *iterateTransfers(filter: TransferFilter = {}): AsyncGenerator<StreamdEvent<Transfer>, void, unknown> {
    const [query, rest] = this.transferQuery(filter);
    yield* query.iterateEvents<Transfer>(TRANSFER_DESCRIPTOR, rest);
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
   * `StreamdTimeoutError` when it is not committed within `timeout` (after
   * retrying `429` and `5xx` lookup errors and honouring `Retry-After`, naming
   * the last error in its message and `cause`); with `signal.reason` on abort;
   * and at once with any other lookup error.
   */
  async eventsOf<TData = unknown, TEvent = StreamdEvent<TData>>(
    transactionHash: string,
    descriptor?: EventDescriptor<TData, TEvent>,
    options: EventsOfOptions = {},
  ): Promise<TEvent[]> {
    const badTimeout = checkTimeout(options.timeout);
    if (badTimeout !== undefined) throw badTimeout;
    const events = await eventsOf(
      this.adapter,
      this.limiter,
      this.defaults.pollInterval!,
      transactionHash,
      descriptor as EventDescriptor | undefined,
      options,
    );
    return events as unknown as TEvent[];
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
  async eventsAt<TData = unknown, TEvent = StreamdEvent<TData>>(
    version: bigint,
    descriptor?: EventDescriptor<TData, TEvent>,
    options: EventsAtOptions = {},
  ): Promise<TEvent[]> {
    const events = await eventsAt(
      this.adapter,
      this.limiter,
      version,
      descriptor as EventDescriptor | undefined,
      options,
    );
    return events as unknown as TEvent[];
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
  async previewEvents<TData = unknown, TEvent = StreamdEvent<TData>>(
    transaction: SimpleTransaction | MultiAgentTransaction,
    descriptor?: EventDescriptor<TData, TEvent>,
    options: PreviewEventsOptions = {},
  ): Promise<TEvent[]> {
    const events = await previewEvents(
      this.adapter,
      this.limiter,
      transaction,
      descriptor as EventDescriptor | undefined,
      options,
    );
    return events as unknown as TEvent[];
  }

  /**
   * What one transaction did, as one `TransactionExplanation`: status, sender
   * and fee payer, entry function, gas from its fee statement, the transfers
   * it made (paired as `transfers()` pairs them) with each account's net
   * change per asset, its swaps and sales, and every event. `target` is a
   * transaction hash, waited for as `eventsOf` waits (`timeout`, `signal`), or
   * a built, unsigned transaction, simulated as `previewEvents` simulates it,
   * with its views read at the newest state the node had. A failed
   * transaction resolves with `success: false`, its gas and no transfers.
   * Asset details are read unless `withAssetInfo: false`; `withNames` adds
   * `names`. Store and asset reads go through the client's limiter; a failed
   * read rejects. `formatExplanation` renders the result as text.
   */
  async explain(
    target: string | SimpleTransaction | MultiAgentTransaction,
    options: ExplainOptions = {},
  ): Promise<TransactionExplanation> {
    const host = {
      adapter: this.adapter,
      limiter: this.limiter,
      names: this.names,
      pollInterval: this.defaults.pollInterval!,
      requestTimeout: this.requestTimeout,
    };
    return explain(host, target, options);
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
