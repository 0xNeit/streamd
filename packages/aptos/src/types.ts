import type { MultiAgentTransaction, PublicKey, SimpleTransaction } from "@aptos-labs/ts-sdk";
import type { IndexerAccountAdapter } from "./indexer.js";
import type {
  EntryFunctionCall,
  EventFilter,
  EventPredicate,
  NameOptions,
  ReduceOptions,
  ResourceChange,
  StreamdEvent,
  SubscriptionOptions,
  Transfer,
  TransactionSimulation,
} from "@streamd/core";

export type {
  BalanceChange,
  Balances,
  BalancesOptions,
  EventExpectation,
  EventsAtOptions,
  EventsOfOptions,
  ExpectEventsOptions,
  Transfer,
} from "@streamd/core";

/** Options for `StreamdClient.expectPreview`: the simulation's signer keys and the assertions' `decode`. */
export type ExpectPreviewOptions = Pick<
  PreviewEventsOptions,
  "signerPublicKey" | "secondarySignersPublicKeys" | "feePayerPublicKey" | "decode"
>;

/** Options for `StreamdClient.previewEvents`. */
export interface PreviewEventsOptions
  extends Pick<AptosTransactionSimulation, "secondarySignersPublicKeys" | "feePayerPublicKey"> {
  /** The sender's public key, so the simulation checks its auth key; omitted, that check is skipped. */
  signerPublicKey?: PublicKey;
  /** Keep only events satisfying this predicate, after `decode`. */
  where?: EventPredicate;
  /** `data` fields, by path, turned from decimal strings into `bigint`, as `EventFilter.decode`. */
  decode?: readonly string[];
}

export interface TransferOptions {
  /**
   * Only transfers of this asset: a fungible-asset metadata address or
   * token object address (any form, e.g. `"0xa"` for APT), a coin type
   * (`"0x1::aptos_coin::AptosCoin"`), or a legacy token id
   * (`"<creator>::<collection>::<name>::<property_version>"`).
   */
  asset?: string;
  /**
   * Only transfers this address or Aptos name sent or received, by the
   * records' `from` and `to`. Those are direct owners, so a store held
   * through an object reports the object. A name needs `aptos` in the client
   * config and resolves once. Costs the same reads as no filter, since a
   * deposit needs its withdraw to report `from`; `asset` is what saves reads.
   */
  account?: string;
  /**
   * Adds `symbol` and `decimals` to fungible-asset and coin records, read by
   * two views per asset, cached. A view that aborts leaves them off.
   */
  withAssetInfo?: boolean;
}

/** Options for `StreamdClient.transfers`. */
export type TransferListenerOptions = SubscriptionOptions & TransferOptions;

/** Filter for `StreamdClient.getTransfers`. */
export type TransferFilter = EventFilter & TransferOptions;

/** One asset's net movement in or out of an account within one transaction, in base units. */
export interface BalanceDelta {
  /** In `Transfer.asset`'s form. */
  asset: string;
  /** Positive when the account gained. */
  delta: bigint;
}

/**
 * One transaction that affected an account, the `data` of the records
 * `activity` and `getActivity` deliver; the record's envelope carries the
 * transaction's hash, sender, timestamp and entry function.
 */
export interface Activity {
  /** `sent` when the account is the transaction's sender; anything else that affected it is `received`. */
  direction: "sent" | "received";
  /** The transaction's transfers whose `from` or `to` is the account, paired as `transfers()` pairs them. */
  transfers: Transfer[];
  /**
   * Net per asset over `transfers`, in order of first appearance, zero nets
   * left out. Gas is not in it: the fee is burned without a withdraw event.
   */
  balanceChanges: BalanceDelta[];
  /** Every event the transaction emitted, in `eventIndex` order. */
  events: StreamdEvent[];
  /**
   * Under `withNames`, the primary name (`null` for none) of the account, the
   * sender and every `from` and `to` in `transfers`, keyed by long-form address.
   */
  names?: Record<string, string | null>;
}

export interface ActivityOptions {
  /** Adds `symbol` and `decimals` to the records in `transfers`, as `TransferOptions.withAssetInfo`. */
  withAssetInfo?: boolean;
}

/** Options for `AptosClient.activity`. */
export type ActivityListenerOptions = SubscriptionOptions & ActivityOptions;

/** Filter for `AptosClient.getActivity`. */
export type ActivityFilter = EventFilter & ActivityOptions;

/** A DEX whose swap events `swaps` and `getSwaps` read. */
export type SwapProtocol =
  | "hyperion"
  | "thala-clamm"
  | "thala-v2"
  | "cellana"
  | "tapp"
  | "liquidswap"
  | "pancakeswap"
  | "thala-v1";

/**
 * One swap through one pool, the `data` of the records `swaps` and
 * `getSwaps` deliver. A multi-hop route gives one per hop.
 */
export interface Swap {
  protocol: SwapProtocol;
  /**
   * The pool object's address, in long form. A coin pool that is no object
   * (LiquidSwap, PancakeSwap, ThalaSwap v1) is named by its resource type
   * instead, such as `<package>::liquidity_pool::LiquidityPool<X, Y, Curve>`,
   * addresses in long form.
   */
  poolId: string;
  /** The transaction's sender, in long form; absent when the source reports none. */
  trader?: string;
  /**
   * What the pool took in: a fungible-asset metadata address, or a coin type
   * (Cellana and the coin pools), in long form, as `Transfer.asset` reports them.
   */
  assetIn: string;
  /** Base units, as the swap event reports them. */
  amountIn: bigint;
  /** What the pool paid out, in the same form as `assetIn`. */
  assetOut: string;
  amountOut: bigint;
}

export interface SwapOptions {
  /** Only these DEXes' swaps, one or a list; every supported one when omitted. */
  protocol?: SwapProtocol | readonly SwapProtocol[];
  /** Only swaps with this asset on either side, in any form `TransferOptions.asset` takes. */
  asset?: string;
}

/** Options for `AptosClient.swaps`. */
export type SwapListenerOptions = SubscriptionOptions & SwapOptions;

/** Filter for `AptosClient.getSwaps`. */
export type SwapFilter = EventFilter & SwapOptions;

/** An NFT marketplace whose fill events `sales` and `getSales` read. */
export type SaleMarketplace = "wapal" | "tradeport";

/** How a sale filled: a listing bought, an offer on one token accepted, or an offer on any token of a collection accepted. */
export type SaleKind = "listing" | "token-offer" | "collection-offer";

/**
 * One NFT sold on one marketplace, the `data` of the records `sales` and
 * `getSales` deliver. A bulk buy gives one per token.
 */
export interface Sale {
  /** The marketplace whose listing or offer filled, not the front end or aggregator that sent the transaction. */
  marketplace: SaleMarketplace;
  kind: SaleKind;
  /**
   * A token object's address in long form, or a legacy token's id as
   * `<creator>::<collection>::<name>::<property_version>`, as `Transfer.asset` reports them.
   */
  token: string;
  /** The collection object's address in long form, when the event names one (Wapal, token objects). */
  collection?: string;
  /** Who sold, from the event, in long form. */
  seller: string;
  /** Who bought, from the event, in long form. */
  buyer: string;
  /** What the buyer paid, in base units of the order's coin (APT on every fill seen), royalty and fee inside. */
  price: bigint;
  /** The creator's royalty inside `price`, when the event reports it (Wapal). */
  royalty?: bigint;
  /** The marketplace's commission inside `price`, when the event reports it (Wapal). */
  fee?: bigint;
}

export interface SaleOptions {
  /** Only these marketplaces' sales, one or a list; every supported one when omitted. */
  marketplace?: SaleMarketplace | readonly SaleMarketplace[];
  /** Only sales of this token: a token object address in any form, or a legacy token id. */
  token?: string;
}

/** Options for `AptosClient.sales`. */
export type SaleListenerOptions = SubscriptionOptions & SaleOptions;

/** Filter for `AptosClient.getSales`. */
export type SaleFilter = EventFilter & SaleOptions;

/**
 * The gas a transaction was charged, from its `0x1::transaction_fee::FeeStatement`.
 * Amounts in octas; the payer is the sender, or the fee payer of a sponsored transaction.
 */
export interface GasUsage {
  /** `total_charge_gas_units`, the transaction's `gas_used`: execution, IO and storage together. */
  units: bigint;
  executionUnits: bigint;
  ioUnits: bigint;
  /** The storage fee inside `units`, in octas. */
  storageFee: bigint;
  /** Octas credited back to the payer for storage the transaction freed. */
  storageRefund: bigint;
  /** Octas per unit, when the source reports it. */
  unitPrice?: bigint;
  /** `units * unitPrice`, in octas, when the price is known; `storageRefund` comes back on top. */
  fee?: bigint;
}

/** One account's net change in one asset across a transaction's transfers. */
export interface AccountBalanceChange {
  /** In long form. */
  account: string;
  /** In `Transfer.asset`'s form. */
  asset: string;
  /** Positive when the account gained. */
  delta: bigint;
  /** From the transfers, when they carry asset details. */
  symbol?: string;
  decimals?: number;
}

/**
 * What one committed or simulated transaction did, as `AptosClient.explain`
 * reports it and `formatExplanation` renders it.
 */
export interface TransactionExplanation {
  /** True for a simulation, which commits nothing. */
  simulated: boolean;
  /** The committed version, or the one the node simulated at. */
  version: bigint;
  /** Absent for a simulation: an unsigned transaction's hash is not the one it would commit under. */
  transactionHash?: string;
  success: boolean;
  /** The VM's verdict, such as `"Executed successfully"` or the abort it hit. */
  vmStatus: string;
  /** In long form, when the source reports one. */
  sender?: string;
  /** Who paid the gas of a sponsored transaction, in long form. */
  feePayer?: string;
  /** Microseconds since the Unix epoch. */
  timestamp?: bigint;
  entryFunction?: EntryFunctionCall;
  /** Absent when the transaction has no fee statement. */
  gas?: GasUsage;
  /** Paired as `transfers()` pairs them; empty for a failed transaction, whose other events were discarded. */
  transfers: Transfer[];
  /**
   * Net per account and asset over `transfers`, grouped by account in order of
   * first appearance, each account's assets in theirs, zero nets left out; gas is not in it.
   */
  balanceChanges: AccountBalanceChange[];
  /** Decoded as `swaps()` decodes them. */
  swaps: Swap[];
  /** Decoded as `sales()` decodes them. */
  sales: Sale[];
  /** Every event, in `eventIndex` order. */
  events: StreamdEvent[];
  /**
   * Under `withNames`, the primary name (`null` for none) of the sender, the
   * fee payer, every transfer's `from` and `to`, and every sale's buyer and
   * seller, keyed by long-form address.
   */
  names?: Record<string, string | null>;
}

/** Options for `AptosClient.explain`. */
export interface ExplainOptions
  extends Pick<PreviewEventsOptions, "signerPublicKey" | "secondarySignersPublicKeys" | "feePayerPublicKey"> {
  /** For a hash: how long to wait for the commit, in ms (default 20000), as `eventsOf` waits. */
  timeout?: number;
  /** Aborting rejects with the signal's `reason`. */
  signal?: AbortSignal;
  /** Adds `symbol` and `decimals` to transfers and balance changes, read by view (default true). */
  withAssetInfo?: boolean;
  /** Attaches `names`, through the client's name cache (`true` for the default expiry, or `{ ttl }`). */
  withNames?: boolean | NameOptions;
}

/** A built, unsigned Aptos transaction to simulate, with the keys whose checks the node should run. */
export interface AptosTransactionSimulation extends TransactionSimulation {
  /** From `aptos.transaction.build.simple`, or `build.multiAgent` for secondary signers. */
  transaction: SimpleTransaction | MultiAgentTransaction;
  /** The sender's public key, so the node checks its auth key; omitted, that check is skipped. */
  signerPublicKey?: PublicKey;
  /**
   * One per secondary signer address of a multi-agent transaction, in order; an
   * `undefined` entry, or omitting the list, skips that signer's check.
   */
  secondarySignersPublicKeys?: (PublicKey | undefined)[];
  /** The fee payer's public key, for a sponsored transaction; omitted, its check is skipped. */
  feePayerPublicKey?: PublicKey;
}

/** One `aptos_governance::Vote`: a stake pool's voter spending some of the pool's voting power. */
export interface ProposalVote {
  /** Long form. A delegation pool's vote names the pool's own signer, not the delegator. */
  voter: string;
  /** Long form. */
  stakePool: string;
  votes: bigint;
  shouldPass: boolean;
  version: bigint;
}

/**
 * What `voting::ResolveProposal` reported. Present once any step resolved: a
 * multi-step proposal is resolved on chain (`is_resolved`) only after its last
 * step, which no event marks, so one whose later steps never ran has a
 * resolution here and `is_resolved` false.
 */
export interface ProposalResolution {
  /** The first resolution's version. */
  version: bigint;
  /** Final: no vote lands once voting has closed. */
  yesVotes: bigint;
  noVotes: bigint;
  early: boolean;
  /** Resolution events seen: a multi-step proposal resolves once per step. */
  steps: number;
}

/** As `0x1::voting::get_proposal_state` reports it: `PENDING`, `SUCCEEDED` or `FAILED`. */
export type ProposalState = "pending" | "succeeded" | "failed";

/**
 * One governance proposal as far as its events have been seen. The creation
 * fields are absent when the creation was not seen, and `yesVotes`, `noVotes`
 * and `votes` count only the votes seen.
 */
export interface Proposal {
  id: bigint;
  /** Long form, from `aptos_governance::CreateProposal`. */
  proposer?: string;
  stakePool?: string;
  /**
   * The rest of the creation fields come from `voting::CreateProposal`. The
   * hash is the first step's: a multi-step proposal's view moves on to later steps.
   */
  executionHash?: string;
  /** As created. Each value's bytes as text, or as the fullnode writes them (hex) when they are not printable UTF-8. */
  metadata?: Record<string, string>;
  expirationSecs?: bigint;
  minVoteThreshold?: bigint;
  earlyResolutionVoteThreshold?: bigint;
  /** The creation's version. */
  createdAt?: bigint;
  yesVotes: bigint;
  noVotes: bigint;
  /** In the order they landed. */
  votes: ProposalVote[];
  resolution?: ProposalResolution;
}

/**
 * Options for `AptosClient.proposals`: `fromVersion` bounds every walk, and
 * the live options tune each of them, as for `reduce()`.
 */
export interface ProposalsOptions
  extends Pick<
    ReduceOptions,
    "fromVersion" | "pollInterval" | "batchSize" | "maxErrors" | "maxBackoff" | "maxPollInterval" | "retries" | "signal" | "onError"
  > {
  /** Called after each live event changes a proposal, with that proposal. A throw goes to `onError`. */
  onChange?: (proposal: Proposal, event: StreamdEvent) => void;
}

/** A running `proposals()`: every proposal seen so far, kept current as events arrive. */
export interface Proposals {
  /** Every proposal seen, by id; the map and its records are updated in place. */
  readonly value: ReadonlyMap<bigint, Proposal>;
  get(id: bigint): Proposal | undefined;
  /** Resolves once every walk has read its history and is following; rejects if one fails or it is stopped first. */
  readonly ready: Promise<ReadonlyMap<bigint, Proposal>>;
  /** False once stopped: unsubscribed, aborted, or a walk failed or gave up, which stops them all. */
  readonly active: boolean;
  unsubscribe(): void;
  [Symbol.dispose](): void;
}

/** What one pool was paid at one epoch change, in octas. */
export interface EpochRewards {
  /** The epoch the rewards were earned in: the one this change ended. */
  epoch: bigint;
  /** `stake::DistributeRewards`, zero included. */
  rewards: bigint;
  /** `stake::DistributeTransactionFee`; `0n` when the pool was paid none. */
  fees: bigint;
  /** The epoch change's version. */
  version: bigint;
}

/** One `delegation_pool::CommissionPercentageChange`. */
export interface CommissionChange {
  /** The next lockup cycle's commission, in hundredths of a percent (`1000n` is 10%). */
  percentage: bigint;
  /** Long form. */
  owner: string;
  version: bigint;
}

/** One delegator's stake moves in a delegation pool, as far as they have been seen, in octas. */
export interface Delegator {
  /** Long form. */
  address: string;
  /** `delegation_pool::AddStake` amounts, fees included. */
  added: bigint;
  addStakeFees: bigint;
  unlocked: bigint;
  reactivated: bigint;
  withdrawn: bigint;
  /** The first `AddStake` seen: a join, when the monitor saw the pool from before it. */
  firstAddedAt?: bigint;
  /** The latest move's version. */
  lastVersion: bigint;
}

/** One stake pool as far as its events have been seen. */
export interface StakePool {
  /** Long form. */
  address: string;
  /** One entry per epoch change seen, oldest first. */
  rewards: EpochRewards[];
  /** The latest `CommissionChange`'s percentage; absent for a pool with none seen. */
  commissionPercentage?: bigint;
  commissionChanges: CommissionChange[];
  /** By long-form address; empty for a pool that is not a delegation pool. */
  delegators: Map<string, Delegator>;
}

/**
 * Options for `AptosClient.stakePools`: `pool` narrows to some pools, the
 * start options bound the history, and the live options tune the walk, as for `reduce()`.
 */
export interface StakePoolsOptions
  extends Pick<
    ReduceOptions,
    | "fromVersion"
    | "fromTime"
    | "pollInterval"
    | "batchSize"
    | "maxErrors"
    | "maxBackoff"
    | "maxPollInterval"
    | "retries"
    | "signal"
    | "onError"
  > {
  /** A pool address or a non-empty list of them; other pools' events are ignored, though a malformed one still stops the monitor. */
  pool?: string | readonly string[];
  /**
   * Called after each live event changes a pool, with that pool: once per
   * pool an epoch change paid, with its `NewEpoch` event. A throw goes to `onError`.
   */
  onChange?: (pool: StakePool, event: StreamdEvent) => void;
}

/** A running `stakePools()`: every pool seen so far, kept current as events arrive. */
export interface StakePools {
  /** Every pool seen, by long-form address; the map and its records are updated in place. */
  readonly value: ReadonlyMap<string, StakePool>;
  /** By address, in any form. */
  get(address: string): StakePool | undefined;
  /** Resolves once the history is folded and following has started; rejects if it fails or is stopped first. */
  readonly ready: Promise<ReadonlyMap<string, StakePool>>;
  /** False once stopped: unsubscribed, aborted, failed, or polling gave up. */
  readonly active: boolean;
  unsubscribe(): void;
  [Symbol.dispose](): void;
}

/** A `#[event]` struct or enum as its module ABI declares it. */
export interface EventStructSignature {
  /** `address::module::Struct`, the address in long form. */
  name: string;
  abilities: string[];
  typeParameters: { constraints: string[]; isPhantom: boolean }[];
  /** A struct's fields; empty for an enum. */
  fields: { name: string; type: string }[];
  /** An enum's variants. */
  variants?: { name: string; fields: { name: string; type: string }[] }[];
}

/** An entry or view function as its module ABI declares it. */
export interface FunctionSignature {
  /** `address::module::function`, the address in long form. */
  name: string;
  /** `public`, `private` or `friend`. */
  visibility: string;
  typeParameters: { constraints: string[] }[];
  params: string[];
  returns: string[];
}

/** What one publish did to a set of signatures, each list sorted by name. */
export interface AbiChanges<T> {
  added: T[];
  changed: { before: T; after: T }[];
  removed: T[];
}

/**
 * One package publish at one address, the `data` of the records `upgrades`
 * and `getUpgrades` deliver: how the address's event structs, entry functions
 * and view functions differ between the version before the publish and its own.
 */
export interface PackageUpgrade {
  /** The code address, long form. */
  address: string;
  /** False for a package's first publish. */
  isUpgrade: boolean;
  events: AbiChanges<EventStructSignature>;
  entryFunctions: AbiChanges<FunctionSignature>;
  views: AbiChanges<FunctionSignature>;
}

/** One property of a `0x4::property_map::PropertyMap`, decoded from its BCS bytes. */
export interface TokenProperty {
  key: string;
  /** The Move type: `bool`, `u8` to `u256`, `address`, `vector<u8>` or `0x1::string::String`. */
  type: string;
  /** Integers as `bigint`, an address in long form, bytes as `0x` hex. */
  value: boolean | bigint | string;
}

/** What one transaction did to the object, as `Provenance.actions` lists it. Addresses are long form. */
export type ProvenanceAction =
  /** A `0x4::collection` mint event named the object. */
  | { kind: "mint"; collection?: string }
  | { kind: "transfer"; from: string; to: string }
  /** A `0x4::token` or `0x4::collection` mutation event; the legacy collection one carries no values. */
  | { kind: "mutation"; field: string; oldValue?: string; newValue?: string }
  /** A `0x4::collection` burn event named the object, or its `ObjectCore` was deleted without one. */
  | { kind: "burn"; collection?: string; previousOwner?: string };

/**
 * One transaction that touched an object, the `data` of the records
 * `provenance` and `getProvenance` deliver; the record's envelope carries
 * the transaction's hash, sender, timestamp and entry function.
 */
export interface Provenance {
  /** The object, in long form. */
  object: string;
  /** False when the transaction aborted, which leaves the object as it was. */
  success: boolean;
  /** In event order, then a `burn` from a deleted `ObjectCore`. Empty when nothing named the object. */
  actions: ProvenanceAction[];
  /**
   * The object's `PropertyMap` as the transaction left it, every property in
   * stored order, when the write set holds it. An object's resources form one
   * group that is written whole, so this appears on any transaction that
   * wrote the object; a property changed where it differs from the previous
   * record's.
   */
  properties?: TokenProperty[];
  /** Every resource the transaction wrote or deleted at the object, in write-set order. */
  changes: ResourceChange[];
  /**
   * Under `withNames`, the primary name (`null` for none) of the sender and
   * of every address in `actions`, keyed by long-form address.
   */
  names?: Record<string, string | null>;
}

/** What lists an object's transactions for `provenance`; `IndexerAccountAdapter` is one. */
export type TransactionLister = Pick<IndexerAccountAdapter, "getHeadVersion" | "listTransactionVersions">;

export interface ProvenanceOptions {
  /**
   * Lists the transactions that touched the object (default: an
   * `IndexerAccountAdapter` over the client's `aptos`, anonymous). Pass one
   * built with an `apiKey` for more than the anonymous rate.
   */
  indexer?: TransactionLister;
}

/** Options for `AptosClient.provenance`. */
export type ProvenanceListenerOptions = SubscriptionOptions & ProvenanceOptions;

/** Filter for `AptosClient.getProvenance`. */
export type ProvenanceFilter = EventFilter & ProvenanceOptions;
