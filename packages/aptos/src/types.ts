import type { MultiAgentTransaction, PublicKey, SimpleTransaction } from "@aptos-labs/ts-sdk";
import type {
  EventFilter,
  EventPredicate,
  SubscriptionOptions,
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
