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
