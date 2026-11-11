import type { MultiAgentTransaction, PublicKey, SimpleTransaction } from "@aptos-labs/ts-sdk";
import type {
  EventDescriptor,
  EventFilter,
  EventOf,
  EventPredicate,
  StreamdEvent,
  ListenerOptions,
  SubscriptionOptions,
  TransactionSimulation,
} from "@streamd/core";

/** Options for `StreamdClient.eventsOf`. */
export interface EventsOfOptions {
  /** Keep only events satisfying this predicate, after `decode`; a rejection rejects the call. */
  where?: EventPredicate;
  /** `data` fields, by path, turned from decimal strings into `bigint`, as `EventFilter.decode`. */
  decode?: readonly string[];
  /**
   * How long to wait for the transaction to be committed, in ms (default
   * 20000), before rejecting with `StreamdTimeoutError`.
   */
  timeout?: number;
  /** Aborting rejects with the signal's `reason` and stops looking. */
  signal?: AbortSignal;
}

/** Options for `StreamdClient.expectEvents`. */
export type ExpectEventsOptions = Pick<EventsOfOptions, "decode" | "timeout" | "signal">;

/**
 * Assertions on one transaction's events, from `StreamdClient.expectEvents`.
 * Each waits for the transaction as `eventsOf` does, then narrows its events
 * by the descriptor, the expectation's `decode`, then `where`. A failed
 * assertion rejects with `StreamdAssertionError`.
 */
export interface EventExpectation {
  /** Resolves with the matching events, in transaction order; rejects when there are none. */
  toEmit<TData = unknown>(
    descriptor: EventDescriptor<TData>,
    where?: EventPredicate,
  ): Promise<EventOf<TData>[]>;
  /** Resolves when no event matches; rejects naming the ones that do. */
  notToEmit(descriptor: EventDescriptor, where?: EventPredicate): Promise<void>;
}

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

/**
 * One coin, fungible-asset or token movement, the `data` of the events
 * `StreamdClient.transfers` and `getTransfers` deliver. A deposit paired with
 * the withdraw that funded it carries both sides; a deposit nothing funded (a
 * mint) has no `from`, and a withdraw nothing received (a burn) no `to`. A
 * token object's transfer always carries both.
 */
export interface Transfer {
  /**
   * Fungible-asset metadata address in long form, the coin type as reported,
   * a token object's address in long form, or a legacy token's id as
   * `creator::collection::name::property_version` (creator in long form).
   */
  asset: string;
  /** Base units; `1n` for a token object. */
  amount: bigint;
  /** Owner of the withdrawn store, the coin or token account, or the object's previous owner, in long form. */
  from?: string;
  /** Owner of the receiving store, the coin or token account, or the object's new owner, in long form. */
  to?: string;
  /** The asset's symbol, with `withAssetInfo`; absent for tokens and when the asset has none to read. */
  symbol?: string;
  /** The asset's decimals, with `withAssetInfo`; use `formatAmount` to apply them. */
  decimals?: number;
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

/** One asset's balance at a version; `balances()` delivers these to `onChange`. */
export interface BalanceChange {
  /** The asset in long form, with APT always its fungible-asset address. */
  asset: string;
  /** Base units, read by view at the change's `version`. */
  balance: bigint;
}

/** Options for `StreamdClient.balances`. Balances are read at the head, so nothing moves the start. */
export interface BalancesOptions
  extends Pick<
    ListenerOptions,
    "pollInterval" | "batchSize" | "maxErrors" | "maxBackoff" | "dedupWindow" | "onError" | "onCaughtUp" | "signal"
  > {
  /** The fungible-asset addresses or coin types to follow, one or a list (default: APT). */
  asset?: string | readonly string[];
  /** Called after a balance changes, with every balance known so far. A throw goes to `onError`. */
  onChange?: (balances: ReadonlyMap<string, bigint>, change: StreamdEvent<BalanceChange>) => void;
}

/** A running `balances()`: one wallet's balances, kept current. */
export interface Balances {
  /** A copy of every balance known so far, by asset in long form. */
  readonly value: ReadonlyMap<string, bigint>;
  /** One asset's balance, given in any form; `undefined` until it is known. */
  get(asset: string): bigint | undefined;
  /** Resolves once following has started; rejects if the first read fails or it is stopped first. */
  readonly ready: Promise<ReadonlyMap<string, bigint>>;
  /** False once stopped: unsubscribed, aborted, failed, or polling gave up. */
  readonly active: boolean;
  unsubscribe(): void;
  [Symbol.dispose](): void;
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
