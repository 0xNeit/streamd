import { useMemo } from "react";
import {
  StreamdError,
  type DescriptorData,
  type EventDescriptor,
  type StreamdClient,
  type SubscriptionOptions,
  type TransactionGroup,
} from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsStatus } from "./follow.js";
import { valueKey } from "./key.js";

/** `onTransaction` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseTransactionsOptions extends Omit<SubscriptionOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent transactions to keep (default: 100). */
  maxTransactions?: number;
}

export interface UseTransactionsResult<TData> {
  /** The most recent `maxTransactions` delivered transactions, oldest first. */
  data: TransactionGroup<TData>[];
  /** The last polling or handler error, cleared when a transaction next arrives. */
  error: Error | undefined;
  status: UseEventsStatus;
}

type Descriptors = EventDescriptor | readonly EventDescriptor[];

/**
 * Follows one descriptor, or a merged list, with `client.onTransaction` while
 * the component is mounted, each transaction's matching events together.
 * Resubscribes, with an empty list, only when the descriptors or options
 * change by value. A `null` or `undefined` descriptor waits.
 */
export function useTransactions<TData = unknown>(
  descriptor: EventDescriptor<TData> | null | undefined,
  options?: UseTransactionsOptions,
): UseTransactionsResult<TData>;
export function useTransactions<const TList extends readonly EventDescriptor<any>[]>(
  descriptors: TList | null | undefined,
  options?: UseTransactionsOptions,
): UseTransactionsResult<DescriptorData<TList[number]>>;
export function useTransactions(
  descriptor: Descriptors | null | undefined,
  options: UseTransactionsOptions = {},
): UseTransactionsResult<unknown> {
  const { client: explicit, maxTransactions = DEFAULT_MAX_EVENTS, ...listener } = options;
  if (!Number.isSafeInteger(maxTransactions) || maxTransactions < 1) {
    throw new StreamdError(`Invalid maxTransactions: ${String(maxTransactions)} (expected a positive integer)`);
  }
  // Refused here rather than by onTransaction inside the effect.
  if (Array.isArray(descriptor)) {
    if (descriptor.length === 0) throw new StreamdError("useTransactions needs at least one descriptor");
    if (descriptor.length > 1 && listener.cursor !== undefined) {
      throw new StreamdError("cursor cannot resume several descriptors; pass one descriptor");
    }
  }
  const client = useStreamdClient(explicit);
  const followed = useFollowing<TransactionGroup, typeof listener>(
    descriptor != null,
    valueKey([descriptor, listener]),
    client,
    listener,
    maxTransactions,
    (deliver, opts) => client.onTransaction(descriptor!, (group) => deliver([group]), opts),
  );
  return useMemo(
    () => ({ data: followed.events, error: followed.error, status: followed.status }),
    [followed],
  );
}
