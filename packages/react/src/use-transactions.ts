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
import { DEFAULT_MAX_EVENTS, readThenFollow, useFollowing, type Following, type UseEventsStatus } from "./follow.js";
import { valueKey } from "./key.js";

/** `onTransaction` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseTransactionsOptions extends Omit<SubscriptionOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent transactions to keep (default: 100). */
  maxTransactions?: number;
  /**
   * Show this many of the most recent matching transactions between
   * `fromVersion` or `fromTime` (one is required) and the head in one update,
   * then follow live from the version after the newest of them.
   */
  history?: number;
}

export interface UseTransactionsResult<TData> {
  /** The most recent `maxTransactions` delivered transactions, oldest first. */
  data: TransactionGroup<TData>[];
  /** The last polling or handler error, cleared when a transaction next arrives. */
  error: Error | undefined;
  status: UseEventsStatus;
}

type Descriptors = EventDescriptor | readonly EventDescriptor[];
type Listener = Omit<UseTransactionsOptions, "client" | "maxTransactions" | "history">;

/**
 * Reads the newest `limit` transactions of the window, delivers them oldest
 * first, then follows from the version after the newest. That is exact: the
 * read finishes the newest group and walks the rest of the window, so nothing
 * between that group and the read's end matched. An empty window follows from
 * its own start.
 */
function withHistory(
  client: StreamdClient,
  descriptor: Descriptors,
  limit: number,
  deliver: (groups: TransactionGroup[]) => void,
  options: Listener,
): Following {
  const { fromVersion, fromTime, ...following } = options;
  const { where, decode, withState, onError } = following;
  return readThenFollow(
    "useTransactions",
    (signal) =>
      client.getTransactions(descriptor, {
        ...(fromVersion !== undefined && { fromVersion }),
        ...(fromTime !== undefined && { fromTime }),
        ...(where !== undefined && { where }),
        ...(decode !== undefined && { decode }),
        ...(withState !== undefined && { withState }),
        orderBy: "desc",
        limit,
        signal,
      }).then((groups) => groups.reverse()),
    (groups) =>
      client.onTransaction(
        descriptor,
        (group) => deliver([group]),
        groups.length === 0 ? options : { ...following, fromVersion: groups.at(-1)!.version + 1n },
      ),
    deliver,
    onError,
  );
}

/**
 * Follows one descriptor, or a merged list, with `client.onTransaction` while
 * the component is mounted, each transaction's matching events together.
 * Resubscribes, with an empty list, only when the descriptors or options
 * change by value. A `null` or `undefined` descriptor waits. Errors leave the
 * transactions in place and the client's backoff in charge; `options.onError`
 * still runs, the client's does not.
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
  const { client: explicit, maxTransactions = DEFAULT_MAX_EVENTS, history, ...listener } = options;
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
  if (history !== undefined) {
    if (!Number.isSafeInteger(history) || history < 1) {
      throw new StreamdError(`Invalid history: ${String(history)} (expected a positive integer)`);
    }
    if (listener.fromVersion === undefined && listener.fromTime === undefined) {
      throw new StreamdError("history needs fromVersion or fromTime, where the read starts");
    }
    if (listener.cursor !== undefined) {
      throw new StreamdError("history cannot take cursor: following starts after the newest transaction read");
    }
  }
  const client = useStreamdClient(explicit);
  const followed = useFollowing<TransactionGroup, typeof listener>(
    descriptor != null,
    valueKey([descriptor, listener, history ?? null]),
    client,
    listener,
    maxTransactions,
    (deliver, opts) =>
      history === undefined
        ? client.onTransaction(descriptor!, (group) => deliver([group]), opts)
        : withHistory(client, descriptor!, history, deliver, opts),
    history === undefined ? "live" : "loading",
  );
  return useMemo(
    () => ({ data: followed.events, error: followed.error, status: followed.status }),
    [followed],
  );
}
