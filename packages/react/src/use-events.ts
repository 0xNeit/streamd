import type { EventDescriptor, StreamdClient, SubscriptionOptions } from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

export type { UseEventsResult, UseEventsStatus } from "./follow.js";

/** `onBatch` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseEventsOptions extends Omit<SubscriptionOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent events to keep (default: 100). */
  maxEvents?: number;
}

/**
 * Follows `descriptor` live with `client.onBatch` while the component is
 * mounted, resubscribing, with an empty list, only when the descriptor or
 * options change by value. Errors leave the events in place and the client's
 * backoff in charge; `options.onError` still runs, the client's does not.
 */
export function useEvents<TData = unknown>(
  descriptor: EventDescriptor<TData> | null | undefined,
  options: UseEventsOptions = {},
): UseEventsResult<TData> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options;
  const client = useStreamdClient(explicit);
  return useFollowing<TData, typeof listener>(
    descriptor != null,
    valueKey([descriptor, listener]),
    client,
    listener,
    maxEvents,
    (deliver, opts) => client.onBatch<TData>(descriptor!, deliver, opts),
  );
}
