import type { EventCallback, StreamdClient, StreamdEvent, Subscription } from "@streamd/core";
import type { Activity, ActivityListenerOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

/** A client that follows an account's activity: an `AptosClient`, or one shaped like it. */
export interface ActivityClient extends StreamdClient {
  activity(address: string, callback: EventCallback<Activity>, options?: ActivityListenerOptions): Subscription;
}

/** `activity` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseActivityOptions extends Omit<ActivityListenerOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: ActivityClient;
  /** How many of the most recent records to keep (default: 100). */
  maxEvents?: number;
}

/**
 * Follows `client.activity(address)` with `useTransfers`' result and lifetime,
 * one record per transaction. A `null`, `undefined` or empty address waits.
 */
export function useActivity(
  address: string | null | undefined,
  options: UseActivityOptions = {},
): UseEventsResult<Activity> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options;
  const client = useClientWith<ActivityClient>("activity", explicit);
  return useFollowing<StreamdEvent<Activity>, typeof listener>(
    address != null && address !== "",
    valueKey([address, listener]),
    client,
    listener,
    maxEvents,
    (deliver, opts) => client.activity(address!, (event) => deliver([event]), opts),
  );
}
