import type { EventCallback, StreamdClient, StreamdEvent, Subscription } from "@streamd/core";
import type { Swap, SwapListenerOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

/** A client that follows DEX swaps: an `AptosClient`, or one shaped like it. */
export interface SwapsClient extends StreamdClient {
  swaps(callback: EventCallback<Swap>, options?: SwapListenerOptions): Subscription;
}

/** `swaps` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseSwapsOptions extends Omit<SwapListenerOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: SwapsClient;
  /** How many of the most recent swaps to keep (default: 100). */
  maxEvents?: number;
}

/** Follows `client.swaps` with `useTransfers`' result and lifetime. Pass `null` to wait. */
export function useSwaps(options: UseSwapsOptions | null = {}): UseEventsResult<Swap> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options ?? {};
  const client = useClientWith<SwapsClient>("swaps", explicit);
  return useFollowing<StreamdEvent<Swap>, typeof listener>(
    options !== null,
    valueKey(listener),
    client,
    listener,
    maxEvents,
    (deliver, opts) => client.swaps((event) => deliver([event]), opts),
  );
}
