import type { EventCallback, StreamdClient, Subscription, Transfer } from "@streamd/core";
import type { TransferListenerOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

/** A client that follows transfers: an `AptosClient`, a `SuiClient`, or one shaped like them. */
export interface TransfersClient extends StreamdClient {
  transfers(callback: EventCallback<Transfer>, options?: TransferListenerOptions): Subscription;
}

/** `transfers` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseTransfersOptions extends Omit<TransferListenerOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: TransfersClient;
  /** How many of the most recent transfers to keep (default: 100). */
  maxEvents?: number;
}

/**
 * Follows `client.transfers` while the component is mounted, with `useEvents`'s
 * result and lifetime. Pass `null` to wait, for example for a connected wallet.
 */
export function useTransfers(options: UseTransfersOptions | null = {}): UseEventsResult<Transfer> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options ?? {};
  const client = useClientWith<TransfersClient>("transfers", explicit);
  return useFollowing<Transfer, typeof listener>(
    options !== null,
    valueKey(listener),
    client,
    listener,
    maxEvents,
    (deliver, opts) => client.transfers((event) => deliver([event]), opts),
  );
}
