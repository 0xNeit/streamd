import type { EventCallback, StreamdClient, StreamdEvent, Subscription } from "@streamd/core";
import type { Sale, SaleListenerOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

/** A client that follows NFT marketplace sales: an `AptosClient`, or one shaped like it. */
export interface SalesClient extends StreamdClient {
  sales(callback: EventCallback<Sale>, options?: SaleListenerOptions): Subscription;
}

/** `sales` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseSalesOptions extends Omit<SaleListenerOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: SalesClient;
  /** How many of the most recent sales to keep (default: 100). */
  maxEvents?: number;
}

/** Follows `client.sales` with `useTransfers`' result and lifetime. Pass `null` to wait. */
export function useSales(options: UseSalesOptions | null = {}): UseEventsResult<Sale> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options ?? {};
  const client = useClientWith<SalesClient>("sales", explicit);
  return useFollowing<StreamdEvent<Sale>, typeof listener>(
    options !== null,
    valueKey(listener),
    client,
    listener,
    maxEvents,
    (deliver, opts) => client.sales((event) => deliver([event]), opts),
  );
}
