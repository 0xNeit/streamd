import type { EventDescriptor, StreamdClient, Subscription, SubscriptionOptions } from "@streamd/core";
import type { SqliteEventStore } from "./store.js";

export interface StoreEventsOptions extends SubscriptionOptions {
  /** The name the subscription's cursor is saved under, so a restart resumes from it. */
  name: string;
}

/**
 * An `onBatch` subscription writing each batch to `store` together with the
 * subscription's cursor, in one transaction. It resumes from the cursor saved
 * under `options.name` unless `options.cursor` is given. The saved cursor is
 * the one before the batch, so a restart reads the last batch again and the
 * upsert absorbs it. A failed write reaches `onError` and ends the
 * subscription before its cursor moves past the batch; start it again to retry.
 */
export function storeEvents(
  client: StreamdClient,
  store: SqliteEventStore,
  descriptor: EventDescriptor,
  options: StoreEventsOptions,
): Subscription {
  const { name, ...listener } = options;
  const cursor = listener.cursor ?? store.readCursor(name);
  const subscription: Subscription = client.onBatch(
    descriptor,
    (events) => {
      try {
        store.write(events, { name, cursor: subscription.cursor });
      } catch (err) {
        // Pausing stops the cursor moving past this batch and still lets the
        // throw reach onError, which an ended subscription would drop. It then
        // ends, because a resumed one would skip the batch it has marked seen.
        subscription.pause();
        queueMicrotask(() => subscription.unsubscribe());
        throw err;
      }
    },
    { ...listener, cursor },
  );
  return subscription;
}
