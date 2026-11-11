import { StreamdError } from "@streamd/core";
import type { EventDescriptor, StreamdClient, Subscription, SubscriptionOptions } from "@streamd/core";
import type { PostgresEventStore } from "./store.js";

export interface StoreEventsOptions extends SubscriptionOptions {
  /** The name the subscription's cursor is saved under, so a restart resumes from it. */
  name: string;
}

/**
 * An `onBatch` subscription writing each batch to `store` together with the
 * subscription's cursor, in one statement. It resumes from the cursor saved
 * under `options.name` unless `options.cursor` is given, which is why it
 * resolves rather than returning at once. The saved cursor is the one before
 * the batch, then the one after it once the poller has moved on; a restart
 * between the two reads the batch again and the upsert absorbs it. The cursor
 * is also saved on first catching up, which fixes a relative or head start
 * before any event matches. A failed write reaches `onError` and ends the
 * subscription before its cursor moves past the batch; start it again to retry.
 */
export async function storeEvents(
  client: StreamdClient,
  store: PostgresEventStore,
  descriptor: EventDescriptor,
  options: StoreEventsOptions,
): Promise<Subscription> {
  const { name, ...listener } = options;
  const cursor = listener.cursor ?? (await store.readCursor(name));
  // These run off the poller's stack, so route throws as it does: to onError,
  // and a throw from onError itself to an uncaught error on a microtask.
  const report = (error: Error) => {
    try {
      listener.onError?.(error);
    } catch (thrown) {
      queueMicrotask(() => {
        throw thrown;
      });
    }
  };
  const subscription: Subscription = client.onBatch(
    descriptor,
    async (events) => {
      try {
        await store.write(events, { name, cursor: subscription.cursor });
      } catch (err) {
        // Pausing stops the cursor moving past this batch and still lets the
        // rejection reach onError, which an ended subscription would drop. It
        // then ends, because a resumed one would skip the batch it has marked
        // seen; on a timer, since the poller sees the rejection a microtask later.
        subscription.pause();
        setTimeout(() => subscription.unsubscribe(), 0);
        throw err;
      }
      // The poller moves its cursor past this page once this promise settles;
      // a timer runs after that. Saving that cursor means a relative or head
      // start never re-resolves after the first batch.
      setTimeout(() => {
        // The cursor saved with the batch stands; resuming there only redelivers it.
        store.write([], { name, cursor: subscription.cursor }).catch(() => {});
      }, 0);
    },
    {
      ...listener,
      cursor,
      onCaughtUp: () => {
        // Every page so far is stored, so the live cursor is safe to save. A
        // failed save reaches onError, and onCaughtUp is still called.
        void store
          .write([], { name, cursor: subscription.cursor })
          .catch((err: unknown) => report(new StreamdError("Saving the cursor on catching up failed", err)))
          .then(() => {
            try {
              listener.onCaughtUp?.();
            } catch (err) {
              report(new StreamdError("onCaughtUp threw", err));
            }
          });
      },
    },
  );
  return subscription;
}
