import { StreamdError } from "@streamd/core";
import type {
  EventCallback,
  EventDescriptor,
  StreamdClient,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  Transfer,
} from "@streamd/core";
import type { PostgresEventStore, StoreCheckpoint } from "./store.js";

export interface StoreEventsOptions extends SubscriptionOptions {
  /** The name the subscription's cursor is saved under, so a restart resumes from it. */
  name: string;
}

/** Anything with a chain client's `transfers()`: `AptosClient` and `SuiClient` both fit. */
export interface TransferClient<O extends SubscriptionOptions = SubscriptionOptions> {
  transfers(callback: EventCallback<Transfer>, options?: O): Subscription;
}

/** The client's own `transfers()` options, plus the `name` its cursor is saved under. */
export type StoreTransfersOptions<O extends SubscriptionOptions = SubscriptionOptions> = O & { name: string };

// Transfer cursors are saved apart from event cursors of the same name.
const TRANSFER_CURSOR_PREFIX = "transfers:";

type Write = (events: StreamdEvent[], checkpoint: StoreCheckpoint) => Promise<void>;
type Subscribe = (deliver: (events: StreamdEvent[]) => Promise<void>, options: SubscriptionOptions) => Subscription;

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
  return sink(
    store,
    name,
    listener,
    (events, checkpoint) => store.write(events, checkpoint),
    (deliver, resolved) => client.onBatch(descriptor, deliver, resolved),
  );
}

/**
 * `transfers()` on `client`, writing each record to `streamd_transfers`
 * together with the subscription's cursor, as `storeEvents` writes events:
 * the same resume, catch-up save and failure rules, one statement per
 * record. The cursor is saved under `transfers:` and `options.name`, so it
 * never meets an event cursor of the same name; give each filter its own name.
 */
export async function storeTransfers<O extends SubscriptionOptions>(
  client: TransferClient<O>,
  store: PostgresEventStore,
  options: StoreTransfersOptions<NoInfer<O>>,
): Promise<Subscription> {
  const { name, ...listener } = options;
  if (typeof name !== "string" || name === "") throw new StreamdError("storeTransfers needs a name to save its cursor under");
  // ponytail: one statement per record, since transfers() delivers one at a time; a batch delivery on it would cut that.
  return sink(
    store,
    TRANSFER_CURSOR_PREFIX + name,
    listener,
    (records, checkpoint) => store.writeTransfers(records as StreamdEvent<Transfer>[], checkpoint),
    (deliver, resolved) => client.transfers((record) => deliver([record]), resolved as O),
  );
}

async function sink(
  store: PostgresEventStore,
  name: string,
  listener: SubscriptionOptions,
  write: Write,
  subscribe: Subscribe,
): Promise<Subscription> {
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
  let saving = false;
  const subscription: Subscription = subscribe(
    async (events) => {
      try {
        await write(events, { name, cursor: subscription.cursor });
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
      // start never re-resolves after the first batch. One pending save covers
      // every write of a page.
      if (saving) return;
      saving = true;
      setTimeout(() => {
        saving = false;
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
