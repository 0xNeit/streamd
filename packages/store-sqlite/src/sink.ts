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
import type { StoreCheckpoint, SqliteEventStore } from "./store.js";

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

const SAVE_RETRY_MS = 10;

// Transfer cursors are saved apart from event cursors of the same name.
const TRANSFER_CURSOR_PREFIX = "transfers:";

type Write = (events: StreamdEvent[], checkpoint: StoreCheckpoint) => void;
type Subscribe = (deliver: (events: StreamdEvent[]) => void, options: SubscriptionOptions) => Subscription;

/**
 * An `onBatch` subscription writing each batch to `store` together with the
 * subscription's cursor, in one transaction. It resumes from the cursor saved
 * under `options.name` unless `options.cursor` is given. The saved cursor is
 * the one before the batch, then the one after it once the poller has moved
 * on; a restart between the two reads the batch again and the upsert absorbs
 * it. The cursor is also saved on first catching up, which fixes a relative or
 * head start before any event matches. A failed write reaches `onError` and
 * ends the subscription before its cursor moves past the batch; start it again
 * to retry.
 */
export function storeEvents(
  client: StreamdClient,
  store: SqliteEventStore,
  descriptor: EventDescriptor,
  options: StoreEventsOptions,
): Subscription {
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
 * the same resume, catch-up save and failure rules, one transaction per
 * record. The cursor is saved under `transfers:` and `options.name`, so it
 * never meets an event cursor of the same name; give each filter its own name.
 */
export function storeTransfers<O extends SubscriptionOptions>(
  client: TransferClient<O>,
  store: SqliteEventStore,
  options: StoreTransfersOptions<NoInfer<O>>,
): Subscription {
  const { name, ...listener } = options;
  if (typeof name !== "string" || name === "") throw new StreamdError("storeTransfers needs a name to save its cursor under");
  // One transaction per record, since transfers() delivers one at a time.
  return sink(
    store,
    TRANSFER_CURSOR_PREFIX + name,
    listener,
    (records, checkpoint) => store.writeTransfers(records as StreamdEvent<Transfer>[], checkpoint),
    (deliver, resolved) => client.transfers((record) => deliver([record]), resolved as O),
  );
}

function sink(
  store: SqliteEventStore,
  name: string,
  listener: SubscriptionOptions,
  write: Write,
  subscribe: Subscribe,
): Subscription {
  const cursor = listener.cursor ?? store.readCursor(name);
  let saving = false;
  const subscription: Subscription = subscribe(
    (events) => {
      const before = subscription.cursor;
      try {
        write(events, { name, cursor: before });
      } catch (err) {
        // Pausing stops the cursor moving past this batch and still lets the
        // throw reach onError, which an ended subscription would drop. It then
        // ends, because a resumed one would skip the batch it has marked seen.
        subscription.pause();
        queueMicrotask(() => subscription.unsubscribe());
        throw err;
      }
      // The poller moves its cursor past this page as soon as the page's last
      // delivery returns, so a microtask usually sees the cursor after it.
      // Saving that one means a relative or head start never re-resolves after
      // the first batch. One pending save covers every write of a page, and
      // waits while an async predicate or view later in the page holds the
      // cursor where it was.
      if (saving) return;
      saving = true;
      const save = () => {
        if (subscription.active && subscription.cursor === before) {
          setTimeout(save, SAVE_RETRY_MS);
          return;
        }
        saving = false;
        try {
          store.write([], { name, cursor: subscription.cursor });
        } catch {
          // The cursor saved with the batch stands; resuming there only redelivers it.
        }
      };
      queueMicrotask(save);
    },
    {
      ...listener,
      cursor,
      onCaughtUp: () => {
        // Every page so far is stored, so the live cursor is safe to save. A
        // failed save still reaches onError, and the caller still hears of it.
        try {
          store.write([], { name, cursor: subscription.cursor });
        } finally {
          listener.onCaughtUp?.();
        }
      },
    },
  );
  return subscription;
}
