import { StreamdError, requireEventTables } from "@streamd/core";
import type {
  EventCallback,
  EventDescriptor,
  EventTable,
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

const SAVE_RETRY_MS = 10;
const CURSOR_SAVE_INTERVAL_MS = 5_000;
const DISPOSE: typeof Symbol.dispose = (Symbol.dispose ?? Symbol.for("Symbol.dispose")) as typeof Symbol.dispose;
const ASYNC_DISPOSE: typeof Symbol.asyncDispose = (Symbol.asyncDispose ?? Symbol.for("Symbol.asyncDispose")) as typeof Symbol.asyncDispose;

// Transfer cursors are saved apart from event cursors of the same name.
const TRANSFER_CURSOR_PREFIX = "transfers:";
const TABLE_CURSOR_PREFIX = "tables:";

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
 * A `resource` descriptor's changes are stored with its address, so a
 * `PostgresStoreAdapter` can serve the descriptor back.
 */
export async function storeEvents(
  client: StreamdClient,
  store: PostgresEventStore,
  descriptor: EventDescriptor,
  options: StoreEventsOptions,
): Promise<Subscription> {
  const { name, ...listener } = options;
  // A resource change carries only its type, so the address comes from the descriptor.
  const written = descriptor?.kind === "resource" ? { accountAddress: descriptor.accountAddress } : undefined;
  return sink(
    store,
    name,
    listener,
    (events, checkpoint) => store.write(events, checkpoint, written),
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
  // One statement per record, since transfers() delivers one at a time.
  return sink(
    store,
    TRANSFER_CURSOR_PREFIX + name,
    listener,
    (records, checkpoint) => store.writeTransfers(records as StreamdEvent<Transfer>[], checkpoint),
    (deliver, resolved) => client.transfers((record) => deliver([record]), resolved as O),
  );
}

/**
 * One `onBatch` subscription over every event type in `tables`, a generic
 * one in every instantiation, upserting each event into its table together
 * with the subscription's cursor, in one statement, as `storeEvents` writes:
 * the same resume, catch-up save and failure rules. The tables are created
 * before it subscribes. The cursor is saved under `tables:` and `options.name`.
 */
export async function storeTables(
  client: StreamdClient,
  store: PostgresEventStore,
  tables: readonly EventTable[],
  options: StoreEventsOptions,
): Promise<Subscription> {
  const { name, ...listener } = options;
  if (typeof name !== "string" || name === "") throw new StreamdError("storeTables needs a name to save its cursor under");
  const eventType = requireEventTables(tables).map((t) => t.eventType);
  await store.writeTables(tables, []);
  return sink(
    store,
    TABLE_CURSOR_PREFIX + name,
    listener,
    (events, checkpoint) => store.writeTables(tables, events, checkpoint),
    (deliver, resolved) => client.onBatch({ kind: "module", eventType }, deliver, resolved),
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
  let lastSaved = cursor;
  let saving = false;
  let failed = false;
  let stopped = false;
  let inflight: Promise<void> = Promise.resolve();
  const saveCursor = (target: string | undefined): Promise<void> => {
    if (failed || stopped || target === undefined || target === lastSaved) return inflight;
    const next = inflight.then(async () => {
      if (failed || stopped || target === lastSaved) return;
      try {
        await store.write([], { name, cursor: target });
        lastSaved = target;
      } catch {
        // The cursor saved with the batch stands; resuming there only redelivers it.
      }
    });
    inflight = next;
    return next;
  };
  const subscription: Subscription = subscribe(
    async (events) => {
      const before = subscription.cursor;
      try {
        await write(events, { name, cursor: before });
        if (before !== undefined) lastSaved = before;
      } catch (err) {
        failed = true;
        clearInterval(timer);
        // Pausing stops the cursor moving past this batch and still lets the
        // rejection reach onError, which an ended subscription would drop. It
        // then ends, because a resumed one would skip the batch it has marked
        // seen; on a timer, since the poller sees the rejection a microtask later.
        subscription.pause();
        setTimeout(() => subscription.unsubscribe(), 0);
        throw err;
      }
      // The poller moves its cursor past this page once the page's last
      // delivery settles; a timer usually runs after that. Saving that cursor
      // means a relative or head start never re-resolves after the first
      // batch. One pending save covers every write of a page, and waits while
      // a later write, async predicate or view holds the cursor where it was.
      if (saving) return;
      saving = true;
      const save = () => {
        if (stopped || failed) {
          saving = false;
          return;
        }
        if (subscription.active && subscription.cursor === before) {
          setTimeout(save, SAVE_RETRY_MS);
          return;
        }
        saving = false;
        void saveCursor(subscription.cursor);
      };
      setTimeout(save, 0);
    },
    {
      ...listener,
      cursor,
      onCaughtUp: () => {
        // Every page so far is stored, so the live cursor is safe to save. A
        // failed save reaches onError, and onCaughtUp is still called.
        const cur = subscription.cursor;
        const writeCatchUp = inflight.then(async () => {
          if (failed || cur === undefined) return;
          try {
            await store.write([], { name, cursor: cur });
            lastSaved = cur;
          } catch (err: unknown) {
            report(new StreamdError("Saving the cursor on catching up failed", err));
          }
        });
        inflight = writeCatchUp;
        void writeCatchUp.then(() => {
          try {
            listener.onCaughtUp?.();
          } catch (err) {
            report(new StreamdError("onCaughtUp threw", err));
          }
        });
      },
    },
  );
  const timer = setInterval(() => {
    if (stopped || (!subscription.active && !subscription.paused)) {
      clearInterval(timer);
      return;
    }
    if (!failed && !saving && subscription.active) void saveCursor(subscription.cursor);
  }, CURSOR_SAVE_INTERVAL_MS);
  timer.unref?.();

  const stopOnly = () => {
    clearInterval(timer);
    subscription.unsubscribe();
    void saveCursor(subscription.cursor).finally(() => {
      stopped = true;
    });
  };
  return {
    id: subscription.id,
    get active() {
      return subscription.active;
    },
    get paused() {
      return subscription.paused;
    },
    get cursor() {
      return subscription.cursor;
    },
    get status() {
      return subscription.status;
    },
    caughtUp: subscription.caughtUp,
    pause: () => subscription.pause(),
    resume: () => subscription.resume(),
    unsubscribe: stopOnly,
    [DISPOSE]: stopOnly,
    [ASYNC_DISPOSE]: async () => {
      clearInterval(timer);
      await subscription[ASYNC_DISPOSE]();
      await saveCursor(subscription.cursor);
      stopped = true;
    },
  };
}
