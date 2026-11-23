import { StreamdError, requireEventTables } from "@streamd/core";
import type {
  EventCallback,
  EventDescriptor,
  EventTable,
  PointsFold,
  PointsState,
  ReduceOptions,
  Reduction,
  Score,
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
const CURSOR_SAVE_INTERVAL_MS = 5_000;
const DISPOSE: typeof Symbol.dispose = (Symbol.dispose ?? Symbol.for("Symbol.dispose")) as typeof Symbol.dispose;
const ASYNC_DISPOSE: typeof Symbol.asyncDispose = (Symbol.asyncDispose ?? Symbol.for("Symbol.asyncDispose")) as typeof Symbol.asyncDispose;

// Transfer cursors are saved apart from event cursors of the same name.
const TRANSFER_CURSOR_PREFIX = "transfers:";
const TABLE_CURSOR_PREFIX = "tables:";
const POINTS_CURSOR_PREFIX = "points:";

/** `reduce()`'s options but `cursor`, which comes from the store, plus the leaderboard's `name`. */
export type StorePointsOptions<TEvent = StreamdEvent> = Omit<ReduceOptions<PointsState, unknown, TEvent>, "cursor"> & {
  name: string;
};

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
 * to retry. A `resource` descriptor's changes are stored with its address, so
 * a `StoreAdapter` can serve the descriptor back.
 */
export function storeEvents(
  client: StreamdClient,
  store: SqliteEventStore,
  descriptor: EventDescriptor,
  options: StoreEventsOptions,
): Subscription {
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

/**
 * One `onBatch` subscription over every event type in `tables`, a generic
 * one in every instantiation, upserting each event into its table together
 * with the subscription's cursor, in one transaction, as `storeEvents` writes:
 * the same resume, catch-up save and failure rules. The tables are created
 * before it subscribes. The cursor is saved under `tables:` and `options.name`.
 */
export function storeTables(
  client: StreamdClient,
  store: SqliteEventStore,
  tables: readonly EventTable[],
  options: StoreEventsOptions,
): Subscription {
  const { name, ...listener } = options;
  if (typeof name !== "string" || name === "") throw new StreamdError("storeTables needs a name to save its cursor under");
  const eventType = requireEventTables(tables).map((t) => t.eventType);
  store.writeTables(tables, []);
  return sink(
    store,
    TABLE_CURSOR_PREFIX + name,
    listener,
    (events, checkpoint) => store.writeTables(tables, events, checkpoint),
    (deliver, resolved) => client.onBatch({ kind: "module", eventType }, deliver, resolved),
  );
}

/**
 * `client.reduce()` over a `points()` fold, keeping the leaderboard `name` in
 * `streamd_points` with the reduction's cursor under `points:` and `name`.
 * With a saved cursor it resumes from the stored rows, ignoring the history
 * options; otherwise it folds from the given start and, once the backfill is
 * done, writes every row and the cursor in one write. Each live change then
 * writes the changed rows and the cursor in one write. A failed write stops
 * the reduction and reaches `onError`; a failed backfill write also rejects
 * `ready`.
 */
export function storePoints<TEvent extends StreamdEvent>(
  client: StreamdClient,
  store: SqliteEventStore,
  descriptor: EventDescriptor<any, TEvent>,
  fold: PointsFold<TEvent>,
  options: StorePointsOptions<NoInfer<TEvent>>,
): Reduction<PointsState> {
  const { name, onChange, ...rest } = options;
  if (typeof name !== "string" || name === "") throw new StreamdError("storePoints needs a name to save its cursor under");
  if ((rest as ReduceOptions).cursor !== undefined) throw new StreamdError("storePoints takes its cursor from the store");
  const checkpoint = POINTS_CURSOR_PREFIX + name;
  const saved = store.readCursor(checkpoint);
  // What the store holds; undefined until the backfill is written.
  let written: PointsState | undefined = saved === undefined ? undefined : store.readPoints(name);
  let failed = false;
  const save = (scores: Score[], state: PointsState) => {
    try {
      store.writePoints(name, scores, { name: checkpoint, cursor: reduction.cursor });
      written = state;
    } catch (err) {
      failed = true;
      // Ended later, as storeEvents ends: an ended subscription drops the throw before it reaches onError.
      queueMicrotask(() => reduction.unsubscribe());
      throw err;
    }
  };
  const reduction: Reduction<PointsState> = client.reduce(descriptor, fold, written ?? fold.initial, {
    ...rest,
    ...(saved !== undefined && { cursor: saved }),
    onChange: (state, event) => {
      if (failed) return;
      // Before the backfill write, that write covers this change too.
      if (written !== undefined) {
        const before = written;
        save([...state.values()].filter((score) => before.get(score.address) !== score), state);
      }
      onChange?.(state, event);
    },
  } as ReduceOptions<PointsState, any, any>);
  const ready = reduction.ready.then((value) => {
    if (written === undefined) {
      try {
        save([...reduction.value.values()], reduction.value);
      } catch (err) {
        rest.onError?.(err as Error);
        throw err;
      }
    }
    return value;
  });
  ready.catch(() => {});
  return Object.create(reduction, { ready: { value: ready } });
}

function sink(
  store: SqliteEventStore,
  name: string,
  listener: SubscriptionOptions,
  write: Write,
  subscribe: Subscribe,
): Subscription {
  const cursor = listener.cursor ?? store.readCursor(name);
  let lastSaved = cursor;
  let saving = false;
  let failed = false;
  let stopped = false;
  const saveCursor = (target: string | undefined) => {
    if (failed || stopped || target === undefined || target === lastSaved) return;
    try {
      store.write([], { name, cursor: target });
      lastSaved = target;
    } catch {
      // The cursor saved with the batch stands; resuming there only redelivers it.
    }
  };
  const subscription: Subscription = subscribe(
    (events) => {
      const before = subscription.cursor;
      try {
        write(events, { name, cursor: before });
        if (before !== undefined) lastSaved = before;
      } catch (err) {
        failed = true;
        clearInterval(timer);
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
        if (stopped || failed) {
          saving = false;
          return;
        }
        if (subscription.active && subscription.cursor === before) {
          setTimeout(save, SAVE_RETRY_MS);
          return;
        }
        saving = false;
        saveCursor(subscription.cursor);
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
          const cur = subscription.cursor;
          if (!failed && cur !== undefined) {
            store.write([], { name, cursor: cur });
            lastSaved = cur;
          }
        } finally {
          listener.onCaughtUp?.();
        }
      },
    },
  );
  const timer = setInterval(() => {
    if (stopped || (!subscription.active && !subscription.paused)) {
      clearInterval(timer);
      return;
    }
    if (!failed && !saving && subscription.active) saveCursor(subscription.cursor);
  }, CURSOR_SAVE_INTERVAL_MS);
  timer.unref?.();

  const stopAndSave = () => {
    clearInterval(timer);
    subscription.unsubscribe();
    saveCursor(subscription.cursor);
    stopped = true;
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
    unsubscribe: stopAndSave,
    [DISPOSE]: stopAndSave,
    [ASYNC_DISPOSE]: async () => {
      clearInterval(timer);
      await subscription[ASYNC_DISPOSE]();
      saveCursor(subscription.cursor);
      stopped = true;
    },
  };
}
