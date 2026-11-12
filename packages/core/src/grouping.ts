import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  ListenerDefaults,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  SubscriptionStatus,
  TransactionCallback,
  TransactionGroup,
} from "./types.js";
import type { Delivery } from "./poller.js";
import { StreamdError } from "./errors.js";
import { sameValue } from "./compare.js";
import { CursorManager } from "./cursor.js";
import { isThenable } from "./filter.js";
import { ASYNC_DISPOSE } from "./utils.js";

/** What `onTransaction()` needs from its client. */
export interface TransactionHost {
  defaults: ListenerDefaults;
  onError: ((error: Error) => void) | undefined;
  /** The client's adapter with shared polling off. */
  unshared: EventAdapter;
  subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options: SubscriptionOptions,
    id: string | undefined,
    wrap: ((source: EventAdapter) => EventAdapter) | undefined,
  ): Subscription;
}

function highestOf(page: EventPage): bigint | undefined {
  let highest = page.throughVersion;
  for (const e of page.events) if (highest === undefined || e.version > highest) highest = e.version;
  return highest;
}

function max(a: bigint | undefined, b: bigint | undefined): bigint | undefined {
  return a === undefined ? b : b === undefined || a > b ? a : b;
}

/**
 * The walk's settled bound after `page`. Without `throughVersion`, a page
 * that reached a version has every lower version's events, since a committed
 * transaction's events exist all at once; the highest one may continue on the
 * next page unless the source reports nothing more. A caught-up page covers
 * only what was committed when its fetch started: every version up to
 * `known`, the highest any walk had reached by then.
 */
function boundOf(
  page: EventPage,
  previous: bigint | undefined,
  known: bigint | undefined,
): bigint | undefined {
  if (page.throughVersion !== undefined) return page.throughVersion;
  const highest = highestOf(page);
  if (!page.hasMore) return max(max(previous, highest), known) ?? -1n;
  return highest === undefined ? previous : highest - 1n;
}

/** The lowest of the walks' bounds, or `undefined` while any walk has none. */
function lowest(bounds: (bigint | undefined)[]): bigint | undefined {
  let low: bigint | undefined;
  for (const b of bounds) {
    if (b === undefined) return undefined;
    if (low === undefined || b < low) low = b;
  }
  return low;
}

/**
 * `source` calling `onFetch` as each fetch starts. Merged walks read their own
 * pages rather than shared ones, since a shared page may come from a request
 * that started before this one.
 */
function stamped(source: EventAdapter, onFetch: () => void): EventAdapter {
  return Object.assign(Object.create(source) as EventAdapter, {
    fetchEvents: (d: EventDescriptor, o: FetchOptions) => (onFetch(), source.fetchEvents(d, o)),
  });
}

function groupOf(version: bigint, events: StreamdEvent[]): TransactionGroup {
  const group: TransactionGroup = { version, events };
  for (const key of ["transactionHash", "sender", "timestamp", "entryFunction"] as const) {
    const from = events.find((e) => e[key] !== undefined);
    if (from !== undefined) (group as unknown as Record<string, unknown>)[key] = from[key];
  }
  // On Sui the events of one transaction can come from different commands, so no one call is the transaction's.
  if (events.some((e) => e.entryFunction !== undefined && !sameValue(e.entryFunction, group.entryFunction))) delete group.entryFunction;
  return group;
}

export function combinedStatus(walks: Subscription[], active: boolean): SubscriptionStatus {
  const statuses = walks.map((w) => w.status);
  let lastVersion: bigint | undefined;
  for (const s of statuses) {
    if (s.lastVersion !== undefined && (lastVersion === undefined || s.lastVersion > lastVersion)) {
      lastVersion = s.lastVersion;
    }
  }
  const successes = statuses.map((s) => s.lastSuccessAt);
  return {
    active,
    cursor: undefined,
    lastVersion,
    consecutiveErrors: Math.max(...statuses.map((s) => s.consecutiveErrors)),
    lastSuccessAt: successes.includes(undefined)
      ? undefined
      : Math.min(...(successes as number[])),
  };
}

/**
 * Follow `descriptors` with one walk each, buffering their events by version
 * and calling `callback` once per transaction once every walk has settled past
 * it, so a transaction's matching events arrive together.
 */
export function startTransactions(
  host: TransactionHost,
  descriptor: EventDescriptor | readonly EventDescriptor[],
  callback: TransactionCallback | { batch: (groups: TransactionGroup[]) => unknown },
  options: SubscriptionOptions = {},
): Subscription {
  const descriptors: readonly EventDescriptor[] = Array.isArray(descriptor)
    ? descriptor
    : [descriptor as EventDescriptor];
  if (descriptors.length === 0) {
    throw new StreamdError("onTransaction needs at least one descriptor");
  }
  const single = descriptors.length === 1;
  if (!single && options.cursor !== undefined) {
    throw new StreamdError("cursor cannot resume several descriptors; pass one descriptor");
  }

  const buffer = new Map<bigint, Map<number, StreamdEvent>>();
  const released = new CursorManager(undefined, options.dedupWindow ?? host.defaults.dedupWindow);
  const bounds: (bigint | undefined)[] = descriptors.map(() => undefined);
  // The highest version any walk has reached, and its value as each walk's fetch started.
  let seen: bigint | undefined;
  const known: (bigint | undefined)[] = descriptors.map(() => undefined);
  const caughtUp = new Set<number>();
  const walks: Subscription[] = [];
  let checkpoint = options.cursor;
  let releasing = Promise.resolve();

  const report = (error: Error) => {
    try {
      (options.onError ?? host.onError)?.(error);
    } catch (handlerError) {
      queueMicrotask(() => {
        throw handlerError;
      });
    }
  };
  const live = () => walks.length === descriptors.length && walks.every((w) => w.active);
  const end = () => {
    for (const w of walks) w.unsubscribe();
  };

  const deliver = async (groups: TransactionGroup[]) => {
    try {
      const result: unknown =
        typeof callback === "function" ? callback(groups[0]!) : callback.batch(groups);
      if (isThenable(result)) await result;
    } catch (err) {
      report(new StreamdError(`Transaction callback threw for ${groups[0]!.version}`, err));
    }
  };

  // Serialised through `releasing`, so walks never deliver alongside each other.
  const release = async (nextCursor: string | undefined) => {
    const limit = lowest(bounds);
    if (limit === undefined) return;
    const ready = [...buffer.keys()]
      .filter((v) => v <= limit)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const take = (version: bigint) => {
      const events = [...buffer.get(version)!.values()].sort((a, b) => a.eventIndex - b.eventIndex);
      buffer.delete(version);
      for (const e of events) released.mark(e);
      return groupOf(version, events);
    };
    if (typeof callback !== "function") {
      // A batch takes everything released at once.
      if (ready.length > 0 && live()) await deliver(ready.map(take));
    } else {
      for (const version of ready) {
        // A pause or stop between groups leaves the rest buffered.
        if (!live()) return;
        await deliver([take(version)]);
      }
    }
    if (single && buffer.size === 0 && live() && nextCursor !== undefined) checkpoint = nextCursor;
  };

  const ingest = (walk: number, events: StreamdEvent[], page: EventPage) => {
    for (const e of events) {
      if (released.hasSeen(e)) continue;
      let tx = buffer.get(e.version);
      if (tx === undefined) buffer.set(e.version, (tx = new Map()));
      tx.set(e.eventIndex, e);
    }
    bounds[walk] = boundOf(page, bounds[walk], known[walk]);
    seen = max(seen, highestOf(page));
    releasing = releasing.then(() => release(page.nextCursor));
    return releasing;
  };

  descriptors.forEach((d, walk) => {
    const sub = host.subscribe(
      d,
      { page: (events, page) => ingest(walk, events, page) },
      {
        ...options,
        cursor: single ? options.cursor : undefined,
        onCaughtUp: () => {
          caughtUp.add(walk);
          if (caughtUp.size === descriptors.length) options.onCaughtUp?.();
        },
        onError: (err) => {
          // A walk that gave up on its own ends the rest: without it nothing settles.
          if (!sub.active && !sub.paused) end();
          report(err);
        },
      },
      walk === 0 ? options.id : undefined,
      (source) => stamped(single ? source : host.unshared, () => (known[walk] = seen)),
    );
    walks.push(sub);
  });

  const allCaughtUp = Promise.all(walks.map((w) => w.caughtUp)).then(() => {});
  allCaughtUp.catch(() => {});

  return {
    id: walks[0]!.id,
    get active() {
      return live();
    },
    get paused() {
      return walks.every((w) => w.paused);
    },
    get cursor() {
      return single ? checkpoint : undefined;
    },
    get status() {
      return single ? { ...walks[0]!.status, cursor: checkpoint } : combinedStatus(walks, live());
    },
    caughtUp: allCaughtUp,
    pause: () => walks.forEach((w) => w.pause()),
    resume: () => walks.forEach((w) => w.resume()),
    unsubscribe: end,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: end,
    [ASYNC_DISPOSE]: async () => {
      end();
      await Promise.all(walks.map((w) => w[ASYNC_DISPOSE]()));
    },
  };
}
