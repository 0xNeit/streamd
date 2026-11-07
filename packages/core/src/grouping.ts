import type {
  EventDescriptor,
  EventPage,
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
import { CursorManager } from "./cursor.js";
import { isThenable } from "./filter.js";

/** What `onTransaction()` needs from its client. */
export interface TransactionHost {
  defaults: ListenerDefaults;
  onError: ((error: Error) => void) | undefined;
  subscribe(
    descriptor: EventDescriptor,
    delivery: Delivery,
    options: SubscriptionOptions,
    id: string | undefined,
  ): Subscription;
}

/** How far a walk has settled: `"all"` once it caught up without a `throughVersion`. */
type Bound = bigint | "all";

/**
 * The walk's settled bound after `page`. Without `throughVersion`, a page
 * that reached a version has every lower version's events, since a committed
 * transaction's events exist all at once; the highest one may continue on the
 * next page unless the source reports nothing more.
 */
function boundOf(page: EventPage, previous: Bound | undefined): Bound | undefined {
  if (page.throughVersion !== undefined) return page.throughVersion;
  if (!page.hasMore) return "all";
  let highest: bigint | undefined;
  for (const e of page.events) if (highest === undefined || e.version > highest) highest = e.version;
  return highest === undefined ? previous : highest - 1n;
}

/** The lowest of the walks' bounds, or `undefined` while any walk has none. */
function lowest(bounds: (Bound | undefined)[]): Bound | undefined {
  let low: Bound = "all";
  for (const b of bounds) {
    if (b === undefined) return undefined;
    if (b !== "all" && (low === "all" || b < low)) low = b;
  }
  return low;
}

function groupOf(version: bigint, events: StreamdEvent[]): TransactionGroup {
  const group: TransactionGroup = { version, events };
  for (const key of ["transactionHash", "sender", "timestamp", "entryFunction"] as const) {
    const from = events.find((e) => e[key] !== undefined);
    if (from !== undefined) (group as unknown as Record<string, unknown>)[key] = from[key];
  }
  return group;
}

function combinedStatus(walks: Subscription[], active: boolean): SubscriptionStatus {
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
  callback: TransactionCallback,
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
  const bounds: (Bound | undefined)[] = descriptors.map(() => undefined);
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

  const deliver = async (group: TransactionGroup) => {
    try {
      const result: unknown = callback(group);
      if (isThenable(result)) await result;
    } catch (err) {
      report(new StreamdError(`Transaction callback threw for ${group.version}`, err));
    }
  };

  // Serialised through `releasing`, so walks never deliver alongside each other.
  const release = async (nextCursor: string | undefined) => {
    const limit = lowest(bounds);
    if (limit === undefined) return;
    const ready = [...buffer.keys()]
      .filter((v) => limit === "all" || v <= limit)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    for (const version of ready) {
      // A pause or stop between groups leaves the rest buffered.
      if (!live()) return;
      const events = [...buffer.get(version)!.values()].sort((a, b) => a.eventIndex - b.eventIndex);
      buffer.delete(version);
      for (const e of events) released.mark(e);
      await deliver(groupOf(version, events));
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
    bounds[walk] = boundOf(page, bounds[walk]);
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
    );
    walks.push(sub);
  });

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
    pause: () => walks.forEach((w) => w.pause()),
    resume: () => walks.forEach((w) => w.resume()),
    unsubscribe: end,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: end,
  };
}
