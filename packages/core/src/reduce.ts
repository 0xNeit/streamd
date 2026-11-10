import type {
  EventCallback,
  EventDescriptor,
  EventFilter,
  ListenerDefaults,
  ListenerOptions,
  ReduceOptions,
  Reducer,
  Reduction,
  StreamdEvent,
  Subscription,
} from "./types.js";
import type { EventQuery } from "./query.js";
import { StreamdError } from "./errors.js";
import { eventId } from "./cursor.js";
import { UNSIGNED_INTEGER } from "./decode.js";
import { onAbort } from "./utils.js";

/** What a reduction needs from its client. */
export interface ReduceHost {
  query: EventQuery;
  defaults: ListenerDefaults;
  onError: ((error: Error) => void) | undefined;
  subscribe(descriptor: EventDescriptor, callback: EventCallback, options: ListenerOptions): Subscription;
  /** Registers `stop` for `destroy()`; the returned function unregisters it. */
  track(stop: () => void): () => void;
}

const HISTORY_KEYS = ["fromVersion", "fromBlock", "fromTime", "maxPages", "retries"] as const;
const LIVE_KEYS = [
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "maxPollInterval",
  "dedupWindow",
  "onCaughtUp",
] as const;
const ALLOWED: readonly string[] = [
  ...HISTORY_KEYS,
  ...LIVE_KEYS,
  "where",
  "decode",
  "onError",
  "signal",
  "onChange",
  "cursor",
] satisfies readonly (keyof ReduceOptions)[];

function pick<K extends string>(source: object, keys: readonly K[]): Record<string, unknown> {
  const picked: Record<string, unknown> = {};
  for (const k of keys) {
    const v = (source as Record<string, unknown>)[k];
    if (v !== undefined) picked[k] = v;
  }
  return picked;
}

type Folded = Pick<StreamdEvent, "version" | "eventIndex">;

function isAfter(event: StreamdEvent, last: Folded): boolean {
  return event.version > last.version ||
    (event.version === last.version && event.eventIndex > last.eventIndex);
}

const POSITION_PREFIX = "streamd-reduction:";

/**
 * An adapter cursor moves per page and the value per event, so a position saved
 * partway through a page also names the last folded event, for the resume to skip.
 */
function encodePosition(cursor: string | undefined, last: Folded | undefined): string {
  return POSITION_PREFIX + JSON.stringify([cursor ?? null, last?.version.toString() ?? null, last?.eventIndex ?? null]);
}

function parsePosition(text: string): { cursor?: string; last?: Folded } {
  if (typeof text !== "string" || text === "") {
    throw new StreamdError(`Invalid reduction cursor: ${String(text)}`);
  }
  if (!text.startsWith(POSITION_PREFIX)) return { cursor: text };
  const [cursor, version, eventIndex] = parseFields(text);
  return {
    ...(cursor !== null && { cursor }),
    ...(version !== null && { last: { version: BigInt(version), eventIndex: eventIndex! } }),
  };
}

function parseFields(text: string): [string | null, string | null, number | null] {
  let fields: unknown;
  try {
    fields = JSON.parse(text.slice(POSITION_PREFIX.length));
  } catch (err) {
    throw new StreamdError(`Invalid reduction cursor: ${text}`, err);
  }
  if (Array.isArray(fields) && fields.length === 3) {
    const [cursor, version, eventIndex] = fields as unknown[];
    const hasLast =
      typeof version === "string" && UNSIGNED_INTEGER.test(version) && Number.isSafeInteger(eventIndex) && (eventIndex as number) >= 0;
    const noLast = version === null && eventIndex === null;
    if ((typeof cursor === "string" || (cursor === null && hasLast)) && (hasLast || noLast)) {
      return fields as [string | null, string | null, number | null];
    }
  }
  throw new StreamdError(`Invalid reduction cursor: ${text}`);
}

/** `client.reduce()`: fold the history, then follow live from where it ended. */
export function startReduction<S, TData>(
  host: ReduceHost,
  descriptor: EventDescriptor<TData>,
  reducer: Reducer<S, TData>,
  initial: S,
  options: ReduceOptions<S, TData> = {},
): Reduction<S> {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported reduce option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }

  const resumed = options.cursor === undefined ? undefined : parsePosition(options.cursor);
  let state = initial;
  let last: Folded | undefined = resumed?.last;
  let live: Subscription | undefined;
  let stopped = false;
  const backfill = new AbortController();
  const report = (err: Error) => (options.onError ?? host.onError)?.(err);
  const decode = options.decode ?? host.defaults.decode;
  const pageSize = options.batchSize ?? host.defaults.batchSize;

  /** `reason` is what `ready` rejects with if the backfill has not finished. */
  const stop = (reason: unknown = new StreamdError("Reduction stopped before its backfill finished")) => {
    if (stopped) return;
    stopped = true;
    detachSignal();
    untrack();
    backfill.abort(reason);
    live?.unsubscribe();
  };
  const untrack = host.track(stop);
  const detachSignal = onAbort(options.signal, () => stop(options.signal!.reason));
  if (options.signal?.aborted) stop(options.signal.reason);

  /** Folds `event`, or stops and reports when the reducer throws. */
  const fold = (event: StreamdEvent): boolean => {
    try {
      state = reducer(state, event as StreamdEvent<TData>);
    } catch (err) {
      const error = new StreamdError(`Reducer threw for ${eventId(event)}`, err);
      stop(error);
      report(error);
      return false;
    }
    last = event;
    return true;
  };

  // Without a cursor, follow from the last folded version; `after` skips what
  // was already folded there or, on a resume, earlier in the cursor's page.
  const follow = (cursor: string | undefined, after: Folded | undefined) => {
    const start = cursor !== undefined ? { cursor } : { fromVersion: last!.version };
    live = host.subscribe(
      descriptor,
      (event) => {
        if (after !== undefined && !isAfter(event, after)) return;
        if (fold(event)) options.onChange?.(state, event as StreamdEvent<TData>);
      },
      {
        ...pick(options, LIVE_KEYS),
        ...start,
        where: options.where,
        decode,
        onError: (err) => {
          if (!live?.active) stop();
          report(err);
        },
      },
    );
  };

  const run = async (): Promise<S> => {
    if (stopped) throw backfill.signal.reason;
    if (resumed !== undefined) {
      follow(resumed.cursor, resumed.last);
      return state;
    }
    let cursor: string | undefined;
    const filter: EventFilter = {
      ...pick(options, HISTORY_KEYS),
      where: options.where,
      decode,
      signal: backfill.signal,
      onCursor: (c) => (cursor = c),
    };
    try {
      for await (const event of host.query.iterateEvents(descriptor, filter, pageSize)) {
        if (!fold(event)) break;
      }
    } catch (err) {
      if (!stopped) {
        stop(err);
        report(err as Error);
      }
    }
    if (!stopped && cursor === undefined && last === undefined) {
      const error = new StreamdError(
        "Nothing to follow from: the backfill folded no events and the adapter reported no cursor",
      );
      stop(error);
      report(error);
    }
    if (stopped) throw backfill.signal.reason;
    follow(cursor, cursor === undefined ? last : undefined);
    return state;
  };

  const ready = run();
  ready.catch(() => {});

  return {
    get value() {
      return state;
    },
    get cursor() {
      return live === undefined ? undefined : encodePosition(live.cursor, last);
    },
    ready,
    get active() {
      return !stopped && live?.active !== false;
    },
    unsubscribe: () => stop(),
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: () => stop(),
  } as Reduction<S>;
}
