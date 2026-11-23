import { eventId } from "./cursor.js";
import { StreamdError } from "./errors.js";
import { MISSING, parseFieldPath, readField, toInteger, type FieldPath } from "./filter.js";
import type { Reducer, StreamdEvent } from "./types.js";

/**
 * A reducer carrying the value it starts from. Pass both to `reduce()` or
 * `useReduction()` as `reduce(d, fold, fold.initial)`; the `initial` slot stays
 * the caller's, since that is where a saved value goes on resume.
 */
export type Fold<S, TData = unknown, TEvent = StreamdEvent<TData>> = Reducer<S, TData, TEvent> & {
  readonly initial: S;
};

function fold<S, TData, TEvent = StreamdEvent<TData>>(
  reducer: Reducer<S, TData, TEvent>,
  initial: S,
): Fold<S, TData, TEvent> {
  return Object.assign(reducer, { initial });
}

/** The number of events folded, from `0n`. */
export function count(): Fold<bigint> {
  return fold((n) => n + 1n, 0n);
}

/**
 * The sum of the integer at `path` in `event.data`, from `0n`, at full precision
 * whether it arrives decoded, as a decimal string, or as a safe-integer number.
 * A missing or `null` field adds nothing; any other non-integer throws, which
 * stops the reduction rather than leave the total silently wrong.
 */
export function sum(path: FieldPath): Fold<bigint> {
  const read = integerReader("sum", path);
  return fold((total, event) => total + (read(event) ?? 0n), 0n);
}

/** The most recently folded event, or `undefined` before the first. */
export function latest<TData = unknown, TEvent = StreamdEvent<TData>>(): Fold<
  TEvent | undefined,
  TData,
  TEvent
> {
  return fold<TEvent | undefined, TData, TEvent>((_, event) => event, undefined);
}

/**
 * `inner` folded per key, the value at `path` in `event.data` as a string
 * (as reported: addresses are not padded, so `0x0a` and `0x000a` stay apart).
 * A missing or `null` field skips the event; an object or array throws.
 */
export function groupBy<S, TData = unknown, TEvent = StreamdEvent<TData>>(
  path: FieldPath,
  inner: Fold<S, TData, TEvent>,
): Fold<Map<string, S>, TData, TEvent> {
  const read = keyReader("groupBy", path);
  return fold<Map<string, S>, any, any>((groups, event) => {
    const key = read(event);
    if (key === undefined) return groups;
    const before = groups.has(key) ? groups.get(key)! : inner.initial;
    // ponytail: copies the key set per event so `initial` and earlier values stay intact; a persistent map if key counts grow large.
    return new Map(groups).set(key, inner(before, event));
  }, new Map());
}

/** The key `groupBy` and `distinct` take from `path`, or `undefined` for a missing or `null` field. */
function keyReader(name: string, path: FieldPath): (event: StreamdEvent) => string | undefined {
  const segments = parseFieldPath(path);
  return (event) => {
    const value = readField(event.data, segments);
    if (value === MISSING || value == null) return undefined;
    if (typeof value === "object") {
      throw new StreamdError(`${name}: field ${JSON.stringify(path)} is not a string, number or boolean`);
    }
    return String(value);
  };
}

function integerReader(name: string, path: FieldPath): (event: StreamdEvent) => bigint | undefined {
  const segments = parseFieldPath(path);
  return (event) => {
    const value = readField(event.data, segments);
    if (value === MISSING || value == null) return undefined;
    const n = toInteger(value);
    if (n === undefined) throw new StreamdError(`${name}: field ${JSON.stringify(path)} is not an integer`);
    return n;
  };
}

/**
 * The smallest integer at `path` in `event.data`, `undefined` before the first,
 * read as `sum` reads it: a missing or `null` field is passed over, any other
 * non-integer throws.
 */
export function min(path: FieldPath): Fold<bigint | undefined> {
  const read = integerReader("min", path);
  return fold<bigint | undefined, unknown>((least, event) => {
    const n = read(event);
    return n !== undefined && (least === undefined || n < least) ? n : least;
  }, undefined);
}

/** The largest integer at `path` in `event.data`, read as `min` reads it. */
export function max(path: FieldPath): Fold<bigint | undefined> {
  const read = integerReader("max", path);
  return fold<bigint | undefined, unknown>((most, event) => {
    const n = read(event);
    return n !== undefined && (most === undefined || n > most) ? n : most;
  }, undefined);
}

/** What `distinct` keeps: the values seen, and whether `max` stopped it adding more. */
export interface Distinct {
  readonly values: ReadonlySet<string>;
  readonly capped: boolean;
}

const DEFAULT_DISTINCT_MAX = 10_000;

/**
 * The distinct values at `path` in `event.data`, keyed as `groupBy` keys them.
 * At most `max` (default 10,000) are kept; past that new values are not added
 * and `capped` turns true, so `values.size` is then a lower bound.
 */
export function distinct(path: FieldPath, options: { max?: number } = {}): Fold<Distinct> {
  const limit = options.max ?? DEFAULT_DISTINCT_MAX;
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new StreamdError(`distinct: max must be a positive safe integer: ${String(options.max)}`);
  }
  const read = keyReader("distinct", path);
  return fold<Distinct, unknown>((state, event) => {
    const key = read(event);
    if (key === undefined || state.values.has(key)) return state;
    if (state.values.size >= limit) return state.capped ? state : { values: state.values, capped: true };
    // ponytail: copies the set per new value so earlier states stay intact; fine at the default cap.
    return { values: new Set(state.values).add(key), capped: state.capped };
  }, { values: new Set<string>(), capped: false });
}

/**
 * `inner` folded per window of `interval` microseconds of `event.timestamp`,
 * keyed by the window's start, aligned to the Unix epoch. An event without a
 * timestamp throws. `keep` holds at most that many windows, dropping the
 * earliest first; omitted, the map gains one entry per window seen.
 */
export function buckets<S, TData = unknown, TEvent = StreamdEvent<TData>>(
  interval: bigint,
  inner: Fold<S, TData, TEvent>,
  options: { keep?: number } = {},
): Fold<Map<bigint, S>, TData, TEvent> {
  if (typeof interval !== "bigint" || interval <= 0n) {
    throw new StreamdError(`buckets: interval must be a positive bigint of microseconds: ${String(interval)}`);
  }
  const { keep } = options;
  if (keep !== undefined && (!Number.isSafeInteger(keep) || keep < 1)) {
    throw new StreamdError(`buckets: keep must be a positive safe integer: ${String(keep)}`);
  }
  return fold<Map<bigint, S>, any, any>((windows, event: StreamdEvent) => {
    if (event.timestamp === undefined) {
      throw new StreamdError(`buckets: event ${eventId(event)} has no timestamp`);
    }
    const offset = event.timestamp % interval;
    const start = event.timestamp - (offset < 0n ? offset + interval : offset);
    const before = windows.has(start) ? windows.get(start)! : inner.initial;
    const next = new Map(windows).set(start, inner(before, event as any));
    while (keep !== undefined && next.size > keep) {
      let earliest = start;
      for (const key of next.keys()) if (key < earliest) earliest = key;
      next.delete(earliest);
    }
    return next;
  }, new Map());
}
