import { StreamdError } from "./errors.js";
import { MISSING, parseFieldPath, readField, toInteger, type FieldPath } from "./filter.js";
import type { EventOf, Reducer } from "./types.js";

/**
 * A reducer carrying the value it starts from. Pass both to `reduce()` or
 * `useReduction()` as `reduce(d, fold, fold.initial)`; the `initial` slot stays
 * the caller's, since that is where a saved value goes on resume.
 */
export type Fold<S, TData = unknown> = Reducer<S, TData> & { readonly initial: S };

function fold<S, TData>(reducer: Reducer<S, TData>, initial: S): Fold<S, TData> {
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
  const segments = parseFieldPath(path);
  return fold((total, event) => {
    const value = readField(event.data, segments);
    if (value === MISSING || value == null) return total;
    const n = toInteger(value);
    if (n === undefined) throw new StreamdError(`sum: field ${JSON.stringify(path)} is not an integer`);
    return total + n;
  }, 0n);
}

/** The most recently folded event, or `undefined` before the first. */
export function latest<TData = unknown>(): Fold<EventOf<TData> | undefined, TData> {
  return fold<EventOf<TData> | undefined, TData>((_, event) => event, undefined);
}

/**
 * `inner` folded per key, the value at `path` in `event.data` as a string
 * (as reported: addresses are not padded, so `0x0a` and `0x000a` stay apart).
 * A missing or `null` field skips the event; an object or array throws.
 */
export function groupBy<S, TData = unknown>(
  path: FieldPath,
  inner: Fold<S, TData>,
): Fold<Map<string, S>, TData> {
  const segments = parseFieldPath(path);
  return fold<Map<string, S>, TData>((groups, event) => {
    const value = readField(event.data, segments);
    if (value === MISSING || value == null) return groups;
    if (typeof value === "object") {
      throw new StreamdError(`groupBy: field ${JSON.stringify(path)} is not a string, number or boolean`);
    }
    const key = String(value);
    const before = groups.has(key) ? groups.get(key)! : inner.initial;
    // ponytail: copies the key set per event so `initial` and earlier values stay intact; a persistent map if key counts grow large.
    return new Map(groups).set(key, inner(before, event));
  }, new Map());
}
