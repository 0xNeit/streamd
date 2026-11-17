import { StreamdError } from "./errors.js";
import { RequestLimiter } from "./limiter.js";
import type { EventPredicate, StreamdEvent } from "./types.js";

const DEFAULT_MAX_CONCURRENT = 4;
// Entries are per (object, version), so overflowing only costs a repeat read.
const CACHE_SIZE = 1000;

export interface OwnedByOptions {
  fields?: string[];
  maxConcurrentRequests?: number;
}

export interface OwnedByHost {
  defaultFields: readonly string[];
  resolveOwner: () => Promise<string>;
  namedObjects: (data: unknown, fields: readonly string[]) => string[];
  checkOwns: (limiter: RequestLimiter, object: string, wallet: string, version: bigint) => Promise<boolean>;
}

/**
 * Calls `resolve` once, on first use, and retries a failed lookup on the
 * next call; a pre-resolved `value` returns at once without calling `resolve`.
 */
export function lazyResolve<T>(value: T | undefined, resolve: () => Promise<T>): () => Promise<T> {
  if (value !== undefined) return () => Promise.resolve(value);
  let pending: Promise<T> | undefined;
  return () => {
    if (pending !== undefined) return pending;
    const next = resolve();
    next.catch(() => pending === next && (pending = undefined));
    return (pending = next);
  };
}

/** Shared `ownedBy` predicate builder for chain packages: validates options and caches `(object, version)` checks. */
export function ownedByPredicate(host: OwnedByHost, options: OwnedByOptions = {}): EventPredicate {
  const fields = options.fields ?? host.defaultFields;
  if (!Array.isArray(fields) || fields.length === 0 || fields.some((f) => typeof f !== "string" || f === "")) {
    throw new StreamdError("ownedBy fields must be a non-empty array of field names");
  }
  const max = options.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT;
  if (!Number.isInteger(max) || max < 1) {
    throw new StreamdError(`Invalid maxConcurrentRequests: ${String(max)} (expected a positive integer)`);
  }
  const limiter = new RequestLimiter(max);
  const cache = new Map<string, Promise<boolean>>();

  const owns = (object: string, version: bigint): Promise<boolean> => {
    const key = `${object}:${version}`;
    let answer = cache.get(key);
    if (answer !== undefined) {
      cache.delete(key);
    } else {
      answer = host.resolveOwner().then((wallet) => host.checkOwns(limiter, object, wallet, version));
      answer.catch(() => cache.get(key) === answer && cache.delete(key));
      if (cache.size >= CACHE_SIZE) cache.delete(cache.keys().next().value!);
    }
    cache.set(key, answer);
    return answer;
  };

  return async (event: StreamdEvent) => {
    for (const object of host.namedObjects(event.data, fields)) {
      if (await owns(object, event.version)) return true;
    }
    return false;
  };
}
