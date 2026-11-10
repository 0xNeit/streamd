import type { EventAdapter, EventDescriptor, FetchOptions } from "./types.js";
import { StreamdError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
import { handlePath, requireAddress, requireResourceType, requireVersion } from "./adapters/validate.js";

let nextId = 0;

export function generateId(): string {
  return `streamd_${++nextId}_${Date.now()}`;
}

/** Runs `handler` once if `signal` aborts; the returned function detaches it. */
export function onAbort(
  signal: AbortSignal | undefined,
  handler: () => void,
): () => void {
  if (signal === undefined) return () => {};
  signal.addEventListener("abort", handler, { once: true });
  return () => signal.removeEventListener("abort", handler);
}

/** Rejects with `signal.reason` as soon as `signal` aborts, without waiting for `promise`. */
export function raceAbort<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) {
    promise.catch(() => {});
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    const detach = onAbort(signal, () => reject(signal.reason));
    promise.then(
      (value) => {
        detach();
        resolve(value);
      },
      (err) => {
        detach();
        reject(err);
      },
    );
  });
}

/** Resolves after `ms`, or rejects with `signal.reason` as soon as it aborts. */
export function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason);
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      detach();
      resolve();
    }, ms);
    const detach = onAbort(signal, () => {
      clearTimeout(timer);
      reject(signal!.reason);
    });
  });
}

/** True when `fromVersion` needs no adapter read to become a start position. */
export function isAbsoluteVersion(
  fromVersion: bigint | "earliest" | undefined,
): fromVersion is bigint | undefined {
  return fromVersion === undefined || (typeof fromVersion === "bigint" && fromVersion >= 0n);
}

/**
 * A negative `fromVersion` counts back from the adapter's head, as ethers
 * reads a negative block tag, and `"earliest"` reads the adapter's oldest
 * servable version; other values pass through without a read. A `head`
 * already read for the same query is reused instead of read again.
 */
export async function resolveFromVersion(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  fromVersion: bigint | "earliest" | undefined,
  head?: bigint,
): Promise<bigint | undefined> {
  if (isAbsoluteVersion(fromVersion)) return fromVersion;
  if (fromVersion === "earliest") {
    if (adapter.getEarliestVersion === undefined) {
      throw new StreamdError(
        'fromVersion "earliest" needs an adapter that implements getEarliestVersion',
      );
    }
    return requireVersion("getEarliestVersion", await adapter.getEarliestVersion(descriptor));
  }
  if (typeof fromVersion !== "bigint") {
    throw new StreamdError(
      `Invalid fromVersion: ${String(fromVersion)} (expected a bigint or "earliest")`,
    );
  }
  if (head === undefined && adapter.getHeadVersion === undefined) {
    throw new StreamdError(
      "A negative fromVersion needs an adapter that implements getHeadVersion",
    );
  }
  const start =
    (head ?? requireVersion("getHeadVersion", await adapter.getHeadVersion!(descriptor))) +
    fromVersion;
  return start < 0n ? 0n : start;
}

function typeSet(types: unknown): unknown[] {
  return Array.isArray(types) ? [...new Set(types)].sort() : [types];
}

function isRefused(d: EventDescriptor): boolean {
  try {
    if (d.kind === "handle") handlePath(d);
    if (d.kind === "resource") {
      requireAddress(d.accountAddress);
      requireResourceType(d.resourceType);
    }
    return false;
  } catch {
    return true;
  }
}

/**
 * Value key for a descriptor: equal for descriptors that select the same
 * events, undefined for anything that is not one of the four kinds.
 */
export function descriptorKey(d: EventDescriptor | undefined): string | undefined {
  switch (d?.kind) {
    case "module":
      return JSON.stringify(["module", typeSet(d.eventType)]);
    case "handle": {
      const address = normalizeAddress(d.accountAddress) ?? d.accountAddress;
      return d.creationNumber !== undefined
        ? JSON.stringify(["handle", address, String(d.creationNumber)])
        : JSON.stringify(["handle", address, d.eventHandleStruct, d.fieldName]);
    }
    case "account":
      return JSON.stringify([
        "account",
        normalizeAddress(d.accountAddress) ?? d.accountAddress,
        d.eventTypes === undefined ? null : typeSet(d.eventTypes),
      ]);
    case "resource":
      return JSON.stringify([
        "resource",
        normalizeAddress(d.accountAddress) ?? d.accountAddress,
        d.resourceType,
      ]);
    default:
      return undefined;
  }
}

/**
 * Value key for one `fetchEvents` request: equal for the same descriptor and
 * the same defined options in any order, undefined when `d` is not a descriptor.
 * A handle or resource descriptor the adapters refuse has no key, so it never shares,
 * caches or replays the answer of a valid one it would otherwise collide with.
 */
export function fetchKey(d: EventDescriptor, options: FetchOptions): string | undefined {
  if (d == null || isRefused(d)) return undefined;
  const selector = descriptorKey(d);
  if (selector === undefined) return undefined;
  const fields = Object.entries(options)
    .filter(([, value]) => value !== undefined)
    .sort(([a], [b]) => a.localeCompare(b));
  const encoded = JSON.stringify(fields, (_key, value: unknown) =>
    typeof value === "bigint" ? `${value}n` : value,
  );
  return `${selector}|${encoded}`;
}

// Runtimes without explicit resource management lack the symbol. The registry
// key is the one core-js polyfills it with.
export const ASYNC_DISPOSE: typeof Symbol.asyncDispose =
  Symbol.asyncDispose ?? (Symbol.for("Symbol.asyncDispose") as typeof Symbol.asyncDispose);
