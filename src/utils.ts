import type { EventAdapter, EventDescriptor } from "./types.js";
import { StreamdError } from "./errors.js";
import { normalizeAddress } from "./filter.js";

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
    return adapter.getEarliestVersion(descriptor);
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
  const start = (head ?? (await adapter.getHeadVersion!(descriptor))) + fromVersion;
  return start < 0n ? 0n : start;
}

function typeSet(types: unknown): unknown[] {
  return Array.isArray(types) ? [...new Set(types)].sort() : [types];
}

/**
 * Value key for a descriptor: equal for descriptors that select the same
 * events, undefined for anything that is not one of the three kinds.
 */
export function descriptorKey(d: EventDescriptor | undefined): string | undefined {
  switch (d?.kind) {
    case "module":
      return JSON.stringify(["module", typeSet(d.eventType)]);
    case "handle":
      return JSON.stringify([
        "handle",
        normalizeAddress(d.accountAddress) ?? d.accountAddress,
        d.eventHandleStruct,
        d.fieldName,
      ]);
    case "account":
      return JSON.stringify([
        "account",
        normalizeAddress(d.accountAddress) ?? d.accountAddress,
        d.eventTypes === undefined ? null : typeSet(d.eventTypes),
      ]);
    default:
      return undefined;
  }
}
