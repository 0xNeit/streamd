import type { StreamdEvent } from "./types.js";

export class StreamdError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "StreamdError";
  }
}

export class StreamdNetworkError extends StreamdError {
  constructor(
    message: string,
    public readonly statusCode?: number,
    cause?: unknown,
    /**
     * How long the source asked callers to wait before retrying, in ms, from
     * its `Retry-After` header (see `parseRetryAfter`). Pollers and query
     * `retries` wait this long, capped, instead of their own backoff.
     */
    public readonly retryAfter?: number,
  ) {
    super(message, cause);
    this.name = "StreamdNetworkError";
  }
}

/**
 * A `Retry-After` header value (RFC 9110: delta-seconds or an HTTP-date) in
 * ms from `now`, for adapters to pass as `StreamdNetworkError.retryAfter`.
 * Undefined when absent or malformed; a date already past gives 0.
 */
export function parseRetryAfter(
  value: string | null | undefined,
  now: number = Date.now(),
): number | undefined {
  const text = value?.trim() ?? "";
  if (/^\d+$/.test(text)) return Number(text) * 1000;
  // Date.parse is lenient enough to read "1.5" as a date; every HTTP-date
  // format starts with a day name.
  if (!/^[A-Za-z]{3,9},? /.test(text)) return undefined;
  const at = Date.parse(text);
  return Number.isNaN(at) ? undefined : Math.max(0, at - now);
}

/** A thrown value's usable retry hint: adapters are a trust boundary. */
export function retryAfterHint(err: unknown): number | undefined {
  if (!(err instanceof StreamdNetworkError)) return undefined;
  const hint = err.retryAfter;
  return typeof hint === "number" && Number.isFinite(hint) && hint >= 0 ? hint : undefined;
}

export class StreamdPollingError extends StreamdError {
  constructor(
    message: string,
    public readonly consecutiveFailures: number,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = "StreamdPollingError";
  }
}

export class StreamdTimeoutError extends StreamdError {
  constructor(
    message: string,
    public readonly timeout: number,
    cause?: unknown,
  ) {
    super(message, cause);
    this.name = "StreamdTimeoutError";
  }
}

/**
 * A transaction that failed, so the events it would have emitted were
 * discarded. `transactionHash` is undefined for a failed simulation.
 */
export class StreamdTransactionError extends StreamdError {
  constructor(
    message: string,
    public readonly transactionHash: string | undefined,
    public readonly version: bigint,
    public readonly vmStatus: string,
  ) {
    super(message);
    this.name = "StreamdTransactionError";
  }
}

/**
 * An `expectEvents` assertion that did not hold. `events` is every event the
 * transaction emitted, before `decode`, and the message lists them.
 */
export class StreamdAssertionError extends StreamdError {
  constructor(
    message: string,
    /** `undefined` for a simulation, whose committed hash is not known. */
    public readonly transactionHash: string | undefined,
    public readonly events: StreamdEvent[],
  ) {
    super(message);
    this.name = "StreamdAssertionError";
  }
}
