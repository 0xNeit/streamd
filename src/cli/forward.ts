import { readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import type { StreamdClient } from "../client.js";
import { parseRetryAfter } from "../errors.js";
import { serializeEvent } from "../serialize.js";
import type { Subscription } from "../types.js";
import type { ForwardOptions } from "./args.js";
import { message } from "./format.js";

export interface ForwardIO {
  client: Pick<StreamdClient, "onBatch">;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting abandons any in-flight POST and ends the forwarder with exit code 0. */
  signal: AbortSignal;
  /** First retry wait in ms, doubling per attempt (default 1000). */
  retryDelay?: number;
}

const MAX_RETRY_DELAY = 60_000;

function readCursor(path: string): string | undefined {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw err;
  }
  const cursor = text.trim();
  if (cursor === "") throw new Error(`cursor file is empty: ${path}`);
  return cursor;
}

// A rename replaces the file whole, so a crash mid-write never leaves a torn cursor.
function writeCursor(path: string, cursor: string): void {
  const temp = `${path}.${process.pid}.tmp`;
  // Exclusive create: a link planted at the predictable name fails the write
  // instead of redirecting it. A stale temp file from this pid is ours to drop.
  rmSync(temp, { force: true });
  writeFileSync(temp, `${cursor}\n`, { flag: "wx", mode: 0o600 });
  renameSync(temp, path);
}

interface Attempt {
  ok: boolean;
  failure?: string;
  retryable?: boolean;
  retryAfter?: number;
}

async function post(body: string, options: ForwardOptions, signal: AbortSignal): Promise<Attempt> {
  let response: Response;
  try {
    response = await fetch(options.to, {
      method: "POST",
      headers: { "content-type": "application/json", ...options.headers },
      body,
      // Not following redirects keeps --header values from reaching another host.
      redirect: "manual",
      signal: AbortSignal.any([signal, AbortSignal.timeout(options.timeout)]),
    });
  } catch (err) {
    // Only the error's own message: a cause can carry the request URL.
    const failure =
      err instanceof Error && err.name === "TimeoutError"
        ? `request timed out after ${options.timeout}ms`
        : err instanceof Error
          ? err.message
          : String(err);
    return { ok: false, failure, retryable: true };
  }
  await response.body?.cancel().catch(() => {});
  const { status } = response;
  if (response.ok) return { ok: true };
  return {
    ok: false,
    failure: `HTTP ${status}`,
    retryable: status === 408 || status === 429 || status >= 500,
    retryAfter:
      status === 429 || status === 503
        ? parseRetryAfter(response.headers.get("retry-after"))
        : undefined,
  };
}

/** POSTs `body` until a 2xx, the retry bound, or abort; true once acknowledged. */
async function deliver(body: string, options: ForwardOptions, io: ForwardIO): Promise<boolean> {
  let delay = io.retryDelay ?? 1000;
  for (let attempt = 1; ; attempt++) {
    const result = await post(body, options, io.signal);
    if (result.ok) return true;
    if (io.signal.aborted) return false;
    if (!result.retryable || attempt > options.retries) {
      io.error(`streamd: delivery failed: ${result.failure}`);
      return false;
    }
    const wait = Math.min(result.retryAfter ?? delay, MAX_RETRY_DELAY);
    io.error(
      `streamd: delivery failed (attempt ${attempt}/${options.retries + 1}): ${result.failure}; retrying in ${wait}ms`,
    );
    const slept = await sleep(wait, true, { signal: io.signal }).catch(() => false);
    if (!slept) return false;
    delay = Math.min(delay * 2, MAX_RETRY_DELAY);
  }
}

/**
 * Follows `options.descriptor`, POSTing each batch to the webhook, and
 * resolves to the process exit code: 0 when the signal aborts, 1 when a
 * delivery fails past its retries, polling gives up, or the cursor file
 * cannot be read or written.
 *
 * Delivery is at least once: the cursor file only ever holds a position
 * whose events the webhook has acknowledged.
 */
export async function runForward(options: ForwardOptions, io: ForwardIO): Promise<number> {
  let saved: string | undefined;
  try {
    saved = readCursor(options.cursorFile);
  } catch (err) {
    io.error(`streamd: cannot read cursor file: ${message(err)}`);
    return 1;
  }
  if (io.signal.aborted) return 0;

  return new Promise<number>((resolve) => {
    let sub: Subscription | undefined;
    let done = false;

    // The subscription's cursor only moves past a page once its batch callback
    // returns, and every failure path stops the subscription first, so it is
    // safe to persist at any moment.
    const checkpoint = (): boolean => {
      const cursor = sub?.cursor;
      if (cursor === undefined || cursor === saved) return true;
      try {
        writeCursor(options.cursorFile, cursor);
        saved = cursor;
        return true;
      } catch (err) {
        io.error(`streamd: cannot write cursor file: ${message(err)}`);
        return false;
      }
    };
    const finish = (code: number) => {
      if (done) return;
      done = true;
      sub?.unsubscribe();
      io.signal.removeEventListener("abort", onAbort);
      resolve(checkpoint() ? code : 1);
    };
    const onAbort = () => finish(0);
    io.signal.addEventListener("abort", onAbort, { once: true });

    const { fromVersion, pollInterval } = options;
    sub = io.client.onBatch(
      options.descriptor,
      async (events) => {
        let delivered = false;
        try {
          delivered = await deliver(`[${events.map(serializeEvent).join(",")}]`, options, io);
        } catch (err) {
          io.error(`streamd: ${message(err)}`);
        } finally {
          // Stopping before returning, even on a throw, keeps the poller's cursor
          // on this undelivered page.
          if (!delivered) finish(io.signal.aborted ? 0 : 1);
        }
        if (!delivered) return;
        // The poller moves its cursor past the page once this callback's promise
        // settles, on a microtask; a timer runs after that.
        setTimeout(() => {
          if (!done && !checkpoint()) finish(1);
        }, 0);
      },
      {
        ...(saved !== undefined && { cursor: saved }),
        ...(fromVersion !== undefined && { fromVersion }),
        ...(pollInterval !== undefined && { pollInterval }),
        onError: (err) => {
          io.error(`streamd: ${message(err)}`);
          if (!sub?.active) finish(1);
        },
      },
    );
  });
}
