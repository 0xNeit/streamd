import { StreamdError, parseRetryAfter } from "./errors.js";
import { sleep } from "./utils.js";

/** The header `streamd forward --secret` signs each POST in. */
export const WEBHOOK_SIGNATURE_HEADER = "x-streamd-signature";

const SIGNATURE = /^sha256=([0-9a-fA-F]{64})$/;
const MAX_RETRY_DELAY = 60_000;
const encoder = new TextEncoder();

function subtleCrypto(name: string): SubtleCrypto {
  const subtle = globalThis.crypto?.subtle;
  if (subtle === undefined) {
    throw new StreamdError(`${name}: this runtime has no Web Crypto (globalThis.crypto.subtle)`);
  }
  return subtle;
}

/**
 * Whether `signature`, the `x-streamd-signature` header value, is a valid
 * HMAC-SHA256 of `body` under `secret`, as `streamd forward --secret` sends it.
 *
 * Pass the raw request body, before parsing: a string is encoded as UTF-8, and
 * re-serialised JSON will not match. The hex is accepted in either case. A
 * missing or malformed header is `false`; the comparison is Web Crypto's
 * `verify`, which runs in constant time. Throws `StreamdError` for an empty
 * secret, a body that is neither a string nor bytes, or no `crypto.subtle`.
 */
export async function verifyWebhook(
  body: string | Uint8Array,
  signature: string | null | undefined,
  secret: string,
): Promise<boolean> {
  if (typeof secret !== "string" || secret === "") {
    throw new StreamdError("verifyWebhook: secret must be a non-empty string");
  }
  if (typeof body !== "string" && !(body instanceof Uint8Array)) {
    throw new StreamdError("verifyWebhook: body must be the raw request body, a string or Uint8Array");
  }
  const subtle = subtleCrypto("verifyWebhook");
  const hex = typeof signature === "string" ? SIGNATURE.exec(signature)?.[1] : undefined;
  if (hex === undefined) return false;
  const mac = new Uint8Array(32);
  for (let i = 0; i < 32; i++) mac[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  const key = await subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
  // Copied, so a Buffer or a view of a shared buffer arrives as the ArrayBuffer-backed bytes Web Crypto takes.
  const data = typeof body === "string" ? encoder.encode(body) : new Uint8Array(body);
  return subtle.verify("HMAC", key, mac, data);
}

/** The `sha256=<hex>` HMAC of `body` under `secret`, the header value `verifyWebhook` checks. */
export async function signWebhook(body: string, secret: string): Promise<string> {
  if (typeof secret !== "string" || secret === "") {
    throw new StreamdError("signWebhook: secret must be a non-empty string");
  }
  const subtle = subtleCrypto("signWebhook");
  const key = await subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = new Uint8Array(await subtle.sign("HMAC", key, encoder.encode(body)));
  return `sha256=${Array.from(mac, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

/** One delivery attempt's outcome; `retryAfter` (ms) replaces the backoff for the next wait. */
export interface DeliveryAttempt {
  ok: boolean;
  failure?: string;
  retryable?: boolean;
  retryAfter?: number;
}

/**
 * Where a body is delivered. `send` makes one attempt and says whether it was
 * acknowledged; `deliver` owns the retries, so every sink shares the same
 * backoff and the same rule that nothing counts until acked.
 */
export interface DeliverySink {
  send(body: string, signal: AbortSignal): Promise<DeliveryAttempt>;
  close?(): unknown;
}

/** What a webhook POST reads: the URL, static headers, the signing secret and the per-request timeout in ms. */
export interface WebhookDelivery {
  to: string;
  headers?: Record<string, string>;
  secret?: string;
  timeout: number;
}

/** POSTs each body to `options.to` as JSON, signed when `secret` is set; a 2xx acknowledges. */
export function webhookSink(options: WebhookDelivery): DeliverySink {
  return { send: (body, signal) => post(body, options, signal) };
}

async function post(body: string, options: WebhookDelivery, signal: AbortSignal): Promise<DeliveryAttempt> {
  let response: Response;
  try {
    response = await fetch(options.to, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...options.headers,
        ...(options.secret !== undefined && { [WEBHOOK_SIGNATURE_HEADER]: await signWebhook(body, options.secret) }),
      },
      body,
      // Not following redirects keeps headers from reaching another host, and a target from pointing elsewhere.
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

/**
 * Sends `body` until the sink acknowledges it, `retries` retries have failed,
 * a failure is not retryable, or `io.signal` aborts; true once acknowledged.
 * Waits double from `retryDelay` (default 1000 ms), a failure's `retryAfter`
 * replacing the wait, both capped at 60 s. Each failure is reported to `io.error`.
 */
export async function deliver(
  body: string,
  sink: DeliverySink,
  retries: number,
  io: { error(message: string): void; signal: AbortSignal; retryDelay?: number },
): Promise<boolean> {
  let delay = io.retryDelay ?? 1000;
  for (let attempt = 1; ; attempt++) {
    const result = await sink.send(body, io.signal);
    if (result.ok) return true;
    if (io.signal.aborted) return false;
    if (!result.retryable || attempt > retries) {
      io.error(`streamd: delivery failed: ${result.failure}`);
      return false;
    }
    const wait = Math.min(result.retryAfter ?? delay, MAX_RETRY_DELAY);
    io.error(
      `streamd: delivery failed (attempt ${attempt}/${retries + 1}): ${result.failure}; retrying in ${wait}ms`,
    );
    const slept = await sleep(wait, io.signal).then(() => true, () => false);
    if (!slept) return false;
    delay = Math.min(delay * 2, MAX_RETRY_DELAY);
  }
}
