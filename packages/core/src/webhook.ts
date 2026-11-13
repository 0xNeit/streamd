import { StreamdError } from "./errors.js";

/** The header `streamd forward --secret` signs each POST in. */
export const WEBHOOK_SIGNATURE_HEADER = "x-streamd-signature";

const SIGNATURE = /^sha256=([0-9a-fA-F]{64})$/;

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
  const subtle = globalThis.crypto?.subtle;
  if (subtle === undefined) {
    throw new StreamdError("verifyWebhook: this runtime has no Web Crypto (globalThis.crypto.subtle)");
  }
  const hex = typeof signature === "string" ? SIGNATURE.exec(signature)?.[1] : undefined;
  if (hex === undefined) return false;
  const mac = new Uint8Array(32);
  for (let i = 0; i < 32; i++) mac[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  const encoder = new TextEncoder();
  const key = await subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["verify"]);
  // Copied, so a Buffer or a view of a shared buffer arrives as the ArrayBuffer-backed bytes Web Crypto takes.
  const data = typeof body === "string" ? encoder.encode(body) : new Uint8Array(body);
  return subtle.verify("HMAC", key, mac, data);
}
