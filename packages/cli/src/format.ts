import { createHmac } from "node:crypto";
import type { StreamdEvent } from "@streamd/core";

export { serializeEvent as formatEvent } from "@streamd/core";

const TIME_WIDTH = 19;
const TYPE_WIDTH = 32;
const SENDER_WIDTH = 11;
const PAYLOAD_WIDTH = 80;
// JSON.stringify escapes C0 controls in strings but leaves C1, which some terminals obey.
const CONTROL = /[\u0000-\u001f\u007f-\u009f]/g;

function clip(text: string, width: number): string {
  return text.length > width ? `${text.slice(0, width - 1)}\u2026` : text;
}

/** A UTC time to the second, or `-` when there is none. */
export function clock(timestamp: bigint | undefined): string {
  if (timestamp === undefined) return "-";
  const date = new Date(Number(timestamp / 1000n));
  return Number.isNaN(date.getTime()) ? "-" : date.toISOString().slice(0, TIME_WIDTH).replace("T", " ");
}

function shortAddress(address: string | undefined): string {
  if (address === undefined) return "-";
  return address.length > SENDER_WIDTH ? `${address.slice(0, 6)}\u2026${address.slice(-4)}` : address;
}

/** `text` with every control character shown as `?`, safe to print to a terminal. */
export function printable(text: string): string {
  return text.replace(CONTROL, "?");
}

/** One aligned line for a person watching a stream: UTC time, type, sender, payload. */
export function prettyEvent(event: StreamdEvent): string {
  const payload =
    JSON.stringify(event.data, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v)) ?? "null";
  return printable(
    [
      clock(event.timestamp).padEnd(TIME_WIDTH),
      clip(event.type.replace(/^[^:]*::/, ""), TYPE_WIDTH).padEnd(TYPE_WIDTH),
      shortAddress(event.sender).padEnd(SENDER_WIDTH),
      clip(payload, PAYLOAD_WIDTH),
    ].join("  "),
  );
}

/** The error's message, plus its cause's when the message does not already carry it. */
export function message(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const cause = (err as { cause?: unknown }).cause;
  return cause instanceof Error && !err.message.includes(cause.message)
    ? `${err.message}: ${cause.message}`
    : err.message;
}

/** `url` with its password shown as `***` and its query string dropped, safe to print. */
export function redactUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.password !== "") parsed.password = "***";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return "the URL";
  }
}

/** Replaces `url` in a message with its redacted form, and each secret it carries with `***`. */
export function scrubber(url: string): (text: string) => string {
  const secrets = new Set<string>();
  try {
    const parsed = new URL(url);
    secrets.add(parsed.password).add(decodeURIComponent(parsed.password));
    for (const [key, value] of parsed.searchParams) {
      if (/pass/i.test(key)) secrets.add(value).add(encodeURIComponent(value));
    }
  } catch {
    // An unparsed URL, such as Kafka's list of brokers, still has its whole text replaced below, and its password here.
    const password = /^[a-z][a-z0-9+.-]*:\/\/[^:@/?#]*:([^@/?#]*)@/i.exec(url)?.[1];
    if (password !== undefined) {
      secrets.add(password);
      try {
        secrets.add(decodeURIComponent(password));
      } catch {
        // Kept raw only.
      }
    }
  }
  secrets.delete("");
  const redacted = redactUrl(url);
  return (text) => {
    let out = text.split(url).join(redacted);
    for (const secret of secrets) out = out.split(secret).join("***");
    return out;
  };
}

/** The `sha256=<hex>` HMAC of `body`, as the webhook header and a stream entry's field carry it. */
export function signature(body: string, secret: string): string {
  return `sha256=${createHmac("sha256", secret).update(body, "utf8").digest("hex")}`;
}
