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

function clock(timestamp: bigint | undefined): string {
  if (timestamp === undefined) return "-";
  const date = new Date(Number(timestamp / 1000n));
  return Number.isNaN(date.getTime()) ? "-" : date.toISOString().slice(0, TIME_WIDTH).replace("T", " ");
}

function shortAddress(address: string | undefined): string {
  if (address === undefined) return "-";
  return address.length > SENDER_WIDTH ? `${address.slice(0, 6)}\u2026${address.slice(-4)}` : address;
}

/** One aligned line for a person watching a stream: UTC time, type, sender, payload. */
export function prettyEvent(event: StreamdEvent): string {
  const payload =
    JSON.stringify(event.data, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v)) ?? "null";
  return [
    clock(event.timestamp).padEnd(TIME_WIDTH),
    clip(event.type.replace(/^[^:]*::/, ""), TYPE_WIDTH).padEnd(TYPE_WIDTH),
    shortAddress(event.sender).padEnd(SENDER_WIDTH),
    clip(payload, PAYLOAD_WIDTH),
  ]
    .join("  ")
    .replace(CONTROL, "?");
}

/** The error's message, plus its cause's when the message does not already carry it. */
export function message(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const cause = (err as { cause?: unknown }).cause;
  return cause instanceof Error && !err.message.includes(cause.message)
    ? `${err.message}: ${cause.message}`
    : err.message;
}
