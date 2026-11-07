export { serializeEvent as formatEvent } from "@streamd/core";

/** The error's message, plus its cause's when the message does not already carry it. */
export function message(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const cause = (err as { cause?: unknown }).cause;
  return cause instanceof Error && !err.message.includes(cause.message)
    ? `${err.message}: ${cause.message}`
    : err.message;
}
