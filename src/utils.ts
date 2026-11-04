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
