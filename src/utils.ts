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
