/**
 * A string equal for values that are equal by value: object keys in any order,
 * `bigint` distinct from numbers and strings, and functions keyed by presence
 * only, since hooks call the latest function through a ref.
 */
export function valueKey(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) => {
    if (typeof v === "bigint") return { $bigint: String(v) };
    if (typeof v === "function") return { $function: true };
    if (v !== null && typeof v === "object" && !Array.isArray(v)) {
      return Object.fromEntries(
        Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      );
    }
    return v;
  }) ?? "undefined";
}

/**
 * A copy of `options` whose function fields call whatever `latest()` holds at
 * call time, so a re-render with a new closure is seen without resubscribing.
 */
export function withLatestFunctions<T extends object>(options: T, latest: () => T): T {
  const out: Record<string, unknown> = { ...(options as Record<string, unknown>) };
  for (const [name, value] of Object.entries(options)) {
    if (typeof value === "function") {
      out[name] = (...args: unknown[]) =>
        (latest() as Record<string, (...a: unknown[]) => unknown>)[name]!(...args);
    }
  }
  return out as T;
}

export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}
