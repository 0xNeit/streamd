import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { EventDescriptor, EventFilter, EventOf, StreamdClient } from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** `getEvents` options, minus `signal`: the hook aborts on unmount and on input change. */
export interface UseLogsOptions extends Omit<EventFilter, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
}

export interface UseLogsResult<TData> {
  /** The last successful result; kept through later refetches and errors. */
  data: EventOf<TData>[] | undefined;
  /** The last request's failure, cleared by the next success. */
  error: Error | undefined;
  /** True while a request is in flight. */
  loading: boolean;
  /**
   * Runs the query again for the current inputs, aborting any request in flight. The same
   * function on every render. Resolves when its request settles, never rejecting; with no
   * descriptor, after unmount or during server rendering it does nothing.
   */
  refetch: () => Promise<void>;
}

type LogsState<TData> = Omit<UseLogsResult<TData>, "refetch">;

/**
 * `client.getEvents(descriptor, options)` for a component: runs after mount,
 * again whenever the descriptor or options change by value or `refetch` is
 * called, aborting the request it replaces. A `null` or `undefined` descriptor
 * fetches nothing.
 */
export function useLogs<TData = unknown>(
  descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[] | null | undefined,
  options: UseLogsOptions = {},
): UseLogsResult<TData> {
  const { client: explicit, ...filter } = options;
  const client = useStreamdClient(explicit);
  const latest = useRef(filter);
  latest.current = filter;
  const start = useRef<(() => Promise<void>) | undefined>(undefined);
  const key = valueKey([descriptor, filter]);
  const [state, setState] = useState<LogsState<TData>>({
    data: undefined,
    error: undefined,
    loading: descriptor != null,
  });

  useEffect(() => {
    if (descriptor == null) {
      setState((s) => (s.loading ? { ...s, loading: false } : s));
      return;
    }
    let controller: AbortController | undefined;
    const run = () => {
      controller?.abort();
      const current = (controller = new AbortController());
      setState((s) => (s.loading ? s : { ...s, loading: true }));
      const live = { ...withLatestFunctions(filter, () => latest.current), signal: current.signal };
      return client.getEvents<TData>(descriptor, live).then(
        (data) => {
          if (!current.signal.aborted) setState({ data, error: undefined, loading: false });
        },
        (err: unknown) => {
          if (!current.signal.aborted) {
            setState((s) => ({ data: s.data, error: toError(err), loading: false }));
          }
        },
      );
    };
    start.current = run;
    void run();
    return () => {
      start.current = undefined;
      controller?.abort();
    };
    // `key` stands for `descriptor` and `filter` by value.
  }, [client, key]);

  const refetch = useCallback(() => start.current?.() ?? Promise.resolve(), []);
  return useMemo(() => ({ ...state, refetch }), [state, refetch]);
}
