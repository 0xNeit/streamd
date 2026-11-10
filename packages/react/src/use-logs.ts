import { useEffect, useRef, useState } from "react";
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
}

/**
 * `client.getEvents(descriptor, options)` for a component: runs after mount,
 * again whenever the descriptor or options change by value, aborting the
 * request it replaces. A `null` or `undefined` descriptor fetches nothing.
 */
export function useLogs<TData = unknown>(
  descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[] | null | undefined,
  options: UseLogsOptions = {},
): UseLogsResult<TData> {
  const { client: explicit, ...filter } = options;
  const client = useStreamdClient(explicit);
  const latest = useRef(filter);
  latest.current = filter;
  const key = valueKey([descriptor, filter]);
  const [state, setState] = useState<UseLogsResult<TData>>({
    data: undefined,
    error: undefined,
    loading: descriptor != null,
  });

  useEffect(() => {
    if (descriptor == null) {
      setState((s) => (s.loading ? { ...s, loading: false } : s));
      return;
    }
    const controller = new AbortController();
    setState((s) => (s.loading ? s : { ...s, loading: true }));
    const live = { ...withLatestFunctions(filter, () => latest.current), signal: controller.signal };
    client.getEvents<TData>(descriptor, live).then(
      (data) => {
        if (!controller.signal.aborted) setState({ data, error: undefined, loading: false });
      },
      (err: unknown) => {
        if (!controller.signal.aborted) {
          setState((s) => ({ data: s.data, error: toError(err), loading: false }));
        }
      },
    );
    return () => controller.abort();
    // `key` stands for `descriptor` and `filter` by value.
  }, [client, key]);

  return state;
}
