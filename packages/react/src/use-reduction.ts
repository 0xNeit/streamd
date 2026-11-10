import { useEffect, useRef, useState } from "react";
import type { EventDescriptor, ReduceOptions, Reducer, Reduction, StreamdClient } from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** `reduce` options, minus `signal`: the reduction ends on unmount and on input change. */
export interface UseReductionOptions<S, TData> extends Omit<ReduceOptions<S, TData>, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
}

/** `loading` until the backfill finishes; `stopped` once it fails, the reducer throws, or polling gives up. */
export type UseReductionStatus = "idle" | "loading" | "live" | "stopped";

export interface UseReductionResult<S> {
  /** `initial` until the backfill finishes, then the folded value. */
  value: S;
  /** The last error, cleared when the value next changes. */
  error: Error | undefined;
  status: UseReductionStatus;
}

/**
 * `client.reduce` for a component: restarts only when the descriptor, options
 * or `initial` change by value, and calls the latest `reducer` without
 * restarting. A `null` or `undefined` descriptor waits.
 */
export function useReduction<S, TData = unknown>(
  descriptor: EventDescriptor<TData> | null | undefined,
  reducer: Reducer<S, TData>,
  initial: S,
  options: UseReductionOptions<S, TData> = {},
): UseReductionResult<S> {
  const { client: explicit, ...reduce } = options;
  const client = useStreamdClient(explicit);
  const latest = useRef({ reducer, reduce });
  latest.current = { reducer, reduce };
  const key = valueKey([descriptor, reduce, initial]);
  const [state, setState] = useState<UseReductionResult<S>>({
    value: initial,
    error: undefined,
    status: "idle",
  });

  useEffect(() => {
    if (descriptor == null) {
      setState({ value: initial, error: undefined, status: "idle" });
      return;
    }
    let current = true;
    let reduction: Reduction<S> | undefined;
    const opts = withLatestFunctions(reduce, () => latest.current.reduce);
    setState({ value: initial, error: undefined, status: "loading" });
    reduction = client.reduce<S, TData>(
      descriptor,
      (s, event) => latest.current.reducer(s, event),
      initial,
      {
        ...opts,
        onChange: (value, event) => {
          opts.onChange?.(value, event);
          if (current) setState({ value, error: undefined, status: "live" });
        },
        onError: (err) => {
          opts.onError?.(err);
          if (!current) return;
          const stopped = reduction !== undefined && !reduction.active;
          setState((s) => ({ ...s, error: toError(err), status: stopped ? "stopped" : s.status }));
        },
      },
    );
    reduction.ready.then(
      (value) => {
        if (current) setState((s) => ({ ...s, value, status: "live" }));
      },
      () => {},
    );
    return () => {
      current = false;
      reduction.unsubscribe();
    };
    // `key` stands for `descriptor`, `reduce` and `initial` by value.
  }, [client, key]);

  return state;
}
