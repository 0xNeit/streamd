import { useEffect, useRef, useState } from "react";
import {
  StreamdError,
  type EventDescriptor,
  type StreamdClient,
  type StreamdEvent,
  type Subscription,
  type SubscriptionOptions,
} from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { valueKey, withLatestFunctions } from "./key.js";

const DEFAULT_MAX_EVENTS = 100;

/** `onBatch` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseEventsOptions extends Omit<SubscriptionOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent events to keep (default: 100). */
  maxEvents?: number;
}

/** `idle` before mount, on the server, or with no descriptor; `stopped` once polling gives up. */
export type UseEventsStatus = "idle" | "live" | "stopped";

export interface UseEventsResult<TData> {
  /** The most recent `maxEvents` delivered events, oldest first. */
  events: StreamdEvent<TData>[];
  /** The last polling or handler error, cleared when events next arrive. */
  error: Error | undefined;
  status: UseEventsStatus;
}

const IDLE: UseEventsResult<never> = { events: [], error: undefined, status: "idle" };

/**
 * Follows `descriptor` live with `client.onBatch` while the component is
 * mounted, resubscribing, with an empty list, only when the descriptor or
 * options change by value. Errors leave the events in place and the client's
 * backoff in charge; `options.onError` still runs, the client's does not.
 */
export function useEvents<TData = unknown>(
  descriptor: EventDescriptor<TData> | null | undefined,
  options: UseEventsOptions = {},
): UseEventsResult<TData> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options;
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1) {
    throw new StreamdError(`Invalid maxEvents: ${String(maxEvents)} (expected a positive integer)`);
  }
  const client = useStreamdClient(explicit);
  const latest = useRef({ listener, maxEvents });
  latest.current = { listener, maxEvents };
  const key = valueKey([descriptor, listener]);
  const [state, setState] = useState<UseEventsResult<TData>>(IDLE);

  useEffect(() => {
    if (descriptor == null) {
      setState(IDLE);
      return;
    }
    let current = true;
    let sub: Subscription | undefined;
    const opts = withLatestFunctions(listener, () => latest.current.listener);
    setState({ events: [], error: undefined, status: "live" });
    sub = client.onBatch<TData>(
      descriptor,
      (batch) => {
        if (!current) return;
        setState((s) => ({
          events: [...s.events, ...batch].slice(-latest.current.maxEvents),
          error: undefined,
          status: "live",
        }));
      },
      {
        ...opts,
        onError: (err) => {
          opts.onError?.(err);
          if (!current) return;
          const stopped = sub !== undefined && !sub.active && !sub.paused;
          setState((s) => ({ ...s, error: err, status: stopped ? "stopped" : "live" }));
        },
      },
    );
    return () => {
      current = false;
      sub.unsubscribe();
    };
    // `key` stands for `descriptor` and `listener` by value.
  }, [client, key]);

  return state;
}
