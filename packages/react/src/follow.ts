import { useEffect, useRef, useState } from "react";
import {
  StreamdError,
  type StreamdEvent,
  type Subscription,
  type SubscriptionOptions,
} from "@streamd/core";
import { withLatestFunctions } from "./key.js";

export const DEFAULT_MAX_EVENTS = 100;

/** `idle` before mount, on the server, or with nothing to follow; `stopped` once polling gives up. */
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
 * Runs `start` while the component is mounted and `enabled`, restarting with
 * an empty list only when `key` changes, and keeps the latest `maxEvents`.
 */
export function useFollowing<TData, TOptions extends Omit<SubscriptionOptions, "signal">>(
  enabled: boolean,
  key: string,
  deps: unknown,
  listener: TOptions,
  maxEvents: number,
  start: (deliver: (events: StreamdEvent<TData>[]) => void, options: TOptions) => Subscription,
): UseEventsResult<TData> {
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1) {
    throw new StreamdError(`Invalid maxEvents: ${String(maxEvents)} (expected a positive integer)`);
  }
  const latest = useRef({ listener, maxEvents });
  latest.current = { listener, maxEvents };
  const [state, setState] = useState<UseEventsResult<TData>>(IDLE);

  useEffect(() => {
    if (!enabled) {
      setState(IDLE);
      return;
    }
    let current = true;
    let sub: Subscription | undefined;
    const opts = withLatestFunctions(listener, () => latest.current.listener);
    setState({ events: [], error: undefined, status: "live" });
    sub = start(
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
        onError: (err: Error) => {
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
    // `key` stands for the inputs by value.
  }, [deps, key, enabled]);

  return state;
}
