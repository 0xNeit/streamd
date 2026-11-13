import { useEffect, useRef, useState } from "react";
import {
  StreamdError,
  type EventOf,
  type Subscription,
  type SubscriptionOptions,
} from "@streamd/core";
import { withLatestFunctions } from "./key.js";

export const DEFAULT_MAX_EVENTS = 100;

/**
 * `idle` before mount, on the server, or with nothing to follow; `loading`
 * while a `history` read runs; `stopped` once polling gives up.
 */
export type UseEventsStatus = "idle" | "loading" | "live" | "stopped";

/** What the hook needs of a running subscription. */
export type Following = Pick<Subscription, "unsubscribe" | "active" | "paused">;

export interface UseEventsResult<TData> {
  /** The most recent `maxEvents` delivered events, oldest first. */
  events: EventOf<TData>[];
  /** The last polling or handler error, cleared when events next arrive. */
  error: Error | undefined;
  status: UseEventsStatus;
}

const IDLE: Followed<never> = { events: [], error: undefined, status: "idle" };

/** What `useFollowing` keeps: `UseEventsResult`'s shape over any item. */
export interface Followed<TItem> {
  events: TItem[];
  error: Error | undefined;
  status: UseEventsStatus;
}

/**
 * Runs `read`, then starts `follow` with what it returned and delivers that in
 * one update. Until `unsubscribe()`, which abandons either, it reads as active;
 * a failed read or a `follow` that throws goes to `onError` and leaves it
 * inactive.
 */
export function readThenFollow<TItem>(
  hook: string,
  read: (signal: AbortSignal) => Promise<TItem[]>,
  follow: (items: TItem[]) => Following,
  deliver: (items: TItem[]) => void,
  onError: ((err: Error) => void) | undefined,
): Following {
  const controller = new AbortController();
  let live: Following | undefined;
  let failed = false;
  let ended = false;
  const fail = (err: unknown) => {
    if (ended) return;
    failed = true;
    onError?.(err instanceof Error ? err : new StreamdError("History read failed", err));
  };
  read(controller.signal).then((items) => {
    // A read can settle in the same tick as an unmount, before its abort lands.
    if (ended) return;
    try {
      live = follow(items);
    } catch (err) {
      return fail(err);
    }
    // Following delivers on a later poll, so the read's items always land first.
    deliver(items);
  }, fail);
  return {
    unsubscribe() {
      ended = true;
      controller.abort(new StreamdError(`${hook} history read abandoned`));
      live?.unsubscribe();
    },
    get active() {
      return !ended && !failed && (live?.active ?? true);
    },
    get paused() {
      return live?.paused ?? false;
    },
  };
}

/**
 * Runs `start` while the component is mounted and `enabled`, restarting with
 * an empty list only when `key` changes, and keeps the latest `maxEvents`.
 */
export function useFollowing<TItem, TOptions extends Omit<SubscriptionOptions, "signal">>(
  enabled: boolean,
  key: string,
  deps: unknown,
  listener: TOptions,
  maxEvents: number,
  start: (deliver: (items: TItem[]) => void, options: TOptions) => Following,
  initial: UseEventsStatus = "live",
): Followed<TItem> {
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1) {
    throw new StreamdError(`Invalid maxEvents: ${String(maxEvents)} (expected a positive integer)`);
  }
  const latest = useRef({ listener, maxEvents });
  latest.current = { listener, maxEvents };
  const idle = IDLE as Followed<TItem>;
  const [state, setState] = useState(idle);

  useEffect(() => {
    if (!enabled) {
      setState(idle);
      return;
    }
    let current = true;
    let sub: Following | undefined;
    const opts = withLatestFunctions(listener, () => latest.current.listener);
    setState({ events: [], error: undefined, status: initial });
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
  }, [deps, key, enabled, initial]);

  return state;
}
