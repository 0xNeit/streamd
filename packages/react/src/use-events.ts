import {
  StreamdError,
  type EventDescriptor,
  type EventOf,
  type StreamdClient,
  type SubscriptionOptions,
} from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { DEFAULT_MAX_EVENTS, readThenFollow, useFollowing, type Following, type UseEventsResult } from "./follow.js";
import { valueKey } from "./key.js";

export type { UseEventsResult, UseEventsStatus } from "./follow.js";

/** `onBatch` options, minus `signal`: the subscription ends on unmount and on input change. */
export interface UseEventsOptions extends Omit<SubscriptionOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent events to keep (default: 100). */
  maxEvents?: number;
  /**
   * Show this many of the most recent matches between `fromVersion` or
   * `fromTime` (one is required) and the head in one update, then follow
   * live from where that read ended.
   */
  history?: number;
}

type Listener = Omit<UseEventsOptions, "client" | "maxEvents" | "history">;

/** Reads the newest `limit` matches of the window, delivers them oldest first, then follows from the read's end. */
function withHistory<TData>(
  client: StreamdClient,
  descriptor: EventDescriptor<TData>,
  limit: number,
  deliver: (events: EventOf<TData>[]) => void,
  options: Listener,
): Following {
  const { fromVersion, fromTime, ...following } = options;
  const { where, decode, withState, onError } = following;
  let cursor: string | undefined;
  return readThenFollow(
    "useEvents",
    (signal) =>
      client.getEvents<TData>(descriptor, {
        ...(fromVersion !== undefined && { fromVersion }),
        ...(fromTime !== undefined && { fromTime }),
        ...(where !== undefined && { where }),
        ...(decode !== undefined && { decode }),
        ...(withState !== undefined && { withState }),
        orderBy: "desc",
        limit,
        signal,
        // A descending read walks the whole window, so its end cursor is exact.
        onCursor: (c) => (cursor = c),
      }).then((events) => events.reverse()),
    () => {
      if (cursor === undefined) throw new StreamdError("The history read gave no cursor to follow from");
      return client.onBatch<TData>(descriptor, deliver, { ...following, cursor });
    },
    deliver,
    onError,
  );
}

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
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, history, ...listener } = options;
  if (history !== undefined) {
    if (!Number.isSafeInteger(history) || history < 1) {
      throw new StreamdError(`Invalid history: ${String(history)} (expected a positive integer)`);
    }
    if (listener.fromVersion === undefined && listener.fromTime === undefined) {
      throw new StreamdError("history needs fromVersion or fromTime, where the read starts");
    }
  }
  const client = useStreamdClient(explicit);
  return useFollowing<EventOf<TData>, typeof listener>(
    descriptor != null,
    valueKey([descriptor, listener, history ?? null]),
    client,
    listener,
    maxEvents,
    (deliver, opts) =>
      history === undefined
        ? client.onBatch<TData>(descriptor!, deliver, opts)
        : withHistory(client, descriptor!, history, deliver, opts),
    history === undefined ? "live" : "loading",
  );
}
