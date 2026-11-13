import { useEffect, useMemo, useRef, useState } from "react";
import { requireAddress, type EventOf, type StreamdClient, type Watch, type WatchOptions } from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { DEFAULT_MAX_EVENTS, useFollowing, type UseEventsStatus } from "./follow.js";
import { valueKey } from "./key.js";

/** `watch` options, minus `signal`: the watch ends on unmount and on input change. */
export interface UseWatchOptions extends Omit<WatchOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** How many of the most recent events to keep (default: 100). */
  maxEvents?: number;
}

/** One delivered event and the watched address whose walk delivered it, as the list first gave it. */
export interface WatchedEvent<TData> {
  event: EventOf<TData>;
  address: string;
}

export interface UseWatchResult<TData> {
  /** The most recent `maxEvents` deliveries, oldest first. */
  data: WatchedEvent<TData>[];
  /** The last polling or handler error, cleared when an event next arrives. */
  error: Error | undefined;
  /** `idle` with no list or before mount, else `live`: a watch never stops on its own. */
  status: UseEventsStatus;
  /** The addresses being watched, in the order added, without any whose walk gave up. */
  addresses: readonly string[];
}

const NONE: readonly string[] = [];

const same = (a: readonly string[], b: readonly string[]) =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/**
 * Follows each address with `client.watch` while the component is mounted.
 * A changed list adds and removes walks on the running watch, compared by
 * canonical address, so reordering or another spelling changes nothing; an
 * added address starts where `Watch.add` starts it, not at `fromVersion` or
 * `fromTime`. Restarts, emptying `data`, only when the
 * client or options change by value. A `null` or `undefined` list waits.
 */
export function useWatch<TData = unknown>(
  addresses: readonly string[] | null | undefined,
  options: UseWatchOptions = {},
): UseWatchResult<TData> {
  const { client: explicit, maxEvents = DEFAULT_MAX_EVENTS, ...listener } = options;
  const client = useStreamdClient(explicit);
  const wanted = new Map<string, string>();
  for (const address of addresses ?? NONE) {
    const key = requireAddress(address);
    if (!wanted.has(key)) wanted.set(key, address);
  }
  const listKey = valueKey([...wanted.keys()].sort());
  // The running watch and the list it was last brought in line with.
  const running = useRef<{ watch: Watch; applied: Map<string, string> }>(undefined);
  const [watched, setWatched] = useState(NONE);
  const sync = (watch: Watch) => {
    if (running.current?.watch !== watch) return;
    const now = watch.addresses;
    setWatched((prev) => (same(prev, now) ? prev : now));
  };

  const followed = useFollowing<WatchedEvent<TData>, typeof listener>(
    addresses != null,
    valueKey(listener),
    client,
    listener,
    maxEvents,
    (deliver, opts) => {
      const watch: Watch = client.watch<TData>(
        [...wanted.values()],
        (event, address) => deliver([{ event, address }]),
        {
          ...opts,
          onError: (err) => {
            opts.onError?.(err);
            // A walk that gave up has already left `watch.addresses`.
            sync(watch);
          },
        },
      );
      running.current = { watch, applied: new Map(wanted) };
      sync(watch);
      return watch;
    },
  );

  useEffect(() => {
    const run = running.current;
    if (run === undefined) return;
    for (const key of run.applied.keys()) {
      if (!wanted.has(key)) {
        run.watch.remove(key);
        run.applied.delete(key);
      }
    }
    // Only addresses new to the list: one whose walk gave up stays out until the list drops and relists it.
    for (const [key, address] of wanted) {
      if (!run.applied.has(key)) {
        run.watch.add(address);
        run.applied.set(key, address);
      }
    }
    sync(run.watch);
    // `listKey` stands for the canonical set of `addresses`.
  }, [listKey]);

  const live = addresses == null ? NONE : watched;
  return useMemo(
    () => ({ data: followed.events, error: followed.error, status: followed.status, addresses: live }),
    [followed, live],
  );
}
