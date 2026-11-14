import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { eventId, type EventDescriptor, type EventFilter, type EventOf, type StreamdClient } from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** `getEventPages` options, minus `signal`: the hook aborts on unmount and on input change. */
export interface UseEventPagesOptions extends Omit<EventFilter, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
  /** `getEventPages`' page size: the most events each adapter request asks for (default 25). */
  pageSize?: number;
}

export interface UseEventPagesResult<TData> {
  /** Every page loaded so far, as `getEventPages` yielded them, in walk order. */
  pages: EventOf<TData>[][];
  /**
   * False once a fetch found the walk finished. The last page cannot tell it
   * is the last, so the `loadMore()` after it is the one that turns this off.
   */
  hasMore: boolean;
  /** The last fetch's failure, cleared by the next success. */
  error: Error | undefined;
  /** True while a page is in flight. */
  loading: boolean;
  /**
   * Fetches the next page. The same function on every render. Resolves when
   * its page settles, never rejecting; while a page is in flight, once
   * `hasMore` is false, with no descriptor, after unmount or during server
   * rendering it does nothing.
   */
  loadMore: () => Promise<void>;
}

interface PagesState<TData> extends Omit<UseEventPagesResult<TData>, "loadMore"> {
  /** The client and key these pages belong to, so a changed input renders empty at once. */
  client: StreamdClient;
  key: string;
}

function fresh<TData>(client: StreamdClient, key: string, active: boolean): PagesState<TData> {
  return { client, key, pages: [], hasMore: active, error: undefined, loading: active };
}

/**
 * The filter that carries on a walk that failed after delivering `held`, since
 * a generator that threw cannot be asked again. It starts at the last held
 * event's version (ends there under `"desc"`), so the events held at that
 * version come again and count against `limit` again; the caller skips them.
 */
function resumeFilter(filter: EventFilter, held: readonly { version: bigint }[]): EventFilter {
  const last = held.at(-1);
  if (last === undefined) return filter;
  const again = held.filter((e) => e.version === last.version).length;
  const limit = filter.limit === undefined ? undefined : filter.limit - held.length + again;
  return filter.orderBy === "desc"
    ? { ...filter, toVersion: last.version, toBlock: undefined, limit }
    : { ...filter, fromVersion: last.version, fromBlock: undefined, fromTime: undefined, offset: undefined, limit };
}

/**
 * `client.getEventPages(descriptor, options, pageSize)` for a component: the
 * first page loads after mount and `loadMore()` appends the next. A change of
 * descriptor or options by value, or of client, starts the walk over with no
 * pages, aborting the page in flight. After a failure the next `loadMore()`
 * retries the same page. A `null` or `undefined` descriptor fetches nothing.
 */
export function useEventPages<TData = unknown>(
  descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[] | null | undefined,
  options: UseEventPagesOptions = {},
): UseEventPagesResult<TData> {
  const { client: explicit, pageSize, ...filter } = options;
  const client = useStreamdClient(explicit);
  const latest = useRef(filter);
  latest.current = filter;
  const more = useRef<(() => Promise<void>) | undefined>(undefined);
  const key = valueKey([descriptor, filter, pageSize ?? null]);
  const active = descriptor != null;
  const [state, setState] = useState(() => fresh<TData>(client, key, active));
  const current = state.client === client && state.key === key ? state : fresh<TData>(client, key, active);

  useEffect(() => {
    setState((s) => (s.client === client && s.key === key ? s : fresh(client, key, descriptor != null)));
    if (descriptor == null) return;
    const controller = new AbortController();
    const live = { ...withLatestFunctions(filter, () => latest.current), signal: controller.signal };
    const held: EventOf<TData>[] = [];
    const seen = new Set<string>();
    let walk: AsyncGenerator<EventOf<TData>[], void> | undefined;
    let busy = false;
    let done = false;
    const next = async () => {
      if (busy || done) return;
      busy = true;
      setState((s) => (s.loading ? s : { ...s, loading: true }));
      try {
        walk ??= client.getEventPages<TData>(descriptor, resumeFilter(live, held), pageSize);
        let page: EventOf<TData>[] = [];
        while (page.length === 0) {
          const step = await walk.next();
          if (step.done) break;
          page = step.value.filter((e) => !seen.has(eventId(e)));
        }
        if (controller.signal.aborted) return;
        done = page.length === 0;
        for (const e of page) seen.add(eventId(e));
        held.push(...page);
        setState((s) => ({
          ...s,
          pages: done ? s.pages : [...s.pages, page],
          hasMore: !done,
          error: undefined,
          loading: false,
        }));
      } catch (err) {
        walk = undefined;
        if (!controller.signal.aborted) setState((s) => ({ ...s, error: toError(err), loading: false }));
      } finally {
        busy = false;
      }
    };
    more.current = next;
    void next();
    return () => {
      more.current = undefined;
      controller.abort();
    };
    // `key` stands for `descriptor`, `filter` and `pageSize` by value.
  }, [client, key]);

  const loadMore = useCallback(() => more.current?.() ?? Promise.resolve(), []);
  return useMemo(() => {
    const { pages, hasMore, error, loading } = current;
    return { pages, hasMore, error, loading, loadMore };
  }, [current, loadMore]);
}
