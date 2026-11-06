import type {
  EventAdapter,
  EventDescriptor,
  EventFilter,
  StreamdEvent,
} from "./types.js";
import { raceAbort, resolveFromVersion } from "./utils.js";

const DEFAULT_PAGE_SIZE = 25;

/**
 * Historical event querying: `getEvents` and its paged form `getEventPages`.
 */
export class EventQuery {
  constructor(private readonly adapter: EventAdapter) {}

  async getEvents<TData = unknown>(
    descriptor: EventDescriptor,
    filter: EventFilter = {},
  ): Promise<StreamdEvent<TData>[]> {
    const collected: StreamdEvent<TData>[] = [];
    for await (const page of this.getEventPages<TData>(descriptor, filter)) {
      collected.push(...page);
    }
    return collected;
  }

  /**
   * Async iterator for paginating through large result sets.
   *
   * Pages are chained by the adapter's cursor rather than a numeric offset,
   * so regions containing no matching events are skipped over instead of
   * terminating the walk early.
   */
  async *getEventPages<TData = unknown>(
    descriptor: EventDescriptor,
    filter: EventFilter = {},
    pageSize = DEFAULT_PAGE_SIZE,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    const { where, maxPages, signal, ...adapterFilter } = filter;
    const limit = filter.limit;
    let cursor: string | undefined;
    let fromVersion = filter.fromVersion;
    let fetched = 0;
    let pages = 0;

    while (true) {
      if (signal?.aborted) throw signal.reason;
      if (maxPages !== undefined && pages >= maxPages) break;
      pages++;

      const batchLimit =
        limit !== undefined ? Math.min(pageSize, limit - fetched) : pageSize;
      if (batchLimit <= 0) break;

      // Only a relative start awaits, so an absolute one fetches synchronously.
      if (fromVersion !== undefined && fromVersion < 0n) {
        fromVersion = await raceAbort(
          resolveFromVersion(this.adapter, descriptor, filter.fromVersion),
          signal,
        );
      }

      const page = await raceAbort(
        this.adapter.fetchEvents(descriptor, {
          ...adapterFilter,
          // The cursor carries position after the first page; fromVersion and
          // offset only seed the initial request.
          fromVersion: cursor === undefined ? fromVersion : undefined,
          offset: cursor === undefined ? filter.offset : undefined,
          cursor,
          limit: batchLimit,
          orderBy: "asc",
        }),
        signal,
      );

      // `where` runs here rather than in the adapter: predicates are functions
      // and cannot be pushed down to a remote source. `limit` counts what
      // survives it, so a restrictive predicate keeps paging rather than
      // returning short.
      const matched =
        where === undefined ? page.events : page.events.filter(where);

      // Adapters may overshoot `limit` — a single transaction can emit several
      // matching events, and the scan adapter keeps them rather than dropping
      // events its cursor has already moved past. Clamp here so the caller's
      // limit is exact.
      const events =
        limit !== undefined && matched.length > limit - fetched
          ? matched.slice(0, limit - fetched)
          : matched;

      if (events.length > 0) {
        fetched += events.length;
        yield events as StreamdEvent<TData>[];
      }

      if (limit !== undefined && fetched >= limit) break;

      // Stop only when the adapter says there is nothing further. An empty
      // page is not a terminator — it just means this window held no matches.
      if (!page.hasMore || page.nextCursor === undefined) break;
      if (page.nextCursor === cursor) break; // adapter made no progress
      cursor = page.nextCursor;
    }
  }
}
