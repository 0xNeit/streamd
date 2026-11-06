import type {
  EventAdapter,
  EventDescriptor,
  EventFilter,
  StreamdEvent,
} from "./types.js";
import { raceAbort, resolveFromVersion, sleep } from "./utils.js";
import { RequestLimiter } from "./limiter.js";
import { fieldDecoder } from "./decode.js";
import { StreamdError, StreamdNetworkError } from "./errors.js";

const DEFAULT_PAGE_SIZE = 25;
const RETRY_BASE_DELAY = 1000;
const RETRY_MAX_DELAY = 60_000;

function validateRetries(retries: number | undefined): void {
  if (retries === undefined) return;
  if (!Number.isSafeInteger(retries) || retries < 0) {
    throw new StreamdError(
      `Invalid retries: ${String(retries)} (expected a non-negative integer)`,
    );
  }
}

/**
 * Runs `call` until it succeeds or `retries` extra attempts are spent. A
 * `StreamdError` that is not a network failure is the caller's input, which
 * fails the same way every time, so it is never retried.
 */
async function withRetries<T>(
  call: () => Promise<T>,
  retries: number,
  signal: AbortSignal | undefined,
): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await raceAbort(call(), signal);
    } catch (err) {
      const invalidInput =
        err instanceof StreamdError && !(err instanceof StreamdNetworkError);
      if (attempt >= retries || invalidInput || signal?.aborted) throw err;
      await sleep(Math.min(RETRY_BASE_DELAY * 2 ** attempt, RETRY_MAX_DELAY), signal);
    }
  }
}

/**
 * Historical event querying: `getEvents` and its paged form `getEventPages`.
 */
export class EventQuery {
  constructor(
    private readonly source: EventAdapter,
    private readonly limiter = new RequestLimiter(),
  ) {}

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
    const { orderBy, ...rest } = filter;
    if (orderBy !== undefined && orderBy !== "asc" && orderBy !== "desc") {
      throw new StreamdError('orderBy must be "asc" or "desc"');
    }
    validateRetries(rest.retries);
    if (orderBy !== "desc") {
      yield* this.ascendingPages<TData>(descriptor, rest, pageSize);
      return;
    }

    // Adapters only page forward, so walk the whole range and keep the tail.
    const limit = rest.limit;
    if (limit !== undefined && limit <= 0) return;
    let latest: StreamdEvent<TData>[] = [];
    const walk = { ...rest, limit: undefined };
    for await (const page of this.ascendingPages<TData>(descriptor, walk, pageSize)) {
      latest.push(...page);
      if (limit !== undefined && latest.length > limit) latest = latest.slice(-limit);
    }
    latest.reverse();
    for (let i = 0; i < latest.length; i += pageSize) {
      yield latest.slice(i, i + pageSize);
    }
  }

  private async *ascendingPages<TData>(
    descriptor: EventDescriptor,
    filter: EventFilter,
    pageSize: number,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    const { where, decode: fields, maxPages, retries = 0, signal, ...adapterFilter } =
      filter;
    const decode = fieldDecoder(fields);
    const adapter = this.limiter.wrap(this.source, () => signal?.aborted === true);
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
        fromVersion = await withRetries(
          () => resolveFromVersion(adapter, descriptor, filter.fromVersion),
          retries,
          signal,
        );
      }

      const page = await withRetries(
        () =>
          adapter.fetchEvents(descriptor, {
          ...adapterFilter,
          // The cursor carries position after the first page; fromVersion and
          // offset only seed the initial request.
          fromVersion: cursor === undefined ? fromVersion : undefined,
          offset: cursor === undefined ? filter.offset : undefined,
          cursor,
          limit: batchLimit,
          orderBy: "asc",
        }),
        retries,
        signal,
      );

      // `where` runs here rather than in the adapter: predicates are functions
      // and cannot be pushed down to a remote source. `limit` counts what
      // survives it, so a restrictive predicate keeps paging rather than
      // returning short.
      const decoded =
        decode === undefined ? page.events : page.events.map(decode);
      const matched = where === undefined ? decoded : decoded.filter(where);

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
