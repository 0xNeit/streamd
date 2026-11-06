import type {
  BlockInfo,
  EventAdapter,
  EventDescriptor,
  EventFilter,
  StreamdEvent,
} from "./types.js";
import { isAbsoluteVersion, raceAbort, resolveFromVersion, sleep } from "./utils.js";
import { RequestLimiter } from "./limiter.js";
import { fieldDecoder } from "./decode.js";
import { resolveFromTime, validateFromTime } from "./time.js";
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

/** Rejects block bounds that are malformed, empty, or clash with a version bound. */
function validateBlockRange(filter: EventFilter): void {
  const { fromBlock, toBlock } = filter;
  for (const [name, value] of [["fromBlock", fromBlock], ["toBlock", toBlock]] as const) {
    if (value !== undefined && (typeof value !== "bigint" || value < 0n)) {
      throw new StreamdError(
        `Invalid ${name}: ${String(value)} (expected a non-negative bigint)`,
      );
    }
  }
  if (fromBlock !== undefined && filter.fromVersion !== undefined) {
    throw new StreamdError("Set fromBlock or fromVersion, not both");
  }
  if (toBlock !== undefined && filter.toVersion !== undefined) {
    throw new StreamdError("Set toBlock or toVersion, not both");
  }
  if (fromBlock !== undefined && toBlock !== undefined && fromBlock > toBlock) {
    throw new StreamdError(`fromBlock ${fromBlock} is after toBlock ${toBlock}`);
  }
}

function validateToVersion(toVersion: unknown): void {
  if (toVersion !== undefined && toVersion !== "head" && typeof toVersion !== "bigint") {
    throw new StreamdError(
      `Invalid toVersion: ${String(toVersion)} (expected a bigint or "head")`,
    );
  }
}

async function headVersion(adapter: EventAdapter, descriptor: EventDescriptor): Promise<bigint> {
  if (adapter.getHeadVersion === undefined) {
    throw new StreamdError('toVersion "head" needs an adapter that implements getHeadVersion');
  }
  return adapter.getHeadVersion(descriptor);
}

function blockAt(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  height: bigint,
): Promise<BlockInfo> {
  if (adapter.getBlock === undefined) {
    throw new StreamdError("fromBlock and toBlock need an adapter that implements getBlock");
  }
  return adapter.getBlock(descriptor, height);
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
    const { orderBy, onCursor, ...rest } = filter;
    if (orderBy !== undefined && orderBy !== "asc" && orderBy !== "desc") {
      throw new StreamdError('orderBy must be "asc" or "desc"');
    }
    validateRetries(rest.retries);
    validateToVersion(rest.toVersion);
    validateBlockRange(rest);
    validateFromTime(rest);
    if (orderBy !== "desc") {
      const end = yield* this.ascendingPages<TData>(descriptor, rest, pageSize);
      onCursor?.(end);
      return;
    }

    // Adapters only page forward, so walk the whole range and keep the tail.
    // The walk still ends at the end of the range, so its cursor is exact.
    const limit = rest.limit;
    if (limit !== undefined && limit <= 0) {
      onCursor?.(undefined);
      return;
    }
    let latest: StreamdEvent<TData>[] = [];
    const walk = this.ascendingPages<TData>(descriptor, { ...rest, limit: undefined }, pageSize);
    let step = await walk.next();
    for (; !step.done; step = await walk.next()) {
      latest.push(...step.value);
      if (limit !== undefined && latest.length > limit) latest = latest.slice(-limit);
    }
    latest.reverse();
    for (let i = 0; i < latest.length; i += pageSize) {
      yield latest.slice(i, i + pageSize);
    }
    onCursor?.(step.value);
  }

  /**
   * `getEventPages` one event at a time. Pages are buffered as fetched, so
   * `signal` is checked before each event rather than only between pages.
   */
  async *iterateEvents<TData = unknown>(
    descriptor: EventDescriptor,
    filter: EventFilter = {},
    pageSize = DEFAULT_PAGE_SIZE,
  ): AsyncGenerator<StreamdEvent<TData>, void, unknown> {
    for await (const page of this.getEventPages<TData>(descriptor, filter, pageSize)) {
      for (const event of page) {
        if (filter.signal?.aborted) throw filter.signal.reason;
        yield event;
      }
    }
  }

  private async *ascendingPages<TData>(
    descriptor: EventDescriptor,
    filter: EventFilter,
    pageSize: number,
  ): AsyncGenerator<StreamdEvent<TData>[], string | undefined, unknown> {
    const {
      where,
      decode: fields,
      maxPages,
      retries = 0,
      signal,
      fromBlock,
      toBlock,
      fromTime,
      ...adapterFilter
    } = filter;
    const decode = fieldDecoder(fields);
    const adapter = this.limiter.wrap(this.source, () => signal?.aborted === true);
    const limit = filter.limit;
    let cursor: string | undefined;
    let fromVersion = isAbsoluteVersion(filter.fromVersion)
      ? filter.fromVersion
      : undefined;
    let toVersion = filter.toVersion === "head" ? undefined : filter.toVersion;
    let resolved =
      isAbsoluteVersion(filter.fromVersion) &&
      filter.toVersion !== "head" &&
      fromBlock === undefined &&
      toBlock === undefined &&
      fromTime === undefined;
    let fetched = 0;
    let pages = 0;
    // The cursor after the last page read in full; see `EventFilter.onCursor`.
    let end: string | undefined;

    while (true) {
      if (signal?.aborted) throw signal.reason;
      if (maxPages !== undefined && pages >= maxPages) break;
      pages++;

      const batchLimit =
        limit !== undefined ? Math.min(pageSize, limit - fetched) : pageSize;
      if (batchLimit <= 0) break;

      // Only a relative, earliest, head, time or block bound awaits, so an
      // absolute range fetches synchronously.
      if (!resolved) {
        const head =
          filter.toVersion === "head"
            ? await withRetries(() => headVersion(adapter, descriptor), retries, signal)
            : undefined;
        toVersion ??= head;
        const start = (): Promise<bigint | undefined> =>
          fromTime !== undefined
            ? resolveFromTime(adapter, descriptor, fromTime, () => signal?.aborted === true)
            : fromBlock !== undefined
              ? blockAt(adapter, descriptor, fromBlock).then((b) => b.firstVersion)
              : resolveFromVersion(adapter, descriptor, filter.fromVersion, head);
        fromVersion = await withRetries(start, retries, signal);
        if (toBlock !== undefined) {
          toVersion = (
            await withRetries(() => blockAt(adapter, descriptor, toBlock), retries, signal)
          ).lastVersion;
        }
        resolved = true;
      }

      const page = await withRetries(
        () =>
          adapter.fetchEvents(descriptor, {
          ...adapterFilter,
          // The cursor carries position after the first page; fromVersion and
          // offset only seed the initial request.
          fromVersion: cursor === undefined ? fromVersion : undefined,
          toVersion,
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

      // A clamped page has unread events behind its cursor, and a version
      // bound lets adapters move the cursor past events beyond it.
      end =
        events.length < matched.length || toVersion !== undefined
          ? undefined
          : page.nextCursor;

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
    return end;
  }
}
