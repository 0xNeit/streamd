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
import { StreamdError, StreamdNetworkError, retryAfterHint } from "./errors.js";
import { mergeEvents, validateMerge } from "./merge.js";
import { filterEvents } from "./filter.js";
import { requireBlock, requireVersion } from "./adapters/validate.js";
import { buildStateRequest, readState, validateWithState } from "./state.js";

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
  return requireVersion("getHeadVersion", await adapter.getHeadVersion(descriptor));
}

async function blockAt(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  height: bigint,
): Promise<BlockInfo> {
  if (adapter.getBlock === undefined) {
    throw new StreamdError("fromBlock and toBlock need an adapter that implements getBlock");
  }
  return requireBlock(await adapter.getBlock(descriptor, height), height);
}

/**
 * Runs `call` until it succeeds or `retries` extra attempts are spent. A
 * `StreamdError` that is not a network failure is the caller's input, which
 * fails the same way every time, so it is never retried. A Retry-After hint
 * replaces the exponential wait, under the same cap.
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
      const delay = retryAfterHint(err) ?? RETRY_BASE_DELAY * 2 ** attempt;
      await sleep(Math.min(delay, RETRY_MAX_DELAY), signal);
    }
  }
}

/**
 * The range's start and end as absolute versions, reading the adapter once
 * for whichever bound is relative, `"earliest"`, `"head"`, a block or a time.
 */
async function resolveBounds(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  filter: EventFilter,
): Promise<{ fromVersion: bigint | undefined; toVersion: bigint | undefined }> {
  const { fromBlock, toBlock, fromTime, retries = 0, signal } = filter;
  const head =
    filter.toVersion === "head"
      ? await withRetries(() => headVersion(adapter, descriptor), retries, signal)
      : undefined;
  const start = (): Promise<bigint | undefined> =>
    fromTime !== undefined
      ? resolveFromTime(adapter, descriptor, fromTime, () => signal?.aborted === true)
      : fromBlock !== undefined
        ? blockAt(adapter, descriptor, fromBlock).then((b) => b.firstVersion)
        : resolveFromVersion(adapter, descriptor, filter.fromVersion, head);
  const fromVersion = await withRetries(start, retries, signal);
  const toVersion =
    toBlock !== undefined
      ? (await withRetries(() => blockAt(adapter, descriptor, toBlock), retries, signal))
          .lastVersion
      : filter.toVersion === "head"
        ? head
        : filter.toVersion;
  return { fromVersion, toVersion };
}

/** One descriptor, or several whose events are merged into one history. */
type Descriptors = EventDescriptor | readonly EventDescriptor[];

/**
 * Historical event querying: `getEvents` and its paged form `getEventPages`.
 */
export class EventQuery {
  constructor(
    private readonly source: EventAdapter,
    private readonly limiter = new RequestLimiter(),
  ) {}

  async getEvents<TData = unknown>(
    descriptor: Descriptors,
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
   *
   * An array of descriptors walks each one and merges their events in
   * ascending `(version, eventIndex)` order (reversed for `"desc"`), an event
   * selected by more than one returned once.
   *
   * `withState` is applied last, to the pages as returned; a merge reads it
   * through its first descriptor, as it does its shared bounds.
   */
  async *getEventPages<TData = unknown>(
    descriptor: Descriptors,
    filter: EventFilter = {},
    pageSize = DEFAULT_PAGE_SIZE,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    const { withState, ...rest } = filter;
    const { retries = 0, signal } = rest;
    validateWithState(withState);
    const pages = this.pages<TData>(descriptor, rest, pageSize);
    if (withState === undefined) return yield* pages;
    const adapter = this.limiter.wrap(this.source, signal);
    const first = Array.isArray(descriptor)
      ? (descriptor as readonly EventDescriptor[])[0]!
      : (descriptor as EventDescriptor);
    for await (const page of pages) {
      const withStates: StreamdEvent[] = [];
      for (const event of page as StreamdEvent[]) {
        if (signal?.aborted) throw signal.reason;
        // Built outside the retries: a throw from the caller's builder is not worth retrying.
        const request = buildStateRequest(withState, event);
        withStates.push(
          request === undefined
            ? event
            : await withRetries(() => readState(adapter, first, event, request), retries, signal),
        );
      }
      yield withStates as StreamdEvent<TData>[];
    }
  }

  private async *pages<TData>(
    descriptor: Descriptors,
    filter: EventFilter,
    pageSize: number,
  ): AsyncGenerator<StreamdEvent<TData>[], void, unknown> {
    const { orderBy, onCursor, ...rest } = filter;
    if (orderBy !== undefined && orderBy !== "asc" && orderBy !== "desc") {
      throw new StreamdError('orderBy must be "asc" or "desc"');
    }
    validateRetries(rest.retries);
    validateToVersion(rest.toVersion);
    validateBlockRange(rest);
    validateFromTime(rest);
    const merged = Array.isArray(descriptor) ? (descriptor as readonly EventDescriptor[]) : undefined;
    if (merged !== undefined) validateMerge(merged, filter);
    const ascending = (f: EventFilter) =>
      merged !== undefined
        ? this.mergedPages<TData>(merged, f, pageSize)
        : this.ascendingPages<TData>(descriptor as EventDescriptor, f, pageSize);
    if (orderBy !== "desc") {
      const end = yield* ascending(rest);
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
    const walk = ascending({ ...rest, limit: undefined });
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
    descriptor: Descriptors,
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

  /**
   * One ascending walk per descriptor, merged, in pages of `pageSize`. Each
   * walk gets `limit` too: no descriptor supplies more than `limit` of the
   * first `limit` merged events. `maxPages` therefore caps each walk.
   */
  private async *mergedPages<TData>(
    descriptors: readonly EventDescriptor[],
    filter: EventFilter,
    pageSize: number,
  ): AsyncGenerator<StreamdEvent<TData>[], undefined, unknown> {
    if (filter.limit !== undefined && filter.limit <= 0) return undefined;
    const shared = await this.sharedBounds(descriptors[0]!, filter);
    const walks = descriptors.map((d) => this.ascendingPages<TData>(d, shared, pageSize));
    let page: StreamdEvent<TData>[] = [];
    for await (const event of mergeEvents(walks, filter.limit)) {
      page.push(event);
      if (page.length < pageSize) continue;
      yield page;
      page = [];
    }
    if (page.length > 0) yield page;
    return undefined;
  }

  /**
   * `filter` with its head, block and time bounds read once, against the
   * first descriptor, so every walk of a merge covers the same versions.
   * `"earliest"` is left for each walk, since sources prune differently.
   */
  private async sharedBounds(
    descriptor: EventDescriptor,
    filter: EventFilter,
  ): Promise<EventFilter> {
    const earliest = filter.fromVersion === "earliest";
    const shared =
      filter.toVersion === "head" ||
      filter.fromBlock !== undefined ||
      filter.toBlock !== undefined ||
      filter.fromTime !== undefined ||
      (!earliest && !isAbsoluteVersion(filter.fromVersion));
    if (!shared) return filter;
    const adapter = this.limiter.wrap(this.source, filter.signal);
    const bounds = await resolveBounds(adapter, descriptor, {
      ...filter,
      fromVersion: earliest ? undefined : filter.fromVersion,
    });
    return {
      ...filter,
      fromBlock: undefined,
      toBlock: undefined,
      fromTime: undefined,
      fromVersion: earliest ? "earliest" : bounds.fromVersion,
      toVersion: bounds.toVersion,
    };
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
    const adapter = this.limiter.wrap(this.source, signal);
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
        ({ fromVersion, toVersion } = await resolveBounds(adapter, descriptor, filter));
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
      // returning short. Only an async predicate's result is awaited.
      // Adapters honour fromVersion only on the first request, so account and
      // handle walks, which start below it, are bounded here on every page.
      const start = fromVersion;
      const inRange = start === undefined ? page.events : page.events.filter((e) => e.version >= start);
      const decoded = decode === undefined ? inRange : inRange.map(decode);
      const filtered = where === undefined ? decoded : filterEvents(decoded, where);
      const matched = Array.isArray(filtered) ? filtered : await raceAbort(filtered, signal);

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
