import type {
  BatchCallback,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventPage,
  EventPredicate,
  ListenerOptions,
  StateRequestBuilder,
  StreamdEvent,
  SubscriptionStatus,
  StateRequest,
} from "./types.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  retryAfterHint,
} from "./errors.js";
import { CursorManager } from "./cursor.js";
import { raceAbort, resolveFromVersion } from "./utils.js";
import { RequestLimiter } from "./limiter.js";
import { fieldDecoder } from "./decode.js";
import { resolveFromTime, validateFromTime } from "./time.js";
import { buildStateRequest, readState, validateWithState } from "./state.js";
import { nameTtl, type SenderNames } from "./names.js";
import { isThenable } from "./filter.js";

const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_MAX_ERRORS = 10;
const DEFAULT_MAX_BACKOFF = 60_000;
const MAX_DELAY = 2_147_483_647;

export function resolveMaxBackoff(maxBackoff: number | undefined): number {
  if (maxBackoff === undefined) return DEFAULT_MAX_BACKOFF;
  if (
    typeof maxBackoff !== "number" ||
    !Number.isFinite(maxBackoff) ||
    maxBackoff < 0 ||
    maxBackoff > MAX_DELAY
  ) {
    throw new StreamdError(
      `Invalid maxBackoff: ${String(maxBackoff)} (expected 0..${MAX_DELAY}ms)`,
    );
  }
  return maxBackoff;
}

export function resolveMaxPollInterval(max: number | undefined): number | undefined {
  if (max === undefined) return undefined;
  if (typeof max !== "number" || !Number.isFinite(max) || max < 0 || max > MAX_DELAY) {
    throw new StreamdError(
      `Invalid maxPollInterval: ${String(max)} (expected 0..${MAX_DELAY}ms)`,
    );
  }
  return max;
}

/** An event ready to deliver, and the state request to read for it, if any. */
type Prepared = { event: StreamdEvent; request?: StateRequest };

/**
 * Called once per finished page, empty or not, with its new events that
 * passed `where`, and the page itself for its bounds. Awaited before the
 * cursor moves past the page; nothing is handed over from a failed page.
 */
export type PageCallback = (events: StreamdEvent[], page: EventPage) => void | Promise<void>;

/**
 * One event at a time, `{ batch }` for each page's new events in one call, or
 * `{ page }` for every finished page.
 */
export type Delivery = EventCallback | { batch: BatchCallback } | { page: PageCallback };

export class EventPoller {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveErrors = 0;
  private lastVersion: bigint | undefined = undefined;
  private lastSuccessAt: number | undefined = undefined;
  private currentInterval: number;
  /** The wait after the next empty page; grows only under `maxPollInterval`. */
  private quietInterval: number;
  private _active = false;
  private _paused = false;
  private caughtUp = false;
  /** Bumped by start(), so a poll left over from an earlier run can tell. */
  private generation = 0;
  /**
   * The fetch or page delivery still running, if any. A restart's first poll
   * waits for it, so a new run never fetches or delivers alongside the old one.
   */
  private delivering: Promise<void> = Promise.resolve();
  /** Settles when the page being delivered has finished; a fetch does not count. */
  private handlers: Promise<void> = Promise.resolve();
  /** Aborted when this run ends, so a hung view never holds the next run or its request slot. */
  private runAbort = new AbortController();
  private readonly position: CursorManager;
  private readonly dedupWindow?: number;
  private readonly callback?: EventCallback;
  private readonly onBatch?: BatchCallback;
  private readonly onPage?: PageCallback;

  private readonly pollInterval: number;
  private readonly batchSize: number;
  private readonly maxErrors: number;
  private readonly maxBackoff: number;
  private readonly maxPollInterval?: number;
  /** Absolute once the first poll has resolved a relative or earliest start. */
  private fromVersion?: bigint | "earliest";
  /** Cleared once the first poll has resolved it into `fromVersion`. */
  private fromTime?: bigint;
  /** The start a run with no cursor resolved to; every page is bounded by it. */
  private floor?: bigint;
  /** Whether a page has reached `floor`, before which no position is worth saving. */
  private reachedStart = false;
  private readonly where?: EventPredicate;
  private readonly decode?: (event: StreamdEvent) => StreamdEvent;
  private readonly withState?: StateRequestBuilder;
  /** The `withNames` expiry, `undefined` when off. */
  private readonly nameTtl?: number;
  private readonly onError?: (error: Error) => void;
  private readonly onCaughtUp?: () => void;
  private readonly signal?: AbortSignal;
  private readonly abort = () => this.stop();

  constructor(
    private readonly adapter: EventAdapter,
    private readonly descriptor: EventDescriptor,
    delivery: Delivery,
    options: ListenerOptions = {},
    private readonly limiter = new RequestLimiter(),
    private readonly names?: SenderNames,
  ) {
    if (typeof delivery === "function") this.callback = delivery;
    else if ("batch" in delivery) this.onBatch = delivery.batch;
    else this.onPage = delivery.page;
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.maxErrors = options.maxErrors ?? DEFAULT_MAX_ERRORS;
    this.maxBackoff = resolveMaxBackoff(options.maxBackoff);
    this.maxPollInterval = resolveMaxPollInterval(options.maxPollInterval);
    validateFromTime(options);
    this.fromVersion = options.fromVersion;
    this.fromTime = options.fromTime;
    this.where = options.where;
    this.decode = fieldDecoder(options.decode);
    validateWithState(options.withState);
    this.withState = options.withState;
    this.nameTtl = nameTtl(options.withNames, names);
    this.onError = options.onError;
    this.onCaughtUp = options.onCaughtUp;
    this.signal = options.signal;
    this.currentInterval = this.pollInterval;
    this.quietInterval = this.pollInterval;
    this.dedupWindow = options.dedupWindow;
    this.position = new CursorManager(options.cursor, options.dedupWindow);
  }

  get active(): boolean {
    return this._active;
  }

  /** The adapter's resume token after the last finished page; see `ListenerOptions.cursor`. */
  get cursor(): string | undefined {
    // Below its start a walk's position would resume delivering older events.
    if (this.floor !== undefined && !this.reachedStart) return undefined;
    return this.position.token;
  }

  /** Settles, never rejecting, once the handler now running has finished. */
  get handling(): Promise<void> {
    return this.handlers;
  }

  /** Snapshot of the poller's current state; see `SubscriptionStatus`. */
  get status(): SubscriptionStatus {
    return {
      active: this._active,
      cursor: this.cursor,
      lastVersion: this.lastVersion,
      consecutiveErrors: this.consecutiveErrors,
      lastSuccessAt: this.lastSuccessAt,
    };
  }

  /** True between pause() and resume(), and false once stopped. */
  get paused(): boolean {
    return this._paused;
  }

  start(): void {
    if (this._active || this.signal?.aborted) return;
    // A stopped poller starts a new run; only resume() keeps the error budget.
    if (!this._paused) {
      this.consecutiveErrors = 0;
      this.currentInterval = this.pollInterval;
      this.quietInterval = this.pollInterval;
    }
    this._active = true;
    this._paused = false;
    this.generation++;
    this.runAbort = new AbortController();
    this.signal?.addEventListener("abort", this.abort, { once: true });
    this.schedulePoll(0);
  }

  stop(): void {
    this._active = false;
    this._paused = false;
    this.signal?.removeEventListener("abort", this.abort);
    this.clearTimer();
    this.runAbort.abort();
  }

  /**
   * Stop polling but keep the cursor, dedup window, and error budget for
   * resume(). The abort listener stays attached, so an abort still ends it.
   */
  pause(): void {
    if (!this._active) return;
    this._active = false;
    this._paused = true;
    this.clearTimer();
    this.runAbort.abort();
  }

  /** Undo a pause(); a stopped poller stays stopped. */
  resume(): void {
    if (this._paused) this.start();
  }

  private clearTimer(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private schedulePoll(delay: number): void {
    if (!this._active) return;
    this.timer = setTimeout(() => void this.poll(), delay);
  }

  /**
   * The caller's handler is outside the poller's control. A throw from it is
   * rethrown on a microtask: still surfaced as uncaught, but off this stack,
   * so scheduling and the error budget never depend on it.
   */
  private report(error: Error): void {
    try {
      this.onError?.(error);
    } catch (handlerError) {
      queueMicrotask(() => {
        throw handlerError;
      });
    }
  }

  /** True while the run that started poll `gen` is still the live one. */
  private isCurrent(gen: number): boolean {
    return this._active && gen === this.generation;
  }

  private async poll(): Promise<void> {
    const gen = this.generation;
    const cancelled = () => !this.isCurrent(gen);
    // stop() and pause() abort the run, so its queued calls leave the limiter.
    const run = this.runAbort.signal;
    const adapter = this.limiter.wrap(this.adapter, run);
    // Fetches are left to settle, since a restart waits for them; views are not.
    const viewer = this.limiter.wrap(this.adapter, run, run);
    try {
      await this.delivering;
      if (!this.isCurrent(gen)) return;
      // Once we have a cursor it supersedes fromVersion, which only seeds
      // the very first poll.
      let fromVersion: bigint | undefined;
      if (this.position.token === undefined) {
        fromVersion =
          this.fromTime === undefined
            ? await resolveFromVersion(adapter, this.descriptor, this.fromVersion)
            : await resolveFromTime(
                adapter,
                this.descriptor,
                this.fromTime,
                cancelled,
              );
        this.fromVersion = fromVersion;
        this.fromTime = undefined;
        // Only a run that began with no cursor has a start to hold; a cursor wins.
        this.floor = fromVersion;
        if (!this.isCurrent(gen)) return;
      }
      const fetching = adapter.fetchEvents(this.descriptor, {
        cursor: this.position.token,
        fromVersion,
        limit: this.batchSize,
        orderBy: "asc",
      });
      // The request runs on after a pause or stop, so a restart waits for it
      // rather than sending a second one beside it.
      this.delivering = fetching.then(
        () => {},
        () => {},
      );
      const page = await fetching;
      // A fetch that resolves after stop(), or after stop() and start(), must
      // not touch the cursor or the error budget the live run resumes with.
      if (!this.isCurrent(gen)) return;

      // Adapters honour fromVersion only on the first request, so account and
      // handle walks, which start below it, are bounded here on every page.
      const floor = this.floor;
      if (floor !== undefined && !this.reachedStart) {
        this.reachedStart =
          (page.throughVersion !== undefined && page.throughVersion >= floor) ||
          page.events.some((e) => e.version >= floor);
      }
      const bounded =
        floor === undefined ? page : { ...page, events: page.events.filter((e) => e.version >= floor) };

      const delivery = this.processPage(bounded, gen, viewer, run);
      // A failed page is this poll's error alone; later polls must not inherit it.
      this.delivering = delivery.catch(() => {});
      this.handlers = this.delivering;
      await delivery;
      // The callback may have stopped or restarted the poller.
      if (!this.isCurrent(gen)) return;
      this.consecutiveErrors = 0;
      this.currentInterval = this.pollInterval;
      this.lastSuccessAt = Date.now();
      if (!page.hasMore) this.signalCaughtUp();
      // onCaughtUp or its error handler may have stopped or paused the poller.
      if (!this.isCurrent(gen)) return;

      // Empty is what the source returned, before `where`: filtered events
      // still show it is busy.
      const empty = page.events.length === 0;
      if (!empty) this.quietInterval = this.pollInterval;
      // Catch-up mode: the adapter says more is waiting, so don't idle.
      this.schedulePoll(page.hasMore ? 0 : this.idleDelay(empty));
    } catch (err) {
      this.handlePollError(err, gen);
    }
  }

  private idleDelay(empty: boolean): number {
    const delay = this.quietInterval;
    if (empty && this.maxPollInterval !== undefined) {
      const cap = Math.max(this.maxPollInterval, this.pollInterval);
      this.quietInterval = Math.min(Math.max(delay * 2, 1), cap);
    }
    return delay;
  }

  private signalCaughtUp(): void {
    if (this.caughtUp || this.onCaughtUp === undefined) return;
    this.caughtUp = true;
    try {
      this.onCaughtUp();
    } catch (err) {
      this.report(new StreamdError("onCaughtUp threw", err));
    }
  }

  private handlePollError(err: unknown, gen: number): void {
    if (!this.isCurrent(gen)) return;
    this.consecutiveErrors++;

    // A StreamdError that is not a network failure is the caller's input,
    // which fails the same way on every retry.
    const invalidInput =
      err instanceof StreamdError && !(err instanceof StreamdNetworkError);
    const pollingError = new StreamdPollingError(
      invalidInput
        ? `Polling stopped on invalid input: ${err.message}`
        : `Polling failed (${this.consecutiveErrors}/${this.maxErrors})`,
      this.consecutiveErrors,
      err,
    );

    if (invalidInput || this.consecutiveErrors >= this.maxErrors) {
      // Stop before notifying, so a handler inspecting `active` sees the
      // subscription already dead rather than briefly alive.
      this.stop();
      this.report(pollingError);
      return;
    }

    this.report(pollingError);
    // The handler may have stopped or restarted the poller.
    if (!this.isCurrent(gen)) return;

    // Exponential backoff capped at maxBackoff. A source's Retry-After hint
    // replaces this one wait, under the same cap; the sequence still advances.
    this.currentInterval = Math.min(this.currentInterval * 2, this.maxBackoff);
    const hint = retryAfterHint(err);
    this.schedulePoll(
      hint === undefined ? this.currentInterval : Math.min(hint, this.maxBackoff),
    );
  }

  private async processPage(
    page: EventPage,
    gen: number,
    adapter: EventAdapter,
    run: AbortSignal,
  ): Promise<void> {
    const batch: StreamdEvent[] = [];
    const batched: StreamdEvent[] = [];
    // Dedups the batch's events until they are marked seen on delivery.
    const pending = new CursorManager(undefined, this.dedupWindow);
    // A rejected predicate or failed view is the source's: it fails this poll
    // with the event unmarked, so a network failure's retry delivers it rather
    // than losing it.
    let failure: unknown;
    for (const raw of page.events) {
      // The callback itself may have unsubscribed.
      if (!this.isCurrent(gen)) return;
      if (this.position.hasSeen(raw) || pending.hasSeen(raw)) continue;
      let prepared = this.prepare(raw);
      if (prepared !== undefined && "verdict" in prepared) {
        let matched: unknown;
        try {
          // Raced against the run, so a hung predicate never holds a restart.
          matched = await raceAbort(Promise.resolve(prepared.verdict), run);
        } catch (err) {
          failure = err;
          break;
        }
        if (!this.isCurrent(gen)) return;
        prepared = this.admit(raw, prepared.event, matched);
      }
      if (prepared === undefined) {
        if (!this.isCurrent(gen)) return;
        continue;
      }
      let event = prepared.event;
      if (prepared.request !== undefined) {
        try {
          event = await readState(adapter, this.descriptor, event, prepared.request);
        } catch (err) {
          failure = err;
          break;
        }
        if (!this.isCurrent(gen)) return;
      }
      if (this.nameTtl !== undefined) {
        try {
          event = await this.names!.attach(event, this.nameTtl, run);
        } catch (err) {
          failure = err;
          break;
        }
        if (!this.isCurrent(gen)) return;
      }
      if (this.callback === undefined) {
        // Marked seen only once handed over, so a batch a pause abandons is redelivered.
        batch.push(event);
        batched.push(raw);
        pending.mark(raw);
        continue;
      }
      this.see(raw);
      try {
        // Awaited one event at a time, so a slow handler holds back the rest
        // of the page and the next poll rather than piling up work.
        const result: unknown = this.callback!(event);
        if (isThenable(result)) await result;
      } catch (err) {
        this.reportHandlerError(callbackError(event, err));
        if (!this.isCurrent(gen)) return;
      }
    }

    if (this.onPage !== undefined) {
      await this.handOver(batch, batched, page, gen, failure);
    } else if (batch.length > 0 && this.isCurrent(gen)) {
      // Delivered even when the page failed: these events are marked seen, so a
      // retried page would skip them.
      for (const raw of batched) this.see(raw);
      try {
        const result: unknown = this.onBatch!(batch);
        if (isThenable(result)) await result;
      } catch (err) {
        this.reportHandlerError(batchError(batch, err));
      }
    }
    if (failure !== undefined) throw failure;

    // A run stopped while a handler was pending must not move the cursor past
    // a page it did not finish: resuming from it delivers the page again.
    if (!this.isCurrent(gen)) return;

    // Advance even on an empty page — the adapter reports how far it scanned,
    // not where the last match was. Without this, an event type sparser than
    // the scan window would re-read the same transactions forever.
    if (page.nextCursor !== undefined) {
      this.position.token = page.nextCursor;
    }
  }

  /**
   * A failed page hands over nothing and leaves its events unmarked, so the
   * retry re-reads them: the page callback needs whole pages to know what is
   * settled.
   */
  private async handOver(
    events: StreamdEvent[],
    raws: StreamdEvent[],
    page: EventPage,
    gen: number,
    failure: unknown,
  ): Promise<void> {
    if (failure !== undefined || !this.isCurrent(gen)) return;
    for (const raw of raws) this.see(raw);
    try {
      const result: unknown = this.onPage!(events, page);
      if (isThenable(result)) await result;
    } catch (err) {
      this.reportHandlerError(new StreamdError("Page callback threw", err));
    }
  }

  /**
   * `raw` decoded, filtered and with its view request built, `undefined` when
   * it is not to be delivered, or the async predicate's pending `verdict` for
   * `admit` once it settles. A skipped event is marked processed, so
   * overlapping pages do not re-test it. A decode, predicate or builder throw
   * is the caller's, not the source's: reported without counting toward
   * maxErrors or abandoning the rest of the page.
   */
  private prepare(
    raw: StreamdEvent,
  ): Prepared | { event: StreamdEvent; verdict: PromiseLike<boolean> } | undefined {
    let event = raw;
    let verdict: boolean | PromiseLike<boolean> = true;
    try {
      if (this.decode !== undefined) event = this.decode(raw);
      if (this.where !== undefined) verdict = this.where(event);
    } catch (err) {
      this.see(raw);
      this.reportHandlerError(callbackError(event, err));
      return undefined;
    }
    return isThenable(verdict) ? { event, verdict } : this.admit(raw, event, verdict);
  }

  /** `event` with its view request built when `matched`, else marked processed and skipped. */
  private admit(raw: StreamdEvent, event: StreamdEvent, matched: unknown): Prepared | undefined {
    if (!matched) {
      this.see(raw);
      return undefined;
    }
    try {
      const request =
        this.withState === undefined ? undefined : buildStateRequest(this.withState, event);
      return { event, request };
    } catch (err) {
      this.see(raw);
      this.reportHandlerError(callbackError(event, err));
      return undefined;
    }
  }

  private see(raw: StreamdEvent): void {
    this.position.mark(raw);
    if (this.lastVersion === undefined || raw.version > this.lastVersion) {
      this.lastVersion = raw.version;
    }
  }

  /**
   * Reported while any run is live or paused: the handler is the same one,
   * and this path touches neither the cursor nor the error budget.
   */
  private reportHandlerError(error: StreamdError): void {
    if (this._active || this._paused) this.report(error);
  }
}

function callbackError(event: StreamdEvent, cause: unknown): StreamdError {
  return new StreamdError(
    `Event filter or callback threw for ${event.version}:${event.eventIndex}`,
    cause,
  );
}

function batchError(events: StreamdEvent[], cause: unknown): StreamdError {
  const first = events[0]!;
  const last = events[events.length - 1]!;
  return new StreamdError(
    `Batch callback threw for ${events.length} events from ${first.version}:${first.eventIndex} to ${last.version}:${last.eventIndex}`,
    cause,
  );
}
