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
  ViewRequest,
} from "./types.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  retryAfterHint,
} from "./errors.js";
import { CursorManager } from "./cursor.js";
import { resolveFromVersion } from "./utils.js";
import { RequestLimiter } from "./limiter.js";
import { fieldDecoder } from "./decode.js";
import { resolveFromTime, validateFromTime } from "./time.js";
import { buildStateRequest, readState, validateWithState } from "./state.js";

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

/** One event at a time, or `{ batch }` for each page's new events in one call. */
export type Delivery = EventCallback | { batch: BatchCallback };

export class EventPoller {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveErrors = 0;
  private lastVersion: bigint | undefined = undefined;
  private lastSuccessAt: number | undefined = undefined;
  private currentInterval: number;
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
  /** Aborted when this run ends, so a hung view never holds the next run or its request slot. */
  private runAbort = new AbortController();
  private readonly position: CursorManager;
  private readonly callback?: EventCallback;
  private readonly onBatch?: BatchCallback;

  private readonly pollInterval: number;
  private readonly batchSize: number;
  private readonly maxErrors: number;
  private readonly maxBackoff: number;
  /** Absolute once the first poll has resolved a relative or earliest start. */
  private fromVersion?: bigint | "earliest";
  /** Cleared once the first poll has resolved it into `fromVersion`. */
  private fromTime?: bigint;
  private readonly where?: EventPredicate;
  private readonly decode?: (event: StreamdEvent) => StreamdEvent;
  private readonly withState?: StateRequestBuilder;
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
  ) {
    if (typeof delivery === "function") this.callback = delivery;
    else this.onBatch = delivery.batch;
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.maxErrors = options.maxErrors ?? DEFAULT_MAX_ERRORS;
    this.maxBackoff = resolveMaxBackoff(options.maxBackoff);
    validateFromTime(options);
    this.fromVersion = options.fromVersion;
    this.fromTime = options.fromTime;
    this.where = options.where;
    this.decode = fieldDecoder(options.decode);
    validateWithState(options.withState);
    this.withState = options.withState;
    this.onError = options.onError;
    this.onCaughtUp = options.onCaughtUp;
    this.signal = options.signal;
    this.currentInterval = this.pollInterval;
    this.position = new CursorManager(options.cursor, options.dedupWindow);
  }

  get active(): boolean {
    return this._active;
  }

  /** The adapter's resume token after the last finished page; see `ListenerOptions.cursor`. */
  get cursor(): string | undefined {
    return this.position.token;
  }

  /** Snapshot of the poller's current state; see `SubscriptionStatus`. */
  get status(): SubscriptionStatus {
    return {
      active: this._active,
      cursor: this.position.token,
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
    // A call still queued once this run has ended is dropped unstarted.
    const cancelled = () => !this.isCurrent(gen);
    const adapter = this.limiter.wrap(this.adapter, cancelled);
    // Fetches are left to settle, since a restart waits for them; views are not.
    const viewer = this.limiter.wrap(this.adapter, cancelled, this.runAbort.signal);
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

      const delivery = this.processPage(page, gen, viewer);
      // A failed page is this poll's error alone; later polls must not inherit it.
      this.delivering = delivery.catch(() => {});
      await delivery;
      // The callback may have stopped or restarted the poller.
      if (!this.isCurrent(gen)) return;
      this.consecutiveErrors = 0;
      this.currentInterval = this.pollInterval;
      this.lastSuccessAt = Date.now();
      if (!page.hasMore) this.signalCaughtUp();
      // onCaughtUp or its error handler may have stopped or paused the poller.
      if (!this.isCurrent(gen)) return;

      // Catch-up mode: the adapter says more is waiting, so don't idle.
      this.schedulePoll(page.hasMore ? 0 : this.pollInterval);
    } catch (err) {
      this.handlePollError(err, gen);
    }
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

  private async processPage(page: EventPage, gen: number, adapter: EventAdapter): Promise<void> {
    const batch: StreamdEvent[] = [];
    let viewFailure: unknown;
    for (const raw of page.events) {
      // The callback itself may have unsubscribed.
      if (!this.isCurrent(gen)) return;
      if (this.position.hasSeen(raw)) continue;
      const prepared = this.prepare(raw);
      if (prepared === undefined) {
        if (!this.isCurrent(gen)) return;
        continue;
      }
      let event = prepared.event;
      if (prepared.request !== undefined) {
        // A failed view is the source's: it fails this poll with the event
        // unmarked, so a network failure's retry delivers it rather than losing it.
        try {
          event = await readState(adapter, this.descriptor, event, prepared.request);
        } catch (err) {
          viewFailure = err;
          break;
        }
        if (!this.isCurrent(gen)) return;
      }
      this.see(raw);
      if (this.onBatch !== undefined) {
        batch.push(event);
        continue;
      }
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

    // Delivered even when a view failed: these events are marked seen, so a
    // retried page would skip them.
    if (batch.length > 0 && this.isCurrent(gen)) {
      try {
        const result: unknown = this.onBatch!(batch);
        if (isThenable(result)) await result;
      } catch (err) {
        this.reportHandlerError(batchError(batch, err));
      }
    }
    if (viewFailure !== undefined) throw viewFailure;

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
   * `raw` decoded, filtered and with its view request built, or `undefined`
   * when it is not to be delivered. A skipped event is marked processed, so
   * overlapping pages do not re-test it. A decode, predicate or builder bug
   * is the caller's, not the source's: reported without counting toward
   * maxErrors or abandoning the rest of the page.
   */
  private prepare(raw: StreamdEvent): { event: StreamdEvent; request?: ViewRequest } | undefined {
    let event = raw;
    try {
      if (this.decode !== undefined) event = this.decode(raw);
      if (this.where !== undefined && !this.where(event)) {
        this.see(raw);
        return undefined;
      }
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

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
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
