import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventPage,
  EventPredicate,
  ListenerOptions,
  StreamdEvent,
} from "./types.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
} from "./errors.js";
import { CursorManager } from "./cursor.js";

const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_BATCH_SIZE = 25;
const DEFAULT_MAX_ERRORS = 10;
const MAX_BACKOFF = 60_000;

export class EventPoller {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveErrors = 0;
  private currentInterval: number;
  private _active = false;
  /** Bumped by start(), so a poll left over from an earlier run can tell. */
  private generation = 0;
  private readonly position: CursorManager;

  private readonly pollInterval: number;
  private readonly batchSize: number;
  private readonly maxErrors: number;
  private readonly fromVersion?: bigint;
  private readonly where?: EventPredicate;
  private readonly onError?: (error: Error) => void;

  constructor(
    private readonly adapter: EventAdapter,
    private readonly descriptor: EventDescriptor,
    private readonly callback: EventCallback,
    options: ListenerOptions = {},
  ) {
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL;
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.maxErrors = options.maxErrors ?? DEFAULT_MAX_ERRORS;
    this.fromVersion = options.fromVersion;
    this.where = options.where;
    this.onError = options.onError;
    this.currentInterval = this.pollInterval;
    this.position = new CursorManager(options.cursor);
  }

  get active(): boolean {
    return this._active;
  }

  /** The adapter's resume token after the last finished page; see `ListenerOptions.cursor`. */
  get cursor(): string | undefined {
    return this.position.token;
  }

  start(): void {
    if (this._active) return;
    this._active = true;
    this.generation++;
    this.schedulePoll(0);
  }

  stop(): void {
    this._active = false;
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
    try {
      const page = await this.adapter.fetchEvents(this.descriptor, {
        // Once we have a cursor it supersedes fromVersion, which only seeds
        // the very first poll.
        cursor: this.position.token,
        fromVersion:
          this.position.token === undefined ? this.fromVersion : undefined,
        limit: this.batchSize,
        orderBy: "asc",
      });
      // A fetch that resolves after stop(), or after stop() and start(), must
      // not touch the cursor or the error budget the live run resumes with.
      if (!this.isCurrent(gen)) return;

      this.processPage(page, gen);
      // The callback may have stopped or restarted the poller.
      if (!this.isCurrent(gen)) return;
      this.consecutiveErrors = 0;
      this.currentInterval = this.pollInterval;

      // Catch-up mode: the adapter says more is waiting, so don't idle.
      this.schedulePoll(page.hasMore ? 0 : this.pollInterval);
    } catch (err) {
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

      // Exponential backoff capped at MAX_BACKOFF
      this.currentInterval = Math.min(this.currentInterval * 2, MAX_BACKOFF);
      this.schedulePoll(this.currentInterval);
    }
  }

  private processPage(page: EventPage, gen: number): void {
    for (const event of page.events) {
      // The callback itself may have unsubscribed.
      if (!this.isCurrent(gen)) return;
      if (this.position.hasSeen(event)) continue;
      // Mark before filtering: a rejected event has still been processed, and
      // marking it keeps overlapping pages from re-testing it.
      this.position.mark(event);
      // A predicate or handler bug is the caller's, not the source's: report
      // it without counting toward maxErrors or abandoning the rest of the page.
      try {
        if (this.where !== undefined && !this.where(event)) continue;
        const result: unknown = this.callback(event);
        // An async callback's rejection never reaches this catch, so it gets
        // its own handler. Delivery does not wait for it.
        if (isThenable(result)) {
          // Reported while any run is live: the handler is the same one, and
          // this path touches neither the cursor nor the error budget.
          result.then(undefined, (err: unknown) => {
            if (this._active) this.report(callbackError(event, err));
          });
        }
      } catch (err) {
        if (this._active) this.report(callbackError(event, err));
        if (!this.isCurrent(gen)) return;
      }
    }

    // Advance even on an empty page — the adapter reports how far it scanned,
    // not where the last match was. Without this, an event type sparser than
    // the scan window would re-read the same transactions forever.
    if (page.nextCursor !== undefined) {
      this.position.token = page.nextCursor;
    }
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
