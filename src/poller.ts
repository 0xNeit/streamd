import type {
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventPage,
  EventPredicate,
  ListenerOptions,
} from "./types.js";
import { StreamdError, StreamdPollingError } from "./errors.js";
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
  private cursor: CursorManager;

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
    this.cursor = new CursorManager();
  }

  get active(): boolean {
    return this._active;
  }

  start(): void {
    if (this._active) return;
    this._active = true;
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

  private async poll(): Promise<void> {
    if (!this._active) return;

    try {
      const page = await this.adapter.fetchEvents(this.descriptor, {
        // Once we have a cursor it supersedes fromVersion, which only seeds
        // the very first poll.
        cursor: this.cursor.token,
        fromVersion:
          this.cursor.token === undefined ? this.fromVersion : undefined,
        limit: this.batchSize,
        orderBy: "asc",
      });
      // A fetch that resolves after stop() must not touch the cursor or the
      // error budget a later start() resumes with.
      if (!this._active) return;

      this.processPage(page);
      this.consecutiveErrors = 0;
      this.currentInterval = this.pollInterval;

      // Catch-up mode: the adapter says more is waiting, so don't idle.
      this.schedulePoll(page.hasMore ? 0 : this.pollInterval);
    } catch (err) {
      if (!this._active) return;
      this.consecutiveErrors++;

      const pollingError = new StreamdPollingError(
        `Polling failed (${this.consecutiveErrors}/${this.maxErrors})`,
        this.consecutiveErrors,
        err,
      );

      if (this.consecutiveErrors >= this.maxErrors) {
        // Stop before notifying, so a handler inspecting `active` sees the
        // subscription already dead rather than briefly alive.
        this.stop();
        this.onError?.(pollingError);
        return;
      }

      this.onError?.(pollingError);

      // Exponential backoff capped at MAX_BACKOFF
      this.currentInterval = Math.min(this.currentInterval * 2, MAX_BACKOFF);
      this.schedulePoll(this.currentInterval);
    }
  }

  private processPage(page: EventPage): void {
    for (const event of page.events) {
      // The callback itself may have unsubscribed.
      if (!this._active) return;
      if (this.cursor.hasSeen(event)) continue;
      // Mark before filtering: a rejected event has still been processed, and
      // marking it keeps overlapping pages from re-testing it.
      this.cursor.mark(event);
      // A predicate or handler bug is the caller's, not the source's: report
      // it without counting toward maxErrors or abandoning the rest of the page.
      try {
        if (this.where !== undefined && !this.where(event)) continue;
        this.callback(event);
      } catch (err) {
        if (!this._active) return;
        this.onError?.(
          new StreamdError(
            `Event filter or callback threw for ${event.version}:${event.eventIndex}`,
            err,
          ),
        );
      }
    }

    // Advance even on an empty page — the adapter reports how far it scanned,
    // not where the last match was. Without this, an event type sparser than
    // the scan window would re-read the same transactions forever.
    if (page.nextCursor !== undefined) {
      this.cursor.token = page.nextCursor;
    }
  }
}
