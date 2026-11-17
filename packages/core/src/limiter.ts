import type { EventAdapter } from "./types.js";
import { StreamdError, StreamdNetworkError } from "./errors.js";
import { isPrimaryOnly, withPrimaryOnly } from "./time.js";
import { onAbort, raceAbort } from "./utils.js";

const SELF_TIMED = new WeakSet<EventAdapter>();
const DESCENDING = new WeakSet<EventAdapter>();

/**
 * Mark `adapter` as applying `requestTimeout` to each internal call on its
 * own, so `RequestLimiter.wrap` enforces concurrency and rate spacing on
 * `fetchEvents` without aborting an entire multi-read page on one timeout.
 */
export function markSelfTimed<T extends EventAdapter>(adapter: T): T {
  SELF_TIMED.add(adapter);
  return adapter;
}

/**
 * Mark `adapter` as natively serving `orderBy: "desc"` pages from newest to
 * oldest so `EventQuery` can page backward rather than scanning from version 0.
 */
export function markDescending<T extends EventAdapter>(adapter: T): T {
  DESCENDING.add(adapter);
  return adapter;
}

/** Whether `adapter` natively pages descending when asked for `orderBy: "desc"`. */
export function supportsDescending(adapter: EventAdapter): boolean {
  return DESCENDING.has(adapter);
}

interface Waiter {
  start: () => void;
  reject: (reason: unknown) => void;
}

/**
 * Caps how many adapter calls are in flight at once and spaces their starts.
 * Calls past either limit wait in one FIFO queue; a waiting call whose `drop`
 * signal aborts leaves the queue at once, so a stopped subscription costs the
 * source nothing and holds nothing.
 * A call past `timeout` rejects and frees its slot, though the adapter call
 * itself, which takes no signal, runs on uncounted.
 */
export class RequestLimiter {
  private active = 0;
  private readonly queue: Waiter[] = [];
  private readonly interval: number;
  private earliest = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly max = Infinity,
    perSecond = Infinity,
    private readonly timeout?: number,
  ) {
    this.interval = 1000 / perSecond;
  }

  /**
   * `drop` removes the call while it still waits, rejecting with its reason.
   * `signal` also abandons a started task: its promise rejects and its slot
   * frees, though the adapter call itself, which takes no signal, runs on.
   * `untimed` skips this limiter's `timeout` wrapper when a self-timed
   * adapter bounds its own internal reads.
   */
  run<T>(task: () => Promise<T>, drop?: AbortSignal, signal?: AbortSignal, untimed = false): Promise<T> {
    const primaryOnly = isPrimaryOnly();
    return new Promise<T>((resolve, reject) => {
      const start = () => {
        this.active++;
        if (this.interval > 0) this.earliest = performance.now() + this.interval;
        // Wrapped so a task that throws synchronously still frees its slot.
        const started = new Promise<T>((r) => r(primaryOnly ? withPrimaryOnly(task) : task()));
        const settled = untimed ? started : this.withTimeout(started);
        raceAbort(settled, signal)
          .then(resolve, reject)
          .finally(() => {
            this.active--;
            this.next();
          });
      };
      // Checked first: a call made after its caller gave up must not start either.
      if (drop?.aborted) return reject(drop.reason);
      if (this.queue.length === 0 && this.active < this.max && this.delay() <= 0) return start();
      const waiter: Waiter = {
        start: () => (detach(), start()),
        reject: (reason) => (detach(), reject(reason)),
      };
      const detach = onAbort(drop, () => {
        this.queue.splice(this.queue.indexOf(waiter), 1);
        reject(drop!.reason);
      });
      this.queue.push(waiter);
      this.next();
    });
  }

  /** Reject every waiting call, so a destroyed client holds none. */
  clear(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
    for (const waiter of this.queue.splice(0)) {
      waiter.reject(new StreamdError("Request dropped: the client was destroyed"));
    }
  }

  /** An adapter view whose calls go through this limiter. */
  wrap(adapter: EventAdapter, drop?: AbortSignal, signal?: AbortSignal): EventAdapter {
    const untimed = SELF_TIMED.has(adapter);
    const view: EventAdapter = {
      fetchEvents: (descriptor, options) =>
        this.run(() => adapter.fetchEvents(descriptor, options), drop, signal, untimed),
    };
    if (adapter.getHeadVersion !== undefined) {
      view.getHeadVersion = (descriptor) =>
        this.run(() => adapter.getHeadVersion!(descriptor), drop, signal);
    }
    if (adapter.getEarliestVersion !== undefined) {
      view.getEarliestVersion = (descriptor) =>
        this.run(() => adapter.getEarliestVersion!(descriptor), drop, signal);
    }
    if (adapter.getBlock !== undefined) {
      view.getBlock = (descriptor, height) =>
        this.run(() => adapter.getBlock!(descriptor, height), drop, signal);
    }
    if (adapter.getBlockHeights !== undefined) {
      view.getBlockHeights = (descriptor) =>
        this.run(() => adapter.getBlockHeights!(descriptor), drop, signal);
    }
    if (adapter.getTransactionEvents !== undefined) {
      view.getTransactionEvents = (hash, descriptor) =>
        this.run(() => adapter.getTransactionEvents!(hash, descriptor), drop, signal);
    }
    if (adapter.getTransactionEventsByVersion !== undefined) {
      view.getTransactionEventsByVersion = (version, descriptor) =>
        this.run(() => adapter.getTransactionEventsByVersion!(version, descriptor), drop, signal);
    }
    if (adapter.simulateTransactionEvents !== undefined) {
      view.simulateTransactionEvents = (simulation, descriptor) =>
        this.run(() => adapter.simulateTransactionEvents!(simulation, descriptor), drop, signal);
    }
    if (adapter.view !== undefined) {
      view.view = (request, ledgerVersion, descriptor) =>
        this.run(() => adapter.view!(request, ledgerVersion, descriptor), drop, signal);
    }
    if (adapter.readObjects !== undefined) {
      view.readObjects = (request, at, descriptor) =>
        this.run(() => adapter.readObjects!(request, at, descriptor), drop, signal);
    }
    return view;
  }

  // A network failure, because pollers and query retries stop at once on any other StreamdError.
  private withTimeout<T>(work: Promise<T>): Promise<T> {
    const ms = this.timeout;
    if (ms === undefined) return work;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new StreamdNetworkError(`Adapter call timed out after ${ms}ms`)),
        ms,
      );
      work.then(resolve, reject).finally(() => clearTimeout(timer));
    });
  }

  private delay(): number {
    return this.earliest - performance.now();
  }

  private next(): void {
    while (this.active < this.max && this.queue.length > 0) {
      const delay = this.delay();
      if (delay > 0) {
        this.timer ??= setTimeout(() => {
          this.timer = undefined;
          this.next();
        }, Math.ceil(delay));
        return;
      }
      this.queue.shift()!.start();
    }
  }
}
