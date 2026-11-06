import type { EventAdapter } from "./types.js";
import { StreamdError } from "./errors.js";

interface Waiter {
  start: () => void;
  drop: () => void;
  cancelled: () => boolean;
}

/**
 * Caps how many adapter calls are in flight at once. Calls past the cap wait
 * in one FIFO queue; a waiting call whose caller no longer wants it is dropped
 * before it starts, so a stopped subscription costs the source nothing.
 */
export class RequestLimiter {
  private active = 0;
  private readonly queue: Waiter[] = [];

  constructor(private readonly max = Infinity) {}

  run<T>(task: () => Promise<T>, cancelled: () => boolean = () => false): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const start = () => {
        this.active++;
        // Wrapped so a task that throws synchronously still frees its slot.
        new Promise<T>((r) => r(task()))
          .then(resolve, reject)
          .finally(() => {
            this.active--;
            this.next();
          });
      };
      const drop = () => reject(new StreamdError("Request dropped before it started"));
      if (this.active < this.max) start();
      else this.queue.push({ start, drop, cancelled });
    });
  }

  /** An adapter view whose calls go through this limiter. */
  wrap(adapter: EventAdapter, cancelled?: () => boolean): EventAdapter {
    const view: EventAdapter = {
      fetchEvents: (descriptor, options) =>
        this.run(() => adapter.fetchEvents(descriptor, options), cancelled),
    };
    if (adapter.getHeadVersion !== undefined) {
      view.getHeadVersion = (descriptor) =>
        this.run(() => adapter.getHeadVersion!(descriptor), cancelled);
    }
    if (adapter.getEarliestVersion !== undefined) {
      view.getEarliestVersion = (descriptor) =>
        this.run(() => adapter.getEarliestVersion!(descriptor), cancelled);
    }
    if (adapter.getBlock !== undefined) {
      view.getBlock = (descriptor, height) =>
        this.run(() => adapter.getBlock!(descriptor, height), cancelled);
    }
    if (adapter.getBlockHeights !== undefined) {
      view.getBlockHeights = (descriptor) =>
        this.run(() => adapter.getBlockHeights!(descriptor), cancelled);
    }
    return view;
  }

  private next(): void {
    while (this.active < this.max && this.queue.length > 0) {
      const waiter = this.queue.shift()!;
      if (waiter.cancelled()) waiter.drop();
      else waiter.start();
    }
  }
}
