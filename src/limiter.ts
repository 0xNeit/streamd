import type { EventAdapter } from "./types.js";
import { StreamdError } from "./errors.js";
import { onAbort, raceAbort } from "./utils.js";

interface Waiter {
  start: () => void;
  reject: (reason: unknown) => void;
}

/**
 * Caps how many adapter calls are in flight at once. Calls past the cap wait
 * in one FIFO queue; a waiting call whose `drop` signal aborts leaves the queue
 * at once, so a stopped subscription costs the source nothing and holds nothing.
 */
export class RequestLimiter {
  private active = 0;
  private readonly queue: Waiter[] = [];

  constructor(private readonly max = Infinity) {}

  /**
   * `drop` removes the call while it still waits, rejecting with its reason.
   * `signal` also abandons a started task: its promise rejects and its slot
   * frees, though the adapter call itself, which takes no signal, runs on.
   */
  run<T>(task: () => Promise<T>, drop?: AbortSignal, signal?: AbortSignal): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const start = () => {
        this.active++;
        // Wrapped so a task that throws synchronously still frees its slot.
        raceAbort(new Promise<T>((r) => r(task())), signal)
          .then(resolve, reject)
          .finally(() => {
            this.active--;
            this.next();
          });
      };
      if (this.active < this.max) return start();
      if (drop?.aborted) return reject(drop.reason);
      const waiter: Waiter = {
        start: () => (detach(), start()),
        reject: (reason) => (detach(), reject(reason)),
      };
      const detach = onAbort(drop, () => {
        this.queue.splice(this.queue.indexOf(waiter), 1);
        reject(drop!.reason);
      });
      this.queue.push(waiter);
    });
  }

  /** Reject every waiting call, so a destroyed client holds none. */
  clear(): void {
    for (const waiter of this.queue.splice(0)) {
      waiter.reject(new StreamdError("Request dropped: the client was destroyed"));
    }
  }

  /** An adapter view whose calls go through this limiter. */
  wrap(adapter: EventAdapter, drop?: AbortSignal, signal?: AbortSignal): EventAdapter {
    const view: EventAdapter = {
      fetchEvents: (descriptor, options) =>
        this.run(() => adapter.fetchEvents(descriptor, options), drop, signal),
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
    if (adapter.simulateTransactionEvents !== undefined) {
      view.simulateTransactionEvents = (simulation, descriptor) =>
        this.run(() => adapter.simulateTransactionEvents!(simulation, descriptor), drop, signal);
    }
    if (adapter.view !== undefined) {
      view.view = (request, ledgerVersion, descriptor) =>
        this.run(() => adapter.view!(request, ledgerVersion, descriptor), drop, signal);
    }
    return view;
  }

  private next(): void {
    while (this.active < this.max && this.queue.length > 0) {
      this.queue.shift()!.start();
    }
  }
}
