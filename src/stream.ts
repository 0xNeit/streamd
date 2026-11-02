import type { EventCallback, StreamdEvent, Subscription } from "./types.js";

type Subscribe<TData> = (
  callback: EventCallback<TData>,
  onError: (error: Error) => void,
) => Subscription;

interface Waiter<TData> {
  resolve(result: IteratorResult<StreamdEvent<TData>>): void;
  reject(error: unknown): void;
}

const DONE = { value: undefined, done: true } as const;

/**
 * A single-consumer async iterator over a live subscription.
 *
 * It subscribes on the first `next()`, and each event's handler stays pending
 * until the consumer asks for the next one, so the poller (which awaits
 * handlers) neither fetches ahead nor moves the cursor past an event the loop
 * body has not finished. `onClose` runs once, however the stream ends.
 *
 * Because the poller only runs again once `next()` has released the previous
 * handler, and `next()` sets the waiter in that same call, every event and
 * every fatal error finds a waiting `next()`.
 */
export function eventStream<TData>(
  subscribe: Subscribe<TData>,
  onClose: () => void = () => {},
): AsyncIterableIterator<StreamdEvent<TData>> {
  let sub: Subscription | undefined;
  let waiter: Waiter<TData> | undefined;
  let release: (() => void) | undefined;
  let closed = false;

  const take = (): Waiter<TData> => {
    const w = waiter!;
    waiter = undefined;
    return w;
  };

  const finish = (error?: unknown) => {
    if (closed) return;
    closed = true;
    sub?.unsubscribe();
    release?.();
    release = undefined;
    if (waiter !== undefined) {
      const w = take();
      if (error !== undefined) w.reject(error);
      else w.resolve(DONE);
    }
    onClose();
  };

  const start = () =>
    subscribe(
      (event) =>
        new Promise<void>((done) => {
          release = done;
          take().resolve({ value: event, done: false });
        }),
      (error) => {
        // Only a subscription that stopped itself ends the stream.
        if (sub !== undefined && !sub.active) finish(error);
      },
    );

  return {
    next() {
      if (waiter !== undefined) {
        return Promise.reject(new TypeError("stream() has one consumer: await each next()"));
      }
      if (closed) return Promise.resolve(DONE);
      return new Promise((resolve, reject) => {
        waiter = { resolve, reject };
        // Asking for the next event means the loop body finished the last one.
        const finished = release;
        release = undefined;
        sub ??= start();
        finished?.();
      });
    },
    return() {
      finish();
      return Promise.resolve(DONE);
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}
