import type { EventCallback, EventStream, StreamdEvent, Subscription } from "./types.js";
import { onAbort } from "./utils.js";

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
 * Every event and every fatal error finds a waiting `next()`: the poller only
 * runs again once `next()` has released the previous handler, `next()` sets
 * the waiter in that same synchronous call, and the poller resumes on a later
 * microtask. An `AbortSignal` is the one exception: it can fire while the loop
 * body is running, so its reason is held for the next `next()`.
 */
export function eventStream<TData>(
  subscribe: Subscribe<TData>,
  onClose: () => void = () => {},
  signal?: AbortSignal,
  startCursor?: string,
): EventStream<TData> {
  let sub: Subscription | undefined;
  let waiter: Waiter<TData> | undefined;
  let release: (() => void) | undefined;
  let failure: { reason: unknown } | undefined;
  let detach = () => {};
  let closed = false;

  const take = (): Waiter<TData> => {
    const w = waiter!;
    waiter = undefined;
    return w;
  };

  const finish = (err?: { reason: unknown }) => {
    if (closed) return;
    closed = true;
    detach();
    sub?.unsubscribe();
    release?.();
    release = undefined;
    if (waiter !== undefined) {
      const w = take();
      if (err !== undefined) w.reject(err.reason);
      else w.resolve(DONE);
    } else {
      failure = err;
    }
    onClose();
  };

  const start = () => {
    detach = onAbort(signal, () => finish({ reason: signal!.reason }));
    return subscribe(
      (event) =>
        new Promise<void>((done) => {
          release = done;
          take().resolve({ value: event, done: false });
        }),
      (error) => {
        // Only a subscription that stopped itself ends the stream.
        if (sub !== undefined && !sub.active) finish({ reason: error });
      },
    );
  };

  return {
    next() {
      if (waiter !== undefined) {
        return Promise.reject(new TypeError("stream() has one consumer: await each next()"));
      }
      if (!closed && signal?.aborted) finish({ reason: signal.reason });
      if (failure !== undefined) {
        const { reason } = failure;
        failure = undefined;
        return Promise.reject(reason);
      }
      if (closed) return Promise.resolve(DONE);
      return new Promise((resolve, reject) => {
        waiter = { resolve, reject };
        // Asking for the next event means the loop body finished the last one.
        const finished = release;
        release = undefined;
        try {
          sub ??= start();
        } catch (err) {
          return finish({ reason: err });
        }
        finished?.();
      });
    },
    return() {
      failure = undefined;
      finish();
      return Promise.resolve(DONE);
    },
    get cursor() {
      return sub === undefined ? startCursor : sub.cursor;
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}
