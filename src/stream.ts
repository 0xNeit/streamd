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
 */
export function eventStream<TData>(
  subscribe: Subscribe<TData>,
  onClose: () => void = () => {},
): AsyncIterableIterator<StreamdEvent<TData>> {
  let sub: Subscription | undefined;
  let waiter: Waiter<TData> | undefined;
  let offered: { event: StreamdEvent<TData>; done: () => void } | undefined;
  let release: (() => void) | undefined;
  let failure: { error: unknown } | undefined;
  let closed = false;

  const hand = (event: StreamdEvent<TData>, done: () => void) => {
    release = done;
    const w = waiter!;
    waiter = undefined;
    w.resolve({ value: event, done: false });
  };

  // Settles everything still pending; a failure goes to a waiting next(), or
  // is kept for the next call when nobody is waiting.
  const finish = (error?: { error: unknown }) => {
    if (closed) return;
    closed = true;
    sub?.unsubscribe();
    release?.();
    offered?.done();
    release = offered = undefined;
    if (waiter !== undefined) {
      if (error !== undefined) waiter.reject(error.error);
      else waiter.resolve(DONE);
      waiter = undefined;
    } else {
      failure = error;
    }
    onClose();
  };

  const start = () =>
    subscribe(
      (event) =>
        new Promise<void>((done) => {
          if (waiter !== undefined) hand(event, done);
          else offered = { event, done };
        }),
      (error) => {
        // Only a subscription that stopped itself ends the stream.
        if (sub !== undefined && !sub.active) finish({ error });
      },
    );

  return {
    next() {
      if (waiter !== undefined) {
        return Promise.reject(new TypeError("stream() has one consumer: await each next()"));
      }
      // Asking for the next event means the loop body finished the last one.
      release?.();
      release = undefined;
      if (failure !== undefined) {
        const { error } = failure;
        failure = undefined;
        return Promise.reject(error);
      }
      if (closed) return Promise.resolve(DONE);
      return new Promise((resolve, reject) => {
        waiter = { resolve, reject };
        sub ??= start();
        if (offered !== undefined) {
          const o = offered;
          offered = undefined;
          hand(o.event, o.done);
        }
      });
    },
    return() {
      finish();
      failure = undefined;
      return Promise.resolve(DONE);
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}
