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
 * Each event's handler stays pending until the consumer asks for the next one,
 * so the poller (which awaits handlers) neither fetches ahead nor moves the
 * cursor past an event the loop body has not finished.
 */
export function eventStream<TData>(
  subscribe: Subscribe<TData>,
): AsyncIterableIterator<StreamdEvent<TData>> {
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

  const sub = subscribe(
    (event) =>
      new Promise<void>((done) => {
        if (waiter !== undefined) hand(event, done);
        else offered = { event, done };
      }),
    (error) => {
      // Only a subscription that stopped itself ends the stream.
      if (sub.active || closed) return;
      closed = true;
      if (waiter !== undefined) {
        waiter.reject(error);
        waiter = undefined;
      } else {
        failure = { error };
      }
    },
  );

  const close = () => {
    closed = true;
    sub.unsubscribe();
    release?.();
    offered?.done();
    release = offered = undefined;
    waiter?.resolve(DONE);
    waiter = undefined;
  };

  return {
    next() {
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
        if (offered !== undefined) {
          const o = offered;
          offered = undefined;
          hand(o.event, o.done);
        }
      });
    },
    return() {
      close();
      return Promise.resolve(DONE);
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}
