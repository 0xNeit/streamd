import type {
  EventOf,
  EventCallback,
  EventDescriptor,
  EventFilter,
  Replay,
  ReplayOptions,
  StreamdEvent,
} from "./types.js";
import type { ReduceHost } from "./reduce.js";
import { StreamdError } from "./errors.js";
import { eventId } from "./cursor.js";
import { isThenable } from "./filter.js";
import { onAbort } from "./utils.js";

const MAX_WAIT = 2_147_483_647;

/** What a replay needs from its client. */
export type ReplayHost = Pick<ReduceHost, "query" | "defaults" | "onError" | "track">;

const QUERY_KEYS = [
  "fromVersion",
  "toVersion",
  "fromTime",
  "fromBlock",
  "toBlock",
  "limit",
  "offset",
  "where",
  "decode",
  "withState",
  "withNames",
  "maxPages",
  "retries",
  "onCursor",
] as const satisfies readonly (keyof EventFilter)[];
const ALLOWED: readonly string[] = [
  ...QUERY_KEYS,
  "signal",
  "speed",
  "batchSize",
  "onError",
  "onCaughtUp",
] satisfies readonly (keyof ReplayOptions)[];

function checkOptions(options: ReplayOptions): number {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported replay option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  const speed: unknown = options.speed === undefined ? Infinity : options.speed;
  if (typeof speed !== "number" || !(speed > 0)) {
    throw new StreamdError(
      `Invalid speed: ${String(speed)} (expected a positive number or Infinity)`,
    );
  }
  return speed;
}

/** `client.replay()`: past events through an `on()` callback, in order. */
export function startReplay<TData>(
  host: ReplayHost,
  descriptor: EventDescriptor<TData> | readonly EventDescriptor<TData>[],
  callback: EventCallback<TData>,
  options: ReplayOptions = {},
): Replay {
  const speed = checkOptions(options);
  const controller = new AbortController();
  let stopped = false;
  let paused = false;
  let release: (() => void) | undefined;
  let cancelWait: (() => void) | undefined;

  const report = (error: Error) => {
    try {
      (options.onError ?? host.onError)?.(error);
    } catch (handlerError) {
      queueMicrotask(() => {
        throw handlerError;
      });
    }
  };
  const finish = () => {
    stopped = true;
    paused = false;
    detachSignal();
    untrack();
    release?.();
    cancelWait?.();
  };
  const stop = (reason: unknown) => {
    if (stopped) return;
    finish();
    controller.abort(reason);
  };
  const untrack = host.track(() => stop(new StreamdError("Replay stopped before it finished")));
  const detachSignal = onAbort(options.signal, () => stop(options.signal!.reason));
  if (options.signal?.aborted) stop(options.signal.reason);

  const wait = (ms: number) =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, Math.min(ms, MAX_WAIT));
      cancelWait = () => (clearTimeout(timer), resolve());
    });

  // Measured from the previous timestamped delivery, so fetch and handler
  // time do not stretch the replay.
  let lastStamp: bigint | undefined;
  let lastAt = 0;
  const pace = async (event: StreamdEvent) => {
    if (speed === Infinity || event.timestamp === undefined) return;
    if (lastStamp !== undefined) {
      const ms = Number(event.timestamp - lastStamp) / 1000 / speed - (Date.now() - lastAt);
      if (ms > 0) await wait(ms);
    }
    lastStamp = event.timestamp;
    lastAt = Date.now();
  };

  const deliver = async (event: StreamdEvent) => {
    try {
      const result: unknown = callback(event as unknown as EventOf<TData>);
      if (isThenable(result)) await result;
    } catch (err) {
      if (!stopped) report(new StreamdError(`Event callback threw for ${eventId(event)}`, err));
    }
  };

  const run = async (): Promise<number> => {
    let delivered = 0;
    const filter: EventFilter = {};
    for (const key of QUERY_KEYS) {
      if (options[key] !== undefined) Object.assign(filter, { [key]: options[key] });
    }
    filter.decode ??= host.defaults.decode;
    filter.signal = controller.signal;
    const pageSize = options.batchSize ?? host.defaults.batchSize;
    try {
      for await (const event of host.query.iterateEvents(descriptor, filter, pageSize)) {
        await pace(event);
        while (paused) await new Promise<void>((resolve) => (release = resolve));
        if (stopped) break;
        await deliver(event);
        delivered++;
        if (stopped) break;
      }
    } catch (err) {
      if (!stopped) {
        stop(err);
        report(err as Error);
      }
    }
    if (controller.signal.aborted) throw controller.signal.reason;
    try {
      options.onCaughtUp?.();
    } catch (err) {
      report(new StreamdError("onCaughtUp threw", err));
    }
    finish();
    return delivered;
  };

  const done = run();
  done.catch(() => {});
  const unsubscribe = () => stop(new StreamdError("Replay stopped before it finished"));

  return {
    done,
    get active() {
      return !stopped && !paused;
    },
    get paused() {
      return paused;
    },
    pause: () => {
      if (!stopped) paused = true;
    },
    resume: () => {
      if (!paused) return;
      paused = false;
      release?.();
    },
    unsubscribe,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: unsubscribe,
  } as Replay;
}
