import type {
  EventCallback,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  SubscriptionStatus,
  Watch,
  WatchCallback,
  WatchOptions,
} from "./types.js";
import type { ReduceHost } from "./reduce.js";
import { StreamdError } from "./errors.js";
import { requireAddress } from "./adapters/validate.js";
import { combinedStatus } from "./grouping.js";
import { normalizeAddress } from "./filter.js";
import { generateId, onAbort } from "./utils.js";

/** What `watch()` needs from its client. */
export type WatchHost = Pick<ReduceHost, "onError" | "subscribe" | "track">;

interface Walk {
  address: string;
  sub: Subscription;
}

function statusOf(walks: Subscription[], active: boolean): SubscriptionStatus {
  if (walks.length > 0) return combinedStatus(walks, active);
  return { active, cursor: undefined, lastVersion: undefined, consecutiveErrors: 0, lastSuccessAt: undefined };
}

function readPositions(positions: unknown): Map<string, string> {
  if (positions === undefined) return new Map();
  if (typeof positions !== "object" || positions === null || Array.isArray(positions)) {
    throw new StreamdError("watch positions must be an object of address to cursor");
  }
  return new Map(
    Object.entries(positions).map(([address, cursor]) => {
      if (typeof cursor !== "string" || cursor === "") {
        throw new StreamdError(`Invalid watch position for ${address}: expected a non-empty cursor`);
      }
      return [requireAddress(address), cursor];
    }),
  );
}

/**
 * Follow each address's `account` events with one walk per address, calling
 * `callback(event, address)`; walks are added and removed at runtime.
 */
export function startWatch(
  host: WatchHost,
  addresses: readonly string[],
  callback: WatchCallback,
  options: WatchOptions = {},
): Watch {
  for (const key of ["cursor", "id"] as const) {
    if ((options as SubscriptionOptions)[key] !== undefined) {
      throw new StreamdError(`watch does not take ${key}: it follows several addresses`);
    }
  }
  if (!Array.isArray(addresses)) throw new StreamdError("watch needs an array of addresses");
  const initial = addresses.map((a) => [requireAddress(a), a] as const);
  const { positions, ...startOptions } = options;
  // Positions not in use: from the last run until a walk takes one, or left by a walk that ended.
  const saved = readPositions(positions);

  const walks = new Map<string, Walk>();
  const pending = new Set<string>();
  let ended = false;
  let paused = false;
  // A start position chosen at watch() is in the past by the time of a later add().
  const { fromVersion, fromTime, signal, onCaughtUp, ...live } = startOptions;

  const caughtUp = (key: string) => {
    if (pending.delete(key) && pending.size === 0) onCaughtUp?.();
  };

  const retain = (key: string, sub: Subscription) => {
    if (sub.cursor !== undefined) saved.set(key, sub.cursor);
  };

  const start = (key: string, address: string, first: boolean) => {
    const cursor = saved.get(key);
    saved.delete(key);
    const sub = host.subscribe(
      { kind: "account", accountAddress: address },
      ((event: StreamdEvent) => callback(event, address)) as EventCallback,
      {
        ...(first ? startOptions : live),
        ...(cursor !== undefined && { cursor }),
        signal: undefined,
        onCaughtUp: () => caughtUp(key),
        onError: (err) => {
          // A walk that gave up on its own leaves the watch; the rest carry on.
          if (!sub.active && !sub.paused && walks.get(key)?.sub === sub) {
            retain(key, sub);
            drop(key);
          }
          (options.onError ?? host.onError)?.(err);
        },
      },
    );
    walks.set(key, { address, sub });
    if (paused) sub.pause();
  };

  const drop = (key: string) => {
    walks.delete(key);
    caughtUp(key);
  };

  const end = () => {
    if (ended) return;
    ended = true;
    paused = false;
    for (const [key, { sub }] of walks) {
      retain(key, sub);
      sub.unsubscribe();
    }
    walks.clear();
    pending.clear();
    untrack();
    detach();
  };
  const untrack = host.track(end);
  const detach = onAbort(signal, end);

  for (const [key, address] of initial) {
    if (walks.has(key)) continue;
    pending.add(key);
    start(key, address, true);
  }
  if (initial.length === 0) queueMicrotask(() => onCaughtUp?.());
  if (signal?.aborted) end();

  const subs = () => [...walks.values()].map((w) => w.sub);
  return {
    id: generateId(),
    add(address: string) {
      const key = requireAddress(address);
      if (ended || walks.has(key)) return false;
      start(key, address, false);
      return true;
    },
    remove(address: string) {
      const key = normalizeAddress(address);
      const walk = key === undefined ? undefined : walks.get(key);
      if (walk === undefined) return false;
      walk.sub.unsubscribe();
      drop(key!);
      return true;
    },
    get addresses() {
      return [...walks.values()].map((w) => w.address);
    },
    get active() {
      return !ended && !paused;
    },
    get paused() {
      return paused;
    },
    get cursor() {
      return undefined;
    },
    get positions() {
      const current = Object.fromEntries(saved);
      for (const [key, { sub }] of walks) {
        if (sub.cursor !== undefined) current[key] = sub.cursor;
      }
      return current;
    },
    get status() {
      return statusOf(subs(), !ended && !paused);
    },
    pause() {
      if (ended || paused) return;
      paused = true;
      for (const s of subs()) s.pause();
    },
    resume() {
      if (!paused) return;
      paused = false;
      for (const s of subs()) s.resume();
    },
    unsubscribe: end,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: end,
  };
}
