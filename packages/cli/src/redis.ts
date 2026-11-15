import { message, scrubber, signature } from "./format.js";
import type { Attempt, Sink } from "./forward.js";
import { isMissingModule } from "./sui.js";

/** The one call the Redis sink makes, which `connectRedis` gives over Bun's client or ioredis. */
export interface RedisLike {
  send(command: string, args: string[]): Promise<unknown>;
  close(): unknown;
}

export interface RedisSinkOptions {
  /** Only for scrubbing messages: the client was opened with it. */
  to: string;
  stream: string;
  secret?: string;
  /** Per-`XADD` timeout in ms. */
  timeout: number;
}

// Error replies that will not change on a retry; every other failure, transient
// replies such as LOADING, READONLY or OOM included, is retried under backoff.
const FINAL_REPLIES = new Set(["ERR", "WRONGTYPE", "NOPERM", "NOAUTH", "WRONGPASS"]);

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    if (signal.aborted) onAbort();
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}

/**
 * Adds each body to `options.stream` as one entry, `body` plus `signature`
 * when signing; the entry id `XADD` replies with is the acknowledgement.
 */
export function redisSink(client: RedisLike, options: RedisSinkOptions): Sink {
  const scrub = scrubber(options.to);
  return {
    async send(body, signal): Promise<Attempt> {
      const fields = ["body", body];
      if (options.secret !== undefined) fields.push("signature", signature(body, options.secret));
      const timeout = AbortSignal.timeout(options.timeout);
      try {
        await abortable(client.send("XADD", [options.stream, "*", ...fields]), AbortSignal.any([signal, timeout]));
        return { ok: true };
      } catch (err) {
        if (signal.aborted) return { ok: false, failure: "aborted" };
        if (timeout.aborted) {
          // A wedged connection would time out every retry; the next attempt opens a fresh one.
          client.close();
          return { ok: false, failure: `XADD timed out after ${options.timeout}ms`, retryable: true };
        }
        const failure = scrub(message(err));
        return { ok: false, failure, retryable: !FINAL_REPLIES.has(/^[A-Z]+\b/.exec(failure)?.[0] ?? "") };
      }
    },
    close: () => client.close(),
  };
}

interface IoRedis {
  status: string;
  connect(): Promise<void>;
  call(command: string, ...args: string[]): Promise<unknown>;
  disconnect(): void;
  on(event: "error", listener: () => void): unknown;
}
type IoRedisModule = { default?: IoRedisConstructor; Redis?: IoRedisConstructor };
type IoRedisConstructor = new (url: string, options: Record<string, unknown>) => IoRedis;

// A variable, so neither the bundle nor the typecheck needs ioredis installed.
const IOREDIS = "ioredis";

/**
 * Opens the runtime's own Redis client, `Bun.RedisClient`, or under Node
 * ioredis when the user installed it, so the CLI ships no driver. Nothing is
 * sent until the first command. Neither queues a command while disconnected
 * nor reconnects on its own: a lost connection fails the attempt, and the
 * next attempt, under the retry backoff, connects again.
 */
export async function connectRedis(url: string, timeout: number): Promise<RedisLike> {
  if ("Bun" in globalThis) {
    const client = new Bun.RedisClient(url, { enableOfflineQueue: false, autoReconnect: false, connectionTimeout: timeout });
    return {
      send: async (command, args) => {
        if (!client.connected) await client.connect();
        return client.send(command, args);
      },
      close: () => client.close(),
    };
  }
  let module: IoRedisModule;
  try {
    module = (await import(IOREDIS)) as IoRedisModule;
  } catch (err) {
    if (isMissingModule(err)) throw new Error("--to redis:// under Node needs ioredis installed: npm i ioredis (Bun has its own client)");
    throw err;
  }
  const Redis = (module.default ?? module.Redis)!;
  const client = new Redis(url, {
    lazyConnect: true,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 0,
    connectTimeout: timeout,
    retryStrategy: () => null,
  });
  // A dropped connection is emitted here, and unheard would crash the process; the next attempt reconnects.
  client.on("error", () => {});
  return {
    send: async (command, args) => {
      if (client.status === "wait" || client.status === "end") await client.connect();
      return client.call(command, ...args);
    },
    close: () => client.disconnect(),
  };
}
