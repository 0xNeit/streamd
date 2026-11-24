import {
  StreamdError,
  WEBHOOK_SIGNATURE_HEADER,
  eventId,
  fieldDecoder,
  parseEvent,
  verifyWebhook,
} from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { MemoryDedupStore } from "./store.js";
import type { DedupStore } from "./store.js";

export interface ReceiverOptions<TData = unknown> {
  /** The secret `streamd forward --secret` signs with. Unsigned deliveries are refused. */
  secret: string;
  /**
   * Called with the delivery's events not handled before, in body order, and
   * not called when there are none. They are recorded as handled only once
   * this resolves; a throw or rejection answers 500, so `forward` sends them again.
   */
  onEvents(events: StreamdEvent<TData>[], request: Request): void | PromiseLike<void>;
  /** Where handled event ids are kept. Default a `MemoryDedupStore`. */
  store?: DedupStore;
  /**
   * Prefixes every id the store sees, as `<namespace>/<version>:<eventIndex>`.
   * Versions repeat across networks and chains, so give each forwarder sharing
   * a receiver or store its own, or one network's events pass as another's repeats.
   */
  namespace?: string;
  /** `data` fields, by path, to turn back into `bigint`, as `parseEvent` takes them. */
  decode?: readonly string[];
  /** The largest body read, in bytes; a larger one answers 413. Default 4 MiB. */
  maxBodyBytes?: number;
  /** Told why a delivery answered 400 or 500, since the response never says. */
  onError?(error: unknown, request: Request): void;
}

const DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024;

export function reply(status: number, text?: string, headers?: Record<string, string>): Response {
  return new Response(text ?? null, {
    status,
    headers: text === undefined ? headers : { "content-type": "text/plain; charset=utf-8", ...headers },
  });
}

/** The body's bytes, or `undefined` once it passes `max` (the rest is not read). */
async function readBody(request: Request, max: number): Promise<Uint8Array | undefined> {
  if (request.body === null) return new Uint8Array(0);
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > max) {
      await reader.cancel().catch(() => {});
      return undefined;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function parseBody<TData>(body: Uint8Array, decode: readonly string[] | undefined): StreamdEvent<TData>[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
  } catch (err) {
    throw new StreamdError("Webhook body is not UTF-8 JSON", err);
  }
  if (!Array.isArray(parsed)) throw new StreamdError("Webhook body must be a JSON array of events");
  return parsed.map((item) =>
    parseEvent<TData>(JSON.stringify(item), decode === undefined ? {} : { decode }),
  );
}

/** The options both receivers take besides their handler. */
export interface SharedOptions {
  secret: string;
  store?: DedupStore;
  namespace?: string;
  maxBodyBytes?: number;
  onError?(error: unknown, request: Request): void;
}

/** What both receivers share: the option checks, the request checks before parsing, and the dedup flow. */
export function receiverCore(name: string, options: SharedOptions) {
  const { secret, namespace } = options;
  if (typeof secret !== "string" || secret === "") {
    throw new StreamdError(`${name}: secret must be a non-empty string`);
  }
  if (namespace !== undefined && (typeof namespace !== "string" || namespace === "")) {
    throw new StreamdError(`${name}: namespace must be a non-empty string`);
  }
  const maxBodyBytes = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
  if (!Number.isSafeInteger(maxBodyBytes) || maxBodyBytes <= 0) {
    throw new StreamdError(`Invalid maxBodyBytes: ${String(maxBodyBytes)} (expected a positive integer)`);
  }
  const store = options.store ?? new MemoryDedupStore();
  if (typeof store.seen !== "function" || typeof store.add !== "function") {
    throw new StreamdError(`${name}: store must have seen and add methods`);
  }
  // A throwing logger must not turn a 400 into a rejection the server answers as a retryable 500.
  const report = (err: unknown, request: Request) => {
    try {
      const result: unknown = options.onError?.(err, request);
      if (result instanceof Promise) result.catch(() => {});
    } catch {}
  };
  // Ids a delivery is handling now, so a retry overlapping it waits rather than running twice.
  const inFlight = new Set<string>();

  return {
    report,
    keyOf: (id: string): string => (namespace === undefined ? id : `${namespace}/${id}`),

    /** The signed body, or the response refusing the request. */
    async body(request: Request): Promise<Uint8Array | Response> {
      if (request.method !== "POST") return reply(405, "method not allowed", { allow: "POST" });
      const type = request.headers.get("content-type")?.split(";")[0]!.trim().toLowerCase();
      if (type !== "application/json") return reply(415, "expected application/json");
      const declared = request.headers.get("content-length");
      if (declared !== null && Number(declared) > maxBodyBytes) return reply(413, "body too large");
      const body = await readBody(request, maxBodyBytes);
      if (body === undefined) return reply(413, "body too large");
      if (!(await verifyWebhook(body, request.headers.get(WEBHOOK_SIGNATURE_HEADER), secret))) {
        return reply(401, "invalid signature");
      }
      return body;
    },

    /** Runs `handle` on the ids the store has not seen, recording them once it resolves. */
    async deliver(ids: string[], handle: (fresh: string[]) => void | PromiseLike<void>, request: Request): Promise<Response> {
      if (ids.some((id) => inFlight.has(id))) return reply(503, "delivery in progress");
      for (const id of ids) inFlight.add(id);
      try {
        const seen = await store.seen(ids);
        if (!Array.isArray(seen) || seen.length !== ids.length) {
          throw new StreamdError(`DedupStore.seen answered ${String(seen?.length)} results for ${ids.length} ids`);
        }
        const fresh = ids.filter((_, i) => seen[i] !== true);
        if (fresh.length > 0) {
          await handle(fresh);
          await store.add(fresh);
        }
        return reply(204);
      } catch (err) {
        report(err, request);
        return reply(500, "delivery failed");
      } finally {
        for (const id of ids) inFlight.delete(id);
      }
    },
  };
}

/**
 * A `Request` to `Response` handler for `streamd forward` webhook deliveries.
 * It checks the method, content type, size and `x-streamd-signature` before
 * parsing anything, then hands `onEvents` the events its store has not seen.
 *
 * Answers 204 once handled (or when every event was a repeat); 400 for a body
 * that does not parse, 401 for a missing or wrong signature, 405, 413 and 415,
 * all of which stop `forward`; 500 when the store or `onEvents` fails and 503
 * while another delivery of the same events is still being handled, both of
 * which `forward` retries.
 */
export function createReceiver<TData = unknown>(
  options: ReceiverOptions<TData>,
): (request: Request) => Promise<Response> {
  const { onEvents, decode } = options;
  const core = receiverCore("createReceiver", options);
  if (typeof onEvents !== "function") throw new StreamdError("createReceiver: onEvents must be a function");
  fieldDecoder(decode);

  return async (request) => {
    const body = await core.body(request);
    if (body instanceof Response) return body;
    let events: StreamdEvent<TData>[];
    try {
      events = parseBody<TData>(body, decode);
    } catch (err) {
      core.report(err, request);
      return reply(400, "malformed events");
    }
    const byId = new Map<string, StreamdEvent<TData>>();
    for (const event of events) {
      const key = core.keyOf(eventId(event));
      if (!byId.has(key)) byId.set(key, event);
    }
    return core.deliver([...byId.keys()], (fresh) => onEvents(fresh.map((id) => byId.get(id)!), request), request);
  };
}
