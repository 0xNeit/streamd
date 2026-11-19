import { createHash, timingSafeEqual } from "node:crypto";
import { StreamdError, StreamdNetworkError, redactSecrets } from "@streamd/core";
import type { EventDescriptor, StreamdClient } from "@streamd/core";
import { Topics } from "./topics.js";
import type { Entry, Subscriber, Topic, TopicStatus } from "./topics.js";
import { parseDescriptor, toJson, toWireError } from "./wire.js";

export interface GatewayOptions {
  /** Every topic polls through this client, so its limits and defaults apply. */
  client: StreamdClient;
  /** When set, every request must carry it as `Authorization: Bearer` or `access_token`. */
  token?: string;
  /** Browser origins answered with CORS headers and let open WebSockets; `"*"` for any. */
  allowOrigins?: readonly string[];
  /** Distinct descriptors followed at once (default: 32). */
  maxTopics?: number;
  /** Open streams, SSE and WebSocket together (default: 1000). */
  maxConnections?: number;
  /** Events each topic keeps for resuming (default: 1000). */
  buffer?: number;
  /** How long, in ms, a topic outlives its last client (default: 60000). */
  linger?: number;
  /** Unsent bytes past which a stream is closed, to be resumed by its client (default: 1 MiB). */
  maxQueuedBytes?: number;
  /** Interval, in ms, of the SSE keepalive comment (default: 15000). */
  keepAlive?: number;
  pollInterval?: number;
  batchSize?: number;
  /** A topic's polling errors; the topic keeps polling unless its poller gave up. */
  onError?: (error: Error, descriptor: EventDescriptor) => void;
}

export interface GatewayStatus {
  topics: TopicStatus[];
  connections: number;
}

export interface Gateway {
  /** The HTTP handler, for `Bun.serve`'s `fetch` or any `Request -> Response` host. */
  fetch(request: Request): Promise<Response>;
  status(): GatewayStatus;
  /** Stops every topic and closes every stream. */
  close(): void;
}

interface Connection {
  close(): void;
}

const MAX_DESCRIPTOR = 16 * 1024;
const encoder = new TextEncoder();

function digest(text: string): Buffer {
  return createHash("sha256").update(text).digest();
}

function sseEntry(topic: Topic, entry: Entry): string {
  return `id: ${topic.id(entry.seq)}\ndata: ${entry.line}\n\n`;
}

export function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(toJson(body), { status, headers: { "content-type": "application/json", ...headers } });
}

/** The response for `err`: 400 for bad input, a network error's own status, else 502 or 500. */
export function errorResponse(err: unknown): Response {
  const body = { error: toWireError(err, redactSecrets) };
  if (err instanceof StreamdNetworkError) {
    const code = err.statusCode;
    const status = code !== undefined && code >= 400 && code <= 599 ? code : 502;
    const headers: Record<string, string> = {};
    if (err.retryAfter !== undefined) headers["retry-after"] = String(Math.ceil(err.retryAfter / 1000));
    return json(body, status, headers);
  }
  return json(body, err instanceof StreamdError ? 400 : 500);
}

/** The descriptor a stream request names in its `descriptor` query parameter, as JSON. */
export function descriptorParam(url: URL): EventDescriptor {
  const raw = url.searchParams.get("descriptor");
  if (raw === null) throw new StreamdError('"descriptor" query parameter is required');
  if (raw.length > MAX_DESCRIPTOR) throw new StreamdError('"descriptor" is too long');
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new StreamdError('"descriptor" must be JSON');
  }
  return parseDescriptor(value);
}

/**
 * The topic and the entries a stream starts with: none at the newest event,
 * or everything after `lastEventId`, which must still be held.
 */
export function resume(topics: Topics, descriptor: EventDescriptor, lastEventId: string | null) {
  const topic = topics.open(descriptor);
  if (lastEventId === null || lastEventId === "") return { topic, backlog: [] as Entry[] };
  const backlog = topic.after(lastEventId);
  if (backlog === undefined) {
    topics.touch(topic);
    throw new StreamdNetworkError(`Event ${lastEventId} is no longer held; start the stream again`, 410);
  }
  return { topic, backlog };
}

/**
 * A gateway over `options.client`: one topic per descriptor however many
 * clients follow it, served as Server-Sent Events.
 */
export function createGateway(options: GatewayOptions): Gateway {
  const topics = new Topics({
    client: options.client,
    maxTopics: options.maxTopics ?? 32,
    buffer: options.buffer ?? 1000,
    linger: options.linger ?? 60_000,
    pollInterval: options.pollInterval,
    batchSize: options.batchSize,
    onError: options.onError,
  });
  const maxConnections = options.maxConnections ?? 1000;
  const maxQueuedBytes = options.maxQueuedBytes ?? 1024 * 1024;
  const allowed = new Set(options.allowOrigins ?? []);
  const token = options.token === undefined ? undefined : digest(options.token);
  const connections = new Set<Connection>();
  const keepAlive = new Set<() => void>();
  const ticker = setInterval(() => keepAlive.forEach((ping) => ping()), options.keepAlive ?? 15_000);
  ticker.unref?.();

  function corsHeaders(request: Request): Record<string, string> {
    const origin = request.headers.get("origin");
    if (origin === null) return {};
    if (allowed.has("*")) return { "access-control-allow-origin": "*" };
    return allowed.has(origin) ? { "access-control-allow-origin": origin, vary: "Origin" } : {};
  }

  function authorized(request: Request, url: URL): boolean {
    if (token === undefined) return true;
    const header = request.headers.get("authorization");
    const given = header?.startsWith("Bearer ") ? header.slice(7) : url.searchParams.get("access_token");
    return given !== null && given !== undefined && timingSafeEqual(digest(given), token);
  }

  function reserve(): void {
    if (connections.size >= maxConnections) {
      throw new StreamdNetworkError(`The gateway holds at most ${maxConnections} streams`, 503);
    }
  }

  function events(request: Request, url: URL): Response {
    reserve();
    const descriptor = descriptorParam(url);
    const lastEventId = request.headers.get("last-event-id") ?? url.searchParams.get("lastEventId");
    const { topic, backlog } = resume(topics, descriptor, lastEventId);
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    let open = true;
    const close = () => {
      if (!open) return;
      open = false;
      topics.leave(topic, subscriber);
      connections.delete(connection);
      keepAlive.delete(ping);
      try {
        controller.close();
      } catch {
        // Already closed by the client going away.
      }
    };
    const write = (text: string) => {
      if (!open) return;
      controller.enqueue(encoder.encode(text));
      // A client this far behind is dropped rather than buffered without bound; it resumes from the topic.
      if ((controller.desiredSize ?? 0) < 0) close();
    };
    const subscriber: Subscriber = {
      deliver: (entries) => write(entries.map((entry) => sseEntry(topic, entry)).join("")),
      end: (message) => {
        write(`event: end\ndata: ${JSON.stringify({ message })}\n\n`);
        close();
      },
    };
    const connection: Connection = { close };
    const ping = () => write(": keepalive\n\n");
    const body = new ReadableStream<Uint8Array>(
      {
        start(c) {
          controller = c;
        },
        cancel: close,
      },
      new ByteLengthQueuingStrategy({ highWaterMark: maxQueuedBytes }),
    );
    connections.add(connection);
    keepAlive.add(ping);
    topics.join(topic, subscriber);
    request.signal.addEventListener("abort", close, { once: true });
    write(`: ${topic.id(topic.seq)}\n\n`);
    if (backlog.length > 0) subscriber.deliver(backlog);
    return new Response(body, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        "x-accel-buffering": "no",
      },
    });
  }

  function route(request: Request, url: URL): Response | Promise<Response> {
    if (url.pathname === "/v1/events" && request.method === "GET") return events(request, url);
    if (url.pathname === "/v1/status" && request.method === "GET") return json(status());
    return json({ error: { name: "Error", message: "Not found" } }, 404);
  }

  function status(): GatewayStatus {
    return { topics: topics.status(), connections: connections.size };
  }

  return {
    async fetch(request) {
      const cors = corsHeaders(request);
      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
          headers: {
            ...cors,
            "access-control-allow-methods": "GET, POST, OPTIONS",
            "access-control-allow-headers": "authorization, content-type, last-event-id",
            "access-control-max-age": "600",
          },
        });
      }
      const url = new URL(request.url);
      let response: Response;
      if (!authorized(request, url)) {
        response = json({ error: { name: "StreamdError", message: "Unauthorized" } }, 401, { "www-authenticate": "Bearer" });
      } else {
        try {
          response = await route(request, url);
        } catch (err) {
          response = errorResponse(err);
        }
      }
      for (const [name, value] of Object.entries(cors)) response.headers.set(name, value);
      return response;
    },
    status,
    close() {
      clearInterval(ticker);
      for (const connection of [...connections]) connection.close();
      topics.close();
    },
  };
}
