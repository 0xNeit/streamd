import { createHash, timingSafeEqual } from "node:crypto";
import type { Server, WebSocketHandler } from "bun";
import { DEFAULT_LIMIT, RequestLimiter, SharedFetches, StreamdError, StreamdNetworkError, isObject, pageJson, redactSecrets } from "@streamd/core";
import type { EventAdapter, EventDescriptor, FetchOptions, StreamdClient } from "@streamd/core";
import { Topics } from "./topics.js";
import type { Entry, Subscriber, Topic, TopicStatus } from "./topics.js";
import { parseDescriptor, parseFetchOptions, toJson, toWireError } from "./wire.js";

export interface GatewayOptions {
  /** Every topic polls through this client, so its limits and defaults apply. */
  client: StreamdClient;
  /**
   * The source of `GatewayAdapter` reads that no topic serves (a start, an
   * end, an offset, newest first), shared across identical requests within
   * `pollInterval`. Without it those reads are refused. Its calls go through
   * a limiter of the gateway's own, not `client`'s.
   */
  adapter?: EventAdapter;
  /** Upstream reads in flight at once; past it they wait (default: 4). */
  maxUpstreamRequests?: number;
  /** After this many ms an upstream read fails with a `StreamdNetworkError` (default: 30000). */
  requestTimeout?: number;
  /**
   * When set, every request must carry it as `Authorization: Bearer` or
   * `access_token`. Without it, only requests whose `Host` is loopback are served.
   */
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

/** What a WebSocket carries from its upgrade to its `open`. */
export interface SocketData {
  topic: Topic;
  /** The id to resume after, read again at `open` so nothing published in between is missed. */
  after: string;
  close?: () => void;
}

export interface Gateway {
  /**
   * The HTTP handler, for `Bun.serve`'s `fetch` or any `Request -> Response`
   * host. WebSockets need `server`, from `Bun.serve`, and resolve `undefined`
   * once upgraded.
   */
  fetch(request: Request, server?: Server<SocketData>): Promise<Response | undefined>;
  /** `Bun.serve`'s `websocket` handler for `/v1/ws`. */
  websocket: WebSocketHandler<SocketData>;
  status(): GatewayStatus;
  /** Stops every topic and closes every stream. */
  close(): void;
}

interface Connection {
  close(): void;
}

const MAX_DESCRIPTOR = 16 * 1024;
const MAX_BODY = 64 * 1024;
const LOOPBACK_HOST = /^(?:localhost|127(?:\.\d{1,3}){3}|\[::1\])(?::\d+)?$/i;
const encoder = new TextEncoder();

function digest(text: string): Buffer {
  return createHash("sha256").update(text).digest();
}

function sseEntry(topic: Topic, entry: Entry): string {
  return `id: ${topic.id(entry.seq)}\ndata: ${entry.line}\n\n`;
}

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(toJson(body), { status, headers: { "content-type": "application/json", ...headers } });
}

/** The response for `err`: 400 for bad input, a network error's own status, else 502 or 500. */
function errorResponse(err: unknown): Response {
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
function descriptorParam(url: URL): EventDescriptor {
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

/** The body as text, read no further than `MAX_BODY` bytes, whatever `content-length` claims. */
async function boundedText(request: Request): Promise<string> {
  const tooLarge = () => new StreamdNetworkError(`Request bodies are limited to ${MAX_BODY} bytes`, 413);
  if (Number(request.headers.get("content-length") ?? 0) > MAX_BODY) throw tooLarge();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for await (const chunk of request.body ?? []) {
    size += chunk.byteLength;
    if (size > MAX_BODY) throw tooLarge();
    chunks.push(chunk);
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

async function readBody(request: Request): Promise<{ descriptor: EventDescriptor; options: FetchOptions }> {
  // Anything else is a form a page on another origin could post without a preflight.
  if (!request.headers.get("content-type")?.startsWith("application/json")) {
    throw new StreamdNetworkError("Requests must be sent as application/json", 415);
  }
  const text = await boundedText(request);
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    throw new StreamdError("The request body must be JSON");
  }
  if (!isObject(body)) throw new StreamdError("The request body must be an object");
  return { descriptor: parseDescriptor(body.descriptor), options: parseFetchOptions(body.options) };
}

/** A request the topic serves: no start, end, offset or newest-first order. */
function live(options: FetchOptions): boolean {
  const { cursor, fromVersion, toVersion, offset, orderBy } = options;
  return cursor === undefined && fromVersion === undefined && toVersion === undefined && offset === undefined && orderBy !== "desc";
}

/**
 * The topic and the entries a stream starts with: none at the newest event,
 * or everything after `lastEventId`, which must still be held.
 */
function resume(topics: Topics, descriptor: EventDescriptor, lastEventId: string | null) {
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
 * clients follow it, served as Server-Sent Events and WebSocket streams.
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
  for (const name of ["maxTopics", "maxConnections", "buffer", "maxUpstreamRequests", "requestTimeout"] as const) {
    const value = options[name];
    if (value !== undefined && !(Number.isSafeInteger(value) && value > 0)) {
      throw new StreamdError(`${name} must be a positive integer: ${value}`);
    }
  }
  const requestTimeout = options.requestTimeout ?? 30_000;
  const limited = options.adapter === undefined
    ? undefined
    : new RequestLimiter(options.maxUpstreamRequests ?? 4, Infinity, requestTimeout).wrap(options.adapter);
  const upstream = limited === undefined
    ? undefined
    : new SharedFetches(limited, requestTimeout).view(options.pollInterval ?? 1000);
  const maxConnections = options.maxConnections ?? 1000;
  const maxQueuedBytes = options.maxQueuedBytes ?? 1024 * 1024;
  const allowed = new Set(options.allowOrigins ?? []);
  const token = options.token === undefined ? undefined : digest(options.token);
  const connections = new Set<Connection>();
  /** WebSockets upgraded and not yet open, counted against `maxConnections` too. */
  let upgrading = 0;
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
    // With no token the gateway trusts loopback, so a page that rebinds its own name to 127.0.0.1 is refused.
    if (token === undefined) return LOOPBACK_HOST.test(request.headers.get("host") ?? url.host);
    const header = request.headers.get("authorization");
    const given = header?.startsWith("Bearer ") ? header.slice(7) : url.searchParams.get("access_token");
    return given !== null && given !== undefined && timingSafeEqual(digest(given), token);
  }

  function reserve(): void {
    if (connections.size + upgrading >= maxConnections) {
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
    const close = (reason?: Error) => {
      if (!open) return;
      open = false;
      topics.leave(topic, subscriber);
      connections.delete(connection);
      keepAlive.delete(ping);
      try {
        // Erroring discards what is queued, so a dropped client holds no memory once out of the count.
        if (reason === undefined) controller.close();
        else controller.error(reason);
      } catch {
        // Already closed by the client going away.
      }
    };
    const write = (text: string) => {
      if (!open) return;
      controller.enqueue(encoder.encode(text));
      // A client this far behind is dropped rather than buffered without bound; it resumes from the topic.
      if ((controller.desiredSize ?? 0) < 0) close(new Error("Too slow; resume from the last id"));
    };
    const subscriber: Subscriber = {
      deliver: (entries) => write(entries.map((entry) => sseEntry(topic, entry)).join("")),
      end: (message) => {
        write(`event: end\ndata: ${JSON.stringify({ message })}\n\n`);
        close();
      },
    };
    const connection: Connection = { close: () => close() };
    const ping = () => write(": keepalive\n\n");
    const body = new ReadableStream<Uint8Array>(
      {
        start(c) {
          controller = c;
        },
        cancel: () => close(),
      },
      new ByteLengthQueuingStrategy({ highWaterMark: maxQueuedBytes }),
    );
    connections.add(connection);
    keepAlive.add(ping);
    topics.join(topic, subscriber);
    request.signal.addEventListener("abort", () => close(), { once: true });
    // An id with no data sets EventSource's last id, so a client that saw no event still resumes.
    write(`id: ${topic.id(backlog[0] === undefined ? topic.seq : backlog[0].seq - 1)}\n\n`);
    if (backlog.length > 0) subscriber.deliver(backlog);
    return new Response(body, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        "x-accel-buffering": "no",
      },
    });
  }

  /** A browser sends `Origin` on every WebSocket and no CORS check applies, so it is checked here. */
  function originAllowed(request: Request): boolean {
    const origin = request.headers.get("origin");
    if (origin === null || allowed.has("*") || allowed.has(origin)) return true;
    try {
      return new URL(origin).host === request.headers.get("host");
    } catch {
      return false;
    }
  }

  function upgrade(request: Request, url: URL, server: Server<SocketData> | undefined): Response | undefined {
    if (!originAllowed(request)) return json({ error: { name: "StreamdError", message: "Origin not allowed" } }, 403);
    if (server === undefined) return json({ error: { name: "Error", message: "WebSockets need Bun.serve" } }, 501);
    reserve();
    const { topic, backlog } = resume(topics, descriptorParam(url), url.searchParams.get("lastEventId"));
    const after = topic.id(backlog[0] === undefined ? topic.seq : backlog[0].seq - 1);
    upgrading++;
    if (server.upgrade(request, { data: { topic, after } })) return undefined;
    upgrading--;
    return json({ error: { name: "StreamdError", message: "Expected a WebSocket upgrade" } }, 400);
  }

  const websocket: WebSocketHandler<SocketData> = {
    open(ws) {
      upgrading--;
      const { topic } = ws.data;
      const backlog = topic.after(ws.data.after);
      if (topics.get(topic.descriptor) !== topic || backlog === undefined) {
        ws.close(4410, "Resume point no longer held; start the stream again");
        return;
      }
      const send = (text: string) => {
        // As for SSE: a client this far behind is dropped, to resume from the topic. A send Bun
        // dropped (0) past its own backpressure limit would leave a gap, so it closes too.
        if (ws.send(text) === 0 || ws.getBufferedAmount() > maxQueuedBytes) ws.close(1013, "Too slow; resume from the last id");
      };
      const subscriber: Subscriber = {
        deliver: (entries) => {
          for (const entry of entries) send(`{"id":"${topic.id(entry.seq)}","event":${entry.line}}`);
        },
        end: (message) => {
          ws.send(JSON.stringify({ end: message }));
          ws.close(1011, "Topic ended");
        },
      };
      const connection: Connection = { close: () => ws.close(1001, "Gateway closing") };
      ws.data.close = () => {
        topics.leave(topic, subscriber);
        connections.delete(connection);
      };
      connections.add(connection);
      topics.join(topic, subscriber);
      ws.send(JSON.stringify({ id: ws.data.after }));
      if (backlog.length > 0) subscriber.deliver(backlog);
    },
    message() {},
    close(ws) {
      ws.data.close?.();
    },
  };

  /**
   * A page for `GatewayAdapter`. Topic pages carry `t:<id>` cursors and
   * upstream pages `u:<cursor>`, so each walk stays on the source it began on.
   */
  async function fetchPage(descriptor: EventDescriptor, options: FetchOptions): Promise<Response> {
    const { cursor } = options;
    if (live(options)) {
      const topic = topics.open(descriptor);
      topics.touch(topic);
      return json({ events: [], nextCursor: `t:${topic.id(topic.seq)}`, hasMore: false });
    }
    if (cursor?.startsWith("t:")) {
      const topic = topics.get(descriptor);
      const entries = topic?.after(cursor.slice(2));
      if (topic === undefined || entries === undefined) {
        throw new StreamdNetworkError("This topic cursor is no longer held; start the walk again", 410);
      }
      topics.touch(topic);
      const page = entries.slice(0, options.limit ?? DEFAULT_LIMIT);
      const next = page.length === 0 ? cursor : `t:${topic.id(page.at(-1)!.seq)}`;
      const body = `{"events":[${page.map((e) => e.line).join(",")}],"nextCursor":${JSON.stringify(next)},"hasMore":${entries.length > page.length}}`;
      return new Response(body, { headers: { "content-type": "application/json" } });
    }
    if (cursor !== undefined && !cursor.startsWith("u:")) throw new StreamdError("Not a gateway cursor");
    if (upstream === undefined) throw new StreamdError("This gateway serves live topics only");
    // The page may be shared with other callers, so it is copied, never prefixed in place.
    const page = await upstream.fetchEvents(descriptor, { ...options, cursor: cursor?.slice(2) });
    return json(pageJson({ ...page, nextCursor: page.nextCursor === undefined ? undefined : `u:${page.nextCursor}` }));
  }

  async function version(request: Request, method: "getHeadVersion" | "getEarliestVersion"): Promise<Response> {
    const { descriptor } = await readBody(request);
    const read = limited?.[method];
    if (read === undefined) throw new StreamdError(`This gateway's source has no ${method}`);
    return json({ version: await read(descriptor) });
  }

  function route(request: Request, url: URL, server?: Server<SocketData>): Response | undefined | Promise<Response> {
    if (url.pathname === "/v1/events" && request.method === "GET") return events(request, url);
    if (url.pathname === "/v1/ws" && request.method === "GET") return upgrade(request, url, server);
    if (url.pathname === "/v1/status" && request.method === "GET") return json(status());
    if (request.method === "POST") {
      if (url.pathname === "/v1/fetch") return readBody(request).then(({ descriptor, options }) => fetchPage(descriptor, options));
      if (url.pathname === "/v1/head") return version(request, "getHeadVersion");
      if (url.pathname === "/v1/earliest") return version(request, "getEarliestVersion");
    }
    return json({ error: { name: "Error", message: "Not found" } }, 404);
  }

  function status(): GatewayStatus {
    return { topics: topics.status(), connections: connections.size };
  }

  return {
    async fetch(request, server) {
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
      let response: Response | undefined;
      if (!authorized(request, url)) {
        response = json({ error: { name: "StreamdError", message: "Unauthorized" } }, 401, { "www-authenticate": "Bearer" });
      } else {
        try {
          response = await route(request, url, server);
        } catch (err) {
          response = errorResponse(err);
        }
      }
      for (const [name, value] of Object.entries(cors)) response?.headers.set(name, value);
      return response;
    },
    websocket,
    status,
    close() {
      clearInterval(ticker);
      for (const connection of [...connections]) connection.close();
      topics.close();
    },
  };
}
