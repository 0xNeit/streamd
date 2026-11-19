import { redactSecrets } from "@streamd/core";
import type { EventAdapter, StreamdClient } from "@streamd/core";
import { createGateway } from "@streamd/gateway";
import type { Gateway } from "@streamd/gateway";
import type { ServeOptions } from "./args.js";
import { message } from "./format.js";

export interface ServeDeps {
  client: StreamdClient;
  /** What the gateway's history reads go to. */
  adapter: EventAdapter;
  error(message: string): void;
  signal: AbortSignal;
}

// Above the gateway's 15 s SSE keepalive, so Bun never times out a quiet stream.
const IDLE_TIMEOUT_SECONDS = 30;

/** Runs the gateway until `signal` aborts; 1 if it cannot run here or cannot bind. */
export async function runServe(options: ServeOptions, deps: ServeDeps): Promise<number> {
  const bun = (globalThis as { Bun?: typeof Bun }).Bun;
  if (bun === undefined) {
    deps.error("streamd: serve needs Bun, for its WebSocket server: run it with bun x streamd serve");
    return 1;
  }
  let gateway: Gateway;
  try {
    gateway = createGateway({
      client: deps.client,
      adapter: deps.adapter,
      token: options.token,
      allowOrigins: options.allowOrigins,
      maxTopics: options.maxTopics,
      maxConnections: options.maxConnections,
      pollInterval: options.pollInterval,
      onError: (err, descriptor) => {
        deps.error(`streamd: ${JSON.stringify(descriptor, (_k, v: unknown) => (typeof v === "bigint" ? String(v) : v))}: ${redactSecrets(message(err))}`);
      },
      ...(options.webhooks !== undefined && {
        webhooks: {
          ...options.webhooks,
          onError: (text, id) => deps.error(`streamd: ${id === undefined ? "webhooks" : `webhook ${id}`}: ${redactSecrets(text)}`),
        },
      }),
    });
  } catch (err) {
    deps.error(`streamd: ${message(err)}`);
    return 1;
  }
  const { host, port } = options.listen;
  let server: ReturnType<typeof Bun.serve>;
  try {
    server = bun.serve({
      hostname: host,
      port,
      idleTimeout: IDLE_TIMEOUT_SECONDS,
      fetch: (request, srv) => gateway.fetch(request, srv),
      websocket: gateway.websocket,
    });
  } catch (err) {
    gateway.close();
    deps.error(`streamd: cannot listen on ${host}:${port}: ${(err as { code?: string }).code ?? message(err)}`);
    return 1;
  }
  deps.error(`streamd: gateway listening on http://${host.includes(":") ? `[${host}]` : host}:${server.port}`);
  if (!deps.signal.aborted) await new Promise((resolve) => deps.signal.addEventListener("abort", resolve, { once: true }));
  gateway.close();
  await server.stop(true);
  return 0;
}
