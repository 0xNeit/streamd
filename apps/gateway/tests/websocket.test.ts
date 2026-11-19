import { afterEach, describe, expect, it } from "bun:test";
import { StreamdClient, StreamdNetworkError, parseEvent } from "@streamd/core";
import type { EventAdapter } from "@streamd/core";
import type { Server } from "bun";
import { createGateway } from "../src/server.js";
import type { GatewayOptions, SocketData } from "../src/server.js";
import { CountingAdapter, PINGS, ev, waitFor } from "./helpers.js";

const cleanup: (() => void)[] = [];

afterEach(() => {
  for (const done of cleanup.splice(0)) done();
});

function serve(options: Partial<GatewayOptions> = {}, adapter: EventAdapter = new CountingAdapter()) {
  const client = new StreamdClient({ adapter, defaults: { maxBackoff: 1 } });
  const gateway = createGateway({ client, pollInterval: 10, ...options });
  const server: Server<SocketData> = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch: (request, srv) => gateway.fetch(request, srv),
    websocket: gateway.websocket,
  });
  cleanup.push(() => {
    gateway.close();
    client.destroy();
    server.stop(true);
  });
  return { gateway, url: `ws://127.0.0.1:${server.port}/v1/ws?descriptor=${encodeURIComponent(JSON.stringify(PINGS))}` };
}

function connect(url: string, headers?: Record<string, string>) {
  const ws = new WebSocket(url, { headers } as never);
  const messages: { id?: string; version?: bigint; end?: string }[] = [];
  let closed: { code: number; reason: string } | undefined;
  ws.onmessage = (m) => {
    const body = JSON.parse(String(m.data)) as { id?: string; event?: unknown; end?: string };
    messages.push(body.event === undefined ? { end: body.end } : { id: body.id, version: parseEvent(JSON.stringify(body.event)).version });
  };
  ws.onclose = (e) => (closed = { code: e.code, reason: e.reason });
  cleanup.push(() => ws.close());
  return {
    ws,
    messages,
    get closed() {
      return closed;
    },
    opened: new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("socket error"));
    }),
  };
}

describe("WebSocket streams", () => {
  it("sends each event as {id, event}, the same ids SSE uses, and resumes after lastEventId", async () => {
    const adapter = new CountingAdapter();
    const { gateway, url } = serve({}, adapter);
    const first = connect(url);
    await first.opened;
    adapter.push(ev(1), ev(2));
    await waitFor(() => first.messages.length === 2);
    expect(first.messages.map((m) => m.version)).toEqual([1n, 2n]);
    expect(gateway.status().connections).toBe(1);
    first.ws.close();
    await waitFor(() => gateway.status().connections === 0);
    adapter.push(ev(3));
    await waitFor(() => gateway.status().topics[0]!.buffered === 3);
    const again = connect(`${url}&lastEventId=${first.messages[0]!.id}`);
    await again.opened;
    await waitFor(() => again.messages.length === 2);
    expect(again.messages.map((m) => m.version)).toEqual([2n, 3n]);
  });

  it("refuses a resume point no longer held with 410 before upgrading", async () => {
    const { url } = serve();
    const refused = connect(`${url}&lastEventId=ffffffff:1`);
    await expect(refused.opened).rejects.toThrow();
  });

  it("refuses an Origin not allowed and not the gateway's own host", async () => {
    const { url } = serve({ allowOrigins: ["https://app.example"] });
    await expect(connect(url, { origin: "https://evil.example" }).opened).rejects.toThrow();
    await connect(url, { origin: "https://app.example" }).opened;
    const host = new URL(url).host;
    await connect(url, { origin: `http://${host}` }).opened;
  });

  it("needs the token when one is set", async () => {
    const { url } = serve({ token: "s3cret" });
    await expect(connect(url).opened).rejects.toThrow();
    await connect(`${url}&access_token=s3cret`).opened;
  });

  it("sends the end and closes 1011 when the topic's poller gives up", async () => {
    const { url } = serve({ pollInterval: 1 }, {
      fetchEvents: async () => Promise.reject(new StreamdNetworkError("down")),
    });
    const socket = connect(url);
    await socket.opened;
    await waitFor(() => socket.closed !== undefined, 5000);
    expect(socket.closed!.code).toBe(1011);
    expect(socket.messages).toEqual([{ end: "Polling failed (10/10): down" }]);
  });

  it("closes every socket on close()", async () => {
    const { gateway, url } = serve();
    const socket = connect(url);
    await socket.opened;
    gateway.close();
    await waitFor(() => socket.closed !== undefined);
    expect(socket.closed!.code).toBe(1001);
  });
});
