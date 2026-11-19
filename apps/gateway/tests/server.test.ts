import { afterEach, describe, expect, it } from "bun:test";
import { StreamdClient, StreamdNetworkError, parseEvent } from "@streamd/core";
import type { EventAdapter, EventDescriptor } from "@streamd/core";
import { createGateway } from "../src/server.js";
import type { Gateway, GatewayOptions } from "../src/server.js";
import { CountingAdapter, PING, PINGS, ev, waitFor } from "./helpers.js";

const open: { gateway: Gateway; client: StreamdClient }[] = [];

function setup(options: Partial<GatewayOptions> = {}, adapter: EventAdapter = new CountingAdapter()) {
  const client = new StreamdClient({ adapter });
  const gateway = createGateway({ client, pollInterval: 10, ...options });
  open.push({ gateway, client });
  return gateway;
}

afterEach(() => {
  for (const { gateway, client } of open.splice(0)) {
    gateway.close();
    client.destroy();
  }
});

async function call(gateway: Gateway, request: Request): Promise<Response> {
  return (await gateway.fetch(request))!;
}

function eventsUrl(descriptor: EventDescriptor = PINGS, extra = ""): string {
  return `http://gw/v1/events?descriptor=${encodeURIComponent(JSON.stringify(descriptor))}${extra}`;
}

/** Reads an SSE body as it arrives. */
function reader(response: Response) {
  const stream = response.body!.pipeThrough(new TextDecoderStream()).getReader();
  let text = "";
  let done = false;
  void (async () => {
    for (;;) {
      const chunk = await stream.read();
      if (chunk.done) break;
      text += chunk.value;
    }
    done = true;
  })().catch(() => (done = true));
  return {
    get text() {
      return text;
    },
    get done() {
      return done;
    },
    /** The `id`s and event versions received so far. */
    frames() {
      return [...text.matchAll(/^id: (\S+)\ndata: (.*)$/gm)].map((m) => ({ id: m[1]!, version: parseEvent(m[2]!).version }));
    },
    cancel: () => stream.cancel(),
  };
}

describe("SSE streams", () => {
  it("streams one topic's events to several clients from one walk", async () => {
    const adapter = new CountingAdapter();
    const gateway = setup({}, adapter);
    const a = await call(gateway, new Request(eventsUrl()));
    const b = await call(gateway, new Request(eventsUrl({ kind: "module", eventType: [PING] })));
    expect(a.headers.get("content-type")).toBe("text/event-stream");
    const ra = reader(a);
    const rb = reader(b);
    adapter.push(ev(1), ev(2));
    await waitFor(() => rb.frames().length === 2);
    expect(ra.frames().map((f) => f.version)).toEqual([1n, 2n]);
    expect(ra.frames().map((f) => f.id)).toEqual(rb.frames().map((f) => f.id));
    expect(gateway.status()).toMatchObject({ connections: 2, topics: [{ clients: 2, buffered: 2 }] });
    await ra.cancel();
    await waitFor(() => gateway.status().connections === 1);
  });

  it("resumes after Last-Event-ID, by header or query, with no gap and no repeat", async () => {
    const adapter = new CountingAdapter();
    const gateway = setup({}, adapter);
    const first = reader(await call(gateway, new Request(eventsUrl())));
    adapter.push(ev(1), ev(2));
    await waitFor(() => first.frames().length === 2);
    const last = first.frames()[0]!.id;
    await first.cancel();
    adapter.push(ev(3));
    const byHeader = reader(await call(gateway, new Request(eventsUrl(), { headers: { "last-event-id": last } })));
    const byQuery = reader(await call(gateway, new Request(eventsUrl(PINGS, `&lastEventId=${last}`))));
    await waitFor(() => byHeader.frames().length === 2 && byQuery.frames().length === 2);
    expect(byHeader.frames().map((f) => f.version)).toEqual([2n, 3n]);
    expect(byQuery.frames().map((f) => f.version)).toEqual([2n, 3n]);
  });

  it("answers 410 for a resume point no longer held", async () => {
    const adapter = new CountingAdapter();
    const gateway = setup({ buffer: 1 }, adapter);
    const first = reader(await call(gateway, new Request(eventsUrl())));
    adapter.push(ev(1), ev(2));
    await waitFor(() => first.frames().length === 2);
    const response = await call(gateway, new Request(eventsUrl(), { headers: { "last-event-id": first.frames()[0]!.id.replace(/:\d+$/, ":0") } }));
    expect(response.status).toBe(410);
    expect(((await response.json()) as { error: { statusCode: number } }).error.statusCode).toBe(410);
    expect((await call(gateway, new Request(eventsUrl(), { headers: { "last-event-id": "ffffffff:1" } }))).status).toBe(410);
  });

  it("ends every stream on a topic whose poller gave up", async () => {
    const client = new StreamdClient({
      adapter: { fetchEvents: async () => Promise.reject(new StreamdNetworkError("down")) },
      defaults: { maxErrors: 1 },
    });
    const ending = createGateway({ client, pollInterval: 1 });
    open.push({ gateway: ending, client });
    const stream = reader(await call(ending, new Request(eventsUrl())));
    await waitFor(() => stream.done);
    expect(stream.text).toContain("event: end\ndata: {\"message\":\"Polling failed (1/1): down\"}");
    expect(ending.status().connections).toBe(0);
  });

  it("closes a stream whose client falls behind maxQueuedBytes", async () => {
    const adapter = new CountingAdapter();
    const gateway = setup({ maxQueuedBytes: 200 }, adapter);
    const response = await call(gateway, new Request(eventsUrl()));
    adapter.push(ev(1), ev(2), ev(3));
    await waitFor(() => gateway.status().connections === 0);
    // Nothing read the body, so the queue passed the bound and the stream closed; what was queued still reads.
    const text = await response.text();
    expect(text).toContain("id: ");
  });

  it("sends keepalive comments", async () => {
    const gateway = setup({ keepAlive: 10 });
    const stream = reader(await call(gateway, new Request(eventsUrl())));
    await waitFor(() => stream.text.includes(": keepalive\n\n"));
  });
});

describe("requests", () => {
  it("refuses a bad or missing descriptor with 400", async () => {
    const gateway = setup();
    expect((await call(gateway, new Request("http://gw/v1/events"))).status).toBe(400);
    expect((await call(gateway, new Request("http://gw/v1/events?descriptor=nope"))).status).toBe(400);
    const bad = await call(gateway, new Request(eventsUrl({ kind: "module", eventType: "x" } as never)));
    expect(bad.status).toBe(400);
    expect(((await bad.json()) as { error: { name: string } }).error.name).toBe("StreamdError");
  });

  it("answers 404 for an unknown route", async () => {
    expect((await call(setup(), new Request("http://gw/nope"))).status).toBe(404);
  });

  it("needs the token as a bearer header or access_token when one is set", async () => {
    const gateway = setup({ token: "s3cret" });
    expect((await call(gateway, new Request("http://gw/v1/status"))).status).toBe(401);
    expect((await call(gateway, new Request("http://gw/v1/status", { headers: { authorization: "Bearer wrong" } }))).status).toBe(401);
    expect((await call(gateway, new Request("http://gw/v1/status", { headers: { authorization: "Bearer s3cret" } }))).status).toBe(200);
    expect((await call(gateway, new Request("http://gw/v1/status?access_token=s3cret"))).status).toBe(200);
  });

  it("answers allowed origins with CORS headers and preflights", async () => {
    const gateway = setup({ allowOrigins: ["https://app.example"] });
    const ok = await call(gateway, new Request("http://gw/v1/status", { headers: { origin: "https://app.example" } }));
    expect(ok.headers.get("access-control-allow-origin")).toBe("https://app.example");
    const other = await call(gateway, new Request("http://gw/v1/status", { headers: { origin: "https://evil.example" } }));
    expect(other.headers.get("access-control-allow-origin")).toBeNull();
    const preflight = await call(gateway, new Request("http://gw/v1/fetch", { method: "OPTIONS", headers: { origin: "https://app.example" } }));
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get("access-control-allow-headers")).toContain("authorization");
    const any = setup({ allowOrigins: ["*"] });
    const star = await call(any, new Request("http://gw/v1/status", { headers: { origin: "https://x.example" } }));
    expect(star.headers.get("access-control-allow-origin")).toBe("*");
  });

  it("refuses streams past maxConnections and topics past maxTopics with 503", async () => {
    const gateway = setup({ maxConnections: 1, maxTopics: 1 });
    reader(await call(gateway, new Request(eventsUrl())));
    expect((await call(gateway, new Request(eventsUrl()))).status).toBe(503);
    const topics = setup({ maxTopics: 1 });
    reader(await call(topics, new Request(eventsUrl())));
    const second = await call(topics, new Request(eventsUrl({ kind: "module", eventType: "0x1::other::Event" })));
    expect(second.status).toBe(503);
  });

  it("closes every stream on close()", async () => {
    const gateway = setup();
    const stream = reader(await call(gateway, new Request(eventsUrl())));
    gateway.close();
    await waitFor(() => stream.done);
    expect(gateway.status()).toEqual({ topics: [], connections: 0 });
  });
});
