import { afterEach, describe, expect, it } from "bun:test";
import { StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import { GatewayAdapter } from "../src/adapter.js";
import { createGateway } from "../src/server.js";
import type { Gateway, GatewayOptions } from "../src/server.js";
import { CountingAdapter, PINGS, ev, waitFor } from "./helpers.js";

const cleanup: (() => void)[] = [];

afterEach(() => {
  for (const done of cleanup.splice(0)) done();
});

function setup(options: Partial<GatewayOptions> = {}, source = new CountingAdapter()) {
  const upstream = new StreamdClient({ adapter: source });
  const gateway = createGateway({ client: upstream, adapter: source, pollInterval: 10, ...options });
  cleanup.push(() => {
    gateway.close();
    upstream.destroy();
  });
  return { gateway, source };
}

function through(gateway: Gateway, token?: string): GatewayAdapter {
  return new GatewayAdapter({
    url: "http://gw/",
    token,
    fetch: async (url, init) => (await gateway.fetch(new Request(url, init)))!,
  });
}

function clientOf(adapter: EventAdapter): StreamdClient {
  const client = new StreamdClient({ adapter, defaults: { pollInterval: 10, maxBackoff: 1 } });
  cleanup.push(() => client.destroy());
  return client;
}

async function failure(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected a failure");
}

describe("GatewayAdapter", () => {
  it("follows the gateway's live topic, every client sharing one upstream walk", async () => {
    const { gateway, source } = setup();
    const a: StreamdEvent[] = [];
    const b: StreamdEvent[] = [];
    clientOf(through(gateway)).on(PINGS, (e) => void a.push(e));
    await waitFor(() => gateway.status().topics.length === 1);
    source.push(ev(1));
    await waitFor(() => a.length === 1);
    clientOf(through(gateway)).on(PINGS, (e) => void b.push(e));
    await Bun.sleep(30);
    source.push(ev(2), ev(3));
    await waitFor(() => a.length === 3 && b.length === 2);
    expect(a.map((e) => e.version)).toEqual([1n, 2n, 3n]);
    expect(b.map((e) => e.version)).toEqual([2n, 3n]);
    expect(gateway.status().topics).toHaveLength(1);
    // Only the topic's poller and no client reached the source: none of its requests carries a start.
    expect(source.calls.every((call) => call.fromVersion === undefined)).toBe(true);
  });

  it("reads history through the gateway's node, paging on upstream cursors", async () => {
    const source = new CountingAdapter(Array.from({ length: 30 }, (_, i) => ev(i + 1)));
    const { gateway } = setup({}, source);
    const client = clientOf(through(gateway));
    const all = await client.getEvents(PINGS, { fromVersion: 0n, toVersion: 40n });
    expect(all.map((e) => e.version)).toEqual(Array.from({ length: 30 }, (_, i) => BigInt(i + 1)));
    expect(source.calls.some((call) => call.cursor !== undefined && !call.cursor.startsWith("u:"))).toBe(true);
    const newest = await client.getEvents(PINGS, { fromVersion: -2n });
    expect(newest.map((e) => e.version)).toEqual([28n, 29n, 30n]);
    expect(gateway.status().topics).toHaveLength(0);
  });

  it("maps a stale topic cursor to a 410 network error and a foreign cursor to invalid input", async () => {
    const { gateway } = setup();
    const adapter = through(gateway);
    const stale = await failure(adapter.fetchEvents(PINGS, { cursor: "t:ffffffff:1" }));
    expect(stale).toBeInstanceOf(StreamdNetworkError);
    expect((stale as StreamdNetworkError).statusCode).toBe(410);
    const foreign = await failure(adapter.fetchEvents(PINGS, { cursor: "123" }));
    expect(foreign).toBeInstanceOf(StreamdError);
    expect(foreign).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("serves topic pages of at most limit events, with hasMore", async () => {
    const { gateway, source } = setup();
    const adapter = through(gateway);
    const start = await adapter.fetchEvents(PINGS, {});
    expect(start.events).toEqual([]);
    source.push(ev(1), ev(2), ev(3));
    await waitFor(() => gateway.status().topics[0]!.buffered === 3);
    const first = await adapter.fetchEvents(PINGS, { cursor: start.nextCursor, limit: 2 });
    expect(first.events.map((e) => e.version)).toEqual([1n, 2n]);
    expect(first.hasMore).toBe(true);
    const rest = await adapter.fetchEvents(PINGS, { cursor: first.nextCursor, limit: 2 });
    expect(rest.events.map((e) => e.version)).toEqual([3n]);
    expect(rest.hasMore).toBe(false);
    const empty = await adapter.fetchEvents(PINGS, { cursor: rest.nextCursor });
    expect(empty).toEqual({ events: [], nextCursor: rest.nextCursor, hasMore: false });
  });

  it("refuses history without an upstream adapter, and a missing token, as invalid input", async () => {
    const source = new CountingAdapter();
    const upstream = new StreamdClient({ adapter: source });
    const gateway = createGateway({ client: upstream, token: "s3cret" });
    cleanup.push(() => {
      gateway.close();
      upstream.destroy();
    });
    const noToken = await failure(through(gateway).fetchEvents(PINGS, {}));
    expect(noToken).toBeInstanceOf(StreamdError);
    expect(noToken).not.toBeInstanceOf(StreamdNetworkError);
    const history = await failure(through(gateway, "s3cret").fetchEvents(PINGS, { fromVersion: 1n }));
    expect((history as Error).message).toBe("This gateway serves live topics only");
    expect(await failure(through(gateway, "s3cret").getHeadVersion(PINGS))).toBeInstanceOf(StreamdError);
  });

  it("turns a failed request into a retryable network error, and keeps a Retry-After", async () => {
    const down = new GatewayAdapter({ url: "http://gw", fetch: () => Promise.reject(new Error("refused")) });
    const err = await failure(down.fetchEvents(PINGS, {}));
    expect(err).toBeInstanceOf(StreamdNetworkError);
    const limited = new GatewayAdapter({
      url: "http://gw",
      fetch: async () => new Response("busy", { status: 503, headers: { "retry-after": "2" } }),
    });
    const busy = (await failure(limited.fetchEvents(PINGS, {}))) as StreamdNetworkError;
    expect(busy.statusCode).toBe(503);
    expect(busy.retryAfter).toBe(2000);
    expect(() => new GatewayAdapter({ url: "ftp://gw" })).toThrow(StreamdError);
  });

  it("refuses an oversized body with 413 and a malformed one with 400", async () => {
    const { gateway } = setup();
    const big = await gateway.fetch(new Request("http://gw/v1/fetch", { method: "POST", body: "x".repeat(70_000) }));
    expect(big!.status).toBe(413);
    const bad = await gateway.fetch(new Request("http://gw/v1/fetch", { method: "POST", body: "[]" }));
    expect(bad!.status).toBe(400);
  });
});
