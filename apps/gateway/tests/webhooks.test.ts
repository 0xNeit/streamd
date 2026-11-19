import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, WEBHOOK_SIGNATURE_HEADER, parseEvent, verifyWebhook } from "@streamd/core";
import { createGateway } from "../src/server.js";
import type { Gateway } from "../src/server.js";
import { Webhooks } from "../src/webhooks.js";
import { toJson } from "../src/wire.js";
import type { WebhooksOptions } from "../src/webhooks.js";
import { PINGS, ev, waitFor } from "./helpers.js";

interface Received {
  versions: bigint[];
  verified: boolean;
}

let dir: string;
let file: string;
const cleanup: (() => void)[] = [];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "streamd-webhooks-"));
  file = join(dir, "webhooks.json");
});

afterEach(() => {
  for (const fn of cleanup.splice(0).reverse()) fn();
  rmSync(dir, { recursive: true, force: true });
});

/** A receiver answering each POST with the next status in `statuses`, then 204. */
function receiver(statuses: { status: number; headers?: Record<string, string> }[] = []) {
  const received: Received[] = [];
  const secrets: string[] = [];
  let calls = 0;
  const server = Bun.serve({
    port: 0,
    async fetch(request) {
      const body = await request.text();
      const reply = statuses[calls++] ?? { status: 204 };
      if (reply.status < 300) {
        const verified = await verifyWebhook(body, request.headers.get(WEBHOOK_SIGNATURE_HEADER), secrets[0]!);
        received.push({ versions: (JSON.parse(body) as unknown[]).map((e) => parseEvent(JSON.stringify(e)).version), verified });
      }
      return new Response(null, { status: reply.status, headers: reply.headers });
    },
  });
  cleanup.push(() => server.stop(true));
  return {
    url: `http://127.0.0.1:${server.port}/hook`,
    received,
    secrets,
    get calls() {
      return calls;
    },
    versions: () => received.flatMap((r) => r.versions),
  };
}

function setup(options: Partial<WebhooksOptions> = {}, adapter = new MemoryAdapter()) {
  const client = new StreamdClient({ adapter });
  const webhooks = new Webhooks({
    client,
    file,
    allowPrivateTargets: true,
    pollInterval: 5,
    retryDelay: 1,
    headVersion: () => adapter.getHeadVersion(),
    ...options,
  });
  cleanup.push(() => {
    webhooks.close();
    client.destroy();
  });
  return { webhooks, adapter, client };
}

async function register(webhooks: Webhooks, target: ReturnType<typeof receiver>) {
  const created = await webhooks.create({ url: target.url, descriptor: { kind: "module", eventType: "0x1::ping::Ping" } });
  target.secrets.push(created.secret);
  return created;
}

describe("Webhooks", () => {
  it("posts signed batches of new events and saves the cursor after each", async () => {
    const target = receiver();
    const { webhooks, adapter } = setup();
    const created = await register(webhooks, target);
    expect(created.secret).toMatch(/^[0-9a-f]{64}$/);
    expect(created.id).toMatch(/^[0-9a-f]{16}$/);
    adapter.push(ev(2), ev(3));
    await waitFor(() => target.versions().length === 2);
    expect(target.received.every((r) => r.verified)).toBe(true);
    expect(target.versions()).toEqual([2n, 3n]);
    await waitFor(() => webhooks.get(created.id)!.delivered === 2);
    const saved = JSON.parse(readFileSync(file, "utf8"));
    expect(saved.webhooks[0].cursor).toBe(webhooks.get(created.id)!.cursor);
    expect(statSync(file).mode & 0o777).toBe(0o600);
  });

  it("never lists a secret", async () => {
    const target = receiver();
    const { webhooks } = setup();
    const created = await register(webhooks, target);
    expect(toJson(webhooks.list())).not.toContain(created.secret);
    expect(toJson(webhooks.get(created.id))).not.toContain(created.secret);
    expect(webhooks.list()).toEqual([expect.objectContaining({ id: created.id, status: "active", url: target.url })]);
  });

  it("splits a page at batchSize", async () => {
    const target = receiver();
    const { webhooks, adapter } = setup({ batchSize: 2 });
    await register(webhooks, target);
    adapter.push(ev(2), ev(3), ev(4));
    await waitFor(() => target.versions().length === 3);
    expect(target.received.map((r) => r.versions.length)).toEqual([2, 1]);
  });

  it("retries a 503 after its Retry-After", async () => {
    const target = receiver([{ status: 503, headers: { "retry-after": "0" } }]);
    const errors: string[] = [];
    const { webhooks, adapter } = setup({ onError: (m) => errors.push(m) });
    const created = await register(webhooks, target);
    adapter.push(ev(2));
    await waitFor(() => target.versions().length === 1);
    expect(target.calls).toBe(2);
    expect(errors[0]).toContain("HTTP 503");
    expect(webhooks.get(created.id)!.status).toBe("active");
  });

  it("marks a webhook failed past its retries, keeps its cursor, and resumes it with the same events", async () => {
    const target = receiver([{ status: 500 }, { status: 500 }]);
    const { webhooks, adapter } = setup({ retries: 1 });
    const created = await register(webhooks, target);
    const before = webhooks.get(created.id)!.cursor;
    adapter.push(ev(2));
    await waitFor(() => webhooks.get(created.id)!.status === "failed");
    expect(webhooks.get(created.id)!.cursor).toBe(before);
    expect(webhooks.get(created.id)!.lastError).toBe("delivery failed: HTTP 500");
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks[0].status).toBe("failed");
    adapter.push(ev(3));
    expect(webhooks.resume(created.id)).toMatchObject({ status: "active" });
    expect(webhooks.resume(created.id)).toMatchObject({ status: "active" });
    expect(webhooks.get(created.id)!.lastError).toBeUndefined();
    await waitFor(() => target.versions().length === 2);
    expect(target.versions()).toEqual([2n, 3n]);
  });

  it("does not post a 3xx's redirect, failing instead", async () => {
    const target = receiver([{ status: 302, headers: { location: "http://127.0.0.1:1/" } }]);
    const { webhooks, adapter } = setup({ retries: 3 });
    const created = await register(webhooks, target);
    adapter.push(ev(2));
    await waitFor(() => webhooks.get(created.id)!.status === "failed");
    expect(target.calls).toBe(1);
    expect(webhooks.get(created.id)!.lastError).toBe("delivery failed: HTTP 302");
  });

  it("resumes after a restart from the last acknowledged page, losing nothing", async () => {
    const target = receiver();
    const adapter = new MemoryAdapter();
    const first = setup({}, adapter);
    const created = await register(first.webhooks, target);
    adapter.push(ev(2));
    await waitFor(() => target.versions().length === 1);
    await waitFor(() => JSON.parse(readFileSync(file, "utf8")).webhooks[0].cursor === first.webhooks.get(created.id)!.cursor);
    first.webhooks.close();
    first.client.destroy();
    adapter.push(ev(3), ev(4));
    const second = setup({}, adapter);
    expect(second.webhooks.get(created.id)).toMatchObject({ status: "active" });
    await waitFor(() => target.versions().length === 3);
    expect(target.versions()).toEqual([2n, 3n, 4n]);
  });

  it("saves the cursor of a post in flight at close as the page before it", async () => {
    let release!: () => void;
    let arrive!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const arrived = new Promise<void>((resolve) => (arrive = resolve));
    const versions: bigint[] = [];
    const server = Bun.serve({
      port: 0,
      async fetch(request) {
        const events = JSON.parse(await request.text()) as unknown[];
        if (events.length > 0 && versions.length === 0 && (events[0] as { version: string }).version === "2") {
          arrive();
          await gate;
        }
        versions.push(...events.map((e) => BigInt((e as { version: string }).version)));
        return new Response(null, { status: 204 });
      },
    });
    cleanup.push(() => server.stop(true));
    const adapter = new MemoryAdapter();
    const first = setup({}, adapter);
    const created = await first.webhooks.create({ url: `http://127.0.0.1:${server.port}/`, descriptor: PINGS });
    const caughtUp = first.webhooks.get(created.id)!.cursor;
    adapter.push(ev(2));
    await arrived;
    first.webhooks.close();
    release();
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks[0].cursor).toBe(caughtUp);
    first.client.destroy();
    const second = setup({}, adapter);
    await waitFor(() => versions.filter((v) => v === 2n).length === 2);
    expect(second.webhooks.get(created.id)!.status).toBe("active");
  });

  it("deletes a webhook, which then posts nothing", async () => {
    const target = receiver();
    const control = receiver();
    const { webhooks, adapter } = setup();
    const created = await register(webhooks, target);
    const kept = await register(webhooks, control);
    expect(webhooks.delete(created.id)).toBe(true);
    expect(webhooks.delete(created.id)).toBe(false);
    adapter.push(ev(2));
    await waitFor(() => control.versions().length === 1);
    await Bun.sleep(20);
    expect(target.calls).toBe(0);
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks.map((w: { id: string }) => w.id)).toEqual([kept.id]);
  });

  it("refuses private targets unless allowed, bad input, and more than maxWebhooks", async () => {
    const { webhooks } = setup({ allowPrivateTargets: false, maxWebhooks: 1, lookup: async () => [{ address: "93.184.215.14", family: 4 }] });
    await expect(webhooks.create({ url: "http://127.0.0.1:9/", descriptor: PINGS })).rejects.toThrow("not a public address");
    await expect(webhooks.create({ url: "https://hooks.example.com/", descriptor: { kind: "nope" } })).rejects.toBeInstanceOf(StreamdError);
    await expect(webhooks.create("x")).rejects.toBeInstanceOf(StreamdError);
    await webhooks.create({ url: "https://hooks.example.com/", descriptor: PINGS });
    await expect(webhooks.create({ url: "https://hooks.example.com/", descriptor: PINGS })).rejects.toMatchObject({ statusCode: 503 });
  });

  it("delivers from its registration after failing before any cursor was saved", async () => {
    const target = receiver([{ status: 500 }, { status: 500 }]);
    const { webhooks, adapter } = setup({ retries: 1 });
    adapter.push(ev(1));
    const created = await webhooks.create({ url: target.url, descriptor: PINGS });
    target.secrets.push(created.secret);
    expect(created.fromVersion).toBe(1n);
    await waitFor(() => webhooks.get(created.id)!.status === "failed");
    adapter.push(ev(2));
    webhooks.resume(created.id);
    await waitFor(() => target.versions().length === 2);
    expect(target.versions()).toEqual([1n, 2n]);
  });

  it("starts a restarted webhook that never saved a cursor at its registration, not the later head", async () => {
    const target = receiver();
    const adapter = new MemoryAdapter();
    const first = setup({ pollInterval: 60_000 }, adapter);
    adapter.push(ev(1));
    const created = await first.webhooks.create({ url: target.url, descriptor: PINGS });
    target.secrets.push(created.secret);
    first.webhooks.close();
    first.client.destroy();
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks[0]).toMatchObject({ fromVersion: "1" });
    adapter.push(ev(2));
    setup({}, adapter);
    await waitFor(() => target.versions().length === 2);
    expect(target.versions()).toEqual([1n, 2n]);
  });

  it("marks a webhook failed when its poller gives up", async () => {
    const target = receiver();
    const adapter = new MemoryAdapter();
    const { webhooks } = setup({}, adapter);
    const created = await register(webhooks, target);
    adapter.fetchEvents = async () => {
      throw new StreamdError("bad descriptor");
    };
    await waitFor(() => webhooks.get(created.id)!.status === "failed");
    expect(webhooks.get(created.id)!.lastError).toContain("bad descriptor");
  });

  it("checks the target again before each POST", async () => {
    const target = receiver();
    let address = "93.184.215.14";
    const { webhooks, adapter } = setup({
      allowPrivateTargets: false,
      retries: 0,
      lookup: async () => [{ address, family: 4 }],
    });
    const created = await webhooks.create({ url: target.url.replace("127.0.0.1", "hooks.example.com"), descriptor: PINGS });
    address = "10.0.0.1";
    adapter.push(ev(2));
    await waitFor(() => webhooks.get(created.id)!.status === "failed");
    expect(webhooks.get(created.id)!.lastError).toContain("not a public address");
    expect(target.calls).toBe(0);
  });

  it("leaves nothing registered when the file cannot be written", async () => {
    file = join(dir, "missing", "webhooks.json");
    const { webhooks } = setup();
    await expect(webhooks.create({ url: "http://127.0.0.1:9/", descriptor: PINGS })).rejects.toThrow();
    expect(webhooks.size).toBe(0);
  });

  it("starts nothing once closed", async () => {
    const { webhooks } = setup();
    webhooks.close();
    await expect(webhooks.create({ url: "http://127.0.0.1:9/", descriptor: PINGS })).rejects.toMatchObject({ statusCode: 503 });
  });

  it("refuses to start over a malformed file, naming it", () => {
    writeFileSync(file, '{"webhooks":[{"id":"zz"}]}');
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    cleanup.push(() => client.destroy());
    const headVersion = async () => 0n;
    expect(() => new Webhooks({ client, file, headVersion })).toThrow(`webhook file ${file}: webhook 0: "id" is missing or invalid`);
    writeFileSync(file, "{");
    expect(() => new Webhooks({ client, file, headVersion })).toThrow(StreamdError);
  });
});

describe("webhook routes", () => {
  function gateway(options: { token?: string; webhooks?: boolean } = {}): Gateway {
    const adapter = new MemoryAdapter();
    const client = new StreamdClient({ adapter });
    const g = createGateway({
      client,
      adapter,
      pollInterval: 5,
      token: options.token,
      ...(options.webhooks !== false && { webhooks: { file, allowPrivateTargets: true, maxWebhooks: 1 } }),
    });
    cleanup.push(() => {
      g.close();
      client.destroy();
    });
    return g;
  }

  const call = async (g: Gateway, method: string, path: string, body?: unknown, headers: Record<string, string> = {}) =>
    (await g.fetch(
      new Request(`http://localhost${path}`, {
        method,
        headers: body === undefined ? headers : { "content-type": "application/json", ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
      }),
    ))!;

  it("creates with the secret once, then lists, gets, resumes and deletes without it", async () => {
    const g = gateway();
    const target = receiver();
    const created = await call(g, "POST", "/v1/webhooks", { url: target.url, descriptor: PINGS });
    expect(created.status).toBe(201);
    const body = (await created.json()) as { id: string; secret: string; descriptor: unknown; status: string };
    expect(body).toMatchObject({ descriptor: PINGS, status: "active", delivered: 0, url: target.url });
    expect(body.secret).toMatch(/^[0-9a-f]{64}$/);
    const listed = await (await call(g, "GET", "/v1/webhooks")).text();
    expect(listed).toContain(body.id);
    expect(listed).not.toContain(body.secret);
    const one = await call(g, "GET", `/v1/webhooks/${body.id}`);
    expect(await one.json()).toMatchObject({ id: body.id, status: "active" });
    expect(await (await call(g, "POST", `/v1/webhooks/${body.id}/resume`)).json()).toMatchObject({ status: "active" });
    expect(g.status().webhooks).toBe(1);
    expect((await call(g, "DELETE", `/v1/webhooks/${body.id}`)).status).toBe(204);
    expect((await call(g, "GET", `/v1/webhooks/${body.id}`)).status).toBe(404);
    expect((await call(g, "DELETE", `/v1/webhooks/${body.id}`)).status).toBe(404);
  });

  it("answers bad input 400, a non-JSON body 415, past maxWebhooks 503, and unknown paths 404", async () => {
    const g = gateway();
    const target = receiver();
    expect((await call(g, "POST", "/v1/webhooks", { url: "ftp://x/", descriptor: PINGS })).status).toBe(400);
    expect((await call(g, "POST", "/v1/webhooks", { url: target.url, descriptor: {} })).status).toBe(400);
    const form = await g.fetch(new Request("http://localhost/v1/webhooks", { method: "POST", body: "url=x" }));
    expect(form!.status).toBe(415);
    expect((await call(g, "POST", "/v1/webhooks", { url: target.url, descriptor: PINGS })).status).toBe(201);
    expect((await call(g, "POST", "/v1/webhooks", { url: target.url, descriptor: PINGS })).status).toBe(503);
    expect((await call(g, "GET", "/v1/webhooks/nope")).status).toBe(404);
    expect((await call(g, "POST", "/v1/webhooks/0123456789abcdef/resume")).status).toBe(404);
    expect((await call(g, "PUT", "/v1/webhooks")).status).toBe(404);
  });

  it("requires the token, and is absent without a webhook file", async () => {
    const guarded = gateway({ token: "t0ken" });
    expect((await call(guarded, "GET", "/v1/webhooks")).status).toBe(401);
    expect((await call(guarded, "GET", "/v1/webhooks", undefined, { authorization: "Bearer t0ken" })).status).toBe(200);
    expect((await call(gateway({ webhooks: false }), "GET", "/v1/webhooks")).status).toBe(404);
  });

  it("keeps browsers off the webhook API without a token, and needs a head to start from", async () => {
    const g = gateway();
    expect((await call(g, "GET", "/v1/webhooks", undefined, { origin: "https://evil.example" })).status).toBe(403);
    expect((await call(gateway({ token: "t" }), "GET", "/v1/webhooks", undefined, { origin: "https://app.example", authorization: "Bearer t" })).status).toBe(200);
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    cleanup.push(() => client.destroy());
    expect(() => createGateway({ client, webhooks: { file } })).toThrow("getHeadVersion");
  });
});
