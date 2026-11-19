import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, WEBHOOK_SIGNATURE_HEADER, parseEvent, verifyWebhook } from "@streamd/core";
import { Webhooks } from "../src/webhooks.js";
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
  const webhooks = new Webhooks({ client, file, allowPrivateTargets: true, pollInterval: 5, retryDelay: 1, ...options });
  cleanup.push(() => {
    webhooks.close();
    client.destroy();
  });
  return { webhooks, adapter, client };
}

async function register(webhooks: Webhooks, target: ReturnType<typeof receiver>) {
  const created = await webhooks.create({ url: target.url, descriptor: { kind: "module", eventType: "0x1::ping::Ping" } });
  target.secrets.push(created.secret);
  await waitFor(() => webhooks.get(created.id)?.cursor !== undefined);
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
    expect(JSON.stringify(webhooks.list())).not.toContain(created.secret);
    expect(JSON.stringify(webhooks.get(created.id))).not.toContain(created.secret);
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
    expect(webhooks.get(created.id)).toMatchObject({ cursor: before, lastError: "delivery failed: HTTP 500" });
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks[0].status).toBe("failed");
    adapter.push(ev(3));
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
    const gate = new Promise<void>((resolve) => (release = resolve));
    const versions: bigint[] = [];
    const server = Bun.serve({
      port: 0,
      async fetch(request) {
        const events = JSON.parse(await request.text()) as unknown[];
        if (events.length > 0 && versions.length === 0 && (events[0] as { version: string }).version === "2") await gate;
        versions.push(...events.map((e) => BigInt((e as { version: string }).version)));
        return new Response(null, { status: 204 });
      },
    });
    cleanup.push(() => server.stop(true));
    const adapter = new MemoryAdapter();
    const first = setup({}, adapter);
    const created = await first.webhooks.create({ url: `http://127.0.0.1:${server.port}/`, descriptor: PINGS });
    await waitFor(() => first.webhooks.get(created.id)?.cursor !== undefined);
    const caughtUp = first.webhooks.get(created.id)!.cursor;
    adapter.push(ev(2));
    await Bun.sleep(30);
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
    const { webhooks, adapter } = setup();
    const created = await register(webhooks, target);
    expect(webhooks.delete(created.id)).toBe(true);
    expect(webhooks.delete(created.id)).toBe(false);
    adapter.push(ev(2));
    await Bun.sleep(40);
    expect(target.calls).toBe(0);
    expect(JSON.parse(readFileSync(file, "utf8")).webhooks).toEqual([]);
  });

  it("refuses private targets unless allowed, bad input, and more than maxWebhooks", async () => {
    const { webhooks } = setup({ allowPrivateTargets: false, maxWebhooks: 1, lookup: async () => [{ address: "93.184.215.14", family: 4 }] });
    await expect(webhooks.create({ url: "http://127.0.0.1:9/", descriptor: PINGS })).rejects.toThrow("not a public address");
    await expect(webhooks.create({ url: "https://hooks.example.com/", descriptor: { kind: "nope" } })).rejects.toBeInstanceOf(StreamdError);
    await expect(webhooks.create("x")).rejects.toBeInstanceOf(StreamdError);
    await webhooks.create({ url: "https://hooks.example.com/", descriptor: PINGS });
    await expect(webhooks.create({ url: "https://hooks.example.com/", descriptor: PINGS })).rejects.toMatchObject({ statusCode: 503 });
  });

  it("refuses to start over a malformed file, naming it", () => {
    writeFileSync(file, '{"webhooks":[{"id":"zz"}]}');
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    cleanup.push(() => client.destroy());
    expect(() => new Webhooks({ client, file })).toThrow(`webhook file ${file}: webhook 0: "id" is missing or invalid`);
    writeFileSync(file, "{");
    expect(() => new Webhooks({ client, file })).toThrow(StreamdError);
  });
});
