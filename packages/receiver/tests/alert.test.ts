import { describe, expect, it } from "bun:test";
import { serializeAlert } from "@streamd/core";
import type { AlertPayload } from "@streamd/core";
import { createAlertReceiver } from "../src/alert.js";
import { MemoryDedupStore } from "../src/store.js";

const SECRET = "test-secret";

async function sign(body: string, secret = SECRET): Promise<string> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(body)));
  return `sha256=${Buffer.from(mac).toString("hex")}`;
}

async function post(body: string, init: { signature?: string | null; contentType?: string; method?: string } = {}): Promise<Request> {
  const signature = init.signature === undefined ? await sign(body) : init.signature;
  return new Request("http://127.0.0.1/alerts", {
    method: init.method ?? "POST",
    headers: {
      "content-type": init.contentType ?? "application/json",
      ...(signature !== null && { "x-streamd-signature": signature }),
    },
    body: init.method === "GET" ? undefined : body,
  });
}

const event = { type: "0x1::m::E", data: { amount: "5" }, version: 9n, sequenceNumber: 0n, eventIndex: 1, timestamp: 61_000_000n };

function alert(start = 60_000_000n, rule = "burst"): AlertPayload {
  return { id: `${rule}:${start}`, rule, windowStart: start, windowEnd: start + 60_000_000n, threshold: { count: 2n }, value: 2n, event };
}

const anomaly: AlertPayload = {
  ...alert(60_000_000n, "rate"),
  threshold: { anomaly: { metric: "count", k: "3", baseline: 30, minBaseline: 10, direction: "both" } },
  value: 412n,
  anomaly: { side: "above", median: "201.5", mad: "18", low: "121.4396", high: "281.5604", baselineWindows: 30 },
};

function recorder() {
  const alerts: AlertPayload[] = [];
  const errors: unknown[] = [];
  return { alerts, errors, onAlert: (a: AlertPayload) => void alerts.push(a), onError: (e: unknown) => void errors.push(e) };
}

describe("createAlertReceiver", () => {
  it("hands threshold and anomaly alerts to onAlert once per id and answers 204", async () => {
    const r = recorder();
    const receive = createAlertReceiver({ secret: SECRET, onAlert: r.onAlert });
    for (const a of [alert(), anomaly, alert(), alert(120_000_000n)]) {
      expect((await receive(await post(serializeAlert(a)))).status).toBe(204);
    }
    expect(r.alerts).toEqual([alert(), anomaly, alert(120_000_000n)]);
  });

  it("keys the store by namespaced id", async () => {
    const store = new MemoryDedupStore();
    const receive = createAlertReceiver({ secret: SECRET, namespace: "prod", store, onAlert: () => {} });
    await receive(await post(serializeAlert(alert())));
    expect(store.seen(["prod/burst:60000000", "burst:60000000"])).toEqual([true, false]);
  });

  it("refuses the request before parsing anything", async () => {
    const r = recorder();
    const receive = createAlertReceiver({ secret: SECRET, maxBodyBytes: 4096, onAlert: r.onAlert });
    const body = serializeAlert(alert());
    expect((await receive(await post(body, { method: "GET" }))).status).toBe(405);
    expect((await receive(await post(body, { contentType: "text/plain" }))).status).toBe(415);
    expect((await receive(await post(body + " ".repeat(5000)))).status).toBe(413);
    expect((await receive(await post(body, { signature: null }))).status).toBe(401);
    expect((await receive(await post(body, { signature: await sign(body, "other") }))).status).toBe(401);
    expect((await receive(await post("not json", { signature: "sha256=00" }))).status).toBe(401);
    expect(r.alerts).toEqual([]);
    expect(r.errors).toEqual([]);
  });

  it("answers 400 naming the refused field", async () => {
    const r = recorder();
    const receive = createAlertReceiver({ secret: SECRET, onAlert: r.onAlert, onError: r.onError });
    const body = JSON.parse(serializeAlert(anomaly));
    body.anomaly.side = "sideways";
    const response = await receive(await post(JSON.stringify(body)));
    expect(response.status).toBe(400);
    expect(await response.text()).toContain('"anomaly.side"');
    expect(r.errors).toHaveLength(1);
    expect((await receive(await post("[]"))).status).toBe(400);
    expect(r.alerts).toEqual([]);
  });

  it("answers 500 when onAlert fails and leaves the id for the retry", async () => {
    let fail = true;
    const seen: string[] = [];
    const receive = createAlertReceiver({
      secret: SECRET,
      onAlert: (a) => {
        if (fail) throw new Error("down");
        seen.push(a.id);
      },
    });
    const body = serializeAlert(alert());
    expect((await receive(await post(body))).status).toBe(500);
    fail = false;
    expect((await receive(await post(body))).status).toBe(204);
    expect(seen).toEqual(["burst:60000000"]);
  });

  it("answers 500 when the store fails", async () => {
    const receive = createAlertReceiver({
      secret: SECRET,
      store: { seen: () => [false], add: () => Promise.reject(new Error("disk")) },
      onAlert: () => {},
    });
    expect((await receive(await post(serializeAlert(alert())))).status).toBe(500);
  });

  it("answers 503 to a delivery of an alert still being handled", async () => {
    const pending: (() => void)[] = [];
    const receive = createAlertReceiver({ secret: SECRET, onAlert: () => new Promise<void>((resolve) => pending.push(resolve)) });
    const body = serializeAlert(alert());
    const first = receive(await post(body));
    while (pending.length === 0) await Bun.sleep(1);
    expect((await receive(await post(body))).status).toBe(503);
    const other = receive(await post(serializeAlert(alert(120_000_000n))));
    while (pending.length === 1) await Bun.sleep(1);
    for (const release of pending) release();
    expect((await other).status).toBe(204);
    expect((await first).status).toBe(204);
  });

  it("refuses bad options", () => {
    expect(() => createAlertReceiver({ secret: "", onAlert: () => {} })).toThrow("secret");
    expect(() => createAlertReceiver({ secret: SECRET } as never)).toThrow("onAlert");
  });
});
