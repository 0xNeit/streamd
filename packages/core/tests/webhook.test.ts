import { describe, expect, it } from "bun:test";
import { createHmac } from "node:crypto";
import { StreamdError, WEBHOOK_SIGNATURE_HEADER, deliver, signWebhook, verifyWebhook, webhookSink } from "../src/index.js";
import type { DeliveryAttempt, DeliverySink } from "../src/index.js";

const SECRET = "s3cret-token";
const BODY = JSON.stringify([{ type: "0x1::coin::DepositEvent", data: { memo: "café 🚀" }, version: "7" }]);

// The way `streamd forward` signs.
const sign = (body: string, secret = SECRET) =>
  `sha256=${createHmac("sha256", secret).update(body, "utf8").digest("hex")}`;

describe("verifyWebhook", () => {
  it("names the header forward signs in", () => {
    expect(WEBHOOK_SIGNATURE_HEADER).toBe("x-streamd-signature");
  });

  it("accepts a signature over the string body", async () => {
    expect(await verifyWebhook(BODY, sign(BODY), SECRET)).toBe(true);
  });

  it("accepts the same bytes as a Uint8Array, a Buffer, or a view inside a larger buffer", async () => {
    const bytes = new TextEncoder().encode(BODY);
    expect(await verifyWebhook(bytes, sign(BODY), SECRET)).toBe(true);
    expect(await verifyWebhook(Buffer.from(BODY, "utf8"), sign(BODY), SECRET)).toBe(true);
    const padded = new Uint8Array(bytes.length + 10);
    padded.set(bytes, 5);
    expect(await verifyWebhook(padded.subarray(5, 5 + bytes.length), sign(BODY), SECRET)).toBe(true);
  });

  it("accepts uppercase hex", async () => {
    const signature = sign(BODY);
    expect(await verifyWebhook(BODY, `sha256=${signature.slice(7).toUpperCase()}`, SECRET)).toBe(true);
  });

  it("rejects a tampered body or another secret", async () => {
    expect(await verifyWebhook(BODY.replace("7", "8"), sign(BODY), SECRET)).toBe(false);
    expect(await verifyWebhook(`${BODY} `, sign(BODY), SECRET)).toBe(false);
    expect(await verifyWebhook(BODY, sign(BODY, "other"), SECRET)).toBe(false);
  });

  it("rejects missing and malformed headers", async () => {
    const hex = sign(BODY).slice(7);
    for (const header of [
      null,
      undefined,
      "",
      hex,
      `SHA256=${hex}`,
      `sha1=${hex}`,
      `sha256=${hex.slice(0, 62)}`,
      `sha256=${hex}00`,
      `sha256=${hex.slice(0, 63)}g`,
      ` sha256=${hex}`,
      `sha256=${hex}\n`,
      `sha256=${hex},sha256=${hex}`,
    ]) {
      expect(await verifyWebhook(BODY, header, SECRET)).toBe(false);
    }
    expect(await verifyWebhook(BODY, ["x"] as unknown as string, SECRET)).toBe(false);
  });

  it("throws for an empty secret or a body that is not raw, without echoing the secret", async () => {
    await expect(verifyWebhook(BODY, sign(BODY), "")).rejects.toBeInstanceOf(StreamdError);
    await expect(verifyWebhook(BODY, sign(BODY), undefined as unknown as string)).rejects.toBeInstanceOf(StreamdError);
    const err = await verifyWebhook(JSON.parse(BODY), sign(BODY), SECRET).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(String((err as Error).message)).not.toContain(SECRET);
  });

  it("throws when the runtime has no Web Crypto", async () => {
    const original = Object.getOwnPropertyDescriptor(globalThis, "crypto")!;
    Object.defineProperty(globalThis, "crypto", { value: undefined, configurable: true });
    try {
      const err = await verifyWebhook(BODY, sign(BODY), SECRET).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect((err as Error).message).toContain("Web Crypto");
      expect((err as Error).message).not.toContain(SECRET);
    } finally {
      Object.defineProperty(globalThis, "crypto", original);
    }
    expect(await verifyWebhook(BODY, sign(BODY), SECRET)).toBe(true);
  });
});

describe("signWebhook", () => {
  it("signs as forward does, and verifyWebhook accepts it", async () => {
    const header = await signWebhook(BODY, SECRET);
    expect(header).toBe(sign(BODY));
    expect(await verifyWebhook(BODY, header, SECRET)).toBe(true);
  });

  it("refuses an empty secret", async () => {
    await expect(signWebhook(BODY, "")).rejects.toBeInstanceOf(StreamdError);
  });
});

describe("deliver", () => {
  const io = (errors: string[] = [], signal = new AbortController().signal) => ({
    error: (m: string) => errors.push(m),
    signal,
    retryDelay: 1,
  });
  const sink = (attempts: DeliveryAttempt[]): DeliverySink & { calls: number } => {
    const fake = {
      calls: 0,
      send: async () => attempts[fake.calls++] ?? { ok: true },
    };
    return fake;
  };

  it("retries a retryable failure until acknowledged", async () => {
    const errors: string[] = [];
    const fake = sink([{ ok: false, failure: "HTTP 500", retryable: true }]);
    expect(await deliver("[]", fake, 3, io(errors))).toBe(true);
    expect(fake.calls).toBe(2);
    expect(errors).toEqual(["streamd: delivery failed (attempt 1/4): HTTP 500; retrying in 1ms"]);
  });

  it("waits a failure's retryAfter instead of the backoff", async () => {
    const errors: string[] = [];
    const fake = sink([{ ok: false, failure: "HTTP 429", retryable: true, retryAfter: 30 }]);
    const start = Date.now();
    expect(await deliver("[]", fake, 1, io(errors))).toBe(true);
    expect(Date.now() - start).toBeGreaterThanOrEqual(25);
    expect(errors[0]).toContain("retrying in 30ms");
  });

  it("gives up past its retries and on a failure that is not retryable", async () => {
    const errors: string[] = [];
    const failing = { ok: false, failure: "HTTP 500", retryable: true };
    expect(await deliver("[]", sink([failing, failing]), 1, io(errors))).toBe(false);
    expect(errors.at(-1)).toBe("streamd: delivery failed: HTTP 500");
    const fake = sink([{ ok: false, failure: "HTTP 400", retryable: false }]);
    expect(await deliver("[]", fake, 5, io())).toBe(false);
    expect(fake.calls).toBe(1);
  });

  it("stops waiting when the signal aborts", async () => {
    const controller = new AbortController();
    const fake = sink([{ ok: false, failure: "HTTP 503", retryable: true, retryAfter: 10_000 }]);
    const done = deliver("[]", fake, 3, io([], controller.signal));
    setTimeout(() => controller.abort(), 5);
    expect(await done).toBe(false);
    expect(fake.calls).toBe(1);
  });
});

describe("webhookSink", () => {
  it("POSTs the body signed, acknowledging a 2xx and not following redirects", async () => {
    const seen: { body: string; signature: string | null }[] = [];
    let status = 204;
    using server = Bun.serve({
      port: 0,
      async fetch(request) {
        seen.push({ body: await request.text(), signature: request.headers.get(WEBHOOK_SIGNATURE_HEADER) });
        return new Response(null, { status, headers: status === 302 ? { location: "http://127.0.0.1:1/" } : {} });
      },
    });
    const target = webhookSink({ to: `http://127.0.0.1:${server.port}/hook`, secret: SECRET, timeout: 2000 });
    const signal = new AbortController().signal;
    expect(await target.send(BODY, signal)).toEqual({ ok: true });
    expect(await verifyWebhook(seen[0]!.body, seen[0]!.signature, SECRET)).toBe(true);
    status = 302;
    expect(await target.send(BODY, signal)).toMatchObject({ ok: false, failure: "HTTP 302", retryable: false });
    expect(seen).toHaveLength(2);
  });
});
