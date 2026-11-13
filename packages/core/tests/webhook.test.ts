import { describe, expect, it } from "bun:test";
import { createHmac } from "node:crypto";
import { StreamdError, WEBHOOK_SIGNATURE_HEADER, verifyWebhook } from "../src/index.js";

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
