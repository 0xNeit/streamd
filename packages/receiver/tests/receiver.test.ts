import { describe, expect, it } from "bun:test";
import { StreamdError, serializeEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { createReceiver } from "../src/receiver.js";
import { MemoryDedupStore } from "../src/store.js";
import type { DedupStore } from "../src/store.js";

const SECRET = "test-secret";

function ev(version: bigint, eventIndex = 0, data: unknown = { amount: "1" }): StreamdEvent {
  return { type: "0x1::m::E", data, version, sequenceNumber: 0n, eventIndex };
}

async function sign(body: string | Uint8Array, secret = SECRET): Promise<string> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const data = typeof body === "string" ? encoder.encode(body) : new Uint8Array(body);
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, data));
  return `sha256=${Buffer.from(mac).toString("hex")}`;
}

function bodyOf(events: StreamdEvent[]): string {
  return `[${events.map(serializeEvent).join(",")}]`;
}

async function post(
  body: string | Uint8Array,
  init: { signature?: string | null; contentType?: string; method?: string; headers?: Record<string, string> } = {},
): Promise<Request> {
  const signature = init.signature === undefined ? await sign(body) : init.signature;
  return new Request("http://127.0.0.1/hook", {
    method: init.method ?? "POST",
    headers: {
      "content-type": init.contentType ?? "application/json",
      ...(signature !== null && { "x-streamd-signature": signature }),
      ...init.headers,
    },
    body: init.method === "GET" ? undefined : body,
  });
}

function recorder() {
  const calls: StreamdEvent[][] = [];
  const errors: unknown[] = [];
  return {
    calls,
    errors,
    onEvents: (events: StreamdEvent[]) => {
      calls.push(events);
    },
    onError: (err: unknown) => {
      errors.push(err);
    },
  };
}

describe("createReceiver", () => {
  it("hands the parsed events to onEvents and answers 204", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, onEvents: r.onEvents });
    const response = await receive(await post(bodyOf([ev(10n), ev(10n, 1)])));
    expect(response.status).toBe(204);
    expect(r.calls).toEqual([[ev(10n), ev(10n, 1)]]);
  });

  it("drops repeats of handled events, within a body and across deliveries", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, onEvents: r.onEvents });
    expect((await receive(await post(bodyOf([ev(1n), ev(1n), ev(2n)])))).status).toBe(204);
    expect((await receive(await post(bodyOf([ev(2n), ev(3n)])))).status).toBe(204);
    expect((await receive(await post(bodyOf([ev(1n), ev(3n)])))).status).toBe(204);
    expect(r.calls).toEqual([[ev(1n), ev(2n)], [ev(3n)]]);
  });

  it("answers 500 when onEvents throws and leaves the events unseen for the retry", async () => {
    const calls: StreamdEvent[][] = [];
    const errors: unknown[] = [];
    let fail = true;
    const receive = createReceiver({
      secret: SECRET,
      onEvents: async (events) => {
        calls.push(events);
        if (fail) throw new Error("handler down");
      },
      onError: (err) => errors.push(err),
    });
    const body = bodyOf([ev(5n)]);
    const failed = await receive(await post(body));
    expect(failed.status).toBe(500);
    expect(await failed.text()).toBe("delivery failed");
    expect((errors[0] as Error).message).toBe("handler down");
    fail = false;
    expect((await receive(await post(body))).status).toBe(204);
    expect((await receive(await post(body))).status).toBe(204);
    expect(calls).toEqual([[ev(5n)], [ev(5n)]]);
  });

  it("answers 500 when the store fails, before calling onEvents", async () => {
    const r = recorder();
    const store: DedupStore = { seen: async () => Promise.reject(new Error("db down")), add: () => {} };
    const receive = createReceiver({ secret: SECRET, store, ...r });
    expect((await receive(await post(bodyOf([ev(1n)])))).status).toBe(500);
    expect(r.calls).toEqual([]);
    expect((r.errors[0] as Error).message).toBe("db down");
  });

  it("answers 500 when the store answers the wrong number of results", async () => {
    const r = recorder();
    const store: DedupStore = { seen: () => [false], add: () => {} };
    const receive = createReceiver({ secret: SECRET, store, ...r });
    expect((await receive(await post(bodyOf([ev(1n), ev(2n)])))).status).toBe(500);
    expect(r.calls).toEqual([]);
    expect(r.errors[0]).toBeInstanceOf(StreamdError);
  });

  it("answers 500 when recording fails after onEvents, so the retry handles the events again", async () => {
    const r = recorder();
    const inner = new MemoryDedupStore();
    let failAdd = true;
    const store: DedupStore = {
      seen: (ids) => inner.seen(ids),
      add: (ids) => {
        if (failAdd) throw new Error("write failed");
        inner.add(ids);
      },
    };
    const receive = createReceiver({ secret: SECRET, store, ...r });
    const body = bodyOf([ev(1n)]);
    expect((await receive(await post(body))).status).toBe(500);
    failAdd = false;
    expect((await receive(await post(body))).status).toBe(204);
    expect((await receive(await post(body))).status).toBe(204);
    expect(r.calls).toEqual([[ev(1n)], [ev(1n)]]);
  });

  it("answers 503 to a delivery overlapping one still being handled", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    let entered!: () => void;
    const handling = new Promise<void>((resolve) => (entered = resolve));
    const calls: StreamdEvent[][] = [];
    const receive = createReceiver({
      secret: SECRET,
      onEvents: async (events) => {
        calls.push(events);
        if (calls.length === 1) {
          entered();
          await gate;
        }
      },
    });
    const first = receive(await post(bodyOf([ev(1n), ev(2n)])));
    await handling;
    const overlapping = await receive(await post(bodyOf([ev(2n), ev(3n)])));
    expect(overlapping.status).toBe(503);
    // No Retry-After, so forward keeps its doubling backoff while a slow handler finishes.
    expect(overlapping.headers.get("retry-after")).toBeNull();
    expect((await receive(await post(bodyOf([ev(4n)])))).status).toBe(204);
    release();
    expect((await first).status).toBe(204);
    expect((await receive(await post(bodyOf([ev(2n), ev(3n)])))).status).toBe(204);
    expect(calls).toEqual([[ev(1n), ev(2n)], [ev(4n)], [ev(3n)]]);
  });

  it("answers 401 for a missing, malformed or wrong signature", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, ...r });
    const body = bodyOf([ev(1n)]);
    for (const signature of [null, "", "sha256=zz", await sign(body, "other"), await sign(`${body} `)]) {
      const response = await receive(await post(body, { signature }));
      expect(response.status).toBe(401);
    }
    expect((await receive(await post("not json", { signature: await sign("other") }))).status).toBe(401);
    expect(r.calls).toEqual([]);
    expect(r.errors).toEqual([]);
  });

  it("answers 400 for a signed body that is not an array of events", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, ...r });
    const bodies: (string | Uint8Array)[] = [
      "not json",
      serializeEvent(ev(1n)),
      "[1]",
      '[{"type":"0x1::m::E","version":"1"}]',
      new Uint8Array([0x5b, 0xff, 0x5d]),
      // Valid JSON once a lenient decoder replaces the byte, so only a strict one refuses it.
      new Uint8Array([
        ...new TextEncoder().encode('[{"type":"0x1::m::E'),
        0xff,
        ...new TextEncoder().encode('","data":{},"version":"1","sequenceNumber":"0","eventIndex":0}]'),
      ]),
    ];
    for (const body of bodies) expect((await receive(await post(body))).status).toBe(400);
    expect(r.calls).toEqual([]);
    expect(r.errors.length).toBe(bodies.length);
  });

  it("answers 204 to an empty array without calling onEvents", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, ...r });
    expect((await receive(await post("[]"))).status).toBe(204);
    expect(r.calls).toEqual([]);
  });

  it("answers 405 to anything but POST", async () => {
    const receive = createReceiver({ secret: SECRET, onEvents: () => {} });
    const response = await receive(await post("", { method: "GET" }));
    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("POST");
  });

  it("answers 415 to anything but application/json, parameters and case aside", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, ...r });
    const body = bodyOf([ev(1n)]);
    expect((await receive(await post(body, { contentType: "text/plain" }))).status).toBe(415);
    expect((await receive(await post(body, { contentType: "Application/JSON; charset=utf-8" }))).status).toBe(204);
  });

  it("answers 413 to a body over maxBodyBytes, declared or streamed, without reading past it", async () => {
    const receive = createReceiver({ secret: SECRET, onEvents: () => {}, maxBodyBytes: 10 });
    expect((await receive(await post("[" + " ".repeat(20) + "]"))).status).toBe(413);
    const declared = await post("[]", { headers: { "content-length": "999" } });
    expect(declared.headers.get("content-length")).toBe("999");
    expect((await receive(declared)).status).toBe(413);

    let pulled = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulled++;
        controller.enqueue(new Uint8Array(8).fill(0x20));
        if (pulled > 100) controller.close();
      },
    });
    const request = new Request("http://127.0.0.1/hook", {
      method: "POST",
      headers: { "content-type": "application/json", "x-streamd-signature": await sign("[]") },
      body: stream,
    });
    expect((await receive(request)).status).toBe(413);
    expect(pulled).toBeLessThan(5);
    expect((await receive(await post("[]"))).status).toBe(204);
  });

  it("keeps each namespace's ids apart in a shared store", async () => {
    const store = new MemoryDedupStore();
    const handled: string[] = [];
    const receiver = (namespace: string) =>
      createReceiver({ secret: SECRET, store, namespace, onEvents: (events) => void handled.push(`${namespace} ${events.length}`) });
    const mainnet = receiver("aptos:mainnet");
    const testnet = receiver("aptos:testnet");
    const body = bodyOf([ev(100n)]);
    expect((await mainnet(await post(body))).status).toBe(204);
    expect((await testnet(await post(body))).status).toBe(204);
    expect((await testnet(await post(body))).status).toBe(204);
    expect(handled).toEqual(["aptos:mainnet 1", "aptos:testnet 1"]);
    expect(store.seen(["aptos:mainnet/100:0", "aptos:testnet/100:0", "100:0"])).toEqual([true, true, false]);
  });

  it("still answers 400 and 500 when onError throws", async () => {
    const receive = createReceiver({
      secret: SECRET,
      onEvents: () => {
        throw new Error("handler down");
      },
      onError: () => {
        throw new Error("logger down");
      },
    });
    expect((await receive(await post("not json"))).status).toBe(400);
    expect((await receive(await post(bodyOf([ev(1n)])))).status).toBe(500);
  });

  it("restores decode paths as bigints", async () => {
    const r = recorder();
    const receive = createReceiver({ secret: SECRET, decode: ["amount"], ...r });
    expect((await receive(await post(bodyOf([ev(1n, 0, { amount: "18446744073709551616" })])))).status).toBe(204);
    expect(r.calls[0]![0]!.data).toEqual({ amount: 18446744073709551616n });
  });

  it("refuses bad options at construction", () => {
    const onEvents = () => {};
    expect(() => createReceiver({ secret: "", onEvents })).toThrow(StreamdError);
    expect(() => createReceiver({ secret: SECRET, onEvents: undefined as never })).toThrow(StreamdError);
    expect(() => createReceiver({ secret: SECRET, onEvents, decode: [""] })).toThrow(StreamdError);
    expect(() => createReceiver({ secret: SECRET, onEvents, maxBodyBytes: 0 })).toThrow(StreamdError);
    expect(() => createReceiver({ secret: SECRET, onEvents, store: {} as never })).toThrow(StreamdError);
    expect(() => createReceiver({ secret: SECRET, onEvents, namespace: "" })).toThrow(StreamdError);
  });
});
