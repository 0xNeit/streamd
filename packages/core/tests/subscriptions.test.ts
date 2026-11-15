import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdNetworkError } from "../src/errors.js";
import { redactSecrets } from "../src/utils.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const A = "0x1::test::A";
const B = "0x1::test::B";
const onA: EventDescriptor = { kind: "module", eventType: A };
const onB: EventDescriptor = { kind: "module", eventType: [B] };

function ev(version: number, type = A): StreamdEvent {
  return { type, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

/** Fails every fetch with an SDK-style message naming a keyed URL. */
const failing: EventAdapter = {
  async fetchEvents() {
    throw new StreamdNetworkError(
      "Request failed",
      undefined,
      new Error("GET https://node.example/v1/SECRETKEY/transactions?x=1 failed, Bearer abc.def"),
    );
  },
};

describe("listSubscriptions", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("lists on() and reduce() walks with ids, descriptors, cursor and last version", async () => {
    const memory = new MemoryAdapter([ev(1), ev(2), ev(3, B)]);
    const client = new StreamdClient({ adapter: memory });
    const sub = client.on(onA, () => {}, { fromVersion: 0n, id: "mine" });
    client.reduce(onB, (n: number) => n + 1, 0, { fromVersion: 0n });
    expect(client.listSubscriptions().map((s) => s.id)).toEqual(["mine"]);
    await advanceTimersByTimeAsync(50);

    const [on, walk] = client.listSubscriptions();
    expect(on).toMatchObject({ id: "mine", listener: true, active: true, paused: false, lastVersion: 2n });
    expect(on!.descriptor).toEqual(onA);
    expect(on!.cursor).toBe(sub.cursor);
    expect(on!.cursor).toBeDefined();
    expect(on!.consecutiveErrors).toBe(0);
    expect(on!.lastError).toBeUndefined();
    expect(walk).toMatchObject({ listener: false, active: true, descriptor: { kind: "module", eventType: [B] } });

    memory.push(ev(4));
    await advanceTimersByTimeAsync(1100);
    expect(client.listSubscriptions()[0]!.lastVersion).toBe(4n);
    client.destroy();
    expect(client.listSubscriptions()).toEqual([]);
  });

  it("reports pause, and drops a subscription once it is unsubscribed", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const sub = client.on(onA, () => {}, { fromVersion: 0n });
    sub.pause();
    expect(client.listSubscriptions()[0]).toMatchObject({ active: false, paused: true });
    sub.resume();
    expect(client.listSubscriptions()[0]).toMatchObject({ active: true, paused: false });
    sub.unsubscribe();
    expect(client.listSubscriptions()).toEqual([]);
  });

  it("keeps the last error with URLs and bearer tokens taken out, until polling gives up", async () => {
    const client = new StreamdClient({ adapter: failing, onError: () => {} });
    client.on(onA, () => {}, { fromVersion: 0n, maxErrors: 3, maxBackoff: 10 });
    await advanceTimersByTimeAsync(5);
    const [info] = client.listSubscriptions();
    expect(info!.consecutiveErrors).toBe(1);
    expect(info!.lastError).toMatchObject({ name: "StreamdPollingError", message: "Polling failed (1/3)" });
    expect(info!.lastError!.cause).toBe("Request failed");
    expect(typeof info!.lastError!.at).toBe("number");
    expect(JSON.stringify(info!.lastError)).not.toContain("SECRETKEY");
    await advanceTimersByTimeAsync(100);
    expect(client.listSubscriptions()).toEqual([]);
  });

  it("redacts URLs and bearer tokens in any text", () => {
    expect(redactSecrets("GET https://node.example/v1/KEY/x?y=1 failed, Bearer abc.def, ws://h:1/p"))
      .toBe("GET <url> failed, Bearer <redacted>, <url>");
    expect(redactSecrets("Polling failed (1/3)")).toBe("Polling failed (1/3)");
  });

  it("redacts the error's own message and its direct cause", async () => {
    const adapter: EventAdapter = {
      async fetchEvents() {
        throw new StreamdNetworkError("at https://n.example/KEY", undefined, new Error("Bearer tok"));
      },
    };
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter });
    client.on(onA, () => {}, { fromVersion: 0n, onError: (e) => errors.push(e) });
    await advanceTimersByTimeAsync(5);
    // The poller wraps the adapter's error, so the snapshot shows that wrapper and its cause.
    expect(client.listSubscriptions()[0]!.lastError!.cause).toBe("at <url>");
    expect(JSON.stringify(client.listSubscriptions(), (_, v) => (typeof v === "bigint" ? String(v) : v)))
      .not.toContain("KEY");
    client.destroy();
  });

  it("lists each walk of a merged on()", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    client.on([onA, onB], () => {}, { fromVersion: 0n });
    const list = client.listSubscriptions();
    expect(list.map((s) => s.descriptor)).toEqual([onA, onB]);
    expect(list.every((s) => !s.listener)).toBe(true);
    client.destroy();
  });

  it("hands out frozen copies the caller cannot change", () => {
    const descriptor = { kind: "module", eventType: [A] } as EventDescriptor;
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    client.on(descriptor, () => {}, { fromVersion: 0n });
    const [info] = client.listSubscriptions();
    expect(info!.descriptor).not.toBe(descriptor);
    expect(Object.isFrozen(info)).toBe(true);
    expect(Object.isFrozen(info!.descriptor)).toBe(true);
    expect(Object.isFrozen((info!.descriptor as unknown as { eventType: string[] }).eventType)).toBe(true);
    expect(() => ((info as { paused: boolean }).paused = true)).toThrow();
    (descriptor as unknown as { eventType: string[] }).eventType.push(B);
    expect(client.listSubscriptions()[0]!.descriptor).toEqual({ kind: "module", eventType: [A] });
    expect(client.listSubscriptions()[0]).not.toBe(info);
    client.destroy();
  });
});
