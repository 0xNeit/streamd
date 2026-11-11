import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync, captureMicrotaskThrows } from "./timers.js";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "../src/client.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdPollingError } from "../src/errors.js";
import type { EventAdapter, EventPage, StreamdEvent } from "../src/types.js";

const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

function ev(version: number): StreamdEvent {
  return { type: descriptor.eventType, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

function adapterOf(pages: EventPage[]): EventAdapter {
  let i = 0;
  return {
    fetchEvents: vi.fn(async () => pages[i++] ?? { events: [], nextCursor: "end", hasMore: false }),
  };
}

function client(adapter: EventAdapter = new MemoryAdapter([]), onError?: (error: Error) => void) {
  return new StreamdClient({ adapter, defaultPollInterval: 100, onError });
}

function subscriptionCount(c: StreamdClient): number {
  // Reads the private map: nothing public reports how many pollers remain.
  return (c as unknown as { subscriptions: Map<string, unknown> }).subscriptions.size;
}

function openStreams(c: StreamdClient): number {
  // Reads the private set of open streams, for the same reason.
  return (c as unknown as { streams: Set<unknown> }).streams.size;
}

describe("StreamdClient.stream", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("yields events in order and unsubscribes when the loop breaks", async () => {
    const adapter = adapterOf([{ events: [ev(1), ev(2), ev(3)], nextCursor: "4", hasMore: false }]);
    const c = client(adapter);
    const seen: bigint[] = [];

    const loop = (async () => {
      for await (const e of c.stream(descriptor)) {
        seen.push(e.version);
        if (seen.length === 2) break;
      }
    })();
    await advanceTimersByTimeAsync(0);
    await loop;

    expect(seen).toEqual([1n, 2n]);
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("reports a cursor that resumes a later stream after the last finished page", async () => {
    const pages: Record<string, EventPage> = {
      start: { events: [ev(1), ev(2)], nextCursor: "c1", hasMore: true },
      c1: { events: [ev(3), ev(4)], nextCursor: "c2", hasMore: false },
      c2: { events: [], nextCursor: "c2", hasMore: false },
    };
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async (_d, o) => pages[o.cursor ?? "start"]!),
    };
    const c = client(adapter);

    const it = c.stream(descriptor, { cursor: "start" });
    expect(it.cursor).toBe("start");
    const seen: bigint[] = [];
    const loop = (async () => {
      for await (const e of it) {
        seen.push(e.version);
        if (e.version === 3n) break;
      }
    })();
    await advanceTimersByTimeAsync(50);
    await loop;
    expect(seen).toEqual([1n, 2n, 3n]);
    // The loop left mid-page, so the cursor is the one before that page.
    expect(it.cursor).toBe("c1");

    const resumed = c.stream(descriptor, { cursor: it.cursor });
    const next = resumed.next();
    await advanceTimersByTimeAsync(0);
    expect((await next).value?.version).toBe(3n);
    await resumed.return!();
    c.destroy();
  });

  it("holds the next poll while the loop body is still on an event", async () => {
    const adapter = adapterOf([{ events: [ev(1)], nextCursor: "2", hasMore: false }]);
    const c = client(adapter);
    const it = c.stream(descriptor);

    const first = it.next();
    await advanceTimersByTimeAsync(0);
    expect((await first).value?.version).toBe(1n);

    // The body has not asked for more, so nothing is fetched however long it takes.
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);

    const second = it.next();
    await advanceTimersByTimeAsync(100);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);

    await it.return!();
    expect(await second).toEqual({ value: undefined, done: true });
  });

  it("throws out of the loop when polling gives up on invalid input", async () => {
    const c = client();
    const loop = (async () => {
      for await (const _ of c.stream({ kind: "account", accountAddress: "nope" })) {
        // never reached
      }
    })();
    const outcome = loop.then(() => "done", (e: unknown) => e);
    await advanceTimersByTimeAsync(0);

    expect(await outcome).toBeInstanceOf(StreamdPollingError);
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);
  });

  it("is done after the failure has been reported", async () => {
    const c = client();
    const it = c.stream({ kind: "account", accountAddress: "nope" });
    const first = it.next().catch((e: unknown) => e);
    await advanceTimersByTimeAsync(0);

    expect(await first).toBeInstanceOf(StreamdPollingError);
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it("passes non-fatal errors to onError and keeps going", async () => {
    let calls = 0;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        if (calls++ === 0) throw new Error("blip");
        return { events: [ev(7)], nextCursor: "8", hasMore: false };
      }),
    };
    const onError = vi.fn();
    const c = client(adapter);
    const it = c.stream(descriptor, { onError });

    const first = it.next();
    await advanceTimersByTimeAsync(300);
    expect((await first).value?.version).toBe(7n);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![0]).toBeInstanceOf(StreamdError);

    await it.return!();
  });

  it("ends an open loop when the client is destroyed", async () => {
    const c = client(adapterOf([]));
    const it = c.stream(descriptor);
    const pending = it.next();
    await advanceTimersByTimeAsync(0);

    c.destroy();
    expect(await pending).toEqual({ value: undefined, done: true });
    expect(openStreams(c)).toBe(0);
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it("lets the held handler finish when the loop breaks", async () => {
    const adapter = adapterOf([{ events: [ev(1)], nextCursor: "2", hasMore: false }]);
    const c = client(adapter);
    const it = c.stream(descriptor);
    await advanceTimersByTimeAsync(0);
    const first = it.next();
    await advanceTimersByTimeAsync(0);
    expect((await first).value?.version).toBe(1n);

    // The poller is parked on this event's handler until it is released.
    type Parked = { cursor?: string; delivering: Promise<void> };
    const { poller } = [
      ...(c as unknown as { subscriptions: Map<string, { poller: Parked }> }).subscriptions.values(),
    ][0]!;
    let settled = false;
    void poller.delivering.then(() => (settled = true));
    await it.return!();
    await advanceTimersByTimeAsync(0);

    // Released while stopped: nothing is left suspended, and the page counts as
    // unfinished, so the cursor stays put.
    expect(settled).toBe(true);
    expect(poller.cursor).toBeUndefined();
    expect(subscriptionCount(c)).toBe(0);
  });

  it("is done after return()", async () => {
    const c = client(adapterOf([]));
    const it = c.stream(descriptor);
    await it.return!();
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it("refuses a second next() while one is pending", async () => {
    const c = client(adapterOf([]));
    const it = c.stream(descriptor);
    const first = it.next();
    let outcome: unknown = "pending";
    it.next().then(
      () => (outcome = "resolved"),
      (e: unknown) => (outcome = e),
    );
    await advanceTimersByTimeAsync(0);
    expect(outcome).toBeInstanceOf(TypeError);
    await it.return!();
    expect(await first).toEqual({ value: undefined, done: true });
  });

  it("does not subscribe until the loop asks for an event", async () => {
    const adapter = adapterOf([]);
    const c = client(adapter);
    const it = c.stream(descriptor);
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);
    await it.return!();
  });

  it("rejects a pending next() with the signal reason on abort", async () => {
    const adapter = adapterOf([]);
    const c = client(adapter);
    const controller = new AbortController();
    const onError = vi.fn();
    const reason = new Error("stop stream");
    const it = c.stream(descriptor, { signal: controller.signal, onError });

    const pending = it.next().catch((e: unknown) => e);
    await advanceTimersByTimeAsync(0);
    expect(subscriptionCount(c)).toBe(1);
    expect(openStreams(c)).toBe(1);

    controller.abort(reason);
    expect(await pending).toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);
    expect(onError).not.toHaveBeenCalled();

    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("rejects the following next() once when aborted during the loop body, then is done", async () => {
    const adapter = adapterOf([{ events: [ev(1), ev(2)], nextCursor: "3", hasMore: false }]);
    const c = client(adapter);
    const controller = new AbortController();
    const reason = new Error("aborted in body");
    const it = c.stream(descriptor, { signal: controller.signal });

    const first = it.next();
    await advanceTimersByTimeAsync(0);
    expect((await first).value?.version).toBe(1n);

    // Abort while the loop body is processing the first event (no pending next()).
    controller.abort(reason);
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);

    const second = await it.next().catch((e: unknown) => e);
    expect(second).toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it("rejects the first next() without subscribing when the signal is already aborted", async () => {
    const adapter = adapterOf([{ events: [ev(1)], nextCursor: "2", hasMore: false }]);
    const c = client(adapter);
    const reason = new Error("pre-aborted");
    const it = c.stream(descriptor, { signal: AbortSignal.abort(reason) });

    const first = await it.next().catch((e: unknown) => e);
    expect(first).toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
    expect(subscriptionCount(c)).toBe(0);
    expect(openStreams(c)).toBe(0);
  });

  it("detaches its abort listener when the loop exits and clears a held abort on return()", async () => {
    const adapter = adapterOf([
      { events: [ev(1)], nextCursor: "2", hasMore: false },
      { events: [ev(2)], nextCursor: "3", hasMore: false },
    ]);
    const c = client(adapter);
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const it = c.stream(descriptor, { signal: controller.signal });

    const first = it.next();
    await advanceTimersByTimeAsync(0);
    await first;
    await it.return!();
    expect(remove).toHaveBeenCalledTimes(1);

    // A second stream aborted mid-body and then explicitly returned reports done on next().
    const c2 = new AbortController();
    const it2 = c.stream(descriptor, { signal: c2.signal });
    const ev1 = it2.next();
    await advanceTimersByTimeAsync(0);
    await ev1;
    c2.abort(new Error("aborted before break"));
    await it2.return!();
    expect(await it2.next()).toEqual({ value: undefined, done: true });
  });

  it("falls back to config.onError when stream() omits options.onError, and prefers options.onError when set", async () => {
    let calls = 0;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        if (calls++ === 0) throw new Error("blip");
        return { events: [ev(7)], nextCursor: "8", hasMore: false };
      }),
    };
    const clientOnError = vi.fn();
    const streamOnError = vi.fn();
    const c = client(adapter, clientOnError);

    const it1 = c.stream(descriptor);
    const first = it1.next();
    await advanceTimersByTimeAsync(300);
    expect((await first).value?.version).toBe(7n);
    expect(clientOnError).toHaveBeenCalledTimes(1);
    expect(clientOnError.mock.calls[0]![0]).toBeInstanceOf(StreamdPollingError);
    await it1.return!();

    calls = 0;
    const it2 = c.stream(descriptor, { onError: streamOnError });
    const second = it2.next();
    await advanceTimersByTimeAsync(300);
    expect((await second).value?.version).toBe(7n);
    expect(streamOnError).toHaveBeenCalledTimes(1);
    expect(clientOnError).toHaveBeenCalledTimes(1);
    await it2.return!();
  });

  it("still rejects the stream when a throwing config.onError runs on a fatal error", async () => {
    const c = client(undefined, () => {
      throw new Error("client stream handler bug");
    });
    const surfaced = captureMicrotaskThrows();
    try {
      const it = c.stream({ kind: "account", accountAddress: "nope" });
      const first = it.next().catch((e: unknown) => e);
      await advanceTimersByTimeAsync(0);

      expect(await first).toBeInstanceOf(StreamdPollingError);
      expect(await it.next()).toEqual({ value: undefined, done: true });
      expect(surfaced.thrown.map((e) => (e as Error).message)).toEqual([
        "client stream handler bug",
      ]);
    } finally {
      surfaced.restore();
      c.destroy();
    }
  });
});
