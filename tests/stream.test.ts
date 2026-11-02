import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import { advanceTimersByTimeAsync } from "./timers.js";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "../src/client.js";
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

function client(adapter?: EventAdapter) {
  const aptos = new Aptos(new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }));
  return new StreamdClient({ aptos, adapter, defaultPollInterval: 100 });
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
    const poller = [
      ...(c as unknown as { subscriptions: Map<string, Parked> }).subscriptions.values(),
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
});
