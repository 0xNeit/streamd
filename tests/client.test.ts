import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import {
  advanceTimersByTimeAsync,
  advanceTimersToNextTimerAsync,
  captureMicrotaskThrows,
  mocked,
} from "./timers.js";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { FallbackAdapter } from "../src/adapters/fallback.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { RoutingAdapter } from "../src/adapters/routing.js";
import { StreamdClient } from "../src/client.js";
import { every, involvesAddress, not, whereField } from "../src/filter.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  ModuleEventDescriptor,
  StreamdEvent,
} from "../src/types.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  StreamdTimeoutError,
} from "../src/errors.js";

function makeAptos() {
  // Constructing the SDK performs no network I/O; the injected adapter below
  // is what actually serves events.
  return new Aptos(new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }));
}

function makeEvent(version: number, eventIndex = 0): StreamdEvent {
  return {
    type: "0x1::test::Event",
    data: { value: version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  };
}

/** Serves the given pages, then empty terminal pages. */
function adapterOf(pages: EventPage[]): EventAdapter {
  let i = 0;
  return {
    fetchEvents: vi.fn(async () => {
      const p = pages[i] ?? { events: [], nextCursor: "end", hasMore: false };
      i++;
      return p;
    }),
  };
}

describe("StreamdClient", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("creates subscriptions with on()", () => {
    const client = new StreamdClient({
      aptos: makeAptos(),
      adapter: adapterOf([]),
    });
    const sub = client.on(
      { kind: "module", eventType: "0x1::test::Event" },
      () => {},
    );

    expect(sub.id).toBeDefined();
    expect(sub.active).toBe(true);

    sub.unsubscribe();
    expect(sub.active).toBe(false);

    client.destroy();
  });

  it("destroy stops all subscriptions", () => {
    const client = new StreamdClient({
      aptos: makeAptos(),
      adapter: adapterOf([]),
    });
    const sub1 = client.on(
      { kind: "module", eventType: "0x1::test::Event1" },
      () => {},
    );
    const sub2 = client.on(
      { kind: "module", eventType: "0x1::test::Event2" },
      () => {},
    );

    client.destroy();

    expect(sub1.active).toBe(false);
    expect(sub2.active).toBe(false);
  });

  it("on() forgets a subscription whose signal aborts", async () => {
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const controller = new AbortController();
    const onError = vi.fn();
    const sub = client.on({ kind: "module", eventType: "0x1::test::Event" }, () => {}, {
      pollInterval: 100,
      onError,
      signal: controller.signal,
    });
    await advanceTimersByTimeAsync(0);
    expect(subscriptionCount(client)).toBe(1);

    controller.abort();
    expect(sub.active).toBe(false);
    expect(subscriptionCount(client)).toBe(0);
    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });

  it("on() with an already-aborted signal never polls", async () => {
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const sub = client.on({ kind: "module", eventType: "0x1::test::Event" }, () => {}, {
      signal: AbortSignal.abort(),
    });
    await advanceTimersByTimeAsync(100);

    expect(sub.active).toBe(false);
    expect(subscriptionCount(client)).toBe(0);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
  });

  it("on() detaches from the signal on unsubscribe", () => {
    const client = new StreamdClient({ aptos: makeAptos(), adapter: adapterOf([]) });
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const sub = client.on({ kind: "module", eventType: "0x1::test::Event" }, () => {}, {
      signal: controller.signal,
    });
    sub.unsubscribe();

    // One listener is the poller's, the other the client's.
    expect(remove).toHaveBeenCalledTimes(2);
  });

  it("uses the injected adapter instead of the fullnode default", async () => {
    const mockEvents = [makeEvent(100)];
    const adapter = adapterOf([
      { events: mockEvents, nextCursor: "101", hasMore: false },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { fromVersion: 50n, limit: 10 },
    );

    expect(events).toEqual(mockEvents);
    expect(adapter.fetchEvents).toHaveBeenCalledWith(
      { kind: "module", eventType: "0x1::test::Event" },
      expect.objectContaining({
        fromVersion: 50n,
        limit: 10,
        orderBy: "asc",
      }),
    );

    client.destroy();
  });

  it("getEvents pages past windows with no matches", async () => {
    const adapter = adapterOf([
      { events: [], nextCursor: "10", hasMore: true },
      { events: [], nextCursor: "20", hasMore: true },
      { events: [makeEvent(25)], nextCursor: "30", hasMore: false },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const events = await client.getEvents({
      kind: "module",
      eventType: "0x1::test::Event",
    });

    expect(events).toHaveLength(1);
    expect(events[0].version).toBe(25n);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(3);

    client.destroy();
  });

  it("getEvents honours the overall limit across pages", async () => {
    const adapter = adapterOf([
      { events: [makeEvent(1), makeEvent(2)], nextCursor: "3", hasMore: true },
      { events: [makeEvent(3), makeEvent(4)], nextCursor: "5", hasMore: true },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { limit: 3 },
    );

    expect(events.map((l) => l.version)).toEqual([1n, 2n, 3n]);
    expect(mocked(adapter.fetchEvents).mock.calls[1]![1].limit).toBe(1);

    client.destroy();
  });

  it("getEvents applies `where` and counts limit after filtering", async () => {
    const mine = { owner: "0x1" };
    const theirs = { owner: "0x2" };
    const adapter = adapterOf([
      {
        events: [
          { ...makeEvent(1), data: theirs },
          { ...makeEvent(2), data: mine },
        ],
        nextCursor: "3",
        hasMore: true,
      },
      {
        events: [
          { ...makeEvent(3), data: theirs },
          { ...makeEvent(4), data: mine },
        ],
        nextCursor: "5",
        hasMore: false,
      },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { where: involvesAddress("0x1") },
    );

    // Two of the four match; the walk continued past a page that was
    // partially filtered rather than stopping short.
    expect(events.map((l) => l.version)).toEqual([2n, 4n]);

    client.destroy();
  });

  it("does not forward `where` to the adapter", async () => {
    // Predicates are functions and cannot be pushed to a remote source.
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });

    await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { where: involvesAddress("0x1"), limit: 5 },
    );

    const opts = mocked(adapter.fetchEvents).mock.calls[0][1];
    expect(opts).not.toHaveProperty("where");

    client.destroy();
  });

  it("on() delivers only events matching `where`", async () => {
    const adapter = adapterOf([
      {
        events: [
          { ...makeEvent(1), data: { owner: "0x2" } },
          { ...makeEvent(2), data: { owner: "0x1" } },
        ],
        nextCursor: "3",
        hasMore: false,
      },
    ]);
    const received: bigint[] = [];

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const sub = client.on(
      { kind: "module", eventType: "0x1::test::Event" },
      (e) => received.push(e.version),
      { where: involvesAddress("0x1") },
    );

    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();

    expect(received).toEqual([2n]);

    client.destroy();
  });

  it("filters getEvents and on() by nested payload field via whereField", async () => {
    const longOne =
      "0x0000000000000000000000000000000000000000000000000000000000000001";
    const page = {
      events: [
        { ...makeEvent(1), data: { store: { owner: "0x2" }, to: "0x1" } },
        { ...makeEvent(2), data: { store: { owner: longOne }, to: "0x2" } },
      ],
      nextCursor: "3",
      hasMore: false,
    };

    const client = new StreamdClient({
      aptos: makeAptos(),
      adapter: adapterOf([page, page]),
    });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { where: whereField("store.owner", "0x1") },
    );
    expect(events.map((l) => l.version)).toEqual([2n]);

    const live: bigint[] = [];
    const sub = client.on(
      { kind: "module", eventType: "0x1::test::Event" },
      (e) => live.push(e.version),
      { where: whereField("store.owner", "0x1") },
    );
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();
    expect(live).toEqual([2n]);

    client.destroy();
  });

  it("filters getEvents, getEventPages, on(), once(), and stream() with not()", async () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
    const e1 = { ...makeEvent(10, 0), data: { from: "0x1", to: "0x1" } };
    const e2 = { ...makeEvent(20, 0), data: { from: "0x2", to: "0x1" } };
    const e3 = { ...makeEvent(30, 0), data: { from: "0x1", to: "0x2" } };
    const e4 = { ...makeEvent(40, 0), data: { from: "0x3", to: "0x1" } };
    const where = every(involvesAddress("0x1"), not(whereField("from", "0x1")));

    const client = new StreamdClient({
      adapter: new MemoryAdapter([e1, e2, e3, e4]),
    });

    // `limit` counts events after `where: not(...)` filtering across pages.
    const events = await client.getEvents(descriptor, { where, limit: 2 });
    expect(events.map((l) => l.version)).toEqual([20n, 40n]);

    const iterVersions: bigint[][] = [];
    for await (const page of client.getEventPages(descriptor, { where }, 2)) {
      iterVersions.push(page.map((e) => e.version));
    }
    expect(iterVersions).toEqual([[20n], [40n]]);

    const onceMatch = client.once(descriptor, { where });
    const seenOn: bigint[] = [];
    const sub = client.on(descriptor, (e) => seenOn.push(e.version), { where });
    await advanceTimersByTimeAsync(0);
    expect((await onceMatch).version).toBe(20n);
    expect(seenOn).toEqual([20n, 40n]);
    sub.unsubscribe();

    const seenStream: bigint[] = [];
    const streamLoop = (async () => {
      for await (const e of client.stream(descriptor, { where })) {
        seenStream.push(e.version);
        if (seenStream.length === 2) break;
      }
    })();
    await advanceTimersByTimeAsync(0);
    await streamLoop;
    expect(seenStream).toEqual([20n, 40n]);

    client.destroy();
  });

  it("maxPages bounds a walk whose predicate matches nothing", async () => {
    const endless: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [makeEvent(1)],
        nextCursor: String(Math.random()),
        hasMore: true, // never satisfied
      })),
    };

    const client = new StreamdClient({ aptos: makeAptos(), adapter: endless });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { limit: 100, maxPages: 3, where: () => false },
    );

    expect(events).toHaveLength(0);
    expect(endless.fetchEvents).toHaveBeenCalledTimes(3);

    client.destroy();
  });

  it("resumes a new client from a cursor saved after destroy()", async () => {
    const first = adapterOf([
      { events: [makeEvent(1)], nextCursor: "2", hasMore: false },
    ]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter: first });
    const sub = client.on({ kind: "module", eventType: "0x1::test::Event" }, () => {});
    await advanceTimersByTimeAsync(0);
    client.destroy();
    const saved = sub.cursor;
    expect(saved).toBe("2");

    const second = adapterOf([]);
    const restarted = new StreamdClient({ aptos: makeAptos(), adapter: second });
    restarted.on({ kind: "module", eventType: "0x1::test::Event" }, () => {}, {
      cursor: saved,
    });
    await advanceTimersByTimeAsync(0);
    expect(mocked(second.fetchEvents).mock.calls[0]![1].cursor).toBe("2");

    restarted.destroy();
  });

  it("accepts an account descriptor", async () => {
    const adapter = adapterOf([
      { events: [makeEvent(10)], nextCursor: "5", hasMore: false },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const events = await client.getEvents({
      kind: "account",
      accountAddress: "0x1",
      eventTypes: ["0x1::test::Event"],
    });

    expect(events).toHaveLength(1);
    expect(adapter.fetchEvents).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "account", accountAddress: "0x1" }),
      expect.anything(),
    );

    client.destroy();
  });

  it("once() resolves with the first event", async () => {
    const mockEvent = makeEvent(200);
    const adapter = adapterOf([
      { events: [mockEvent], nextCursor: "201", hasMore: false },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });

    const promise = client.once({
      kind: "module",
      eventType: "0x1::test::Event",
    });

    // Let the poller fire
    await advanceTimersByTimeAsync(0);

    const result = await promise;
    expect(result).toEqual(mockEvent);

    client.destroy();
  });

  it("once() rejects when polling exhausts its error budget", async () => {
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new Error("network down");
      }),
    };

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, maxErrors: 2 },
    );

    // Bun's `.rejects` blocks on a pending promise, so settle it under the
    // fake clock first; the no-op catch keeps the rejection handled meanwhile.
    promise.catch(() => {});

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    await expect(promise).rejects.toThrow(/Polling failed/);

    client.destroy();
  });

  const failing = (): EventAdapter => ({
    fetchEvents: vi.fn(async () => {
      throw new Error("network down");
    }),
  });

  // Reads the private `subscriptions` map: nothing public reports how many
  // pollers the client still holds, which is what these tests are about.
  function subscriptionCount(client: StreamdClient): number {
    return (client as unknown as { subscriptions: Map<string, unknown> })
      .subscriptions.size;
  }

  it("on() forgets a subscription that stopped itself after maxErrors", async () => {
    const client = new StreamdClient({ aptos: makeAptos(), adapter: failing() });
    const errors: Error[] = [];
    const sub = client.on(
      { kind: "module", eventType: "0x1::test::Event" },
      () => {},
      { pollInterval: 100, maxErrors: 2, onError: (e) => errors.push(e) },
    );
    expect(subscriptionCount(client)).toBe(1);

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);

    expect(sub.active).toBe(false);
    expect(subscriptionCount(client)).toBe(0);
    expect(errors).toHaveLength(2);
  });

  it("once() forgets its subscription when polling gives up", async () => {
    const client = new StreamdClient({ aptos: makeAptos(), adapter: failing() });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, maxErrors: 2 },
    );
    promise.catch(() => {});

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    await expect(promise).rejects.toThrow(/Polling failed/);
    expect(subscriptionCount(client)).toBe(0);
  });

  it("once() rejects on the first poll when the descriptor is invalid", async () => {
    // The default adapter refuses the address before building any request.
    const client = new StreamdClient({ aptos: makeAptos() });
    const promise = client.once({ kind: "account", accountAddress: "nope" });
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(0);
    expect(outcome).toBeInstanceOf(StreamdError);
    expect(((outcome as Error).cause as Error).message).toMatch(/Not a valid Aptos address/);
    expect(subscriptionCount(client)).toBe(0);

    client.destroy();
  });

  it("once() rejects when its where predicate throws", async () => {
    const event: StreamdEvent = {
      type: "0x1::test::Event",
      data: {},
      version: 1n,
      sequenceNumber: 0n,
      eventIndex: 0,
    };
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({ events: [event], nextCursor: "2", hasMore: false })),
    };

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      {
        pollInterval: 100,
        where: () => {
          throw new Error("predicate bug");
        },
      },
    );
    // Observe settlement without awaiting, so a promise that never settles
    // fails the assertion instead of hanging the test.
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(0);
    expect(outcome).toBeInstanceOf(StreamdError);
    expect((outcome as Error).message).toMatch(/filter or callback threw/);

    client.destroy();
  });

  it("once() still rejects when the caller's onError throws", async () => {
    const event: StreamdEvent = {
      type: "0x1::test::Event",
      data: {},
      version: 1n,
      sequenceNumber: 0n,
      eventIndex: 0,
    };
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({ events: [event], nextCursor: "2", hasMore: false })),
    };

    const surfaced = captureMicrotaskThrows();
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      {
        pollInterval: 100,
        where: () => {
          throw new Error("predicate bug");
        },
        onError: () => {
          throw new Error("handler bug");
        },
      },
    );
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(0);
    surfaced.restore();
    expect(outcome).toBeInstanceOf(StreamdError);
    expect(surfaced.thrown.map((e) => (e as Error).message)).toEqual(["handler bug"]);

    client.destroy();
  });

  it("once() rejects with the signal reason on abort and stops fetching", async () => {
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const controller = new AbortController();
    const onError = vi.fn();
    const reason = new Error("caller cancelled");
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, onError, signal: controller.signal },
    );
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(subscriptionCount(client)).toBe(1);

    controller.abort(reason);
    await advanceTimersByTimeAsync(500);

    expect(outcome).toBe(reason);
    expect(subscriptionCount(client)).toBe(0);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
  });

  it("once() with an already-aborted signal rejects without fetching", async () => {
    const adapter = adapterOf([
      { events: [makeEvent(1)], nextCursor: "2", hasMore: false },
    ]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const reason = new Error("already aborted");
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { signal: AbortSignal.abort(reason) },
    );
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(100);
    expect(outcome).toBe(reason);
    expect(subscriptionCount(client)).toBe(0);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
  });

  it("once() rejects with StreamdTimeoutError when the timeout elapses", async () => {
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const onError = vi.fn();
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, timeout: 250, onError, signal: controller.signal },
    );
    let outcome: unknown = "pending";
    promise.then(
      () => (outcome = "resolved"),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(200);
    expect(outcome).toBe("pending");
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(3);

    await advanceTimersByTimeAsync(50);
    expect(outcome).toBeInstanceOf(StreamdTimeoutError);
    expect((outcome as StreamdTimeoutError).timeout).toBe(250);
    expect(subscriptionCount(client)).toBe(0);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();

    await advanceTimersByTimeAsync(500);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(3);
  });

  it("once() clears its timer and detaches its abort listener when an event arrives", async () => {
    const adapter = adapterOf([
      { events: [], nextCursor: "1", hasMore: false },
      { events: [makeEvent(42)], nextCursor: "43", hasMore: false },
    ]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const clear = vi.spyOn(globalThis, "clearTimeout");
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, timeout: 500, signal: controller.signal },
    );

    await advanceTimersByTimeAsync(100);
    const event = await promise;
    expect(event.version).toBe(42n);
    expect(subscriptionCount(client)).toBe(0);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(clear).toHaveBeenCalled();

    await advanceTimersByTimeAsync(1000);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(2);
    clear.mockRestore();
  });

  it("once() rejects an invalid timeout with StreamdError before subscribing", async () => {
    const adapter = adapterOf([
      { events: [makeEvent(1)], nextCursor: "2", hasMore: false },
    ]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });

    for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      let outcome: unknown = "pending";
      client
        .once({ kind: "module", eventType: "0x1::test::Event" }, { timeout: bad })
        .then(
          () => (outcome = "resolved"),
          (err) => (outcome = err),
        );
      await advanceTimersByTimeAsync(0);
      expect(outcome).toBeInstanceOf(StreamdError);
      expect(outcome).not.toBeInstanceOf(StreamdTimeoutError);
    }

    expect(subscriptionCount(client)).toBe(0);
    expect(adapter.fetchEvents).not.toHaveBeenCalled();
  });

  it("delivers transactionHash and timestamp on events through getEvents and on()", async () => {
    const enriched = {
      ...makeEvent(10),
      transactionHash: "0xabc123",
      timestamp: 1700000010000000n,
    };
    const adapter = adapterOf([
      { events: [enriched], nextCursor: "11", hasMore: false },
      { events: [enriched], nextCursor: "11", hasMore: false },
    ]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    const events = await client.getEvents(descriptor);
    expect(events[0]?.transactionHash).toBe("0xabc123");
    expect(events[0]?.timestamp).toBe(1700000010000000n);

    const live: Array<{ hash?: string; ts?: bigint }> = [];
    const sub = client.on(descriptor, (e) => {
      live.push({ hash: e.transactionHash, ts: e.timestamp });
    });
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();

    expect(live).toEqual([{ hash: "0xabc123", ts: 1700000010000000n }]);
    client.destroy();
  });

  it("supports multi-type module descriptors across getEvents, on, once, and stream", async () => {
    const ping: StreamdEvent = {
      ...makeEvent(10, 0),
      type: "0x1::test::Ping",
    };
    const pong: StreamdEvent = {
      ...makeEvent(10, 1),
      type: "0x1::test::Pong",
    };
    const page: EventPage = {
      events: [ping, pong],
      nextCursor: "11",
      hasMore: false,
    };
    const adapter = adapterOf([page, page, page, page]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const descriptor = {
      kind: "module",
      eventType: ["0x1::test::Ping", "0x1::test::Pong"],
    } as const;

    const events = await client.getEvents(descriptor);
    expect(events.map((e) => e.type)).toEqual([
      "0x1::test::Ping",
      "0x1::test::Pong",
    ]);

    const seenOn: string[] = [];
    const sub = client.on(descriptor, (e) => seenOn.push(e.type));
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();
    expect(seenOn).toEqual(["0x1::test::Ping", "0x1::test::Pong"]);

    const oncePromise = client.once(descriptor);
    await advanceTimersByTimeAsync(0);
    expect((await oncePromise).type).toBe("0x1::test::Ping");

    const seenStream: string[] = [];
    const streamLoop = (async () => {
      for await (const e of client.stream(descriptor)) {
        seenStream.push(e.type);
        if (seenStream.length === 2) break;
      }
    })();
    await advanceTimersByTimeAsync(0);
    await streamLoop;
    expect(seenStream).toEqual(["0x1::test::Ping", "0x1::test::Pong"]);

    expect(adapter.fetchEvents).toHaveBeenCalledTimes(4);
    expect(mocked(adapter.fetchEvents).mock.calls[0]![0]).toEqual(descriptor);

    client.destroy();
  });

  describe("client-wide onError", () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    it("routes on() polling and callback errors to config.onError when options.onError is omitted", async () => {
      let calls = 0;
      const adapter: EventAdapter = {
        fetchEvents: vi.fn(async () => {
          if (calls++ === 0) throw new Error("blip");
          return { events: [makeEvent(1), makeEvent(2)], nextCursor: "3", hasMore: false };
        }),
      };
      const clientErrors: Error[] = [];
      const client = new StreamdClient({
        aptos: makeAptos(),
        adapter,
        onError: (err) => clientErrors.push(err),
      });
      const seen: bigint[] = [];
      const sub = client.on(
        descriptor,
        (e) => {
          seen.push(e.version);
          if (e.version === 1n) throw new Error("callback bug");
        },
        { pollInterval: 100 },
      );

      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(200);
      sub.unsubscribe();

      expect(seen).toEqual([1n, 2n]);
      expect(clientErrors).toHaveLength(2);
      expect(clientErrors[0]).toBeInstanceOf(StreamdPollingError);
      expect(clientErrors[1]).toBeInstanceOf(StreamdError);
      expect(clientErrors[1]).not.toBeInstanceOf(StreamdPollingError);
      client.destroy();
    });

    it("prefers per-subscription options.onError over config.onError in on() and once()", async () => {
      const clientOnError = vi.fn();
      const subOnError = vi.fn();
      const onceOnError = vi.fn();
      const client = new StreamdClient({
        aptos: makeAptos(),
        adapter: failing(),
        onError: clientOnError,
      });

      const sub = client.on(descriptor, () => {}, {
        pollInterval: 100,
        maxErrors: 2,
        onError: subOnError,
      });
      const oncePromise = client.once(descriptor, {
        pollInterval: 100,
        maxErrors: 2,
        onError: onceOnError,
      });
      oncePromise.catch(() => {});

      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(200);
      await expect(oncePromise).rejects.toThrow(/Polling failed/);

      expect(sub.active).toBe(false);
      expect(subOnError).toHaveBeenCalledTimes(2);
      expect(onceOnError).toHaveBeenCalledTimes(2);
      expect(clientOnError).not.toHaveBeenCalled();
      client.destroy();
    });

    it("routes once() transient and fatal errors to config.onError once per error", async () => {
      let calls = 0;
      const adapter: EventAdapter = {
        fetchEvents: vi.fn(async () => {
          calls++;
          if (calls === 1) throw new Error("transient");
          if (calls === 2) return { events: [makeEvent(9)], nextCursor: "10", hasMore: false };
          throw new Error("down");
        }),
      };
      const clientOnError = vi.fn();
      const client = new StreamdClient({
        aptos: makeAptos(),
        adapter,
        onError: clientOnError,
      });

      const first = client.once(descriptor, { pollInterval: 100, maxErrors: 3 });
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(200);
      expect((await first).version).toBe(9n);
      expect(clientOnError).toHaveBeenCalledTimes(1);

      const second = client.once(descriptor, { pollInterval: 100, maxErrors: 2 });
      second.catch(() => {});
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(200);
      await expect(second).rejects.toThrow(/Polling failed/);
      expect(clientOnError).toHaveBeenCalledTimes(3);
      client.destroy();
    });

    it("does not call config.onError on abort or once() timeout", async () => {
      const clientOnError = vi.fn();
      const client = new StreamdClient({
        aptos: makeAptos(),
        adapter: adapterOf([]),
        onError: clientOnError,
      });
      const c1 = new AbortController();
      const c2 = new AbortController();
      client.on(descriptor, () => {}, { pollInterval: 100, signal: c1.signal });
      const abortedOnce = client.once(descriptor, { pollInterval: 100, signal: c2.signal });
      const timedOnce = client.once(descriptor, { pollInterval: 100, timeout: 150 });
      abortedOnce.catch(() => {});
      timedOnce.catch(() => {});

      await advanceTimersByTimeAsync(0);
      c1.abort(new Error("stop on"));
      c2.abort(new Error("stop once"));
      await advanceTimersByTimeAsync(200);

      await expect(abortedOnce).rejects.toThrow("stop once");
      await expect(timedOnce).rejects.toBeInstanceOf(StreamdTimeoutError);
      expect(clientOnError).not.toHaveBeenCalled();
      client.destroy();
    });

    it("rethrows a throwing config.onError on a microtask without stalling on() or once()", async () => {
      const client = new StreamdClient({
        aptos: makeAptos(),
        adapter: failing(),
        onError: () => {
          throw new Error("client handler bug");
        },
      });
      const surfaced = captureMicrotaskThrows();
      try {
        const sub = client.on(descriptor, () => {}, { pollInterval: 100, maxErrors: 2 });
        const oncePromise = client.once(descriptor, { pollInterval: 100, maxErrors: 2 });
        oncePromise.catch(() => {});

        await advanceTimersByTimeAsync(0);
        await advanceTimersByTimeAsync(200);
        await expect(oncePromise).rejects.toThrow(/Polling failed/);

        expect(sub.active).toBe(false);
        expect(subscriptionCount(client)).toBe(0);
        expect(surfaced.thrown.map((e) => (e as Error).message)).toEqual([
          "client handler bug",
          "client handler bug",
          "client handler bug",
          "client handler bug",
        ]);
      } finally {
        surfaced.restore();
        client.destroy();
      }
    });
  });

  describe("MemoryAdapter offline usage", () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    it("throws StreamdError when constructed with neither aptos nor adapter", () => {
      expect(() => new StreamdClient({})).toThrow(StreamdError);
      expect(() => new StreamdClient({})).toThrow(
        /StreamdClient requires either `aptos` or `adapter` in config/,
      );
    });

    it("drives getEvents, getEventPages, on, once, stream, and cursor resume offline without an Aptos instance", async () => {
      const e1 = { ...makeEvent(10, 0), data: { owner: "0x1", v: 1 } };
      const e2 = { ...makeEvent(10, 1), data: { owner: "0x2", v: 2 } };
      const e3 = { ...makeEvent(20, 0), data: { owner: "0x1", v: 3 } };
      const client = new StreamdClient({
        adapter: new MemoryAdapter([e3, e1, e2]),
      });

      const events = await client.getEvents(descriptor, {
        where: whereField("owner", "0x1"),
      });
      expect(events.map((e) => [e.version, e.eventIndex])).toEqual([
        [10n, 0],
        [20n, 0],
      ]);

      const pages: bigint[][] = [];
      for await (const page of client.getEventPages(descriptor, {}, 2)) {
        pages.push(page.map((e) => e.version));
      }
      expect(pages).toEqual([[10n, 10n], [20n]]);

      const firstMatch = client.once(descriptor, {
        where: whereField("v", 3),
      });
      await advanceTimersByTimeAsync(0);
      expect((await firstMatch).version).toBe(20n);

      const seenOn: number[] = [];
      const sub = client.on<{ owner: string; v: number }>(
        descriptor,
        (e) => seenOn.push(e.data.v),
        { batchSize: 2, pollInterval: 100 },
      );
      // First poll delivers 2 events and schedules an immediate catch-up poll (0ms) for the 3rd.
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(0);
      expect(seenOn).toEqual([1, 2, 3]);
      const savedCursor = sub.cursor;
      expect(savedCursor).toBe("3");
      sub.unsubscribe();

      // Resume against a MemoryAdapter seeded with an additional later event.
      const e4 = { ...makeEvent(30, 0), data: { owner: "0x1", v: 4 } };
      const resumedClient = new StreamdClient({
        adapter: new MemoryAdapter([e1, e2, e3, e4]),
      });
      const resumedSeen: number[] = [];
      const streamLoop = (async () => {
        for await (const e of resumedClient.stream<{ owner: string; v: number }>(
          descriptor,
          { cursor: savedCursor },
        )) {
          resumedSeen.push(e.data.v);
          if (resumedSeen.length === 1) break;
        }
      })();
      await advanceTimersByTimeAsync(0);
      await streamLoop;
      expect(resumedSeen).toEqual([4]);

      client.destroy();
      resumedClient.destroy();
    });
  });

  describe("RoutingAdapter and FallbackAdapter composition", () => {
    const moduleDesc = { kind: "module", eventType: "0x1::test::Event" } as const;
    const accountDesc = { kind: "account", accountAddress: "0x1" } as const;

    it("composes RoutingAdapter and FallbackAdapter across getEvents, on, once, and stream", async () => {
      let primaryCalls = 0;
      const flakyModulePrimary: EventAdapter = {
        fetchEvents: vi.fn(async () => {
          if (primaryCalls++ === 0) {
            throw new StreamdNetworkError("primary fullnode 503", 503);
          }
          return {
            events: [makeEvent(11), makeEvent(12)],
            nextCursor: "2",
            hasMore: false,
          };
        }),
      };
      const moduleBackup = new MemoryAdapter([makeEvent(11), makeEvent(12)]);
      const accountSource = new MemoryAdapter([makeEvent(99)]);

      const adapter = new RoutingAdapter({
        module: new FallbackAdapter([flakyModulePrimary, moduleBackup]),
        default: accountSource,
      });
      const onError = vi.fn();
      const client = new StreamdClient({ adapter, onError });

      // First call on `module` falls back from flakyModulePrimary to moduleBackup seamlessly.
      const moduleEvents = await client.getEvents(moduleDesc);
      expect(moduleEvents.map((e) => e.version)).toEqual([11n, 12n]);
      expect(flakyModulePrimary.fetchEvents).toHaveBeenCalledTimes(1);

      const iterPages: bigint[][] = [];
      for await (const page of client.getEventPages(moduleDesc, {}, 1)) {
        iterPages.push(page.map((e) => e.version));
      }
      expect(iterPages).toEqual([[11n, 12n]]);

      // `account` routes to default (accountSource).
      const accountEvents = await client.getEvents(accountDesc);
      expect(accountEvents.map((e) => e.version)).toEqual([99n]);

      // Subsequent `on`, `once`, and `stream` calls on `module` succeed without firing onError.
      const onceEvent = client.once(moduleDesc);
      const seenOn: bigint[] = [];
      const sub = client.on(moduleDesc, (e) => seenOn.push(e.version));
      await advanceTimersByTimeAsync(0);
      expect((await onceEvent).version).toBe(11n);
      expect(seenOn).toEqual([11n, 12n]);
      sub.unsubscribe();

      const seenStream: bigint[] = [];
      const streamLoop = (async () => {
        for await (const e of client.stream(moduleDesc)) {
          seenStream.push(e.version);
          if (seenStream.length === 2) break;
        }
      })();
      await advanceTimersByTimeAsync(0);
      await streamLoop;
      expect(seenStream).toEqual([11n, 12n]);
      expect(onError).not.toHaveBeenCalled();

      client.destroy();
    });
  });

  describe("sub.status", () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    it("exposes status across polling, transient and fatal errors, unsubscribe, destroy, and abort", async () => {
      let now = 100_000;
      const nowSpy = vi.spyOn(Date, "now").mockImplementation(() => now);
      try {
        let call = 0;
        const adapter: EventAdapter = {
          fetchEvents: vi.fn(async () => {
            call++;
            if (call === 1) {
              return {
                events: [makeEvent(15), makeEvent(22)],
                nextCursor: "23",
                hasMore: false,
              };
            }
            throw new StreamdNetworkError("node 503", 503);
          }),
        };
        const seenErrors: number[] = [];
        const client = new StreamdClient({ adapter });
        const sub = client.on(descriptor, () => {}, {
          pollInterval: 100,
          maxErrors: 2,
          cursor: "10",
          onError: () => seenErrors.push(sub.status.consecutiveErrors),
        });

        expect(sub.status).toEqual({
          active: true,
          cursor: "10",
          lastVersion: undefined,
          consecutiveErrors: 0,
          lastSuccessAt: undefined,
        });

        // Poll 1 at 100_000 succeeds.
        await advanceTimersByTimeAsync(0);
        expect(sub.status).toEqual({
          active: true,
          cursor: "23",
          lastVersion: 22n,
          consecutiveErrors: 0,
          lastSuccessAt: 100_000,
        });

        // Poll 2 at 100_100 fails (1/2); poll 3 at 100_300 fails (2/2) and stops.
        now = 100_100;
        await advanceTimersByTimeAsync(100);
        expect(sub.status.consecutiveErrors).toBe(1);
        expect(sub.status.active).toBe(true);

        now = 100_300;
        await advanceTimersByTimeAsync(200);
        expect(seenErrors).toEqual([1, 2]);
        expect(sub.status).toEqual({
          active: false,
          cursor: "23",
          lastVersion: 22n,
          consecutiveErrors: 2,
          lastSuccessAt: 100_000,
        });

        // Status stays readable after unsubscribe(), destroy(), and signal abort.
        const unsubbed = client.on(descriptor, () => {}, { cursor: "66" });
        unsubbed.unsubscribe();
        expect(unsubbed.status).toEqual({
          active: false,
          cursor: "66",
          lastVersion: undefined,
          consecutiveErrors: 0,
          lastSuccessAt: undefined,
        });

        const c = new AbortController();
        const sub2 = client.on(
          descriptor,
          () => {},
          { signal: c.signal, cursor: "77" },
        );
        c.abort();
        expect(sub2.status).toEqual({
          active: false,
          cursor: "77",
          lastVersion: undefined,
          consecutiveErrors: 0,
          lastSuccessAt: undefined,
        });

        const sub3 = client.on(descriptor, () => {}, { cursor: "88" });
        client.destroy();
        expect(sub3.status.active).toBe(false);
        expect(sub3.status.cursor).toBe("88");
      } finally {
        nowSpy.mockRestore();
      }
    });
  });

  describe("maxBackoff and dedupWindow options", () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    it("forwards maxBackoff and dedupWindow through on(), once(), and stream()", async () => {
      const e1 = makeEvent(1);
      const dupAdapter = adapterOf([
        { events: [e1], nextCursor: "2", hasMore: false },
        { events: [e1], nextCursor: "3", hasMore: false },
      ]);
      const client = new StreamdClient({ adapter: dupAdapter });
      const seenOn: bigint[] = [];
      const sub = client.on(descriptor, (e) => seenOn.push(e.version), {
        pollInterval: 100,
        dedupWindow: 0,
      });
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(100);
      sub.unsubscribe();
      expect(seenOn).toEqual([1n, 1n]);

      // once() with maxBackoff: 50 caps retry waits at 50ms despite pollInterval: 200.
      const at: number[] = [];
      const flakyOnceAdapter: EventAdapter = {
        fetchEvents: vi.fn(async () => {
          at.push(Date.now());
          if (at.length < 3) throw new StreamdNetworkError("node 503", 503);
          return { events: [makeEvent(7)], nextCursor: "8", hasMore: false };
        }),
      };
      const flakyClient = new StreamdClient({ adapter: flakyOnceAdapter });
      const oncePromise = flakyClient.once(descriptor, {
        pollInterval: 200,
        maxBackoff: 50,
        maxErrors: 5,
      });
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(50);
      await advanceTimersByTimeAsync(50);
      expect((await oncePromise).version).toBe(7n);
      expect(at.slice(1).map((t, i) => t - at[i]!)).toEqual([50, 50]);

      // stream() with dedupWindow: 0 delivers repeated events across pages.
      const streamDupAdapter = adapterOf([
        { events: [e1], nextCursor: "2", hasMore: false },
        { events: [e1], nextCursor: "3", hasMore: false },
      ]);
      const streamClient = new StreamdClient({ adapter: streamDupAdapter });
      const seenStream: bigint[] = [];
      const streamLoop = (async () => {
        for await (const e of streamClient.stream(descriptor, {
          pollInterval: 100,
          dedupWindow: 0,
        })) {
          seenStream.push(e.version);
          if (seenStream.length === 2) break;
        }
      })();
      await advanceTimersByTimeAsync(0);
      await advanceTimersByTimeAsync(100);
      await streamLoop;
      expect(seenStream).toEqual([1n, 1n]);

      client.destroy();
      flakyClient.destroy();
      streamClient.destroy();
    });

    it("rejects or throws StreamdError on invalid maxBackoff or dedupWindow across on(), once(), and stream()", async () => {
      const adapter = adapterOf([]);
      const client = new StreamdClient({ adapter });

      for (const badOpts of [
        { maxBackoff: -1 },
        { maxBackoff: Number.NaN },
        { maxBackoff: Number.POSITIVE_INFINITY },
        { maxBackoff: 2_147_483_648 },
        { dedupWindow: -1 },
        { dedupWindow: 1.5 },
        { dedupWindow: Number.NaN },
      ]) {
        const controller = new AbortController();
        const remove = vi.spyOn(controller.signal, "removeEventListener");
        const opts = { ...badOpts, signal: controller.signal };

        expect(() => client.on(descriptor, () => {}, opts)).toThrow(
          StreamdError,
        );
        await expect(client.once(descriptor, opts)).rejects.toThrow(
          StreamdError,
        );
        const iter = client.stream(descriptor, opts);
        await expect(iter.next()).rejects.toThrow(StreamdError);
        await expect(iter.next()).resolves.toEqual({
          value: undefined,
          done: true,
        });
        expect(remove).toHaveBeenCalledTimes(1);
      }

      expect(subscriptionCount(client)).toBe(0);
      expect(
        (client as unknown as { streams: Set<unknown> }).streams.size,
      ).toBe(0);
      expect(adapter.fetchEvents).not.toHaveBeenCalled();
      client.destroy();
    });
  });
});

describe("Subscription pause and resume", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  /** Serves a growing list; the cursor is the index of the next unread event. */
  function growingAdapter(events: StreamdEvent[]): EventAdapter {
    return {
      fetchEvents: vi.fn(async (_d, options) => {
        const from = Number(options.cursor ?? 0);
        return { events: events.slice(from), nextCursor: String(events.length), hasMore: false };
      }),
    };
  }

  function tracked(client: StreamdClient): number {
    return (client as unknown as { subscriptions: Map<string, unknown> }).subscriptions.size;
  }

  it("stops delivery while paused and delivers what arrived meanwhile on resume", async () => {
    const events = [makeEvent(1)];
    const adapter = growingAdapter(events);
    const client = new StreamdClient({ adapter });
    const seen: bigint[] = [];
    const sub = client.on(descriptor, (e) => seen.push(e.version), { pollInterval: 100 });
    await advanceTimersByTimeAsync(0);
    expect(seen).toEqual([1n]);

    sub.pause();
    expect(sub.paused).toBe(true);
    expect(sub.active).toBe(false);
    expect(sub.status.active).toBe(false);
    expect(tracked(client)).toBe(1);
    const fetchesAtPause = mocked(adapter.fetchEvents).mock.calls.length;
    events.push(makeEvent(2), makeEvent(3));
    await advanceTimersByTimeAsync(1000);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBe(fetchesAtPause);
    expect(seen).toEqual([1n]);

    sub.resume();
    expect(sub.paused).toBe(false);
    expect(sub.active).toBe(true);
    await advanceTimersByTimeAsync(0);
    expect(seen).toEqual([1n, 2n, 3n]);
    expect(sub.cursor).toBe("3");

    client.destroy();
  });

  it("ends a paused subscription on unsubscribe, destroy, or abort", async () => {
    const adapter = growingAdapter([makeEvent(1)]);
    const client = new StreamdClient({ adapter });
    const controller = new AbortController();
    const subs = [
      client.on(descriptor, () => {}),
      client.on(descriptor, () => {}),
      client.on(descriptor, () => {}, { signal: controller.signal }),
    ];
    await advanceTimersByTimeAsync(0);
    for (const sub of subs) sub.pause();

    subs[0]!.unsubscribe();
    controller.abort();
    expect(tracked(client)).toBe(1);
    client.destroy();
    expect(tracked(client)).toBe(0);

    const fetches = mocked(adapter.fetchEvents).mock.calls.length;
    for (const sub of subs) {
      expect(sub.paused).toBe(false);
      sub.resume();
      expect(sub.active).toBe(false);
    }
    await advanceTimersByTimeAsync(500);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBe(fetches);
  });

  it("keeps a paused subscription when a pending handler fails while paused", async () => {
    const events = [makeEvent(1)];
    const client = new StreamdClient({ adapter: growingAdapter(events) });
    let reject!: (e: Error) => void;
    const onError = vi.fn();
    const seen: bigint[] = [];
    const sub = client.on(
      descriptor,
      (e) => {
        seen.push(e.version);
        if (e.version === 1n) return new Promise<void>((_, r) => (reject = r));
      },
      { onError },
    );
    await advanceTimersByTimeAsync(0);

    sub.pause();
    reject(new Error("handler failed"));
    await advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(sub.paused).toBe(true);
    expect(tracked(client)).toBe(1);

    events.push(makeEvent(2));
    sub.resume();
    await advanceTimersByTimeAsync(0);
    expect(seen).toEqual([1n, 2n]);

    client.destroy();
  });
});

describe("onCaughtUp", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("fires once on() has delivered the backlog", async () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
    const client = new StreamdClient({
      adapter: new MemoryAdapter([makeEvent(1), makeEvent(2), makeEvent(3)]),
    });
    const order: string[] = [];
    const sub = client.on(descriptor, (e) => void order.push(`event ${e.version}`), {
      batchSize: 2,
      pollInterval: 100,
      onCaughtUp: () => void order.push("caught up"),
    });
    await advanceTimersByTimeAsync(500);
    expect(order).toEqual(["event 1", "event 2", "event 3", "caught up"]);
    sub.unsubscribe();
  });
});

describe("onBatch", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("delivers the backlog one page at a time and stops on destroy()", async () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
    const client = new StreamdClient({
      adapter: new MemoryAdapter([makeEvent(1), makeEvent(2), makeEvent(3)]),
    });
    const order: string[] = [];
    const sub = client.onBatch(
      descriptor,
      (events) => void order.push(events.map((e) => e.version).join(",")),
      { batchSize: 2, pollInterval: 100, onCaughtUp: () => void order.push("caught up") },
    );
    await advanceTimersByTimeAsync(500);
    expect(order).toEqual(["1,2", "3", "caught up"]);
    expect(sub.active).toBe(true);

    client.destroy();
    expect(sub.active).toBe(false);
  });
});

describe("StreamdClient relative start", () => {
  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
  const versions = [5, 8, 9, 10].map((v) => makeEvent(v));

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("on() with a negative fromVersion delivers from that many versions before the head", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(versions) });
    const seen: bigint[] = [];
    client.on(descriptor, (e) => void seen.push(e.version), { fromVersion: -2n });
    await advanceTimersByTimeAsync(0);
    client.destroy();

    expect(seen).toEqual([8n, 9n, 10n]);
  });

  it("getEvents() with a negative fromVersion starts that many versions before the head", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(versions) });
    const events = await client.getEvents(descriptor, { fromVersion: -1n });

    expect(events.map((e) => e.version)).toEqual([9n, 10n]);
  });
});

describe("StreamdClient latest-first queries", () => {
  it("getEvents() with orderBy desc returns the most recent events across pages", async () => {
    const versions = Array.from({ length: 60 }, (_, i) => makeEvent(i + 1));
    const client = new StreamdClient({ adapter: new MemoryAdapter(versions) });
    const events = await client.getEvents(
      { kind: "module", eventType: "0x1::test::Event" },
      { orderBy: "desc", limit: 3, fromVersion: -50n },
    );

    expect(events.map((e) => e.version)).toEqual([60n, 59n, 58n]);
  });
});

describe("maxConcurrentRequests", () => {
  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  /** Every call waits until released, so the test controls how many are in flight. */
  function slowAdapter() {
    const pending: (() => void)[] = [];
    const calls: string[] = [];
    let inFlight = 0;
    let peak = 0;
    const hold = <T>(kind: string, value: T) =>
      new Promise<T>((resolve) => {
        calls.push(kind);
        peak = Math.max(peak, ++inFlight);
        pending.push(() => (inFlight--, resolve(value)));
      });
    const adapter: EventAdapter = {
      fetchEvents: () => hold("fetch", { events: [], nextCursor: "c", hasMore: false }),
      getHeadVersion: () => hold("head", 100n),
    };
    const releaseAll = () => pending.splice(0).forEach((release) => release());
    return { adapter, calls, releaseAll, peak: () => peak };
  }

  it("caps adapter calls across subscriptions and queries, counting getHeadVersion", async () => {
    const slow = slowAdapter();
    const client = new StreamdClient({ adapter: slow.adapter, maxConcurrentRequests: 2 });
    client.on(descriptor, () => {});
    client.on(descriptor, () => {}, { fromVersion: -10n });
    client.on(descriptor, () => {});
    const query = client.getEvents(descriptor);
    await advanceTimersByTimeAsync(0);
    expect(slow.calls.length).toBe(2);

    for (let i = 0; i < 4; i++) {
      slow.releaseAll();
      await advanceTimersByTimeAsync(0);
    }
    expect(await query).toEqual([]);
    expect(slow.calls.filter((c) => c === "head").length).toBe(1);
    expect(slow.calls.filter((c) => c === "fetch").length).toBe(4);
    expect(slow.peak()).toBe(2);
    client.destroy();
  });

  it("never calls the adapter for a subscription unsubscribed while queued", async () => {
    const slow = slowAdapter();
    const client = new StreamdClient({ adapter: slow.adapter, maxConcurrentRequests: 1 });
    client.on(descriptor, () => {});
    const onError = vi.fn();
    const queued = client.on(descriptor, () => {}, { onError });
    await advanceTimersByTimeAsync(0);
    queued.unsubscribe();
    slow.releaseAll();
    await advanceTimersByTimeAsync(0);

    expect(slow.calls).toEqual(["fetch"]);
    expect(onError).not.toHaveBeenCalled();
    client.destroy();
  });

  it("rejects a query aborted while queued without calling the adapter for it", async () => {
    const slow = slowAdapter();
    const client = new StreamdClient({ adapter: slow.adapter, maxConcurrentRequests: 1 });
    client.on(descriptor, () => {});
    await advanceTimersByTimeAsync(0);
    const controller = new AbortController();
    const query = client.getEvents(descriptor, { signal: controller.signal });
    await advanceTimersByTimeAsync(0);
    const reason = new Error("stop");
    controller.abort(reason);
    await expect(query).rejects.toBe(reason);
    slow.releaseAll();
    await advanceTimersByTimeAsync(0);

    expect(slow.calls).toEqual(["fetch"]);
    client.destroy();
  });

  it("rejects a cap that is not a positive integer", () => {
    for (const bad of [0, -1, 1.5, NaN, Infinity, "2"]) {
      expect(
        () => new StreamdClient({ adapter: slowAdapter().adapter, maxConcurrentRequests: bad as number }),
      ).toThrow(StreamdError);
    }
  });
});

describe("StreamdClient decode", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("delivers named integer fields as bigint through on()", async () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
    const source = { ...makeEvent(10), data: { amount: "1000", store: "0x1" } };
    const client = new StreamdClient({ adapter: new MemoryAdapter([source]) });
    const seen: unknown[] = [];
    client.on(descriptor, (e) => void seen.push(e.data), {
      fromVersion: 0n,
      decode: ["amount"],
    });
    await advanceTimersByTimeAsync(0);

    expect(seen).toEqual([{ amount: 1000n, store: "0x1" }]);
    client.destroy();
  });
});

const T = "0x1::test::Event";
const U = "0x1::test::Other";
const moduleOf = (eventType: ModuleEventDescriptor["eventType"]) =>
  ({ kind: "module", eventType }) as const;
const quiet = () => new StreamdClient({ adapter: new MemoryAdapter([]) });

describe("listener management", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("counts on() and onBatch() subscriptions, in total and per descriptor", () => {
    const client = quiet();
    client.on(moduleOf(T), () => {});
    client.onBatch(moduleOf(T), () => {});
    client.on(moduleOf(U), () => {});

    expect(client.listenerCount()).toBe(3);
    expect(client.listenerCount(moduleOf(T))).toBe(2);
    expect(client.listenerCount(moduleOf(U))).toBe(1);
    expect(client.listenerCount(moduleOf("0x1::test::None"))).toBe(0);
    client.destroy();
  });

  it("does not count once() or stream()", async () => {
    const client = quiet();
    const pending = client.once(moduleOf(T));
    pending.catch(() => {});
    const it = client.stream(moduleOf(T));
    const next = it.next();
    await advanceTimersByTimeAsync(0);

    expect(client.listenerCount()).toBe(0);
    expect(client.listenerCount(moduleOf(T))).toBe(0);
    await it.return!();
    await next;
    client.destroy();
  });

  it("matches descriptors by value", () => {
    const client = quiet();
    client.on(moduleOf([T, U]), () => {});
    client.on(moduleOf(T), () => {});
    client.on(
      { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::S", fieldName: "f" },
      () => {},
    );
    client.on({ kind: "account", accountAddress: "0x1" }, () => {});
    client.on({ kind: "account", accountAddress: "0x1", eventTypes: [] }, () => {});

    expect(client.listenerCount(moduleOf([U, T]))).toBe(1);
    expect(client.listenerCount(moduleOf([T]))).toBe(1);
    expect(
      client.listenerCount({
        kind: "handle",
        accountAddress: `0x${"0".repeat(63)}1`,
        eventHandleStruct: "0x1::a::S",
        fieldName: "f",
      }),
    ).toBe(1);
    expect(client.listenerCount({ kind: "account", accountAddress: "0x1" })).toBe(1);
    expect(
      client.listenerCount({ kind: "account", accountAddress: "0x1", eventTypes: [] }),
    ).toBe(1);
    expect(
      client.listenerCount({ kind: "bogus" } as unknown as EventDescriptor),
    ).toBe(0);
    client.destroy();
  });

  it("counts paused subscriptions and drops ended ones", async () => {
    const failing: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new Error("network down");
      }),
    };
    const client = new StreamdClient({ adapter: failing });
    const controller = new AbortController();
    const paused = client.on(moduleOf(T), () => {}, { onError: () => {} });
    paused.pause();
    const unsubscribed = client.on(moduleOf(T), () => {}, { onError: () => {} });
    client.on(moduleOf(T), () => {}, { signal: controller.signal, onError: () => {} });
    client.on(moduleOf(U), () => {}, { pollInterval: 100, maxErrors: 2, onError: () => {} });
    expect(client.listenerCount()).toBe(4);

    unsubscribed.unsubscribe();
    controller.abort();
    expect(client.listenerCount(moduleOf(T))).toBe(1);

    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    expect(client.listenerCount(moduleOf(U))).toBe(0);
    expect(client.listenerCount()).toBe(1);
    client.destroy();
  });
});

describe("off and removeAllListeners", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("off(d, cb) stops only the earliest match", () => {
    const client = quiet();
    const cb = () => {};
    const first = client.on(moduleOf(T), cb);
    const second = client.on(moduleOf(T), cb);
    const other = client.on(moduleOf(T), () => {});
    const batch = client.onBatch(moduleOf(T), cb);

    expect(client.off(moduleOf([T]), cb)).toBe(client);
    expect([first.active, second.active, other.active, batch.active]).toEqual([
      false, true, true, true,
    ]);
    expect(client.listenerCount(moduleOf(T))).toBe(3);

    client.off(moduleOf(T), () => {});
    client.off(moduleOf(U), cb);
    expect(client.listenerCount(moduleOf(T))).toBe(3);
    client.destroy();
  });

  it("off(d, cb) removes an onBatch() subscription by its batch callback", () => {
    const client = quiet();
    const batchCb = () => {};
    const sub = client.onBatch(moduleOf(T), batchCb);
    client.off(moduleOf(T), batchCb);
    expect(sub.active).toBe(false);
    expect(client.listenerCount()).toBe(0);
  });

  it("off(d) stops every listener on d and nothing else", () => {
    const client = quiet();
    const a = client.on(moduleOf(T), () => {});
    const b = client.onBatch(moduleOf(T), () => {});
    const c = client.on(moduleOf(U), () => {});

    client.off(moduleOf(T));
    expect([a.active, b.active, c.active]).toEqual([false, false, true]);
    expect(client.listenerCount()).toBe(1);
    client.destroy();
  });

  it("off() with a descriptor that is not one of the three kinds is a no-op", () => {
    const client = quiet();
    const sub = client.on(moduleOf(T), () => {});
    client.off({ kind: "bogus" } as unknown as EventDescriptor);
    client.off(undefined as unknown as EventDescriptor);
    client.removeAllListeners({ kind: "bogus" } as unknown as EventDescriptor);
    expect(sub.active).toBe(true);
    client.destroy();
  });

  it("removeAllListeners(d) stops only that descriptor's listeners", () => {
    const client = quiet();
    const a = client.on(moduleOf(T), () => {});
    const c = client.on(moduleOf(U), () => {});
    expect(client.removeAllListeners(moduleOf(T))).toBe(client);
    expect([a.active, c.active]).toEqual([false, true]);
    client.destroy();
  });

  it("removeAllListeners() leaves once() and stream() running", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([makeEvent(1)]) });
    const a = client.on(moduleOf(T), () => {});
    const paused = client.onBatch(moduleOf(U), () => {});
    paused.pause();
    const pending = client.once(moduleOf(T), { fromVersion: 0n, pollInterval: 100 });
    const it = client.stream(moduleOf(T), { fromVersion: 0n });

    client.removeAllListeners();
    expect([a.active, paused.active, paused.paused]).toEqual([false, false, false]);
    expect(client.listenerCount()).toBe(0);

    const next = it.next();
    await advanceTimersByTimeAsync(0);
    expect((await pending).version).toBe(1n);
    expect((await next).value?.version).toBe(1n);
    await it.return!();
    client.destroy();
  });

  it("destroy() clears the listener count", () => {
    const client = quiet();
    client.on(moduleOf(T), () => {});
    client.onBatch(moduleOf(U), () => {});
    client.destroy();
    expect(client.listenerCount()).toBe(0);
  });

  it("off() after destroy() and re-subscribing stops the live subscription", () => {
    const client = quiet();
    const cb = () => {};
    client.on(moduleOf(T), cb);
    client.destroy();
    const live = client.on(moduleOf(T), cb);
    client.off(moduleOf(T), cb);
    expect(live.active).toBe(false);
    expect(client.listenerCount()).toBe(0);
  });

  it("destroy() detaches on()'s listener from the signal", () => {
    const client = quiet();
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    client.on(moduleOf(T), () => {}, { signal: controller.signal });
    client.destroy();

    // One listener is the poller's, the other the client's.
    expect(remove).toHaveBeenCalledTimes(2);
  });
});
