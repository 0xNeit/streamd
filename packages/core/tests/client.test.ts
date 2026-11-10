import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import {
  advanceTimersByTimeAsync,
  advanceTimersToNextTimerAsync,
  captureMicrotaskThrows,
  mocked,
} from "./timers.js";
import { AccountAddress, Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { FallbackAdapter } from "../src/adapters/fallback.js";
import { withHooks } from "../src/adapters/hooks.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { RoutingAdapter } from "../src/adapters/routing.js";
import { StreamdClient } from "../src/client.js";
import { every, fromFunction, fromSender, involvesAddress, not, ofType, whereField } from "../src/filter.js";
import { ownedBy } from "../src/ownership.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  ListenerDefaults,
  ModuleEventDescriptor,
  StreamdEvent,
  Transfer,
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

  it("getEvents through withHooks reports each page fetch once, and keeps relative starts working", async () => {
    const requests: unknown[] = [];
    const responses: number[] = [];
    const adapter = withHooks(
      new MemoryAdapter(Array.from({ length: 30 }, (_, i) => makeEvent(i + 1))),
      {
        onRequest: (request) => requests.push(request.options.cursor),
        onResponse: (response) => responses.push(response.page!.events.length),
      },
    );
    const client = new StreamdClient({ adapter });
    const desc = { kind: "module", eventType: "0x1::test::Event" } as const;

    const events = await client.getEvents(desc);
    expect(events).toHaveLength(30);
    expect(requests).toHaveLength(2);
    expect(responses).toEqual([25, 5]);

    const recent = await client.getEvents(desc, { fromVersion: -2n });
    expect(recent.map((e) => e.version)).toEqual([28n, 29n, 30n]);

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

  it("once() reports a throwing onCaughtUp and still resolves with the next match", async () => {
    const pages: EventPage[] = [
      { events: [], nextCursor: "1", hasMore: false },
      { events: [{ type: "0x1::test::Event", data: {}, version: 1n, sequenceNumber: 0n, eventIndex: 0 }], nextCursor: "2", hasMore: false },
    ];
    let i = 0;
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => pages[i++] ?? { events: [], nextCursor: "2", hasMore: false }),
    };
    const onError = vi.fn();
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      {
        pollInterval: 100,
        onError,
        onCaughtUp: () => {
          throw new Error("caught-up bug");
        },
      },
    );
    let outcome: unknown = "pending";
    promise.then(
      (e) => (outcome = e),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(100);
    expect((outcome as StreamdEvent).version).toBe(1n);
    expect(onError).toHaveBeenCalledTimes(1);
    expect((onError.mock.calls[0]![0] as Error).message).toBe("onCaughtUp threw");
    client.destroy();
  });

  it("once() skips an event that fails decode and resolves with the next one", async () => {
    const bad: StreamdEvent = { type: "0x1::test::Event", data: { amount: "x" }, version: 1n, sequenceNumber: 0n, eventIndex: 0 };
    const good: StreamdEvent = { type: "0x1::test::Event", data: { amount: "5" }, version: 2n, sequenceNumber: 0n, eventIndex: 0 };
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({ events: [bad, good], nextCursor: "3", hasMore: false })),
    };
    const onError = vi.fn();
    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const promise = client.once(
      { kind: "module", eventType: "0x1::test::Event" },
      { pollInterval: 100, decode: ["amount"], onError },
    );
    let outcome: unknown = "pending";
    promise.then(
      (e) => (outcome = e),
      (err) => (outcome = err),
    );

    await advanceTimersByTimeAsync(0);
    expect((outcome as StreamdEvent).version).toBe(2n);
    expect((outcome as StreamdEvent).data).toEqual({ amount: 5n });
    expect(onError).toHaveBeenCalledTimes(1);
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

  it("filters a backlog by transaction sender with fromSender", async () => {
    const alice = "0x" + "0".repeat(63) + "a";
    const client = new StreamdClient({
      adapter: new MemoryAdapter([
        { ...makeEvent(1), sender: alice },
        { ...makeEvent(2), sender: "0xb" },
        makeEvent(3),
        { ...makeEvent(4), sender: alice },
      ]),
    });
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    const events = await client.getEvents(descriptor, {
      fromVersion: 0n,
      where: fromSender("0xa"),
    });
    expect(events.map((e) => e.version)).toEqual([1n, 4n]);
    client.destroy();
  });

  it("filters a backlog by entry function with fromFunction", async () => {
    const call = (fn: string) => ({ function: fn, typeArguments: [], arguments: [] });
    const client = new StreamdClient({
      adapter: new MemoryAdapter([
        { ...makeEvent(1), entryFunction: call("0x1::aptos_account::transfer") },
        { ...makeEvent(2), entryFunction: call("0x1::coin::transfer") },
        makeEvent(3),
        { ...makeEvent(4), entryFunction: call("0x" + "0".repeat(63) + "1::aptos_account::transfer") },
      ]),
    });
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

    const events = await client.getEvents(descriptor, {
      fromVersion: 0n,
      where: fromFunction("0x1::aptos_account::transfer"),
    });
    expect(events.map((e) => e.version)).toEqual([1n, 4n]);
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

describe("StreamdClient earliest start", () => {
  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
  const versions = [5, 8, 9, 10].map((v) => makeEvent(v));

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("on() with fromVersion earliest delivers from the oldest available version", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(versions) });
    const seen: bigint[] = [];
    client.on(descriptor, (e) => void seen.push(e.version), { fromVersion: "earliest" });
    await advanceTimersByTimeAsync(0);
    client.destroy();

    expect(seen).toEqual([5n, 8n, 9n, 10n]);
  });

  it("getEvents() with fromVersion earliest reads from the oldest version, in either order", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(versions) });
    const asc = await client.getEvents(descriptor, { fromVersion: "earliest", limit: 2 });
    const desc = await client.getEvents(descriptor, {
      fromVersion: "earliest",
      orderBy: "desc",
      limit: 2,
    });

    expect(asc.map((e) => e.version)).toEqual([5n, 8n]);
    expect(desc.map((e) => e.version)).toEqual([10n, 9n]);
  });
});

describe("StreamdClient block range", () => {
  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("getEvents() with fromBlock and toBlock returns only that range, through the limiter", async () => {
    const memory = new MemoryAdapter([5, 10, 15, 20, 25].map((v) => makeEvent(v)));
    // One block per ten versions: block n is versions 10n..10n+9.
    const getBlock = vi.fn(async (_d: unknown, height: bigint) => ({
      height,
      firstVersion: height * 10n,
      lastVersion: height * 10n + 9n,
      timestamp: 0n,
    }));
    const client = new StreamdClient({
      adapter: { fetchEvents: (d, o) => memory.fetchEvents(d, o), getBlock },
      maxConcurrentRequests: 1,
    });

    const events = await client.getEvents(descriptor, { fromBlock: 1n, toBlock: 1n });
    expect(events.map((e) => e.version)).toEqual([10n, 15n]);
    expect(getBlock).toHaveBeenCalledTimes(2);
  });
});

describe("StreamdClient fromTime queries", () => {
  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("getEvents() with fromTime starts at the first block stamped at or after it", async () => {
    const memory = new MemoryAdapter([5, 10, 15, 20, 25].map((v) => makeEvent(v)));
    // Block n is versions 10n..10n+9, stamped n * 1000.
    const getBlock = vi.fn(async (_d: unknown, height: bigint) => ({
      height,
      firstVersion: height * 10n,
      lastVersion: height * 10n + 9n,
      timestamp: height * 1000n,
    }));
    const client = new StreamdClient({
      adapter: {
        fetchEvents: (d, o) => memory.fetchEvents(d, o),
        getBlock,
        getBlockHeights: async () => ({ oldest: 0n, newest: 2n }),
      },
      maxConcurrentRequests: 1,
    });

    const events = await client.getEvents(descriptor, { fromTime: 1500n });
    expect(events.map((e) => e.version)).toEqual([20n, 25n]);
  });

  it("on() delivers from the resolved block, and rejects fromTime with fromVersion", async () => {
    const memory = new MemoryAdapter([5, 10, 15, 20, 25].map((v) => makeEvent(v)));
    const client = new StreamdClient({
      adapter: {
        fetchEvents: (d, o) => memory.fetchEvents(d, o),
        getBlock: async (_d, height) => ({
          height,
          firstVersion: height * 10n,
          lastVersion: height * 10n + 9n,
          timestamp: height * 1000n,
        }),
        getBlockHeights: async () => ({ oldest: 0n, newest: 2n }),
      },
    });

    expect(() => client.on(descriptor, () => {}, { fromTime: 1n, fromVersion: 0n })).toThrow(
      "fromTime or fromVersion",
    );
    const first = await client.once(descriptor, { fromTime: 1500n });
    expect(first.version).toBe(20n);
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

describe("StreamdClient query retries", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("getEvents() with retries recovers from a failed page", async () => {
    const memory = new MemoryAdapter(Array.from({ length: 30 }, (_, i) => makeEvent(i + 1)));
    let failures = 1;
    const adapter: EventAdapter = {
      fetchEvents: (d, o) =>
        o.cursor !== undefined && failures-- > 0
          ? Promise.reject(new StreamdNetworkError("node down", 503))
          : memory.fetchEvents(d, o),
    };
    const client = new StreamdClient({ adapter });
    const query = client.getEvents(descriptor, { retries: 1 });
    await advanceTimersByTimeAsync(1010);

    expect((await query).map((e) => e.version)).toEqual(
      Array.from({ length: 30 }, (_, i) => BigInt(i + 1)),
    );
  });

  it("a query waiting to retry does not hold a maxConcurrentRequests slot", async () => {
    const calls: string[] = [];
    const adapter: EventAdapter = {
      fetchEvents: async (d) => {
        const type = (d as ModuleEventDescriptor).eventType as string;
        calls.push(type);
        if (type.endsWith("Flaky") && calls.length === 1) {
          throw new StreamdNetworkError("node down", 503);
        }
        return { events: [], nextCursor: "c", hasMore: false };
      },
    };
    const client = new StreamdClient({ adapter, maxConcurrentRequests: 1 });
    const flaky = client.getEvents({ kind: "module", eventType: "0x1::test::Flaky" }, { retries: 1 });
    await advanceTimersByTimeAsync(0);
    await client.getEvents(descriptor);

    expect(calls).toEqual(["0x1::test::Flaky", "0x1::test::Event"]);
    await advanceTimersByTimeAsync(1010);
    expect(await flaky).toEqual([]);
    expect(calls.length).toBe(3);
  });
});

describe("maxPollInterval", () => {
  it("applies from client defaults, and a call's own value wins", async () => {
    vi.useFakeTimers();
    try {
      const at: number[] = [];
      const adapter: EventAdapter = {
        fetchEvents: async () => {
          at.push(Date.now());
          return { events: [], nextCursor: String(at.length), hasMore: false };
        },
      };
      const client = new StreamdClient({ adapter, defaults: { pollInterval: 100, maxPollInterval: 400 } });
      const descriptor = { kind: "account", accountAddress: "0x1" } as const;
      const sub = client.on(descriptor, () => {}, { fromVersion: 0n });
      await advanceTimersByTimeAsync(1200);
      sub.unsubscribe();
      expect(at.slice(1).map((t, i) => t - at[i]!)).toEqual([100, 200, 400, 400]);

      at.length = 0;
      const own = client.on(descriptor, () => {}, { fromVersion: 0n, maxPollInterval: 100 });
      await advanceTimersByTimeAsync(250);
      own.unsubscribe();
      expect(at.slice(1).map((t, i) => t - at[i]!)).toEqual([100, 100]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects an invalid default at construction", () => {
    expect(() => new StreamdClient({ adapter: new MemoryAdapter(), defaults: { maxPollInterval: -1 } })).toThrow(
      /maxPollInterval/,
    );
  });
});

describe("maxRequestsPerSecond", () => {
  it("rejects anything but a positive finite number", () => {
    for (const bad of [0, -1, Number.NaN, Infinity, "2"]) {
      expect(() => new StreamdClient({ adapter: new MemoryAdapter(), maxRequestsPerSecond: bad as number })).toThrow(
        StreamdError,
      );
    }
    expect(() => new StreamdClient({ adapter: new MemoryAdapter(), maxRequestsPerSecond: 0.5 })).not.toThrow();
  });

  it("spaces a client's adapter calls", async () => {
    const memory = new MemoryAdapter();
    const at: number[] = [];
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => {
        at.push(performance.now());
        return memory.fetchEvents(d, o);
      },
    };
    const client = new StreamdClient({ adapter, maxRequestsPerSecond: 40 });
    const descriptor = { kind: "account", accountAddress: "0x1" } as const;
    await Promise.all([client.getEvents(descriptor), client.getEvents(descriptor), client.getEvents(descriptor)]);
    expect(at).toHaveLength(3);
    expect(at[1]! - at[0]!).toBeGreaterThanOrEqual(24);
    expect(at[2]! - at[1]!).toBeGreaterThanOrEqual(24);
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

  it("does not grow the queue across pause and resume while a fetch hangs", async () => {
    const slow = slowAdapter();
    const client = new StreamdClient({ adapter: slow.adapter, maxConcurrentRequests: 1 });
    client.on(descriptor, () => {});
    await advanceTimersByTimeAsync(0);
    // A relative start queues a head read on every run before any fetch.
    const sub = client.on(descriptor, () => {}, { fromVersion: -10n });
    for (let i = 0; i < 200; i++) {
      await advanceTimersByTimeAsync(0);
      sub.pause();
      sub.resume();
    }
    await advanceTimersByTimeAsync(0);
    expect(client["limiter"]["queue"].length).toBe(1);
    client.destroy();
  });

  it("destroy() rejects a query still queued behind a hung call", async () => {
    const slow = slowAdapter();
    const client = new StreamdClient({ adapter: slow.adapter, maxConcurrentRequests: 1 });
    client.on(descriptor, () => {});
    await advanceTimersByTimeAsync(0);
    let outcome: unknown;
    client.getEvents(descriptor).catch((e) => (outcome = e));
    await advanceTimersByTimeAsync(0);
    client.destroy();
    await advanceTimersByTimeAsync(0);
    expect(outcome).toBeInstanceOf(StreamdError);
    expect(client["limiter"]["queue"].length).toBe(0);
    slow.releaseAll();
    await advanceTimersByTimeAsync(0);
    expect(slow.calls).toEqual(["fetch"]);
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

  it("keys a handle by creation number apart from its struct and field form", () => {
    const client = quiet();
    client.on({ kind: "handle", accountAddress: "0x1", creationNumber: 7n }, () => {});
    client.on(
      { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::S", fieldName: "7" },
      () => {},
    );

    const long = `0x${"0".repeat(63)}1`;
    expect(client.listenerCount({ kind: "handle", accountAddress: long, creationNumber: 7n })).toBe(1);
    expect(client.listenerCount({ kind: "handle", accountAddress: "0x1", creationNumber: 8n })).toBe(0);
    expect(client.listenerCount({ kind: "handle", accountAddress: "0x2", creationNumber: 7n })).toBe(0);
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

describe("iterateEvents", () => {
  it("yields single events, and two interleaved loops share a one-request cap", async () => {
    const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
    const events = [1, 2, 3, 4, 5].map((v) => makeEvent(v));
    const client = new StreamdClient({
      adapter: new MemoryAdapter(events),
      maxConcurrentRequests: 1,
    });
    const a = client.iterateEvents(descriptor, { fromVersion: 2n }, 2);
    const b = client.iterateEvents(descriptor, { orderBy: "desc", limit: 3 }, 2);
    const seenA: bigint[] = [];
    const seenB: bigint[] = [];
    for (let i = 0; i < 4; i++) {
      const [x, y] = await Promise.all([a.next(), b.next()]);
      if (!x.done) seenA.push(x.value.version);
      if (!y.done) seenB.push(y.value.version);
    }

    expect(seenA).toEqual([2n, 3n, 4n, 5n]);
    expect(seenB).toEqual([5n, 4n, 3n]);
    expect((await a.next()).done).toBe(true);
  });
});

describe("client-wide listener defaults", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: T } as const;
  const failing = () => {
    const at: number[] = [];
    const adapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        at.push(Date.now());
        throw new StreamdNetworkError("node 503", 503);
      }),
    };
    return { adapter, at };
  };

  it("applies defaults to on() and lets a call override one field", async () => {
    const { adapter, at } = failing();
    const client = new StreamdClient({
      adapter,
      defaults: { pollInterval: 50, maxBackoff: 50, maxErrors: 2 },
      onError: () => {},
    });
    const sub = client.on(descriptor, () => {});
    await advanceTimersByTimeAsync(500);
    expect(at.length).toBe(2);
    expect(at[1]! - at[0]!).toBe(50);
    expect(sub.active).toBe(false);

    const { adapter: a2, at: at2 } = failing();
    const c2 = new StreamdClient({
      adapter: a2,
      defaults: { pollInterval: 50, maxBackoff: 50, maxErrors: 2 },
      onError: () => {},
    });
    c2.on(descriptor, () => {}, { maxErrors: 4, maxBackoff: undefined });
    await advanceTimersByTimeAsync(500);
    // maxErrors from the call, explicit-undefined maxBackoff still 50 from defaults.
    expect(at2.length).toBe(4);
    expect(at2.slice(1).map((t, i) => t - at2[i]!)).toEqual([50, 50, 50]);
  });

  it("prefers defaults.pollInterval over defaultPollInterval, and a call's over both", async () => {
    const source = new MemoryAdapter([]);
    const fetch = vi.spyOn(source, "fetchEvents");
    const client = new StreamdClient({
      adapter: source,
      defaultPollInterval: 1000,
      defaults: { pollInterval: 100 },
    });
    client.on(descriptor, () => {});
    await advanceTimersByTimeAsync(300);
    expect(fetch.mock.calls.length).toBe(4);
    client.destroy();
    fetch.mockClear();

    client.on(descriptor, () => {}, { pollInterval: 300, fromVersion: undefined });
    await advanceTimersByTimeAsync(300);
    expect(fetch.mock.calls.length).toBe(2);
    client.destroy();

    const legacy = new StreamdClient({ adapter: source, defaultPollInterval: 200 });
    fetch.mockClear();
    legacy.on(descriptor, () => {}, { pollInterval: undefined });
    await advanceTimersByTimeAsync(400);
    expect(fetch.mock.calls.length).toBe(3);
    legacy.destroy();
  });

  it("applies a default decode, replaced by a call's own", async () => {
    const source = { ...makeEvent(10), data: { amount: "5", fee: "1" } };
    const client = new StreamdClient({
      adapter: new MemoryAdapter([source]),
      defaults: { decode: ["amount"] },
    });
    const seen: unknown[] = [];
    client.on(descriptor, (e) => void seen.push(e.data), { fromVersion: 0n });
    client.on(descriptor, (e) => void seen.push(e.data), { fromVersion: 0n, decode: ["fee"] });
    await advanceTimersByTimeAsync(0);
    expect(seen).toEqual([
      { amount: 5n, fee: "1" },
      { amount: "5", fee: 1n },
    ]);
    client.destroy();
  });

  it("applies defaults to onBatch(), once() and stream()", async () => {
    const events = [makeEvent(1), makeEvent(2), makeEvent(3)];
    const client = new StreamdClient({
      adapter: new MemoryAdapter(events),
      defaults: { batchSize: 2, pollInterval: 100 },
    });
    const batches: number[] = [];
    client.onBatch(descriptor, (es) => void batches.push(es.length), { fromVersion: 0n });
    await advanceTimersByTimeAsync(150);
    expect(batches).toEqual([2, 1]);
    client.destroy();

    const { adapter, at } = failing();
    const failingClient = new StreamdClient({
      adapter,
      defaults: { maxErrors: 1 },
      onError: () => {},
    });
    const once = failingClient.once(descriptor).catch((e: unknown) => e);
    await advanceTimersByTimeAsync(0);
    expect(await once).toBeInstanceOf(StreamdPollingError);
    expect(at.length).toBe(1);

    const streamFail = failing();
    const streamClient = new StreamdClient({
      adapter: streamFail.adapter,
      defaults: { maxErrors: 1 },
      onError: () => {},
    });
    const loop = (async () => {
      for await (const _ of streamClient.stream(descriptor)) void _;
    })().catch((e: unknown) => e);
    await advanceTimersByTimeAsync(0);
    expect(await loop).toBeInstanceOf(StreamdPollingError);
    expect(streamFail.at.length).toBe(1);
  });

  it("keeps listener tracking working for subscriptions made with defaults", () => {
    const client = new StreamdClient({
      adapter: new MemoryAdapter([]),
      defaults: { pollInterval: 100 },
    });
    const cb = () => {};
    client.on(descriptor, cb);
    expect(client.listenerCount({ kind: "module", eventType: T })).toBe(1);
    client.off({ kind: "module", eventType: T }, cb);
    expect(client.listenerCount()).toBe(0);
  });

  it("rejects invalid or unsupported defaults at construction", () => {
    const adapter = new MemoryAdapter([]);
    const bad: unknown[] = [
      { maxBackoff: -1 },
      { dedupWindow: 1.5 },
      { decode: [""] },
      { decode: "amount" },
      { fromVersion: 0n },
      { onError: () => {} },
      "fast",
      null,
      [],
    ];
    for (const defaults of bad) {
      expect(
        () => new StreamdClient({ adapter, defaults: defaults as never }),
      ).toThrow(StreamdError);
    }
    expect(() => new StreamdClient({ adapter, defaults: { fromVersion: 0n } as never })).toThrow(
      /Unsupported listener default: fromVersion/,
    );
    expect(() => new StreamdClient({ adapter, defaults: undefined })).not.toThrow();
  });
});

describe("shared polling", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;
  const spied = () => {
    const adapter = new MemoryAdapter([makeEvent(1), makeEvent(2), makeEvent(3)]);
    const fetch = vi.spyOn(adapter, "fetchEvents");
    return { adapter, fetch };
  };
  const opts = { fromVersion: 0n, pollInterval: 100, batchSize: 2 };

  async function runAlone(options: typeof opts): Promise<number> {
    const { adapter, fetch } = spied();
    const client = new StreamdClient({ adapter });
    client.on(descriptor, () => {}, options);
    await advanceTimersByTimeAsync(30);
    await advanceTimersByTimeAsync(970);
    client.destroy();
    return fetch.mock.calls.length;
  }

  it("serves identical subscriptions from one fetch per interval, each receiving every event", async () => {
    const alone = await runAlone(opts);
    const { adapter, fetch } = spied();
    const client = new StreamdClient({ adapter });
    const seen: [string, bigint][] = [];
    client.on(descriptor, (e) => void seen.push(["a", e.version]), opts);
    await advanceTimersByTimeAsync(30);
    client.onBatch(descriptor, (es) => es.forEach((e) => seen.push(["b", e.version])), opts);
    await advanceTimersByTimeAsync(970);
    // The second pays only for its first backfill page, which is not kept.
    expect(fetch.mock.calls.length).toBe(alone + 1);
    expect(seen.filter(([s]) => s === "a").map(([, v]) => v)).toEqual([1n, 2n, 3n]);
    expect(seen.filter(([s]) => s === "b").map(([, v]) => v)).toEqual([1n, 2n, 3n]);
    client.destroy();
  });

  it("fetches separately for a different batchSize or pollInterval", async () => {
    const alone = await runAlone(opts);
    for (const other of [{ batchSize: 3 }, { pollInterval: 90 }]) {
      const { adapter, fetch } = spied();
      const client = new StreamdClient({ adapter });
      client.on(descriptor, () => {}, opts);
      await advanceTimersByTimeAsync(30);
      client.on(descriptor, () => {}, { ...opts, ...other });
      await advanceTimersByTimeAsync(970);
      expect(fetch.mock.calls.length).toBeGreaterThan(alone + 5);
      client.destroy();
    }
  });

  it("never shares a start at the head, then shares once the cursors coincide", async () => {
    const { adapter, fetch } = spied();
    const client = new StreamdClient({ adapter });
    client.on(descriptor, () => {}, { pollInterval: 100 });
    client.on(descriptor, () => {}, { pollInterval: 100 });
    await advanceTimersByTimeAsync(0);
    const first = fetch.mock.calls.filter(([, o]) => o?.cursor === undefined);
    expect(first.length).toBe(2);
    const before = fetch.mock.calls.length;
    await advanceTimersByTimeAsync(1000);
    expect(fetch.mock.calls.length - before).toBe(10);
    client.destroy();
  });

  it("keeps the other subscription polling when one unsubscribes or pauses", async () => {
    const { adapter, fetch } = spied();
    const client = new StreamdClient({ adapter });
    const a = client.on(descriptor, () => {}, opts);
    const b = client.on(descriptor, () => {}, opts);
    await advanceTimersByTimeAsync(100);
    a.unsubscribe();
    let before = fetch.mock.calls.length;
    await advanceTimersByTimeAsync(250);
    expect(b.active).toBe(true);
    expect(fetch.mock.calls.length - before).toBe(3);
    expect(client.listenerCount(descriptor)).toBe(1);

    const c = client.on(descriptor, () => {}, opts);
    await advanceTimersByTimeAsync(0);
    b.pause();
    before = fetch.mock.calls.length;
    await advanceTimersByTimeAsync(250);
    expect(c.active).toBe(true);
    expect(fetch.mock.calls.length - before).toBe(3);
    client.destroy();
  });

  it("gives each subscription its own error budget and StreamdPollingError on a shared failure", async () => {
    const fetchEvents = vi.fn(async () => {
      throw new StreamdNetworkError("node 503", 503);
    });
    const client = new StreamdClient({ adapter: { fetchEvents } });
    const errors: [string, Error][] = [];
    const sub = (name: string) =>
      client.on(descriptor, () => {}, {
        ...opts,
        maxErrors: 2,
        onError: (e) => void errors.push([name, e]),
      });
    const a = sub("a");
    const b = sub("b");
    await advanceTimersByTimeAsync(1000);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
    expect(a.active).toBe(false);
    expect(b.active).toBe(false);
    expect(errors.map(([n, e]) => [n, (e as StreamdPollingError).consecutiveFailures])).toEqual([
      ["a", 1],
      ["b", 1],
      ["a", 2],
      ["b", 2],
    ]);
    expect(client.listenerCount()).toBe(0);
  });
});

describe("disposal", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("stops a subscription and drops it from the listeners at scope exit", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    let outer;
    {
      using sub = client.on(descriptor, () => {});
      outer = sub;
      expect(client.listenerCount()).toBe(1);
    }
    expect(outer.active).toBe(false);
    expect(client.listenerCount()).toBe(0);
    client.destroy();
  });

  it("is a no-op after unsubscribe or a second dispose", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    const sub = client.onBatch(descriptor, () => {});
    sub.unsubscribe();
    sub[Symbol.dispose]();
    sub[Symbol.dispose]();
    expect(sub.active).toBe(false);
    expect(client.listenerCount()).toBe(0);
  });

  it("leaves a subscription sharing its fetches polling", async () => {
    const adapter = new MemoryAdapter([makeEvent(1), makeEvent(2), makeEvent(3)]);
    const fetch = vi.spyOn(adapter, "fetchEvents");
    const client = new StreamdClient({ adapter });
    const opts = { fromVersion: 0n, pollInterval: 100, batchSize: 2 };
    const seen: bigint[] = [];
    const live = client.on(descriptor, (e) => void seen.push(e.version), opts);
    {
      using _ = client.on(descriptor, () => {}, opts);
      await advanceTimersByTimeAsync(30);
    }
    const before = fetch.mock.calls.length;
    await advanceTimersByTimeAsync(500);
    expect(live.active).toBe(true);
    expect(seen).toEqual([1n, 2n, 3n]);
    expect(fetch.mock.calls.length).toBeGreaterThan(before);
    expect(client.listenerCount()).toBe(1);
    client.destroy();
  });

  it("destroys the client at scope exit", () => {
    let a, b;
    {
      using client = new StreamdClient({ adapter: adapterOf([]) });
      a = client.on(descriptor, () => {});
      b = client.on({ kind: "module", eventType: "0x1::test::Other" }, () => {});
    }
    expect(a.active).toBe(false);
    expect(b.active).toBe(false);
  });
});

describe("backfill, then follow", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  /** Serves a growing list in `limit`-sized pages; the cursor is the next unread index. */
  function growing(events: StreamdEvent[]): EventAdapter {
    return {
      fetchEvents: async (_d, options) => {
        const from = Number(options.cursor ?? 0);
        const page = events.slice(from, from + (options.limit ?? 25));
        const next = from + page.length;
        return { events: page, nextCursor: String(next), hasMore: next < events.length };
      },
    };
  }

  it("resumes on() from the query's end cursor with no gap and no repeat", async () => {
    const events = [1, 2, 3, 4, 5].map((v) => makeEvent(v));
    const client = new StreamdClient({ adapter: growing(events) });
    let cursor: string | undefined;
    const history = await client.getEvents(descriptor, { onCursor: (c) => (cursor = c) });
    events.push(makeEvent(6), makeEvent(6, 1), makeEvent(7));

    const live: string[] = [];
    const sub = client.on(descriptor, (e) => live.push(`${e.version}:${e.eventIndex}`), {
      cursor,
    });
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();

    expect(cursor).toBe("5");
    expect(history.map((e) => e.version)).toEqual([1n, 2n, 3n, 4n, 5n]);
    expect(live).toEqual(["6:0", "6:1", "7:0"]);
  });

  it("follows a filtered paged backfill with onBatch", async () => {
    const events = [1, 2, 3, 4, 5].map((v) => makeEvent(v));
    const client = new StreamdClient({ adapter: growing(events) });
    const where = (e: StreamdEvent) => e.version % 2n === 0n;
    let cursor: string | undefined;
    const history: bigint[] = [];
    for await (const page of client.getEventPages(
      descriptor,
      { where, onCursor: (c) => (cursor = c) },
      2,
    )) {
      history.push(...page.map((e) => e.version));
    }
    events.push(makeEvent(6), makeEvent(7), makeEvent(8));

    const live: bigint[][] = [];
    const sub = client.onBatch(descriptor, (b) => live.push(b.map((e) => e.version)), {
      cursor,
      where,
    });
    await advanceTimersByTimeAsync(0);
    sub.unsubscribe();

    expect(history).toEqual([2n, 4n]);
    expect(live).toEqual([[6n, 8n]]);
  });
});

describe("caller-supplied subscription ids", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const descriptor = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("uses the given id on on() and onBatch(), and generates one otherwise", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    expect(client.on(descriptor, () => {}, { id: "deposits" }).id).toBe("deposits");
    expect(client.onBatch(descriptor, () => {}, { id: "batches" }).id).toBe("batches");
    expect(client.on(descriptor, () => {}).id).toMatch(/^streamd_\d+_\d+$/);
    client.destroy();
  });

  it("refuses an empty or non-string id", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    expect(() => client.on(descriptor, () => {}, { id: "" })).toThrow(StreamdError);
    expect(() => client.on(descriptor, () => {}, { id: "" })).toThrow('Invalid id: ""');
    expect(() =>
      client.on(descriptor, () => {}, { id: 7 as unknown as string }),
    ).toThrow(/Invalid id: 7/);
    expect(client.listenerCount()).toBe(0);
  });

  it("refuses an id a live subscription holds, leaving that one polling", async () => {
    const adapter = adapterOf([]);
    const client = new StreamdClient({ adapter });
    const first = client.on(descriptor, () => {}, { id: "deposits", pollInterval: 100 });
    expect(() => client.onBatch(descriptor, () => {}, { id: "deposits" })).toThrow(
      /already in use on this client: deposits/,
    );
    expect(client.listenerCount()).toBe(1);
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    expect(first.active).toBe(true);
    expect(mocked(adapter.fetchEvents).mock.calls.length).toBeGreaterThan(1);
    client.destroy();
  });

  it("frees the id when the subscription ends, however it ends", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    const reuse = () => client.on(descriptor, () => {}, { id: "x" });

    reuse().unsubscribe();
    const second = reuse();
    second[Symbol.dispose]();
    const viaOff = reuse();
    client.off(descriptor);
    expect(viaOff.active).toBe(false);
    const controller = new AbortController();
    client.on(descriptor, () => {}, { id: "x", signal: controller.signal });
    controller.abort();
    reuse();
    client.destroy();
    expect(reuse().id).toBe("x");
    client.destroy();
  });

  it("frees the id once polling gives up after maxErrors", async () => {
    const client = new StreamdClient({
      adapter: {
        fetchEvents: vi.fn(async () => {
          throw new Error("network down");
        }),
      },
    });
    const sub = client.on(descriptor, () => {}, {
      id: "x",
      pollInterval: 100,
      maxErrors: 2,
      onError: () => {},
    });
    await advanceTimersByTimeAsync(0);
    await advanceTimersByTimeAsync(200);
    expect(sub.active).toBe(false);
    expect(client.on(descriptor, () => {}, { id: "x" }).id).toBe("x");
    client.destroy();
  });

  it("does not let an ended subscription's unsubscribe() drop the one that reused its id", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    const controller = new AbortController();
    const old = client.on(descriptor, () => {}, { id: "x", signal: controller.signal });
    controller.abort();
    const current = client.on(descriptor, () => {}, { id: "x" });

    old.unsubscribe();
    expect(client.listenerCount()).toBe(1);
    expect(() => client.on(descriptor, () => {}, { id: "x" })).toThrow(/already in use/);

    client.destroy();
    expect(current.active).toBe(false);
  });

  it("gives a never-polling subscription the id without holding it", () => {
    const client = new StreamdClient({ adapter: adapterOf([]) });
    const sub = client.on(descriptor, () => {}, { id: "x", signal: AbortSignal.abort() });
    expect(sub.id).toBe("x");
    expect(sub.active).toBe(false);
    expect(client.on(descriptor, () => {}, { id: "x" }).id).toBe("x");
    client.destroy();
  });

  it("refuses id as a client-wide default", () => {
    expect(
      () =>
        new StreamdClient({
          adapter: adapterOf([]),
          defaults: { id: "x" } as unknown as ListenerDefaults,
        }),
    ).toThrow(/Unsupported listener default: id/);
  });
});

describe("async where on listeners", () => {
  const type = "0x1::test::Ping";
  const pings: EventDescriptor = { kind: "module", eventType: type };
  const ping = (version: number): StreamdEvent => ({
    type,
    data: {},
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  });
  const seeded = () => new MemoryAdapter([1, 2, 3, 4].map(ping));
  const even = async (e: StreamdEvent) => e.version % 2n === 0n;
  const opts = { fromVersion: 0n, pollInterval: 1 };
  const until = async (done: () => boolean) => {
    for (let i = 0; i < 200 && !done(); i++) await new Promise((r) => setTimeout(r, 2));
    expect(done()).toBe(true);
  };

  it("delivers matches in order through on, onBatch, once, stream and reduce", async () => {
    const client = new StreamdClient({ adapter: seeded() });
    let inFlight = 0;
    let most = 0;
    const slowEven = async (e: StreamdEvent) => {
      inFlight++;
      most = Math.max(most, inFlight);
      await new Promise((r) => setTimeout(r, Number(5n - e.version)));
      inFlight--;
      return e.version % 2n === 0n;
    };
    const seen: bigint[] = [];
    const sub = client.on(pings, (e) => void seen.push(e.version), { ...opts, where: slowEven });
    const batches: bigint[][] = [];
    const batchSub = client.onBatch(pings, (es) => void batches.push(es.map((e) => e.version)), {
      ...opts,
      where: even,
    });
    await until(() => seen.length === 2 && batches.length === 1);
    sub.unsubscribe();
    batchSub.unsubscribe();
    expect(seen).toEqual([2n, 4n]);
    expect(batches).toEqual([[2n, 4n]]);
    expect(most).toBe(1);

    expect((await client.once(pings, { ...opts, where: even })).version).toBe(2n);
    for await (const e of client.stream(pings, { ...opts, where: even })) {
      expect(e.version).toBe(2n);
      break;
    }
    const r = client.reduce(pings, (n: bigint, e) => n + e.version, 0n, { ...opts, where: even });
    expect(await r.ready).toBe(6n);
    r.unsubscribe();
  });

  it("fails the poll on a rejection and tests the event again on the retry", async () => {
    let calls = 0;
    const tested: bigint[] = [];
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter: seeded() }).on(pings, (e) => void seen.push(e.version), {
      ...opts,
      onError: (err) => errors.push(err),
      where: async (e) => {
        tested.push(e.version);
        if (++calls === 3) throw new StreamdNetworkError("lookup flaked", 503);
        return true;
      },
    });
    await until(() => seen.length === 4);
    expect(sub.active).toBe(true);
    sub.unsubscribe();
    expect(seen).toEqual([1n, 2n, 3n, 4n]);
    expect(tested).toEqual([1n, 2n, 3n, 3n, 4n]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(StreamdPollingError);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(StreamdNetworkError);
  });

  it("delivers a partial batch before a rejection fails the poll", async () => {
    let calls = 0;
    const batches: bigint[][] = [];
    const sub = new StreamdClient({ adapter: seeded() }).onBatch(
      pings,
      (es) => void batches.push(es.map((e) => e.version)),
      {
        ...opts,
        onError: () => {},
        where: async () => {
          if (++calls === 3) throw new Error("lookup flaked");
          return true;
        },
      },
    );
    await until(() => batches.flat().length === 4);
    sub.unsubscribe();
    expect(batches).toEqual([[1n, 2n], [3n, 4n]]);
  });

  it("stops at once on a StreamdError rejection, which once() rejects with", async () => {
    const refused = new StreamdError("not resolvable");
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter: seeded() });
    const sub = client.on(pings, () => {}, {
      ...opts,
      onError: (err) => errors.push(err),
      where: () => Promise.reject(refused),
    });
    await until(() => !sub.active);
    expect(errors).toHaveLength(1);
    expect((errors[0] as StreamdError).cause).toBe(refused);

    const failed = client.once(pings, { ...opts, where: () => Promise.reject(refused) });
    await expect(failed).rejects.toBeInstanceOf(StreamdPollingError);
  });

  it("keeps a synchronous throw the caller's: reported, skipped, not counted", async () => {
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter: seeded() }).on(pings, (e) => void seen.push(e.version), {
      ...opts,
      onError: (err) => errors.push(err),
      where: (e) => {
        if (e.version === 1n) throw new Error("predicate bug");
        return Promise.resolve(true);
      },
    });
    await until(() => seen.length === 3);
    sub.unsubscribe();
    expect(seen).toEqual([2n, 3n, 4n]);
    expect(errors).toHaveLength(1);
    expect(sub.status.consecutiveErrors).toBe(0);
  });

  it("abandons a pending predicate on pause and on stop", async () => {
    let calls = 0;
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter: seeded() }).on(pings, (e) => void seen.push(e.version), {
      ...opts,
      where: () => (++calls === 1 ? new Promise<boolean>(() => {}) : Promise.resolve(true)),
    });
    await until(() => calls === 1);
    sub.pause();
    sub.resume();
    await until(() => seen.length === 4);
    expect(seen).toEqual([1n, 2n, 3n, 4n]);
    sub.unsubscribe();

    let resolve!: (matched: boolean) => void;
    const delivered: bigint[] = [];
    const stopped = new StreamdClient({ adapter: seeded() }).on(
      pings,
      (e) => void delivered.push(e.version),
      { ...opts, where: () => new Promise<boolean>((r) => (resolve = r)) },
    );
    await until(() => resolve !== undefined);
    stopped.unsubscribe();
    resolve(true);
    await new Promise((r) => setTimeout(r, 10));
    expect(delivered).toEqual([]);
  });
});

describe("ownedBy through the client", () => {
  const DEPOSIT = "0x1::fungible_asset::Deposit";
  const deposits: EventDescriptor = { kind: "module", eventType: DEPOSIT };
  const wallet = `0x${"b0b".padStart(64, "0")}`;
  const store = (n: number) => `0x${String(n).padStart(64, "0")}`;
  const deposit = (version: number, type = DEPOSIT): StreamdEvent => ({
    type,
    data: { store: store(version), amount: "1" },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  });
  const ownsStores = (owned: number[], fail?: () => boolean) =>
    ({
      viewJson: async ({ payload }: { payload: { functionArguments: string[] } }) => {
        if (fail?.()) throw Object.assign(new Error("unavailable"), { status: 503 });
        return [owned.map(store).includes(payload.functionArguments[0]!)];
      },
    }) as unknown as Aptos;

  it("keeps only events on stores the wallet owns in a query", async () => {
    const adapter = new MemoryAdapter([deposit(1), deposit(2), deposit(3, "0x1::other::Deposit"), deposit(4)]);
    const events = await new StreamdClient({ adapter }).getEvents(deposits, {
      fromVersion: 0n,
      where: every(ofType(DEPOSIT), ownedBy(ownsStores([2, 3, 4]), wallet)),
    });
    expect(events.map((e) => e.version)).toEqual([2n, 4n]);
  });

  it("retries an event after a failed ownership read and delivers it once", async () => {
    let reads = 0;
    const errors: Error[] = [];
    const seen: bigint[] = [];
    const sub = new StreamdClient({ adapter: new MemoryAdapter([deposit(1), deposit(2)]) }).on(
      deposits,
      (e) => void seen.push(e.version),
      {
        fromVersion: 0n,
        pollInterval: 1,
        onError: (err) => errors.push(err),
        where: ownedBy(ownsStores([1, 2], () => ++reads === 2), wallet),
      },
    );
    for (let i = 0; i < 200 && seen.length < 2; i++) await new Promise((r) => setTimeout(r, 2));
    sub.unsubscribe();
    expect(seen).toEqual([1n, 2n]);
    expect(errors).toHaveLength(1);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(StreamdNetworkError);
  });
});

describe("transfers through the client", () => {
  const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
  const [ALICE, BOB, USDC] = [long("a11ce"), long("b0b"), long("bae2")];
  const owners: Record<string, string> = { [long("1")]: ALICE, [long("2")]: BOB, [long("3")]: BOB };
  const fa = (version: number, eventIndex: number, side: string, store: number, amount = "5"): StreamdEvent => ({
    type: `0x1::fungible_asset::${side}`,
    data: { store: long(String(store)), amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  });
  const events = [fa(1, 0, "Withdraw", 1), fa(1, 1, "Deposit", 2), fa(2, 0, "Deposit", 3, "8")];
  const withViews = (fail?: () => boolean): EventAdapter => {
    const memory = new MemoryAdapter(events);
    return {
      fetchEvents: (d, o) => memory.fetchEvents(d, o),
      view: async (request, _version) => {
        if (fail?.()) throw new StreamdNetworkError("unavailable", 503);
        const store = String(request.functionArguments![0]);
        if (request.function === "0x1::object::owner") return [owners[store]];
        return [{ inner: store === long("3") ? long("a") : USDC }];
      },
    };
  };

  it("returns paired records from getTransfers, narrowed by asset", async () => {
    const client = new StreamdClient({ adapter: withViews() });
    const all = await client.getTransfers({ fromVersion: 0n });
    expect(all.map((e) => e.data)).toEqual([
      { asset: USDC, amount: 5n, from: ALICE, to: BOB },
      { asset: long("a"), amount: 8n, from: undefined, to: BOB },
    ]);
    const apt = await client.getTransfers({ fromVersion: 0n, asset: "0xa" });
    expect(apt.map((e) => e.version)).toEqual([2n]);
    await expect(client.getTransfers({ asset: "nope" })).rejects.toBeInstanceOf(StreamdError);
  });

  it("returns coin, token object and legacy token records together in identity order", async () => {
    const NFT = long("deed");
    const LEGACY_ID = { token_data_id: { creator: ALICE, collection: "Apes", name: "Ape #1" }, property_version: "0" };
    const memory = new MemoryAdapter([
      ...events,
      { type: "0x1::object::Transfer", data: { object: NFT, from: ALICE, to: BOB }, version: 3n, sequenceNumber: 0n, eventIndex: 0 },
      { type: "0x3::token::TokenWithdraw", data: { account: BOB, id: LEGACY_ID, amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 1 },
      { type: "0x3::token::TokenDeposit", data: { account: ALICE, id: LEGACY_ID, amount: "1" }, version: 3n, sequenceNumber: 0n, eventIndex: 2 },
    ]);
    const views = withViews();
    const client = new StreamdClient({
      adapter: {
        fetchEvents: (d, o) => memory.fetchEvents(d, o),
        view: async (request, version) =>
          request.function === "0x4::token::creator" ? [ALICE] : views.view!(request, version),
      },
    });
    const records = await client.getTransfers({ fromVersion: 0n });
    expect(records.map((e) => [`${e.version}:${e.eventIndex}`, e.data])).toEqual([
      ["1:1", { asset: USDC, amount: 5n, from: ALICE, to: BOB }],
      ["2:0", { asset: long("a"), amount: 8n, from: undefined, to: BOB }],
      ["3:0", { asset: NFT, amount: 1n, from: ALICE, to: BOB }],
      ["3:2", { asset: `${ALICE}::Apes::Ape #1::0`, amount: 1n, from: BOB, to: ALICE }],
    ]);
  });

  it("follows live, retrying a poll whose view failed and delivering each transfer once", async () => {
    let reads = 0;
    const errors: Error[] = [];
    const seen: Transfer[] = [];
    const client = new StreamdClient({ adapter: withViews(() => ++reads === 1) });
    const sub = client.transfers((e) => void seen.push(e.data), {
      fromVersion: 0n,
      pollInterval: 1,
      onError: (err) => errors.push(err),
    });
    expect(client.listenerCount()).toBe(0);
    for (let i = 0; i < 200 && seen.length < 2; i++) await new Promise((r) => setTimeout(r, 2));
    expect(seen.map((t) => t.amount)).toEqual([5n, 8n]);
    expect(errors).toHaveLength(1);
    expect((errors[0] as StreamdError).cause).toBeInstanceOf(StreamdNetworkError);
    expect(sub.status.consecutiveErrors).toBe(0);
    client.destroy();
    expect(sub.active).toBe(false);
  });

  it("refuses an adapter without view", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    expect(() => client.transfers(() => {})).toThrow(StreamdError);
  });

  describe("account", () => {
    const named = (lookup: () => Promise<AccountAddress | undefined>) => {
      const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET }));
      let lookups = 0;
      aptos.getTargetAddress = () => (lookups++, lookup());
      return { client: new StreamdClient({ adapter: withViews(), aptos }), lookups: () => lookups };
    };
    const alice = AccountAddress.from(ALICE);

    it("keeps one account's records, for an address in any form", async () => {
      const client = new StreamdClient({ adapter: withViews() });
      const sent = await client.getTransfers({ fromVersion: 0n, account: `0x${ALICE.slice(2).toUpperCase()}` });
      expect(sent.map((e) => e.version)).toEqual([1n]);
      const bob = await client.getTransfers({ fromVersion: 0n, account: BOB });
      expect(bob.map((e) => e.version)).toEqual([1n, 2n]);
    });

    it("resolves a name once for a query", async () => {
      const { client, lookups } = named(async () => alice);
      const records = await client.getTransfers({ fromVersion: 0n, account: "alice.apt" });
      expect(records.map((e) => e.version)).toEqual([1n]);
      expect(lookups()).toBe(1);
    });

    it("refuses a malformed value at the call, and a name without aptos", async () => {
      const client = new StreamdClient({ adapter: withViews() });
      expect(() => client.transfers(() => {}, { account: "0xnothex" })).toThrow(StreamdError);
      expect(() => client.transfers(() => {}, { account: "alice.apt" })).toThrow("aptos");
      await expect(client.getTransfers({ account: "alice.apt" })).rejects.toThrow("aptos");
    });

    it("stops a listener on an unregistered name, and retries a failed lookup", async () => {
      const gone = named(async () => undefined);
      const errors: Error[] = [];
      const stopped = gone.client.transfers(() => {}, {
        fromVersion: 0n,
        pollInterval: 1,
        account: "nobody.apt",
        onError: (err) => errors.push(err),
      });
      for (let i = 0; i < 200 && stopped.active; i++) await new Promise((r) => setTimeout(r, 2));
      expect(stopped.active).toBe(false);
      expect(errors[0]!.message).toContain("nobody.apt");

      let calls = 0;
      const flaky = named(async () => {
        if (++calls === 1) throw new TypeError("fetch failed");
        return alice;
      });
      const seen: Transfer[] = [];
      const sub = flaky.client.transfers((e) => void seen.push(e.data), {
        fromVersion: 0n,
        pollInterval: 1,
        maxBackoff: 1,
        account: "alice.apt",
        onError: () => {},
      });
      for (let i = 0; i < 200 && seen.length < 1; i++) await new Promise((r) => setTimeout(r, 2));
      expect(seen.map((t) => t.from)).toEqual([ALICE]);
      expect(flaky.lookups()).toBe(2);
      sub.unsubscribe();
    });
  });
});
