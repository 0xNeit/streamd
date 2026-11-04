import { describe, it, expect, vi, beforeEach, afterEach } from "bun:test";
import {
  advanceTimersByTimeAsync,
  advanceTimersToNextTimerAsync,
  captureMicrotaskThrows,
  mocked,
} from "./timers.js";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "../src/client.js";
import { involvesAddress } from "../src/filter.js";
import type { EventAdapter, EventPage, StreamdEvent } from "../src/types.js";
import { StreamdError } from "../src/errors.js";

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
    const logs = await client.getLogs(
      { kind: "module", eventType: "0x1::test::Event" },
      { fromVersion: 50n, limit: 10 },
    );

    expect(logs).toEqual(mockEvents);
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

  it("getLogs pages past windows with no matches", async () => {
    const adapter = adapterOf([
      { events: [], nextCursor: "10", hasMore: true },
      { events: [], nextCursor: "20", hasMore: true },
      { events: [makeEvent(25)], nextCursor: "30", hasMore: false },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const logs = await client.getLogs({
      kind: "module",
      eventType: "0x1::test::Event",
    });

    expect(logs).toHaveLength(1);
    expect(logs[0].version).toBe(25n);
    expect(adapter.fetchEvents).toHaveBeenCalledTimes(3);

    client.destroy();
  });

  it("getLogs honours the overall limit across pages", async () => {
    const adapter = adapterOf([
      { events: [makeEvent(1), makeEvent(2)], nextCursor: "3", hasMore: true },
      { events: [makeEvent(3), makeEvent(4)], nextCursor: "5", hasMore: true },
    ]);

    const client = new StreamdClient({ aptos: makeAptos(), adapter });
    const logs = await client.getLogs(
      { kind: "module", eventType: "0x1::test::Event" },
      { limit: 3 },
    );

    expect(logs.map((l) => l.version)).toEqual([1n, 2n, 3n]);
    expect(mocked(adapter.fetchEvents).mock.calls[1]![1].limit).toBe(1);

    client.destroy();
  });

  it("getLogs applies `where` and counts limit after filtering", async () => {
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
    const logs = await client.getLogs(
      { kind: "module", eventType: "0x1::test::Event" },
      { where: involvesAddress("0x1") },
    );

    // Two of the four match; the walk continued past a page that was
    // partially filtered rather than stopping short.
    expect(logs.map((l) => l.version)).toEqual([2n, 4n]);

    client.destroy();
  });

  it("does not forward `where` to the adapter", async () => {
    // Predicates are functions and cannot be pushed to a remote source.
    const adapter = adapterOf([]);
    const client = new StreamdClient({ aptos: makeAptos(), adapter });

    await client.getLogs(
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

  it("maxPages bounds a walk whose predicate matches nothing", async () => {
    const endless: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [makeEvent(1)],
        nextCursor: String(Math.random()),
        hasMore: true, // never satisfied
      })),
    };

    const client = new StreamdClient({ aptos: makeAptos(), adapter: endless });
    const logs = await client.getLogs(
      { kind: "module", eventType: "0x1::test::Event" },
      { limit: 100, maxPages: 3, where: () => false },
    );

    expect(logs).toHaveLength(0);
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
    const logs = await client.getLogs({
      kind: "account",
      accountAddress: "0x1",
      eventTypes: ["0x1::test::Event"],
    });

    expect(logs).toHaveLength(1);
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
});
