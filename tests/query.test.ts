import { describe, it, expect, vi } from "bun:test";
import { EventQuery } from "../src/query.js";
import type { EventAdapter, EventDescriptor, FetchOptions } from "../src/types.js";

const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::test::Event" };

function event(version: number) {
  return { type: "0x1::test::Event", data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

describe("EventQuery paging", () => {
  it("stops when the adapter returns the cursor it was given", async () => {
    const fetchEvents = vi.fn(async () => {
      // Fail fast rather than hang if the guard is missing.
      if (fetchEvents.mock.calls.length > 10) throw new Error("walk did not stop");
      return { events: [], nextCursor: "x", hasMore: true };
    });
    const logs = await new EventQuery({ fetchEvents }).getLogs(descriptor);

    expect(logs).toEqual([]);
    expect(fetchEvents).toHaveBeenCalledTimes(2);
  });

  it("seeds only the first request from fromVersion and offset", async () => {
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce({ events: [event(5)], nextCursor: "6", hasMore: true })
      .mockResolvedValueOnce({ events: [event(7)], nextCursor: "8", hasMore: false });
    const logs = await new EventQuery({ fetchEvents }).getLogs(descriptor, {
      fromVersion: 5n,
      offset: 2,
    });

    expect(logs.map((e) => e.version)).toEqual([5n, 7n]);
    const [first, second] = fetchEvents.mock.calls.map((c) => c[1] as FetchOptions);
    expect(first).toMatchObject({ fromVersion: 5n, offset: 2, cursor: undefined });
    expect(second).toMatchObject({ cursor: "6" });
    expect(second?.fromVersion).toBeUndefined();
    expect(second?.offset).toBeUndefined();
  });

  it("rejects a pre-aborted getLogs with the signal reason without fetching", async () => {
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>();
    const reason = new Error("already aborted");
    const promise = new EventQuery({ fetchEvents }).getLogs(descriptor, {
      signal: AbortSignal.abort(reason),
    });

    await expect(promise).rejects.toBe(reason);
    expect(fetchEvents).not.toHaveBeenCalled();
  });

  it("rejects during a pending fetch before that fetch resolves, and never passes signal to the adapter", async () => {
    const controller = new AbortController();
    const reason = new Error("abort in flight");
    let resolvePage!: () => void;
    const fetchEvents = vi.fn<EventAdapter["fetchEvents"]>(
      () =>
        new Promise((resolve) => {
          resolvePage = () => resolve({ events: [event(1)], nextCursor: "2", hasMore: false });
        }),
    );

    const promise = new EventQuery({ fetchEvents }).getLogs(descriptor, {
      fromVersion: 1n,
      signal: controller.signal,
    });
    expect(fetchEvents).toHaveBeenCalledTimes(1);
    const passedOptions = fetchEvents.mock.calls[0]![1] as Record<string, unknown>;
    expect("signal" in passedOptions).toBe(false);

    controller.abort(reason);
    await expect(promise).rejects.toBe(reason);

    // Resolving the abandoned fetch afterwards is a no-op.
    resolvePage();
  });

  it("stops fetching further pages when aborted between getLogsIterator pages, and detaches per page", async () => {
    const controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const reason = new Error("stop paging");
    const fetchEvents = vi
      .fn<EventAdapter["fetchEvents"]>()
      .mockResolvedValueOnce({ events: [event(1)], nextCursor: "2", hasMore: true })
      .mockResolvedValueOnce({ events: [event(2)], nextCursor: "3", hasMore: false });

    const it = new EventQuery({ fetchEvents }).getLogsIterator(descriptor, {
      signal: controller.signal,
    });

    const first = await it.next();
    expect(first.value?.map((e) => e.version)).toEqual([1n]);
    expect(remove).toHaveBeenCalledTimes(1);

    controller.abort(reason);
    await expect(it.next()).rejects.toBe(reason);
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(fetchEvents).toHaveBeenCalledTimes(1);
  });
});
