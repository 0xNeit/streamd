import { describe, it, expect, spyOn } from "bun:test";
import { descriptorKey, fetchKey, raceAbort } from "../src/utils.js";
import { StreamdClient } from "../src/client.js";
import { SharedFetches } from "../src/shared.js";
import { CachingAdapter } from "../src/adapters/caching.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { recordingAdapter, replayAdapter } from "../src/adapters/recording.js";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "../src/types.js";

const ev = (v: number): StreamdEvent => ({
  type: "0x1::m::E",
  data: {},
  version: BigInt(v),
  sequenceNumber: 0n,
  eventIndex: 0,
});

const valid: EventDescriptor = { kind: "handle", accountAddress: "0x1", creationNumber: 5n };
const invalid = {
  ...valid,
  eventHandleStruct: "0x1::a::B",
  fieldName: "f",
} as unknown as EventDescriptor;

const LONG_0X1 = `0x${"0".repeat(63)}1`;

describe("descriptorKey", () => {
  it("keeps the key of valid handle descriptors", () => {
    expect(descriptorKey(valid)).toBe(JSON.stringify(["handle", LONG_0X1, "5"]));
    expect(
      descriptorKey({
        kind: "handle",
        accountAddress: "0x1",
        eventHandleStruct: "0x1::a::B",
        fieldName: "f",
      }),
    ).toBe(JSON.stringify(["handle", LONG_0X1, "0x1::a::B", "f"]));
  });

  it("gives no request key to a handle descriptor the adapters refuse", () => {
    const refused = [
      invalid,
      { kind: "handle", accountAddress: "0x1", creationNumber: 5 },
      { kind: "handle", accountAddress: "0x1", creationNumber: -1n },
      { kind: "handle", accountAddress: "0x1", creationNumber: 5n, fieldName: "f" },
      { kind: "handle", accountAddress: "not an address", creationNumber: 5n },
      { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::B", fieldName: "" },
      { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::B" },
    ] as unknown as EventDescriptor[];
    for (const d of refused) expect(fetchKey(d, { fromVersion: 0n })).toBeUndefined();
  });

  it("still matches listeners on a handle descriptor a custom adapter accepts", () => {
    const named = { kind: "handle", accountAddress: "alice.apt", creationNumber: 5n } as EventDescriptor;
    const client = new StreamdClient({
      adapter: { fetchEvents: async () => ({ events: [], nextCursor: "0", hasMore: false }) },
    });
    client.on(named, () => {});
    expect(client.listenerCount(named)).toBe(1);
    client.off(named);
    expect(client.listenerCount()).toBe(0);
    client.destroy();
  });

  it("does not let an invalid descriptor's shared fetch fail a valid one", async () => {
    const memory = new MemoryAdapter([ev(1), ev(2)]);
    const delayed: EventAdapter = {
      fetchEvents: async (d, o) => {
        await new Promise((r) => setTimeout(r, 5));
        return memory.fetchEvents(d, o);
      },
    };
    const view = new SharedFetches(delayed).view(1000);
    const [bad, good] = await Promise.allSettled([
      view.fetchEvents(invalid, { fromVersion: 0n }),
      view.fetchEvents(valid, { fromVersion: 0n }),
    ]);
    expect(bad.status).toBe("rejected");
    expect(good.status === "fulfilled" && good.value.events.length).toBe(2);
  });

  it("does not serve an invalid descriptor a cached page", async () => {
    const memory = new MemoryAdapter([ev(1), ev(2)]);
    const cache = new CachingAdapter({
      fetchEvents: async (d, o) => ({ ...(await memory.fetchEvents(d, o)), final: true }),
    });
    expect((await cache.fetchEvents(valid, { fromVersion: 0n })).events).toHaveLength(2);
    await expect(cache.fetchEvents(invalid, { fromVersion: 0n })).rejects.toThrow("not both");
  });

  it("does not replay a valid descriptor's page to an invalid one", async () => {
    const recorder = recordingAdapter(new MemoryAdapter([ev(1), ev(2)]));
    await recorder.fetchEvents(valid, { fromVersion: 0n });
    const replay = replayAdapter(recorder.toJSON());
    expect((await replay.fetchEvents(valid, { fromVersion: 0n })).events).toHaveLength(2);
    await expect(replay.fetchEvents(invalid, { fromVersion: 0n })).rejects.toThrow(
      "No recorded fetchEvents call",
    );
  });
});

describe("raceAbort", () => {
  it("rejects with the reason of an already-aborted signal and absorbs the inner rejection", async () => {
    const unhandled: unknown[] = [];
    const record = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", record);
    try {
      const controller = new AbortController();
      controller.abort(new Error("stopped"));
      await expect(raceAbort(Promise.reject(new Error("inner")), controller.signal)).rejects.toThrow(
        "stopped",
      );
      await new Promise((r) => setTimeout(r, 0));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", record);
    }
  });

  it("rejects with the promise's error under a live signal and detaches its listener", async () => {
    const controller = new AbortController();
    const remove = spyOn(controller.signal, "removeEventListener");
    await expect(raceAbort(Promise.reject(new Error("fetch failed")), controller.signal)).rejects.toThrow(
      "fetch failed",
    );
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("keeps the abort reason when the promise rejects after the signal aborted", async () => {
    const controller = new AbortController();
    let fail!: (err: Error) => void;
    const raced = raceAbort(new Promise((_, reject) => (fail = reject)), controller.signal);
    controller.abort(new Error("stopped"));
    fail(new Error("fetch failed"));
    await expect(raced).rejects.toThrow("stopped");
  });
});

describe("descriptorKey for resource descriptors", () => {
  const TYPE = "0x1::account::Account";

  it("is equal across address spellings and distinct by type", () => {
    const short = descriptorKey({ kind: "resource", accountAddress: "0x1", resourceType: TYPE });
    expect(short).toBe(JSON.stringify(["resource", LONG_0X1, TYPE]));
    expect(descriptorKey({ kind: "resource", accountAddress: LONG_0X1, resourceType: TYPE })).toBe(short);
    expect(
      descriptorKey({ kind: "resource", accountAddress: "0x1", resourceType: "0x1::coin::CoinInfo" }),
    ).not.toBe(short);
  });

  it("matches a listener by value", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter() });
    client.on({ kind: "resource", accountAddress: "0x1", resourceType: TYPE }, () => {});
    expect(client.listenerCount({ kind: "resource", accountAddress: LONG_0X1, resourceType: TYPE })).toBe(1);
    client.destroy();
  });
});
