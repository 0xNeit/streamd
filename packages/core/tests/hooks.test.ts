import { describe, it, expect } from "bun:test";
import { withHooks } from "../src/adapters/hooks.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { AdapterHooks, AdapterResponse } from "../src/adapters/hooks.js";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  StreamdEvent,
  TransactionSimulation,
} from "../src/types.js";

const PING = "0x1::test::Ping";
const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };

function ev(version: number): StreamdEvent {
  return {
    type: PING,
    data: { version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

const PAGE: EventPage = { events: [ev(1)], nextCursor: "1", hasMore: false };

describe("withHooks", () => {
  it("calls onRequest, then the adapter, then onResponse with the page, and returns it unchanged", async () => {
    const calls: string[] = [];
    let response: AdapterResponse | undefined;
    const inner: EventAdapter = {
      async fetchEvents() {
        calls.push("fetch");
        return PAGE;
      },
    };
    const options = { limit: 10 };
    const adapter = withHooks(inner, {
      onRequest(request) {
        calls.push("request");
        expect(request).toEqual({ descriptor: moduleDesc, options });
      },
      onResponse(r) {
        calls.push("response");
        response = r;
      },
    });

    const page = await adapter.fetchEvents(moduleDesc, options);

    expect(page).toBe(PAGE);
    expect(calls).toEqual(["request", "fetch", "response"]);
    expect(response!.descriptor).toBe(moduleDesc);
    expect(response!.options).toBe(options);
    expect(response!.page).toBe(PAGE);
    expect(response!.error).toBeUndefined();
    expect(typeof response!.durationMs).toBe("number");
    expect(response!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("reports a failed fetch to onResponse and rethrows the same error", async () => {
    const failure = new StreamdNetworkError("down");
    let response: AdapterResponse | undefined;
    const adapter = withHooks(
      {
        async fetchEvents() {
          throw failure;
        },
      },
      { onResponse: (r) => (response = r) },
    );

    await expect(adapter.fetchEvents(moduleDesc, {})).rejects.toBe(failure);
    expect(response!.error).toBe(failure);
    expect(response!.page).toBeUndefined();
    expect(response!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("rejects the fetch when a hook throws", async () => {
    const boom = new Error("hook");
    let fetched = 0;
    const inner: EventAdapter = {
      async fetchEvents() {
        fetched++;
        return PAGE;
      },
    };

    await expect(
      withHooks(inner, {
        onRequest() {
          throw boom;
        },
      }).fetchEvents(moduleDesc, {}),
    ).rejects.toBe(boom);
    expect(fetched).toBe(0);

    await expect(
      withHooks(inner, {
        onResponse() {
          throw boom;
        },
      }).fetchEvents(moduleDesc, {}),
    ).rejects.toBe(boom);
  });

  it("works with no hooks set", async () => {
    const adapter = withHooks(new MemoryAdapter([ev(1)]), {});
    const page = await adapter.fetchEvents(moduleDesc, {});
    expect(page.events).toEqual([ev(1)]);
  });

  it("reads the hooks once, when wrapping", async () => {
    let requests = 0;
    const hooks: AdapterHooks = { onRequest: () => requests++ };
    const adapter = withHooks(new MemoryAdapter([]), hooks);
    hooks.onRequest = () => {
      throw new Error("replaced");
    };
    await adapter.fetchEvents(moduleDesc, {});
    expect(requests).toBe(1);
  });

  it("delegates getHeadVersion when the wrapped adapter has one, without calling hooks", async () => {
    let hooked = 0;
    const adapter = withHooks(new MemoryAdapter([ev(1), ev(7)]), {
      onRequest: () => hooked++,
      onResponse: () => hooked++,
    });
    expect(await adapter.getHeadVersion!(moduleDesc)).toBe(7n);
    expect(hooked).toBe(0);
  });

  it("has no getHeadVersion when the wrapped adapter lacks one", () => {
    const adapter = withHooks({ fetchEvents: async () => PAGE }, {});
    expect("getHeadVersion" in adapter).toBe(false);
  });

  it("delegates getEarliestVersion when the wrapped adapter has one, without calling hooks", async () => {
    let hooked = 0;
    const adapter = withHooks(new MemoryAdapter([ev(3), ev(7)]), {
      onRequest: () => hooked++,
      onResponse: () => hooked++,
    });
    expect(await adapter.getEarliestVersion!(moduleDesc)).toBe(3n);
    expect(hooked).toBe(0);
  });

  it("has no getEarliestVersion when the wrapped adapter lacks one", () => {
    const adapter = withHooks({ fetchEvents: async () => PAGE }, {});
    expect("getEarliestVersion" in adapter).toBe(false);
  });

  it("delegates getBlock when the wrapped adapter has one, without calling hooks", async () => {
    let hooked = 0;
    const block = { height: 5n, firstVersion: 10n, lastVersion: 12n, timestamp: 1n };
    const adapter = withHooks(
      { fetchEvents: async () => PAGE, getBlock: async (_d, height) => ({ ...block, height }) },
      { onRequest: () => hooked++, onResponse: () => hooked++ },
    );
    expect(await adapter.getBlock!(moduleDesc, 9n)).toEqual({ ...block, height: 9n });
    expect(hooked).toBe(0);
  });

  it("has no getBlock when the wrapped adapter lacks one", () => {
    const adapter = withHooks({ fetchEvents: async () => PAGE }, {});
    expect("getBlock" in adapter).toBe(false);
  });

  it("delegates getBlockHeights when the wrapped adapter has one, without calling hooks", async () => {
    let hooked = 0;
    const adapter = withHooks(
      { fetchEvents: async () => PAGE, getBlockHeights: async () => ({ oldest: 1n, newest: 4n }) },
      { onRequest: () => hooked++, onResponse: () => hooked++ },
    );
    expect(await adapter.getBlockHeights!(moduleDesc)).toEqual({ oldest: 1n, newest: 4n });
    expect(hooked).toBe(0);
  });

  it("has no getBlockHeights when the wrapped adapter lacks one", () => {
    const adapter = withHooks({ fetchEvents: async () => PAGE }, {});
    expect("getBlockHeights" in adapter).toBe(false);
  });

  it("refuses an invalid adapter, hooks object, or hook", () => {
    const inner = new MemoryAdapter([]);
    expect(() => withHooks({} as EventAdapter, {})).toThrow(StreamdError);
    expect(() => withHooks(inner, null as unknown as AdapterHooks)).toThrow(
      StreamdError,
    );
    expect(() =>
      withHooks(inner, { onRequest: "log" } as unknown as AdapterHooks),
    ).toThrow(StreamdError);
    expect(() =>
      withHooks(inner, { onResponse: 1 } as unknown as AdapterHooks),
    ).toThrow(StreamdError);
  });
});

describe("withHooks transaction lookup", () => {
  const hash = `0x${"ef".repeat(32)}`;
  const found = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };

  it("passes getTransactionEvents through unhooked, only when the source has it", async () => {
    let hooked = 0;
    const hooks = { onRequest: () => void hooked++ };
    const seen: unknown[] = [];
    const wrapped = withHooks(
      { fetchEvents: async () => PAGE, getTransactionEvents: async (...args) => (seen.push(args), found) },
      hooks,
    );
    expect(await wrapped.getTransactionEvents!(hash, moduleDesc)).toBe(found);
    expect(seen).toEqual([[hash, moduleDesc]]);
    expect(hooked).toBe(0);
    expect(withHooks({ fetchEvents: async () => PAGE }, hooks).getTransactionEvents).toBeUndefined();
  });
});

describe("withHooks simulation", () => {
  const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };

  it("passes simulateTransactionEvents through unhooked, only when the source has it", async () => {
    let hooked = 0;
    const hooks = { onRequest: () => void hooked++ };
    const seen: unknown[] = [];
    const wrapped = withHooks(
      { fetchEvents: async () => PAGE, simulateTransactionEvents: async (...args) => (seen.push(args), simulated) },
      hooks,
    );
    expect(await wrapped.simulateTransactionEvents!(simulation, moduleDesc)).toBe(simulated);
    expect(seen).toEqual([[simulation, moduleDesc]]);
    expect(hooked).toBe(0);
    expect(withHooks({ fetchEvents: async () => PAGE }, hooks).simulateTransactionEvents).toBeUndefined();
  });
});

describe("withHooks view", () => {
  it("passes view through unhooked, only when the source has it", async () => {
    const calls: unknown[] = [];
    const source = Object.assign(new MemoryAdapter(), {
      view: async (...args: unknown[]) => (calls.push(args), ["1"]),
    });
    const requests: unknown[] = [];
    const wrapped = withHooks(source, { onRequest: (r) => void requests.push(r) });
    const request = { function: "0x1::coin::balance" as const };
    expect(await wrapped.view!(request, 4n)).toEqual(["1"]);
    expect(calls).toEqual([[request, 4n, undefined]]);
    expect(requests).toHaveLength(0);
    expect(withHooks(new MemoryAdapter(), {}).view).toBeUndefined();
  });
});

describe("withHooks readObjects", () => {
  it("passes readObjects through unhooked, only when the source has it", async () => {
    const calls: unknown[] = [];
    const source = Object.assign(new MemoryAdapter(), {
      readObjects: async (...args: unknown[]) => (calls.push(args), [null]),
    });
    const at = { version: 4n, transactionHash: "tx" };
    expect(await withHooks(source, {}).readObjects!({ objects: ["0x1"] }, at)).toEqual([null]);
    expect(calls).toEqual([[{ objects: ["0x1"] }, at, undefined]]);
    expect(withHooks(new MemoryAdapter(), {}).readObjects).toBeUndefined();
  });
});
