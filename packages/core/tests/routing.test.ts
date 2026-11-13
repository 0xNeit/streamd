import { describe, it, expect, vi } from "bun:test";
import { RoutingAdapter } from "../src/adapters/routing.js";
import type { RoutingAdapterRoutes } from "../src/adapters/routing.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  FetchOptions,
  StreamdEvent,
  TransactionSimulation,
} from "../src/types.js";

const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";

const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };
const handleDesc: EventDescriptor = {
  kind: "handle",
  accountAddress: "0x1",
  eventHandleStruct: "0x1::test::Store",
  fieldName: "pings",
};
const accountDesc: EventDescriptor = { kind: "account", accountAddress: "0x1" };

function ev(version: number, type = PING): StreamdEvent {
  return {
    type,
    data: { version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

describe("RoutingAdapter", () => {
  it("routes module, handle, and account descriptors to their configured adapters and forwards options", async () => {
    const moduleAdapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(1)],
        nextCursor: "m-1",
        hasMore: false,
      })),
    };
    const handleAdapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(2)],
        nextCursor: "h-2",
        hasMore: true,
      })),
    };
    const accountAdapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(3)],
        nextCursor: "a-3",
        hasMore: false,
      })),
    };

    const router = new RoutingAdapter({
      module: moduleAdapter,
      handle: handleAdapter,
      account: accountAdapter,
    });

    const opts: FetchOptions = { fromVersion: 10n, limit: 5, orderBy: "asc" };
    const mPage = await router.fetchEvents(moduleDesc, opts);
    expect(mPage).toEqual({
      events: [ev(1)],
      nextCursor: "m-1",
      hasMore: false,
    });
    expect(moduleAdapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(moduleAdapter.fetchEvents).toHaveBeenCalledWith(moduleDesc, opts);
    expect(handleAdapter.fetchEvents).not.toHaveBeenCalled();
    expect(accountAdapter.fetchEvents).not.toHaveBeenCalled();

    const hPage = await router.fetchEvents(handleDesc);
    expect(hPage).toEqual({
      events: [ev(2)],
      nextCursor: "h-2",
      hasMore: true,
    });
    expect(handleAdapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(handleAdapter.fetchEvents).toHaveBeenCalledWith(handleDesc, {});

    const aPage = await router.fetchEvents(accountDesc, { cursor: "42" });
    expect(aPage).toEqual({
      events: [ev(3)],
      nextCursor: "a-3",
      hasMore: false,
    });
    expect(accountAdapter.fetchEvents).toHaveBeenCalledTimes(1);
    expect(accountAdapter.fetchEvents).toHaveBeenCalledWith(accountDesc, {
      cursor: "42",
    });
  });

  it("falls back to the default route for descriptor kinds not explicitly configured", async () => {
    const moduleSource = new MemoryAdapter([ev(10, PING)]);
    const defaultSource = new MemoryAdapter([ev(20, PONG)]);

    const router = new RoutingAdapter({
      module: moduleSource,
      default: defaultSource,
    });

    const modPage = await router.fetchEvents(moduleDesc);
    expect(modPage.events.map((e) => e.version)).toEqual([10n]);

    const handlePage = await router.fetchEvents(handleDesc);
    expect(handlePage.events.map((e) => e.version)).toEqual([20n]);

    const accountPage = await router.fetchEvents(accountDesc);
    expect(accountPage.events.map((e) => e.version)).toEqual([20n]);
  });

  it("copies route references on construction so later mutations of the routes object have no effect", async () => {
    const initial = new MemoryAdapter([ev(1)]);
    const replacement = new MemoryAdapter([ev(99)]);
    const routes: RoutingAdapterRoutes = { module: initial };
    const router = new RoutingAdapter(routes);

    routes.module = replacement;
    routes.default = replacement;

    const page = await router.fetchEvents(moduleDesc);
    expect(page.events.map((e) => e.version)).toEqual([1n]);
  });

  it("rejects unconfigured descriptor kinds with a plain StreamdError, not StreamdNetworkError", async () => {
    const router = new RoutingAdapter({
      module: new MemoryAdapter([ev(1)]),
    });

    const handleErr = await router
      .fetchEvents(handleDesc)
      .catch((e: unknown) => e);
    expect(handleErr).toBeInstanceOf(StreamdError);
    expect(handleErr).not.toBeInstanceOf(StreamdNetworkError);
    expect((handleErr as Error).message).toContain(
      'No adapter configured for descriptor kind "handle"',
    );

    const accountErr = await router
      .fetchEvents(accountDesc)
      .catch((e: unknown) => e);
    expect(accountErr).toBeInstanceOf(StreamdError);
    expect(accountErr).not.toBeInstanceOf(StreamdNetworkError);
    expect((accountErr as Error).message).toContain(
      'No adapter configured for descriptor kind "account"',
    );
  });

  it("rejects unknown or prototype-named descriptor kinds even when a default adapter is set", async () => {
    const defaultAdapter: EventAdapter = {
      fetchEvents: vi.fn(async () => ({ events: [], hasMore: false })),
    };
    const router = new RoutingAdapter({ default: defaultAdapter });

    for (const badKind of ["other", "default", "toString", "constructor", undefined]) {
      const err = await router
        .fetchEvents({ kind: badKind } as unknown as EventDescriptor)
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
      expect((err as Error).message).toContain("Unknown descriptor kind");
    }
    expect(defaultAdapter.fetchEvents).not.toHaveBeenCalled();
  });

  it("validates routes in the constructor and throws StreamdError on empty or invalid routes", () => {
    expect(() => new RoutingAdapter(null as unknown as RoutingAdapterRoutes)).toThrow(
      StreamdError,
    );
    expect(() => new RoutingAdapter({})).toThrow(
      /RoutingAdapter requires at least one route or a default adapter/,
    );
    expect(
      () =>
        new RoutingAdapter({
          module: {} as unknown as EventAdapter,
        }),
    ).toThrow(/RoutingAdapter\.module requires a valid EventAdapter/);
    expect(
      () =>
        new RoutingAdapter({
          handle: null as unknown as EventAdapter,
        }),
    ).toThrow(/RoutingAdapter\.handle requires a valid EventAdapter/);
    expect(
      () =>
        new RoutingAdapter({
          account: "bad" as unknown as EventAdapter,
        }),
    ).toThrow(/RoutingAdapter\.account requires a valid EventAdapter/);
    expect(
      () =>
        new RoutingAdapter({
          default: { fetchEvents: "nope" } as unknown as EventAdapter,
        }),
    ).toThrow(/RoutingAdapter\.default requires a valid EventAdapter/);
  });
});

describe("RoutingAdapter head version", () => {
  it("asks the adapter routed for the descriptor's kind", async () => {
    const router = new RoutingAdapter({
      module: new MemoryAdapter([ev(9)]),
      default: new MemoryAdapter([ev(4)]),
    });

    expect(await router.getHeadVersion!(moduleDesc)).toBe(9n);
    expect(await router.getHeadVersion!(handleDesc)).toBe(4n);
  });

  it("throws StreamdError when the routed adapter cannot report a head", async () => {
    const bare: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
    };
    expect(new RoutingAdapter({ account: bare }).getHeadVersion).toBeUndefined();
    const router = new RoutingAdapter({ account: bare, default: new MemoryAdapter([ev(4)]) });

    const err = await router.getHeadVersion!(accountDesc).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain('"account"');
  });
});

describe("RoutingAdapter block heights", () => {
  const heights = { oldest: 3n, newest: 9n };
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("asks the adapter routed for the descriptor's kind", async () => {
    const withHeights: EventAdapter = { ...bare, getBlockHeights: vi.fn(async () => heights) };
    const router = new RoutingAdapter({ module: withHeights, default: bare });

    expect(await router.getBlockHeights!(moduleDesc)).toBe(heights);
    expect(withHeights.getBlockHeights).toHaveBeenCalledWith(moduleDesc);
  });

  it("throws StreamdError when the routed adapter cannot report block heights", async () => {
    expect(new RoutingAdapter({ account: bare }).getBlockHeights).toBeUndefined();
    const withHeights: EventAdapter = { ...bare, getBlockHeights: async () => heights };
    const router = new RoutingAdapter({ account: bare, default: withHeights });

    const err = await router.getBlockHeights!(accountDesc).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain('"account"');
  });
});

describe("RoutingAdapter earliest version", () => {
  it("asks the adapter routed for the descriptor's kind", async () => {
    const router = new RoutingAdapter({
      module: new MemoryAdapter([ev(9), ev(12)]),
      default: new MemoryAdapter([ev(4), ev(6)]),
    });

    expect(await router.getEarliestVersion!(moduleDesc)).toBe(9n);
    expect(await router.getEarliestVersion!(handleDesc)).toBe(4n);
  });

  it("throws StreamdError when the routed adapter cannot report one", async () => {
    const bare: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
    };
    expect(new RoutingAdapter({ account: bare }).getEarliestVersion).toBeUndefined();
    const router = new RoutingAdapter({ account: bare, default: new MemoryAdapter([ev(4)]) });

    const err = await router.getEarliestVersion!(accountDesc).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain('"account"');
  });
});

describe("RoutingAdapter blocks", () => {
  const block = { height: 5n, firstVersion: 10n, lastVersion: 12n, timestamp: 1n };
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("asks the adapter routed for the descriptor's kind", async () => {
    const withBlocks: EventAdapter = { ...bare, getBlock: vi.fn(async () => block) };
    const router = new RoutingAdapter({ module: withBlocks, default: bare });

    expect(await router.getBlock!(moduleDesc, 5n)).toBe(block);
    expect(withBlocks.getBlock).toHaveBeenCalledWith(moduleDesc, 5n);
  });

  it("throws StreamdError when the routed adapter cannot report blocks", async () => {
    expect(new RoutingAdapter({ account: bare }).getBlock).toBeUndefined();
    const withBlocks: EventAdapter = { ...bare, getBlock: async () => block };
    const router = new RoutingAdapter({ account: bare, default: withBlocks });

    const err = await router.getBlock!(accountDesc, 5n).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain('"account"');
  });
});

describe("RoutingAdapter transaction lookup", () => {
  const hash = `0x${"ef".repeat(32)}`;
  const found = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };
  const looking = (label: string): EventAdapter => ({
    fetchEvents: async () => ({ events: [], hasMore: false }),
    getTransactionEvents: async () => ({ ...found, vmStatus: label }),
  });

  it("routes by the descriptor's kind, and to default without a descriptor", async () => {
    const router = new RoutingAdapter({ module: looking("module"), default: looking("default") });
    const moduleDescriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };
    expect((await router.getTransactionEvents!(hash, moduleDescriptor))!.vmStatus).toBe("module");
    expect((await router.getTransactionEvents!(hash))!.vmStatus).toBe("default");
  });

  it("refuses when no route serves the lookup", async () => {
    const bare: EventAdapter = { fetchEvents: async () => ({ events: [], hasMore: false }) };
    await expect(new RoutingAdapter({ module: looking("module") }).getTransactionEvents!(hash)).rejects.toBeInstanceOf(
      StreamdError,
    );
    expect(new RoutingAdapter({ module: bare }).getTransactionEvents).toBeUndefined();
    await expect(
      new RoutingAdapter({ module: bare, default: looking("default") }).getTransactionEvents!(hash, {
        kind: "module",
        eventType: "0x1::t::A",
      }),
    ).rejects.toThrow("does not look up transactions");
  });
});

describe("RoutingAdapter simulation", () => {
  const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };
  const seen: unknown[] = [];
  const simulating = (label: string): EventAdapter => ({
    fetchEvents: async () => ({ events: [], hasMore: false }),
    simulateTransactionEvents: async (...args) => (seen.push(args), { ...simulated, vmStatus: label }),
  });

  it("routes by the descriptor's kind, and to default without a descriptor", async () => {
    const router = new RoutingAdapter({ account: simulating("account"), default: simulating("default") });
    const accountDescriptor: EventDescriptor = { kind: "account", accountAddress: "0x1" };
    expect((await router.simulateTransactionEvents!(simulation, accountDescriptor)).vmStatus).toBe("account");
    expect(seen[0]).toEqual([simulation, accountDescriptor]);
    expect((await router.simulateTransactionEvents!(simulation)).vmStatus).toBe("default");
  });

  it("refuses when no route serves the simulation", async () => {
    const bare: EventAdapter = { fetchEvents: async () => ({ events: [], hasMore: false }) };
    await expect(
      new RoutingAdapter({ module: simulating("module") }).simulateTransactionEvents!(simulation),
    ).rejects.toBeInstanceOf(StreamdError);
    expect(new RoutingAdapter({ module: bare }).simulateTransactionEvents).toBeUndefined();
    await expect(
      new RoutingAdapter({ module: bare, default: simulating("default") }).simulateTransactionEvents!(simulation, {
        kind: "module",
        eventType: "0x1::t::A",
      }),
    ).rejects.toThrow("does not simulate transactions");
  });
});

describe("RoutingAdapter view", () => {
  const request = { function: "0x1::coin::balance" as const, functionArguments: ["0xa"] };
  const viewing = (label: string): EventAdapter => ({
    fetchEvents: async () => ({ events: [], hasMore: false }),
    view: vi.fn(async () => [label]),
  });

  it("routes by descriptor kind, else to the default, and forwards every argument", async () => {
    const module = viewing("module");
    const fallback = viewing("default");
    const router = new RoutingAdapter({ module, default: fallback });
    const descriptor = { kind: "module" as const, eventType: "0x1::a::B" as const };
    expect(await router.view!(request, 7n, descriptor)).toEqual(["module"]);
    expect(module.view).toHaveBeenCalledWith(request, 7n, descriptor);
    expect(await router.view!(request, 7n)).toEqual(["default"]);
  });

  it("refuses when no adapter runs views or none is routable", async () => {
    const plain = new MemoryAdapter();
    const descriptor = { kind: "module" as const, eventType: "0x1::a::B" as const };
    expect(new RoutingAdapter({ module: plain, default: plain }).view).toBeUndefined();
    await expect(
      new RoutingAdapter({ module: plain, default: viewing("default") }).view!(request, 1n, descriptor),
    ).rejects.toThrow('descriptor kind "module" does not run views');
    await expect(new RoutingAdapter({ module: viewing("module") }).view!(request, 1n)).rejects.toThrow(
      "needs a descriptor or a default adapter",
    );
    await expect(
      new RoutingAdapter({ module: viewing("module"), default: plain }).view!(request, 1n),
    ).rejects.toThrow("The default adapter does not run views");
  });
});

describe("RoutingAdapter optional method fields", () => {
  it("has no own property for a method no route has", () => {
    const adapter = new RoutingAdapter({ default: new MemoryAdapter() });
    expect(Object.hasOwn(adapter, "getBlock")).toBe(false);
    expect("getBlock" in adapter).toBe(false);
  });
});

describe("RoutingAdapter resource route", () => {
  const resourceDesc: EventDescriptor = {
    kind: "resource",
    accountAddress: "0x1",
    resourceType: PONG,
  };

  it("sends resource descriptors to the resource route, else the default", async () => {
    const resource = new MemoryAdapter([ev(1, PONG)]);
    const fallback = new MemoryAdapter([ev(2, PONG)]);
    const routed = await new RoutingAdapter({ resource, default: fallback }).fetchEvents(resourceDesc, {
      fromVersion: 0n,
    });
    expect(routed.events.map((e) => e.version)).toEqual([1n]);
    const defaulted = await new RoutingAdapter({ default: fallback }).fetchEvents(resourceDesc, {
      fromVersion: 0n,
    });
    expect(defaulted.events.map((e) => e.version)).toEqual([2n]);
  });

  it("rejects a resource descriptor with no route or default", async () => {
    const adapter = new RoutingAdapter({ module: new MemoryAdapter() });
    await expect(adapter.fetchEvents(resourceDesc, {})).rejects.toThrow('descriptor kind "resource"');
  });
});

describe("RoutingAdapter object descriptors", () => {
  const objectDesc: EventDescriptor = { kind: "object", objectId: "0x6" };

  it("sends them to the default", async () => {
    const seen: EventDescriptor[] = [];
    const fallback = {
      fetchEvents: async (d: EventDescriptor) => {
        seen.push(d);
        return { events: [], nextCursor: undefined, hasMore: false };
      },
    };
    await new RoutingAdapter({ module: new MemoryAdapter(), default: fallback }).fetchEvents(objectDesc, {});
    expect(seen).toEqual([objectDesc]);
  });

  it("rejects them with no default", async () => {
    const adapter = new RoutingAdapter({ module: new MemoryAdapter(), resource: new MemoryAdapter() });
    await expect(adapter.fetchEvents(objectDesc, {})).rejects.toThrow('descriptor kind "object"');
  });
});

describe("RoutingAdapter readObjects", () => {
  const reading = (label: string): EventAdapter => ({
    fetchEvents: async () => ({ events: [], hasMore: false }),
    readObjects: vi.fn(async () => [label]),
  });
  const request = { objects: ["0x1"] };
  const at = { version: 7n, transactionHash: "tx" };
  const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::m::E" };

  it("reads from the adapter for the descriptor's kind, or the default", async () => {
    const module = reading("module");
    const router = new RoutingAdapter({ module, default: reading("default") });
    expect(await router.readObjects!(request, at, descriptor)).toEqual(["module"]);
    expect(module.readObjects).toHaveBeenCalledWith(request, at, descriptor);
    expect(await router.readObjects!(request, at)).toEqual(["default"]);
  });

  it("refuses when no adapter reads objects or none is routable", async () => {
    const plain = new MemoryAdapter();
    expect(new RoutingAdapter({ module: plain, default: plain }).readObjects).toBeUndefined();
    await expect(new RoutingAdapter({ module: plain, default: reading("d") }).readObjects!(request, at, descriptor)).rejects.toThrow(
      'descriptor kind "module" does not read objects',
    );
    await expect(new RoutingAdapter({ module: reading("m") }).readObjects!(request, at)).rejects.toThrow(
      "needs a descriptor or a default adapter to read objects",
    );
    await expect(new RoutingAdapter({ module: reading("m"), default: plain }).readObjects!(request, at)).rejects.toThrow(
      "The default adapter does not read objects",
    );
  });
});

describe("RoutingAdapter lookup by version", () => {
  const found = { version: 4n, success: true, vmStatus: "Executed successfully", events: [] };
  const looking = (label: string): EventAdapter => ({
    fetchEvents: async () => ({ events: [], hasMore: false }),
    getTransactionEventsByVersion: async () => ({ ...found, vmStatus: label }),
  });
  const bare: EventAdapter = { fetchEvents: async () => ({ events: [], hasMore: false }) };
  const moduleDescriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };

  it("routes by the descriptor's kind, and to default without a descriptor", async () => {
    const router = new RoutingAdapter({ module: looking("module"), default: looking("default") });
    expect((await router.getTransactionEventsByVersion!(4n, moduleDescriptor))!.vmStatus).toBe("module");
    expect((await router.getTransactionEventsByVersion!(4n))!.vmStatus).toBe("default");
  });

  it("refuses by name a route that cannot look up by version", async () => {
    expect(new RoutingAdapter({ module: bare }).getTransactionEventsByVersion).toBeUndefined();
    const router = new RoutingAdapter({ module: bare, default: looking("default") });
    const err = await router.getTransactionEventsByVersion!(4n, moduleDescriptor).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toBe('The adapter for descriptor kind "module" does not look up transactions by version');
    const none = await new RoutingAdapter({ module: looking("module") }).getTransactionEventsByVersion!(4n).catch((e: unknown) => e);
    expect((none as Error).message).toContain("needs a descriptor or a default adapter");
  });
});
