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

describe("RoutingAdapter type and module routes", () => {
  const LONG_ONE = `0x${"0".repeat(63)}1`;
  const DEPOSIT = "0x1::fungible_asset::Deposit";
  const WITHDRAW = "0x1::fungible_asset::Withdraw";
  const COIN_DEPOSIT = "0x1::coin::CoinDeposit";
  const OTHER = "0xabc::pool::Swap";
  const mod = (eventType: string | string[]) => ({ kind: "module", eventType }) as EventDescriptor;

  function source(label: string, extra: Partial<EventAdapter> = {}): EventAdapter {
    return {
      fetchEvents: vi.fn(async () => ({ events: [], nextCursor: label, hasMore: false })),
      ...extra,
    };
  }
  const via = async (router: RoutingAdapter, descriptor: EventDescriptor) =>
    (await router.fetchEvents(descriptor)).nextCursor;

  it("takes an exact type, then its module, then the module kind, then the default", async () => {
    const router = new RoutingAdapter({
      types: { [DEPOSIT]: source("type") },
      modules: { "0x1::fungible_asset": source("module-id") },
      module: source("kind"),
      default: source("default"),
    });
    expect(await via(router, mod(DEPOSIT))).toBe("type");
    expect(await via(router, mod(WITHDRAW))).toBe("module-id");
    expect(await via(router, mod(COIN_DEPOSIT))).toBe("kind");
    const noKind = new RoutingAdapter({ types: { [DEPOSIT]: source("type") }, default: source("default") });
    expect(await via(noKind, mod(COIN_DEPOSIT))).toBe("default");
  });

  it("forwards the descriptor and options unchanged to the routed adapter", async () => {
    const routed = source("type");
    const router = new RoutingAdapter({ types: { [DEPOSIT]: routed }, default: source("default") });
    const descriptor = mod(DEPOSIT);
    await router.fetchEvents(descriptor, { fromVersion: 5n, cursor: "c" });
    expect(routed.fetchEvents).toHaveBeenCalledWith(descriptor, { fromVersion: 5n, cursor: "c" });
  });

  it("compares addresses in long form, in keys, descriptors and type arguments", async () => {
    const router = new RoutingAdapter({
      types: { [`${LONG_ONE}::fungible_asset::Deposit`]: source("type") },
      modules: { "0x00ABC::pool": source("pool") },
      default: source("default"),
    });
    expect(await via(router, mod(DEPOSIT))).toBe("type");
    expect(await via(router, mod(`${LONG_ONE}::fungible_asset::Deposit`))).toBe("type");
    expect(await via(router, mod(OTHER))).toBe("pool");
    expect(await via(router, mod(`0x${"0".repeat(61)}abc::pool::Swap`))).toBe("pool");

    const generic = new RoutingAdapter({
      types: { "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>": source("apt") },
      default: source("default"),
    });
    expect(await via(generic, mod(`0x1::coin::CoinStore<${LONG_ONE}::aptos_coin::AptosCoin>`))).toBe("apt");
    expect(await via(generic, mod("0x1::coin::CoinStore< 0x1::aptos_coin::AptosCoin >"))).toBe("apt");
  });

  it("serves a generic type from its base type's key unless an exact generic key exists", async () => {
    const router = new RoutingAdapter({
      types: {
        "0x1::coin::CoinStore": source("base"),
        "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>": source("apt"),
        "0x1::pair::Swap<0x1::a::A, 0x1::b::B>": source("pair"),
      },
      modules: { "0x1::coin": source("module-id") },
      default: source("default"),
    });
    expect(await via(router, mod("0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>"))).toBe("apt");
    expect(await via(router, mod("0x1::coin::CoinStore<0xf::usdc::USDC>"))).toBe("base");
    expect(await via(router, mod("0x1::coin::CoinStore"))).toBe("base");
    expect(await via(router, mod("0x1::coin::CoinInfo<0xf::usdc::USDC>"))).toBe("module-id");
    expect(await via(router, mod("0x1::pair::Swap<0x1::a::A,0x1::b::B>"))).toBe("pair");
    expect(await via(router, mod("0x1::pair::Swap<0x1::a::A, 0x1::c::C>"))).toBe("default");
  });

  it("routes a list whose types all resolve to one adapter, through one or several routes", async () => {
    const shared = source("shared");
    const router = new RoutingAdapter({
      types: { [DEPOSIT]: shared, [COIN_DEPOSIT]: shared },
      modules: { "0xabc::pool": shared },
      default: source("default"),
    });
    expect(await via(router, mod([DEPOSIT, COIN_DEPOSIT]))).toBe("shared");
    expect(await via(router, mod([DEPOSIT, OTHER]))).toBe("shared");
    expect(await via(router, mod([WITHDRAW, "0x2::x::Y"]))).toBe("default");
  });

  it("refuses a list spanning routes, naming every type and its route, calling no adapter", async () => {
    const memory = source("memory");
    const fallback = source("default");
    const router = new RoutingAdapter({
      types: { [DEPOSIT]: memory },
      modules: { "0xabc::pool": source("pool") },
      default: fallback,
    });
    const err = await router.fetchEvents(mod([DEPOSIT, WITHDRAW, OTHER])).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toBe(
      "A module descriptor's event types span routes: " +
        `${DEPOSIT} (types["${LONG_ONE}::fungible_asset::Deposit"]), ` +
        `${WITHDRAW} (default), ` +
        `${OTHER} (modules["0x${"0".repeat(61)}abc::pool"]); ` +
        "use one descriptor per route, or a list of descriptors",
    );
    expect(memory.fetchEvents).not.toHaveBeenCalled();
    expect(fallback.fetchEvents).not.toHaveBeenCalled();

    const noDefault = new RoutingAdapter({ types: { [DEPOSIT]: memory } });
    await expect(noDefault.fetchEvents(mod([DEPOSIT, WITHDRAW]))).rejects.toThrow(
      `${WITHDRAW} (no route)`,
    );
    await expect(noDefault.fetchEvents(mod(WITHDRAW))).rejects.toThrow(
      'No adapter configured for descriptor kind "module"',
    );
  });

  it("sends a type that is not a Move type to the kind route, so its adapter refuses it", async () => {
    const fallback = source("default");
    const router = new RoutingAdapter({ types: { [DEPOSIT]: source("type") }, default: fallback });
    for (const bad of ["not a type", "0x1::m", 7, ""]) {
      expect(await via(router, { kind: "module", eventType: bad } as unknown as EventDescriptor)).toBe("default");
    }
    expect(await via(router, mod([]))).toBe("default");
  });

  it("leaves handle, account and resource descriptors on their kind routes", async () => {
    const router = new RoutingAdapter({
      types: { [DEPOSIT]: source("type"), [PONG]: source("type") },
      modules: { "0x1::test": source("module-id") },
      default: source("default"),
    });
    expect(await via(router, handleDesc)).toBe("default");
    expect(await via(router, { kind: "account", accountAddress: "0x1", eventTypes: [DEPOSIT] })).toBe("default");
    expect(await via(router, { kind: "resource", accountAddress: "0x1", resourceType: PONG })).toBe("default");
  });

  it("routes optional methods by the descriptor's types, and to default without one", async () => {
    const found = { version: 4n, success: true, vmStatus: "", events: [] };
    const typed = source("type", {
      getHeadVersion: async () => 11n,
      view: vi.fn(async () => ["type"]),
      getTransactionEventsByVersion: async () => ({ ...found, vmStatus: "type" }),
    });
    const fallback = source("default", {
      getHeadVersion: async () => 22n,
      view: async () => ["default"],
      getTransactionEventsByVersion: async () => ({ ...found, vmStatus: "default" }),
    });
    const router = new RoutingAdapter({ types: { [DEPOSIT]: typed }, default: fallback });
    expect(await router.getHeadVersion!(mod(DEPOSIT))).toBe(11n);
    expect(await router.getHeadVersion!(mod(WITHDRAW))).toBe(22n);
    const request = { function: "0x1::m::f" as const };
    expect(await router.view!(request, 3n, mod(DEPOSIT))).toEqual(["type"]);
    expect(typed.view).toHaveBeenCalledWith(request, 3n, mod(DEPOSIT));
    expect(await router.view!(request, 3n)).toEqual(["default"]);
    expect((await router.getTransactionEventsByVersion!(4n, mod(DEPOSIT)))!.vmStatus).toBe("type");
    expect((await router.getTransactionEventsByVersion!(4n))!.vmStatus).toBe("default");
    await expect(router.getHeadVersion!(mod([DEPOSIT, WITHDRAW]))).rejects.toThrow("span routes");
  });

  it("exposes a method only a type route has, and names that route when it is missing", async () => {
    const router = new RoutingAdapter({
      types: { [DEPOSIT]: source("type", { getBlockHeights: async () => ({ oldest: 0n, newest: 9n }) }) },
      modules: { "0x1::coin": source("coin") },
      default: source("default"),
    });
    expect(await router.getBlockHeights!(mod(DEPOSIT))).toEqual({ oldest: 0n, newest: 9n });
    await expect(router.getBlockHeights!(mod(COIN_DEPOSIT))).rejects.toThrow(
      `The adapter for route modules["${LONG_ONE}::coin"] does not report block heights`,
    );
    await expect(router.getBlockHeights!(mod(WITHDRAW))).rejects.toThrow(
      'The adapter for descriptor kind "module" does not report block heights',
    );
  });

  it("accepts type or module routes alone", async () => {
    expect(await via(new RoutingAdapter({ types: { [DEPOSIT]: source("type") } }), mod(DEPOSIT))).toBe("type");
    expect(await via(new RoutingAdapter({ modules: { "0x1::coin": source("coin") } }), mod(COIN_DEPOSIT))).toBe("coin");
  });

  it("refuses malformed keys, canonical duplicates, non-object maps and non-adapters at construction", () => {
    const ok = source("ok");
    const refuse = (routes: RoutingAdapterRoutes, message: string | RegExp) => {
      let caught: unknown;
      try {
        new RoutingAdapter(routes);
      } catch (e) {
        caught = e;
      }
      expect(caught).toBeInstanceOf(StreamdError);
      expect((caught as Error).message).toMatch(message);
    };
    for (const key of ["0x1::coin", "coin::CoinDeposit", "0xzz::m::S", "0x1::m::S<>", "0x1::1m::S", "0x1::m::S::T"]) {
      refuse({ types: { [key]: ok } }, `RoutingAdapter.types key must be written address::module::Struct: ${JSON.stringify(key)}`);
    }
    for (const key of ["0x1::coin::CoinDeposit", "0x1", "0x1::", "aptos::coin"]) {
      refuse({ modules: { [key]: ok } }, `RoutingAdapter.modules key must be written address::module: ${JSON.stringify(key)}`);
    }
    refuse(
      { types: { [DEPOSIT]: ok, [`${LONG_ONE}::fungible_asset::Deposit`]: ok } },
      `RoutingAdapter.types key "${LONG_ONE}::fungible_asset::Deposit" names ${LONG_ONE}::fungible_asset::Deposit again`,
    );
    refuse({ modules: { "0x1::coin": ok, "0x01::coin": ok } }, /RoutingAdapter\.modules key "0x01::coin" names .* again/);
    refuse({ types: null as unknown as RoutingAdapterRoutes["types"] }, "RoutingAdapter.types must be an object of adapters by key");
    refuse({ modules: [ok] as unknown as RoutingAdapterRoutes["modules"] }, "RoutingAdapter.modules must be an object of adapters by key");
    refuse({ types: { [DEPOSIT]: {} as EventAdapter } }, `RoutingAdapter.types["${DEPOSIT}"] requires a valid EventAdapter`);
    refuse({ types: {}, modules: {} }, "RoutingAdapter requires at least one route or a default adapter");
  });

  it("copies the route maps, so later changes to them have no effect", async () => {
    const types: Record<string, EventAdapter> = { [DEPOSIT]: source("type") };
    const router = new RoutingAdapter({ types, default: source("default") });
    types[DEPOSIT] = source("replaced");
    types[WITHDRAW] = source("added");
    expect(await via(router, mod(DEPOSIT))).toBe("type");
    expect(await via(router, mod(WITHDRAW))).toBe("default");
  });

  it("serves events from a MemoryAdapter on a type route", async () => {
    const memory = new MemoryAdapter([ev(3, DEPOSIT), ev(4, WITHDRAW)]);
    const router = new RoutingAdapter({ types: { [`${LONG_ONE}::fungible_asset::Deposit`]: memory }, default: new MemoryAdapter() });
    const page = await router.fetchEvents(mod(DEPOSIT), { fromVersion: 0n });
    expect(page.events.map((e) => e.version)).toEqual([3n]);
  });
});
