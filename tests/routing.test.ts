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

    expect(await router.getHeadVersion(moduleDesc)).toBe(9n);
    expect(await router.getHeadVersion(handleDesc)).toBe(4n);
  });

  it("throws StreamdError when the routed adapter cannot report a head", async () => {
    const bare: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
    };
    const router = new RoutingAdapter({ account: bare });

    const err = await router.getHeadVersion(accountDesc).catch((e: unknown) => e);
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

    expect(await router.getBlockHeights(moduleDesc)).toBe(heights);
    expect(withHeights.getBlockHeights).toHaveBeenCalledWith(moduleDesc);
  });

  it("throws StreamdError when the routed adapter cannot report block heights", async () => {
    const router = new RoutingAdapter({ account: bare });

    const err = await router.getBlockHeights(accountDesc).catch((e: unknown) => e);
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

    expect(await router.getEarliestVersion(moduleDesc)).toBe(9n);
    expect(await router.getEarliestVersion(handleDesc)).toBe(4n);
  });

  it("throws StreamdError when the routed adapter cannot report one", async () => {
    const bare: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
    };
    const router = new RoutingAdapter({ account: bare });

    const err = await router.getEarliestVersion(accountDesc).catch((e: unknown) => e);
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

    expect(await router.getBlock(moduleDesc, 5n)).toBe(block);
    expect(withBlocks.getBlock).toHaveBeenCalledWith(moduleDesc, 5n);
  });

  it("throws StreamdError when the routed adapter cannot report blocks", async () => {
    const router = new RoutingAdapter({ account: bare });

    const err = await router.getBlock(accountDesc, 5n).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain('"account"');
  });
});
