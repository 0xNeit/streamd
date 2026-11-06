import { describe, it, expect, vi } from "bun:test";
import { FallbackAdapter } from "../src/adapters/fallback.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import {
  StreamdError,
  StreamdNetworkError,
  StreamdTimeoutError,
} from "../src/errors.js";
import type {
  EventAdapter,
  EventDescriptor,
  FetchOptions,
  StreamdEvent,
  TransactionSimulation,
} from "../src/types.js";

const PING = "0x1::test::Ping";
const moduleDesc: EventDescriptor = { kind: "module", eventType: PING };

function ev(version: number, type = PING): StreamdEvent {
  return {
    type,
    data: { version },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

describe("FallbackAdapter", () => {
  it("returns the first adapter's result without calling later adapters when the primary succeeds", async () => {
    const primary: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(1)],
        nextCursor: "1",
        hasMore: false,
      })),
    };
    const backup: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(2)],
        nextCursor: "2",
        hasMore: false,
      })),
    };

    const fallback = new FallbackAdapter([primary, backup]);
    const page = await fallback.fetchEvents(moduleDesc, { limit: 10 });

    expect(page).toEqual({
      events: [ev(1)],
      nextCursor: "1",
      hasMore: false,
    });
    expect(primary.fetchEvents).toHaveBeenCalledTimes(1);
    expect(primary.fetchEvents).toHaveBeenCalledWith(moduleDesc, { limit: 10 });
    expect(backup.fetchEvents).not.toHaveBeenCalled();
  });

  it("falls back across adapters per call on StreamdNetworkError or generic Error and tries primary again next call", async () => {
    let primaryHealthy = false;
    const primary: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        if (!primaryHealthy) {
          throw new StreamdNetworkError("primary 503", 503);
        }
        return { events: [ev(10)], nextCursor: "10", hasMore: false };
      }),
    };
    let secondaryHealthy = false;
    const secondary: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        if (!secondaryHealthy) {
          throw new Error("socket hang up");
        }
        return { events: [ev(20)], nextCursor: "20", hasMore: false };
      }),
    };
    const tertiary = new MemoryAdapter([ev(30)]);

    const fallback = new FallbackAdapter([primary, secondary, tertiary]);

    // Call 1: primary and secondary both fail -> tertiary serves.
    const first = await fallback.fetchEvents(moduleDesc);
    expect(first.events.map((e) => e.version)).toEqual([30n]);
    expect(primary.fetchEvents).toHaveBeenCalledTimes(1);
    expect(secondary.fetchEvents).toHaveBeenCalledTimes(1);

    // Call 2: secondary recovers -> secondary serves without reaching tertiary.
    secondaryHealthy = true;
    const second = await fallback.fetchEvents(moduleDesc);
    expect(second.events.map((e) => e.version)).toEqual([20n]);
    expect(primary.fetchEvents).toHaveBeenCalledTimes(2);
    expect(secondary.fetchEvents).toHaveBeenCalledTimes(2);

    // Call 3: primary recovers -> primary serves without calling secondary.
    primaryHealthy = true;
    const third = await fallback.fetchEvents(moduleDesc);
    expect(third.events.map((e) => e.version)).toEqual([10n]);
    expect(primary.fetchEvents).toHaveBeenCalledTimes(3);
    expect(secondary.fetchEvents).toHaveBeenCalledTimes(2);
  });

  it("rethrows the last adapter's error when every adapter fails", async () => {
    const first: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new StreamdNetworkError("primary down", 502);
      }),
    };
    const lastErr = new StreamdNetworkError("backup down", 504);
    const second: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw lastErr;
      }),
    };

    const fallback = new FallbackAdapter([first, second]);
    const caught = await fallback
      .fetchEvents(moduleDesc)
      .catch((e: unknown) => e);

    expect(caught).toBe(lastErr);
    expect(first.fetchEvents).toHaveBeenCalledTimes(1);
    expect(second.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it("rethrows invalid-input StreamdError immediately without calling backup adapters", async () => {
    const primary = new MemoryAdapter([ev(1)]);
    const backup: EventAdapter = {
      fetchEvents: vi.fn(async () => ({
        events: [ev(2)],
        nextCursor: "1",
        hasMore: false,
      })),
    };
    const fallback = new FallbackAdapter([primary, backup]);

    // Bad Move eventType throws plain StreamdError from primary.
    const badDesc: EventDescriptor = {
      kind: "module",
      eventType: "bad" as `${string}::${string}::${string}`,
    };
    const err = await fallback.fetchEvents(badDesc).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(backup.fetchEvents).not.toHaveBeenCalled();

    // Non-network StreamdError subclass (e.g. StreamdTimeoutError) also stops without fallback.
    const timeoutAdapter: EventAdapter = {
      fetchEvents: vi.fn(async () => {
        throw new StreamdTimeoutError("timed out", 100);
      }),
    };
    const withTimeout = new FallbackAdapter([timeoutAdapter, backup]);
    const timeoutErr = await withTimeout
      .fetchEvents(moduleDesc)
      .catch((e: unknown) => e);
    expect(timeoutErr).toBeInstanceOf(StreamdTimeoutError);
    expect(backup.fetchEvents).not.toHaveBeenCalled();
  });

  it("isolates options per attempt and copies the adapters array on construction", async () => {
    const mutatingPrimary: EventAdapter = {
      fetchEvents: vi.fn(async (_desc, opts: FetchOptions) => {
        opts.cursor = "corrupted";
        opts.limit = 999;
        throw new StreamdNetworkError("failed after mutating opts", 500);
      }),
    };
    let receivedOptions: FetchOptions | undefined;
    const backup: EventAdapter = {
      fetchEvents: vi.fn(async (_desc, opts: FetchOptions) => {
        receivedOptions = opts;
        return { events: [ev(5)], nextCursor: "1", hasMore: false };
      }),
    };

    const list: EventAdapter[] = [mutatingPrimary, backup];
    const fallback = new FallbackAdapter(list);
    // Mutating the caller's array after construction must not affect fallback order.
    list.length = 0;

    const callerOptions: FetchOptions = { cursor: "0", limit: 2 };
    const page = await fallback.fetchEvents(moduleDesc, callerOptions);

    expect(page.events.map((e) => e.version)).toEqual([5n]);
    expect(callerOptions).toEqual({ cursor: "0", limit: 2 });
    expect(receivedOptions).toEqual({ cursor: "0", limit: 2 });
  });

  it("validates the adapters list in the constructor", () => {
    expect(() => new FallbackAdapter([])).toThrow(
      /FallbackAdapter requires at least one adapter/,
    );
    expect(
      () => new FallbackAdapter(null as unknown as readonly EventAdapter[]),
    ).toThrow(/FallbackAdapter requires at least one adapter/);
    expect(
      () =>
        new FallbackAdapter([
          new MemoryAdapter(),
          {} as unknown as EventAdapter,
        ]),
    ).toThrow(/FallbackAdapter\[1\] requires a valid EventAdapter/);
  });
});

describe("FallbackAdapter page finality", () => {
  const finalPage = (version: number) => ({
    events: [ev(version)],
    nextCursor: String(version),
    hasMore: true,
    final: true,
  });

  it("keeps final on the first source's page and drops it on a backup's", async () => {
    let primaryUp = true;
    const primary: EventAdapter = {
      async fetchEvents() {
        if (!primaryUp) throw new StreamdNetworkError("down");
        return finalPage(1);
      },
    };
    const backup: EventAdapter = { fetchEvents: async () => finalPage(2) };
    const fallback = new FallbackAdapter([primary, backup]);

    expect((await fallback.fetchEvents(moduleDesc, {})).final).toBe(true);
    primaryUp = false;
    const page = await fallback.fetchEvents(moduleDesc, {});
    expect(page).toEqual({ ...finalPage(2), final: false });
  });
});

describe("FallbackAdapter head version", () => {
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("skips adapters that cannot report a head", async () => {
    const fallback = new FallbackAdapter([bare, new MemoryAdapter([ev(6)])]);
    expect(await fallback.getHeadVersion(moduleDesc)).toBe(6n);
  });

  it("falls back on a network error", async () => {
    const down: EventAdapter = {
      ...bare,
      getHeadVersion: vi.fn(async () => {
        throw new StreamdNetworkError("503", 503);
      }),
    };
    const fallback = new FallbackAdapter([down, new MemoryAdapter([ev(8)])]);

    expect(await fallback.getHeadVersion(moduleDesc)).toBe(8n);
    expect(down.getHeadVersion).toHaveBeenCalledTimes(1);
  });

  it("rethrows invalid input without trying later adapters", async () => {
    const invalid: EventAdapter = {
      ...bare,
      getHeadVersion: async () => {
        throw new StreamdError("bad descriptor");
      },
    };
    const later = new MemoryAdapter([ev(8)]);
    const spy = vi.spyOn(later, "getHeadVersion");
    const fallback = new FallbackAdapter([invalid, later]);

    await expect(fallback.getHeadVersion(moduleDesc)).rejects.toThrow("bad descriptor");
    expect(spy).not.toHaveBeenCalled();
  });

  it("throws StreamdError when no adapter can report a head", async () => {
    const err = await new FallbackAdapter([bare])
      .getHeadVersion(moduleDesc)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });
});

describe("FallbackAdapter earliest version", () => {
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("skips adapters that cannot report one", async () => {
    const fallback = new FallbackAdapter([bare, new MemoryAdapter([ev(6), ev(9)])]);
    expect(await fallback.getEarliestVersion(moduleDesc)).toBe(6n);
  });

  it("falls back on a network error", async () => {
    const down: EventAdapter = {
      ...bare,
      getEarliestVersion: vi.fn(async () => {
        throw new StreamdNetworkError("503", 503);
      }),
    };
    const fallback = new FallbackAdapter([down, new MemoryAdapter([ev(8)])]);

    expect(await fallback.getEarliestVersion(moduleDesc)).toBe(8n);
    expect(down.getEarliestVersion).toHaveBeenCalledTimes(1);
  });

  it("rethrows invalid input without trying later adapters", async () => {
    const invalid: EventAdapter = {
      ...bare,
      getEarliestVersion: async () => {
        throw new StreamdError("bad descriptor");
      },
    };
    const later = new MemoryAdapter([ev(8)]);
    const spy = vi.spyOn(later, "getEarliestVersion");
    const fallback = new FallbackAdapter([invalid, later]);

    await expect(fallback.getEarliestVersion(moduleDesc)).rejects.toThrow("bad descriptor");
    expect(spy).not.toHaveBeenCalled();
  });

  it("throws StreamdError when no adapter can report one", async () => {
    const err = await new FallbackAdapter([bare])
      .getEarliestVersion(moduleDesc)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });
});

describe("FallbackAdapter block heights", () => {
  const heights = { oldest: 3n, newest: 9n };
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("skips adapters without block heights and falls back on a network error", async () => {
    const down: EventAdapter = {
      ...bare,
      getBlockHeights: vi.fn(async () => {
        throw new StreamdNetworkError("429", 429);
      }),
    };
    const up: EventAdapter = { ...bare, getBlockHeights: vi.fn(async () => heights) };
    const fallback = new FallbackAdapter([bare, down, up]);

    expect(await fallback.getBlockHeights(moduleDesc)).toBe(heights);
    expect(down.getBlockHeights).toHaveBeenCalledTimes(1);
    expect(up.getBlockHeights).toHaveBeenCalledWith(moduleDesc);
  });

  it("throws StreamdError when no adapter reports block heights", async () => {
    const err = await new FallbackAdapter([bare])
      .getBlockHeights(moduleDesc)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });
});

describe("FallbackAdapter blocks", () => {
  const block = { height: 5n, firstVersion: 10n, lastVersion: 12n, timestamp: 1n };
  const bare: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
  };

  it("skips adapters without blocks and falls back on a network error", async () => {
    const down: EventAdapter = {
      ...bare,
      getBlock: vi.fn(async () => {
        throw new StreamdNetworkError("503", 503);
      }),
    };
    const up: EventAdapter = { ...bare, getBlock: vi.fn(async () => block) };
    const fallback = new FallbackAdapter([bare, down, up]);

    expect(await fallback.getBlock(moduleDesc, 5n)).toBe(block);
    expect(down.getBlock).toHaveBeenCalledTimes(1);
    expect(up.getBlock).toHaveBeenCalledWith(moduleDesc, 5n);
  });

  it("throws StreamdError when no adapter reports blocks", async () => {
    const err = await new FallbackAdapter([bare])
      .getBlock(moduleDesc, 5n)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });
});

describe("FallbackAdapter scan progress", () => {
  it("keeps throughVersion from whichever source answered", async () => {
    const primary: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdNetworkError("down");
      },
    };
    const backup: EventAdapter = {
      fetchEvents: async () => ({ events: [ev(4)], hasMore: false, throughVersion: 9n }),
    };
    const page = await new FallbackAdapter([primary, backup]).fetchEvents(moduleDesc, {});
    expect(page.throughVersion).toBe(9n);
  });
});

describe("FallbackAdapter transaction lookup", () => {
  const hash = `0x${"ef".repeat(32)}`;
  const found = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };
  const bare: EventAdapter = { fetchEvents: async () => ({ events: [], hasMore: false }) };

  it("skips sources without it and falls back past a network failure", async () => {
    const down: EventAdapter = {
      ...bare,
      getTransactionEvents: vi.fn(async () => {
        throw new StreamdNetworkError("down", 503);
      }),
    };
    const up: EventAdapter = { ...bare, getTransactionEvents: vi.fn(async () => found) };
    const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };
    expect(await new FallbackAdapter([bare, down, up]).getTransactionEvents(hash, descriptor)).toBe(found);
    expect(down.getTransactionEvents).toHaveBeenCalledTimes(1);
    expect(up.getTransactionEvents).toHaveBeenCalledWith(hash, descriptor);
  });

  it("refuses when no source looks transactions up", async () => {
    await expect(new FallbackAdapter([bare]).getTransactionEvents(hash)).rejects.toBeInstanceOf(StreamdError);
  });
});

describe("FallbackAdapter simulation", () => {
  const simulation = { transaction: { rawTransaction: {} } } as unknown as TransactionSimulation;
  const simulated = { version: 1n, success: true, vmStatus: "Executed successfully", events: [] };
  const bare: EventAdapter = { fetchEvents: async () => ({ events: [], hasMore: false }) };

  it("skips sources without it and falls back past a network failure", async () => {
    const down: EventAdapter = {
      ...bare,
      simulateTransactionEvents: vi.fn(async () => {
        throw new StreamdNetworkError("down", 503);
      }),
    };
    const up: EventAdapter = { ...bare, simulateTransactionEvents: vi.fn(async () => simulated) };
    const descriptor: EventDescriptor = { kind: "module", eventType: "0x1::t::A" };
    expect(await new FallbackAdapter([bare, down, up]).simulateTransactionEvents(simulation, descriptor)).toBe(simulated);
    expect(down.simulateTransactionEvents).toHaveBeenCalledTimes(1);
    expect(up.simulateTransactionEvents).toHaveBeenCalledWith(simulation, descriptor);
  });

  it("refuses when no source simulates transactions", async () => {
    await expect(new FallbackAdapter([bare]).simulateTransactionEvents(simulation)).rejects.toBeInstanceOf(StreamdError);
  });
});
