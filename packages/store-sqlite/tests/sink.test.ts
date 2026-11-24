import { Database } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "bun:test";
import { MemoryAdapter, StreamdClient, eventId, points } from "@streamd/core";
import type { EventAdapter, EventCallback, EventDescriptor, FetchOptions, PointsState, Reduction, Score, StreamdEvent, Subscription, SubscriptionOptions, Transfer } from "@streamd/core";
import { SqliteEventStore, StoreAdapter, storeEvents, storePoints, storeSales, storeSwaps, storeTransfers } from "../src/index.js";
import type { SaleClient, StoredSale, StoredSwap, SwapClient, TransferClient } from "../src/index.js";

const SWAP = "0x1::dex::Swap";
const swaps: EventDescriptor = { kind: "module", eventType: SWAP };

function history(count: number): StreamdEvent[] {
  return Array.from({ length: count }, (_, i) => ({
    type: SWAP,
    data: { n: String(i) },
    version: BigInt(i + 1),
    sequenceNumber: 0n,
    eventIndex: 0,
  }));
}

function caughtUp(start: (done: () => void) => Subscription): Promise<Subscription> {
  return new Promise((resolve) => {
    const sub = start(() => queueMicrotask(() => resolve(sub)));
  });
}

async function stored(db: Database): Promise<string[]> {
  const events = await new StreamdClient({ adapter: new StoreAdapter(db) }).getEvents(swaps, { limit: 1000 });
  return events.map(eventId);
}

describe("storeEvents", () => {
  it("resumes after a restart with every event stored exactly once", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const options = { name: "swaps", batchSize: 2, pollInterval: 5 };

    const first = new StreamdClient({ adapter: new MemoryAdapter(history(5)) });
    const sub = await caughtUp((onCaughtUp) => storeEvents(first, store, swaps, { ...options, onCaughtUp }));
    sub.unsubscribe();
    first.destroy();
    expect(await stored(db)).toEqual(history(5).map(eventId));
    const saved = store.readCursor("swaps");
    expect(saved).toBeDefined();

    const second = new StreamdClient({ adapter: new MemoryAdapter(history(9)) });
    const seen: string[] = [];
    const resumed = await caughtUp((onCaughtUp) =>
      storeEvents(second, store, swaps, {
        ...options,
        onCaughtUp,
        where: (e) => (seen.push(eventId(e)), true),
      }),
    );
    resumed.unsubscribe();
    second.destroy();
    // The batch in flight when the cursor was saved is read again, never earlier ones.
    expect(seen[0]).not.toBe("1:0");
    expect(await stored(db)).toEqual(history(9).map(eventId));
  });

  it("saves the cursor after the first batch, so a relative start resumes without a gap", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    // One batch only: the cursor saved with it is the one from before the first page.
    const options = { name: "swaps", batchSize: 2, pollInterval: 5, fromVersion: -1n };
    const first = new StreamdClient({ adapter: new MemoryAdapter(history(10)) });
    const sub = await caughtUp((onCaughtUp) => storeEvents(first, store, swaps, { ...options, onCaughtUp }));
    sub.unsubscribe();
    first.destroy();
    expect(await stored(db)).toEqual(["9:0", "10:0"]);

    const second = new StreamdClient({ adapter: new MemoryAdapter(history(30)) });
    const resumed = await caughtUp((onCaughtUp) => storeEvents(second, store, swaps, { ...options, onCaughtUp }));
    resumed.unsubscribe();
    second.destroy();
    expect(await stored(db)).toEqual(history(30).slice(8).map(eventId));
  });

  it("saves the cursor on catching up, so a head start with no matches yet resumes without a gap", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const options = { name: "swaps", fromVersion: -1n, where: (e: StreamdEvent) => e.version > 10n };
    const first = new StreamdClient({ adapter: new MemoryAdapter(history(10)) });
    let caught = 0;
    const sub = await caughtUp((done) =>
      storeEvents(first, store, swaps, { ...options, onCaughtUp: () => (caught++, done()) }),
    );
    sub.unsubscribe();
    first.destroy();
    expect(caught).toBe(1);
    expect(store.readCursor("swaps")).toBe("10");

    const second = new StreamdClient({ adapter: new MemoryAdapter(history(30)) });
    const resumed = await caughtUp((onCaughtUp) => storeEvents(second, store, swaps, { ...options, onCaughtUp }));
    resumed.unsubscribe();
    second.destroy();
    expect(await stored(db)).toEqual(history(30).slice(10).map(eventId));
  });

  it("still calls onCaughtUp when saving the cursor there fails", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_cursors BEGIN SELECT RAISE(ABORT, 'busy'); END`);
    const client = new StreamdClient({ adapter: new MemoryAdapter(history(3)) });
    const errors: Error[] = [];
    const sub = await caughtUp((onCaughtUp) =>
      storeEvents(client, store, swaps, { name: "swaps", fromVersion: -1n, where: () => false, onCaughtUp, onError: (e) => errors.push(e) }),
    );
    expect(sub.active).toBe(true);
    expect(((errors[0]?.cause as Error) ?? errors[0])?.message).toContain("busy");
    sub.unsubscribe();
    client.destroy();
  });

  it("an explicit cursor wins over the saved one", async () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    store.write([], { name: "swaps", cursor: "4" });
    const client = new StreamdClient({ adapter: new MemoryAdapter(history(5)) });
    const seen: string[] = [];
    const sub = await caughtUp((onCaughtUp) =>
      storeEvents(client, store, swaps, {
        name: "swaps",
        cursor: "0",
        onCaughtUp,
        where: (e) => (seen.push(eventId(e)), true),
      }),
    );
    sub.unsubscribe();
    client.destroy();
    expect(seen).toEqual(history(5).map(eventId));
  });

  it("ends the subscription on a failed write without moving the saved cursor", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_events
      WHEN NEW.version >= '00000000000000000003' BEGIN SELECT RAISE(ABORT, 'disk full'); END`);
    const client = new StreamdClient({ adapter: new MemoryAdapter(history(6)) });
    const error = await new Promise<Error>((resolve) => {
      storeEvents(client, store, swaps, { name: "swaps", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(error.message).toContain("from 3:0 to 4:0");
    expect((error.cause as Error).message).toContain("disk full");
    expect(client.listenerCount()).toBe(0);
    expect(await stored(db)).toEqual(["1:0", "2:0"]);
    // Saved after the first batch, so a restart reads from the failed one again.
    const savedAfterFirst = store.readCursor("swaps");
    expect(savedAfterFirst).toBe("2");
    client.destroy();

    db.exec("DROP TRIGGER boom");
    const restarted = new StreamdClient({ adapter: new MemoryAdapter(history(6)) });
    const sub = await caughtUp((onCaughtUp) =>
      storeEvents(restarted, store, swaps, { name: "swaps", batchSize: 2, onCaughtUp }),
    );
    sub.unsubscribe();
    restarted.destroy();
    expect(await stored(db)).toEqual(history(6).map(eventId));
  });

  it("saves the cursor on unsubscribe during a sparse backfill and resumes without restarting at 0", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    let pageCount = 0;
    const cursorsSeen: (string | undefined)[] = [];
    const adapter: EventAdapter = {
      fetchEvents: async (_d, o: FetchOptions) => {
        cursorsSeen.push(o.cursor);
        pageCount++;
        if (pageCount <= 4) {
          return {
            events: [],
            nextCursor: String(pageCount * 10),
            hasMore: true,
            throughVersion: BigInt(pageCount * 10),
          };
        }
        return new Promise(() => {});
      },
    };
    const first = new StreamdClient({ adapter });
    const sub = storeEvents(first, store, swaps, { name: "swaps", fromVersion: 0n, pollInterval: 5 });
    for (let i = 0; i < 200 && pageCount < 5; i++) await new Promise((r) => setTimeout(r, 2));
    sub.unsubscribe();
    first.destroy();
    expect(store.readCursor("swaps")).toBe("40");

    const resumedCursors: (string | undefined)[] = [];
    const second = new StreamdClient({
      adapter: {
        fetchEvents: async (_d, o: FetchOptions) => {
          resumedCursors.push(o.cursor);
          return { events: [], nextCursor: "50", hasMore: false };
        },
      },
    });
    const sub2 = await caughtUp((onCaughtUp) => storeEvents(second, store, swaps, { name: "swaps", fromVersion: 0n, onCaughtUp }));
    sub2.unsubscribe();
    second.destroy();
    expect(cursorsSeen[0]).toBeUndefined();
    expect(resumedCursors[0]).toBe("40");
  });

  describe("periodic empty-page checkpoints", () => {
    const realImmediate = globalThis.setImmediate;
    const flush = () => new Promise<void>((r) => realImmediate(r));
    beforeEach(() => vi.useFakeTimers());
    afterEach(() => vi.useRealTimers());

    it("saves the cursor every 5s while scanning empty pages before catch-up or stop", async () => {
      const db = new Database(":memory:");
      const store = new SqliteEventStore(db);
      let fetches = 0;
      const adapter: EventAdapter = {
        fetchEvents: async () => {
          fetches++;
          if (fetches === 1) return { events: [], nextCursor: "100", hasMore: true, throughVersion: 100n };
          return new Promise(() => {});
        },
      };
      const client = new StreamdClient({ adapter });
      const sub = storeEvents(client, store, swaps, { name: "swaps", fromVersion: 0n, pollInterval: 100 });
      await flush();
      vi.advanceTimersByTime(0);
      await flush();
      expect(fetches).toBeGreaterThanOrEqual(1);
      expect(store.readCursor("swaps")).toBeUndefined();

      vi.advanceTimersByTime(5_000);
      expect(store.readCursor("swaps")).toBe("100");
      sub.unsubscribe();
      client.destroy();
    });
  });
});

const DEPOSIT = "0x1::fungible_asset::Deposit";

function transfers(count: number): StreamdEvent<Transfer>[] {
  return Array.from({ length: count }, (_, i) => ({
    type: DEPOSIT,
    data: { asset: "0xa", amount: BigInt(i + 1), to: "0xb" },
    version: BigInt(i + 1),
    sequenceNumber: 0n,
    eventIndex: 3,
  }));
}

interface FakeOptions extends SubscriptionOptions {
  asset?: string;
}

/** A chain client's transfers(), over a MemoryAdapter whose events already are records. */
function fake(
  records: StreamdEvent<Transfer>[],
  adapter = new MemoryAdapter(records),
): TransferClient<FakeOptions> & { destroy(): void } {
  const client = new StreamdClient({ adapter });
  return {
    transfers: (callback: EventCallback<Transfer>, options?: FakeOptions) => {
      const { asset: _asset, ...rest } = options ?? {};
      return client.on({ kind: "module", eventType: DEPOSIT }, callback as EventCallback, rest);
    },
    destroy: () => client.destroy(),
  };
}

function storedTransfers(db: Database): string[] {
  return (db.query("SELECT version, event_index, amount FROM streamd_transfers ORDER BY version").all() as {
    version: string;
    event_index: number;
    amount: string;
  }[]).map((r) => `${BigInt(r.version)}:${r.event_index}:${r.amount}`);
}

const ids = (records: StreamdEvent<Transfer>[]) => records.map((r) => `${r.version}:${r.eventIndex}:${r.data.amount}`);

describe("storeEvents over a resource descriptor", () => {
  it("stores each change with the descriptor's address, which StoreAdapter then serves", async () => {
    const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
    const coin: EventDescriptor = { kind: "resource", accountAddress: "0xa", resourceType: COIN };
    const changes = history(5).map((e) => ({ ...e, type: COIN, eventIndex: 2 }));
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const client = new StreamdClient({ adapter: new MemoryAdapter([...changes, ...history(5)]) });
    const options = { batchSize: 2, pollInterval: 5 };
    const subs = await Promise.all([
      caughtUp((onCaughtUp) => storeEvents(client, store, coin, { ...options, name: "coin", onCaughtUp })),
      caughtUp((onCaughtUp) => storeEvents(client, store, swaps, { ...options, name: "swaps", onCaughtUp })),
    ]);
    for (const sub of subs) sub.unsubscribe();
    client.destroy();

    const rows = db.query("SELECT type, account_address FROM streamd_events ORDER BY version, event_index").all();
    expect(rows).toHaveLength(10);
    const A = "0x" + "0".repeat(63) + "a";
    for (const row of rows as { type: string; account_address: string | null }[]) {
      expect(row.account_address).toBe(row.type === COIN ? A : null);
    }
    const served = await new StreamdClient({ adapter: new StoreAdapter(db) }).getEvents(coin, { limit: 100 });
    expect(served).toEqual(changes);
  });
});

describe("storeTransfers", () => {
  it("resumes after a restart with every record stored once, under its own cursor name", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.write([], { name: "t", cursor: "event-cursor" });
    const options = { name: "t", batchSize: 2, pollInterval: 5, asset: "0xa" };

    const first = fake(transfers(5));
    const sub = await caughtUp((onCaughtUp) => storeTransfers(first, store, { ...options, onCaughtUp }));
    sub.unsubscribe();
    first.destroy();
    expect(storedTransfers(db)).toEqual(ids(transfers(5)));
    expect(store.readCursor("transfers:t")).toBe("5");
    expect(store.readCursor("t")).toBe("event-cursor");

    const second = fake(transfers(9));
    const seen: string[] = [];
    const resumed = await caughtUp((onCaughtUp) =>
      storeTransfers(second, store, { ...options, onCaughtUp, where: (e) => (seen.push(eventId(e)), true) }),
    );
    resumed.unsubscribe();
    second.destroy();
    expect(seen[0]).toBe("6:3");
    expect(storedTransfers(db)).toEqual(ids(transfers(9)));
  });

  it("absorbs a replayed range: records redelivered from an older cursor replace their rows", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const first = fake(transfers(4));
    const sub = await caughtUp((onCaughtUp) => storeTransfers(first, store, { name: "t", batchSize: 2, onCaughtUp }));
    sub.unsubscribe();
    first.destroy();

    // As after a crash between a record's write and the cursor saved after its page.
    store.write([], { name: "transfers:t", cursor: "0" });
    const again = fake(transfers(4));
    const seen: string[] = [];
    const replayed = await caughtUp((onCaughtUp) =>
      storeTransfers(again, store, { name: "t", batchSize: 2, onCaughtUp, where: (e) => (seen.push(eventId(e)), true) }),
    );
    replayed.unsubscribe();
    again.destroy();
    expect(seen).toEqual(["1:3", "2:3", "3:3", "4:3"]);
    expect(storedTransfers(db)).toEqual(ids(transfers(4)));
  });

  it("ends on a failed write without moving the saved cursor past it, and a restart fills the gap", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeTransfers([]);
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_transfers
      WHEN NEW.version >= '00000000000000000003' BEGIN SELECT RAISE(ABORT, 'disk full'); END`);
    const client = fake(transfers(6));
    const error = await new Promise<Error>((resolve) => {
      storeTransfers(client, store, { name: "t", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    client.destroy();
    expect((error.cause as Error).message).toContain("disk full");
    expect(storedTransfers(db)).toEqual(ids(transfers(2)));
    expect(store.readCursor("transfers:t")).toBe("2");

    db.exec("DROP TRIGGER boom");
    const restarted = fake(transfers(6));
    const sub = await caughtUp((onCaughtUp) => storeTransfers(restarted, store, { name: "t", batchSize: 2, onCaughtUp }));
    sub.unsubscribe();
    restarted.destroy();
    expect(storedTransfers(db)).toEqual(ids(transfers(6)));
  });

  it("saves the cursor after a page whose last record an async predicate drops", async () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const adapter = new MemoryAdapter(transfers(2));
    const client = fake([], adapter);
    // Even versions are dropped, slowly, after the odd one before them was written.
    const where = (e: StreamdEvent) => new Promise<boolean>((r) => setTimeout(() => r(e.version % 2n === 1n), 15));
    const sub = await caughtUp((onCaughtUp) =>
      storeTransfers(client, store, { name: "t", batchSize: 2, pollInterval: 5, where, onCaughtUp }),
    );
    expect(store.readCursor("transfers:t")).toBe("2");
    adapter.push(...transfers(4).slice(2));
    // Without the wait for the page to finish, the save lands on "2" and stays there.
    for (let i = 0; i < 300 && store.readCursor("transfers:t") !== "4"; i++) await new Promise((r) => setTimeout(r, 10));
    sub.unsubscribe();
    client.destroy();
    expect(store.readCursor("transfers:t")).toBe("4");
  });

  it("type-checks the options against the client's own", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const client = fake([]);
    // @ts-expect-error asset is a string on this client
    storeTransfers(client, store, { name: "t", asset: 5 }).unsubscribe();
    // @ts-expect-error name is required
    expect(() => storeTransfers(client, store, { asset: "0xa" })).toThrow("needs a name");
    client.destroy();
  });
});

interface RecordOptions extends SubscriptionOptions {
  protocol?: string[];
  token?: string;
}

function swapRecords(count: number): StreamdEvent<StoredSwap>[] {
  return Array.from({ length: count }, (_, i) => ({
    type: SWAP,
    data: { protocol: "hyperion", poolId: "0xp", assetIn: "0xa", amountIn: BigInt(i + 1), assetOut: "0xb", amountOut: 1n },
    version: BigInt(i + 1),
    sequenceNumber: 0n,
    eventIndex: 1,
  }));
}

function saleRecords(count: number): StreamdEvent<StoredSale>[] {
  return Array.from({ length: count }, (_, i) => ({
    type: SWAP,
    data: { marketplace: "wapal", kind: "listing", token: "0xt", seller: "0xs", buyer: "0xb", price: BigInt(i + 1) },
    version: BigInt(i + 1),
    sequenceNumber: 0n,
    eventIndex: 2,
  }));
}

/** A chain client's swaps() and sales(), over MemoryAdapters whose events already are records. */
function recordClient(swapList: StreamdEvent[], saleList: StreamdEvent[]): SwapClient<RecordOptions> & SaleClient<RecordOptions> & { destroy(): void } {
  const swapsClient = new StreamdClient({ adapter: new MemoryAdapter(swapList) });
  const salesClient = new StreamdClient({ adapter: new MemoryAdapter(saleList) });
  const strip = ({ protocol: _p, token: _t, ...rest }: RecordOptions = {}) => rest;
  return {
    swaps: (callback, options) => swapsClient.on(swaps, callback as EventCallback, strip(options)),
    sales: (callback, options) => salesClient.on(swaps, callback as EventCallback, strip(options)),
    destroy: () => (swapsClient.destroy(), salesClient.destroy()),
  };
}

const keys = (db: Database, table: string) =>
  (db.query(`SELECT version, event_index FROM ${table} ORDER BY version`).all() as { version: string; event_index: number }[]).map(
    (r) => `${BigInt(r.version)}:${r.event_index}`,
  );

describe("storeSwaps and storeSales", () => {
  it("resume after a restart with every record stored once, each under its own cursor name", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const options = { name: "r", batchSize: 2, pollInterval: 5 };
    const first = recordClient(swapRecords(3), saleRecords(4));
    const subs = await Promise.all([
      caughtUp((onCaughtUp) => storeSwaps(first, store, { ...options, protocol: ["hyperion"], onCaughtUp })),
      caughtUp((onCaughtUp) => storeSales(first, store, { ...options, token: "0xt", onCaughtUp })),
    ]);
    for (const sub of subs) sub.unsubscribe();
    first.destroy();
    expect(keys(db, "streamd_swaps")).toEqual(swapRecords(3).map(eventId));
    expect(keys(db, "streamd_sales")).toEqual(saleRecords(4).map(eventId));
    expect(store.readCursor("swaps:r")).toBe("3");
    expect(store.readCursor("sales:r")).toBe("4");

    const second = recordClient(swapRecords(5), saleRecords(6));
    const seen: string[] = [];
    const where = (e: StreamdEvent) => (seen.push(eventId(e)), true);
    const resumed = await Promise.all([
      caughtUp((onCaughtUp) => storeSwaps(second, store, { ...options, where, onCaughtUp })),
      caughtUp((onCaughtUp) => storeSales(second, store, { ...options, where, onCaughtUp })),
    ]);
    for (const sub of resumed) sub.unsubscribe();
    second.destroy();
    expect(seen.sort()).toEqual(["4:1", "5:1", "5:2", "6:2"]);
    expect(keys(db, "streamd_swaps")).toEqual(swapRecords(5).map(eventId));
    expect(keys(db, "streamd_sales")).toEqual(saleRecords(6).map(eventId));
  });

  it("ends on a failed write without moving the saved cursor past it", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writeSwaps([]);
    db.exec(`CREATE TRIGGER boom BEFORE INSERT ON streamd_swaps
      WHEN NEW.version >= '00000000000000000003' BEGIN SELECT RAISE(ABORT, 'disk full'); END`);
    const client = recordClient(swapRecords(6), []);
    const error = await new Promise<Error>((resolve) => {
      storeSwaps(client, store, { name: "s", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    client.destroy();
    expect((error.cause as Error).message).toContain("disk full");
    expect(keys(db, "streamd_swaps")).toEqual(["1:1", "2:1"]);
    expect(store.readCursor("swaps:s")).toBe("2");
  });

  it("needs a name", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const client = recordClient([], []);
    // @ts-expect-error name is required
    expect(() => storeSwaps(client, store, {})).toThrow("needs a name");
    expect(() => storeSales(client, store, { name: "" })).toThrow("needs a name");
    client.destroy();
  });
});

describe("storePoints", () => {
  const A = `0x${"a".padStart(64, "0")}`;
  const B = `0x${"b".padStart(64, "0")}`;
  const deed = (version: number, sender: string): StreamdEvent => ({
    type: SWAP,
    data: {},
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
    sender,
  });
  const deeds = [deed(1, "0xa"), deed(2, "0xb"), deed(3, "0xa")];
  const board = () => points({ rules: [{ id: "swap", points: 10n, firstTime: 5n }] });
  const rows = (db: Database) =>
    db.prepare("SELECT name, address, points, version FROM streamd_points ORDER BY address").all();

  it("writes and reads a leaderboard with its cursor in one write", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    expect(store.readPoints("p")).toEqual(new Map());
    const f = board();
    const state = deeds.reduce(f, f.initial);
    store.writePoints("p", state.values(), { name: "points:p", cursor: "c" });
    expect(store.readPoints("p")).toEqual(state);
    expect(store.readPoints("other")).toEqual(new Map());
    expect(store.readCursor("points:p")).toBe("c");
    expect(() => store.writePoints("", [])).toThrow("non-empty name");
  });

  it("folds the history into rows, then writes only a live event's row with the cursor", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const adapter = new MemoryAdapter(deeds);
    const client = new StreamdClient({ adapter });
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    let result: Reduction<PointsState> | undefined;
    let atChange: string | undefined;
    const changed = new Promise<void>((resolve) => {
      const reduction = storePoints(client, store, swaps, board(), {
        name: "p",
        pollInterval: 5,
        onChange: () => {
          atChange = reduction.cursor;
          resolve();
        },
      });
      void reduction.ready.then(() => {
        expect(rows(db)).toEqual([
          { name: "p", address: A, points: "25", version: "00000000000000000003" },
          { name: "p", address: B, points: "15", version: "00000000000000000002" },
        ]);
        expect(store.readCursor("points:p")).toBe(reduction.cursor);
        spy = vi.spyOn(store, "writePoints");
        adapter.push(deed(4, "0xb"));
      });
      result = reduction;
    });
    await changed;
    expect(spy).toHaveBeenCalledTimes(1);
    const [name, scores, checkpoint] = spy!.mock.calls[0] as [string, Score[], { name: string; cursor: string }];
    expect(name).toBe("p");
    expect(scores.map((s) => s.address)).toEqual([B]);
    expect(checkpoint).toEqual({ name: "points:p", cursor: atChange! });
    expect(store.readCursor("points:p")).toBe(atChange);
    expect(rows(db)).toEqual([
      { name: "p", address: A, points: "25", version: "00000000000000000003" },
      { name: "p", address: B, points: "25", version: "00000000000000000004" },
    ]);
    result!.unsubscribe();
    client.destroy();
  });

  it("resumes from the saved rows and cursor without folding the history again", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const first = new StreamdClient({ adapter: new MemoryAdapter(deeds) });
    const r1 = storePoints(first, store, swaps, board(), { name: "p" });
    await r1.ready;
    r1.unsubscribe();
    first.destroy();

    const seen: string[] = [];
    const later = [...deeds, deed(4, "0xb"), deed(5, "0xa")];
    const second = new StreamdClient({ adapter: new MemoryAdapter(later) });
    let r2: Reduction<PointsState> | undefined;
    await new Promise<void>((resolve) => {
      r2 = storePoints(second, store, swaps, board(), {
        name: "p",
        pollInterval: 5,
        fromVersion: 0n,
        where: (e) => (seen.push(eventId(e)), true),
        onChange: (state) => state.get(A)?.version === 5n && resolve(),
      });
    });
    r2!.unsubscribe();
    second.destroy();
    expect(seen).toEqual(["4:0", "5:0"]);
    const f = board();
    expect(store.readPoints("p")).toEqual(later.reduce(f, f.initial));
  });

  it("stops and reports a failed live write", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    const adapter = new MemoryAdapter(deeds);
    const client = new StreamdClient({ adapter });
    let reduction: Reduction<PointsState> | undefined;
    const error = await new Promise<Error>((resolve) => {
      reduction = storePoints(client, store, swaps, board(), { name: "p", pollInterval: 5, onError: resolve });
      void reduction.ready.then(() => {
        db.exec("CREATE TRIGGER boom BEFORE UPDATE ON streamd_points BEGIN SELECT RAISE(ABORT, 'disk full'); END");
        adapter.push(deed(4, "0xa"));
      });
    });
    await new Promise((r) => setTimeout(r, 20));
    client.destroy();
    expect((error.cause as Error).message).toContain("disk full");
    expect(reduction!.active).toBe(false);
    expect(store.readPoints("p").get(A)?.version).toBe(3n);
  });

  it("rejects ready and reports a failed backfill write", async () => {
    const db = new Database(":memory:");
    const store = new SqliteEventStore(db);
    store.writePoints("p", []);
    db.exec("CREATE TRIGGER boom BEFORE INSERT ON streamd_points BEGIN SELECT RAISE(ABORT, 'disk full'); END");
    const client = new StreamdClient({ adapter: new MemoryAdapter(deeds) });
    const reported: Error[] = [];
    const reduction = storePoints(client, store, swaps, board(), { name: "p", onError: (e) => reported.push(e) });
    await expect(reduction.ready).rejects.toThrow("disk full");
    client.destroy();
    expect(reported.map((e) => e.message)).toEqual([expect.stringContaining("disk full")]);
    expect(reduction.active).toBe(false);
    expect(store.readCursor("points:p")).toBeUndefined();
  });

  it("refuses a missing name or a cursor", () => {
    const store = new SqliteEventStore(new Database(":memory:"));
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    expect(() => storePoints(client, store, swaps, board(), { name: "" })).toThrow("needs a name");
    expect(() => storePoints(client, store, swaps, board(), { name: "p", cursor: "x" } as never)).toThrow("from the store");
    client.destroy();
  });
});
