import { PGlite } from "./helpers.js";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, eventId, points } from "@streamd/core";
import type { EventAdapter, EventCallback, EventDescriptor, PointsState, Reduction, Score, StreamdEvent, Subscription, SubscriptionOptions, Transfer } from "@streamd/core";
import { PostgresEventStore, PostgresStoreAdapter, storeEvents, storePoints, storeSales, storeSwaps, storeTransfers } from "../src/index.js";
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

async function caughtUp(start: (done: () => void) => Promise<Subscription>): Promise<Subscription> {
  let fire!: () => void;
  const done = new Promise<void>((resolve) => (fire = resolve));
  const sub = await start(() => fire());
  await done;
  return sub;
}

async function stored(db: PGlite): Promise<string[]> {
  const events = await new StreamdClient({ adapter: new PostgresStoreAdapter(db) }).getEvents(swaps, { limit: 1000 });
  return events.map(eventId);
}

describe("storeEvents", () => {
  it("resumes after a restart with every event stored exactly once", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const options = { name: "swaps", batchSize: 2, pollInterval: 5 };

    const first = new StreamdClient({ adapter: new MemoryAdapter(history(5)) });
    const sub = await caughtUp((onCaughtUp) => storeEvents(first, store, swaps, { ...options, onCaughtUp }));
    sub.unsubscribe();
    first.destroy();
    expect(await stored(db)).toEqual(history(5).map(eventId));
    const saved = await store.readCursor("swaps");
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
    const db = new PGlite();
    const store = new PostgresEventStore(db);
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
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const options = { name: "swaps", fromVersion: -1n, where: (e: StreamdEvent) => e.version > 10n };
    const first = new StreamdClient({ adapter: new MemoryAdapter(history(10)) });
    let caught = 0;
    const sub = await caughtUp((done) =>
      storeEvents(first, store, swaps, { ...options, onCaughtUp: () => (caught++, done()) }),
    );
    sub.unsubscribe();
    first.destroy();
    expect(caught).toBe(1);
    expect(await store.readCursor("swaps")).toBe("10");

    const second = new StreamdClient({ adapter: new MemoryAdapter(history(30)) });
    const resumed = await caughtUp((onCaughtUp) => storeEvents(second, store, swaps, { ...options, onCaughtUp }));
    resumed.unsubscribe();
    second.destroy();
    expect(await stored(db)).toEqual(history(30).slice(10).map(eventId));
  });

  it("still calls onCaughtUp when saving the cursor there fails", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.readCursor("swaps");
    await db.exec(`
      CREATE FUNCTION busy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'busy'; END $$;
      CREATE TRIGGER boom BEFORE INSERT ON streamd_cursors FOR EACH ROW EXECUTE FUNCTION busy();
    `);
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
    const store = new PostgresEventStore(new PGlite());
    await store.write([], { name: "swaps", cursor: "4" });
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
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.readCursor("swaps");
    await db.exec(`
      CREATE FUNCTION full() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.version >= 3 THEN RAISE EXCEPTION 'disk full'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER boom BEFORE INSERT ON streamd_events FOR EACH ROW EXECUTE FUNCTION full();
    `);
    const client = new StreamdClient({ adapter: new MemoryAdapter(history(6)) });
    const error = await new Promise<Error>((resolve) => {
      void storeEvents(client, store, swaps, { name: "swaps", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(error.message).toContain("from 3:0 to 4:0");
    expect((error.cause as Error).message).toContain("disk full");
    expect(client.listenerCount()).toBe(0);
    expect(await stored(db)).toEqual(["1:0", "2:0"]);
    // Saved after the first batch, so a restart reads from the failed one again.
    const savedAfterFirst = await store.readCursor("swaps");
    expect(savedAfterFirst).toBe("2");
    client.destroy();

    await db.exec("DROP TRIGGER boom ON streamd_events");
    const restarted = new StreamdClient({ adapter: new MemoryAdapter(history(6)) });
    const sub = await caughtUp((onCaughtUp) =>
      storeEvents(restarted, store, swaps, { name: "swaps", batchSize: 2, onCaughtUp }),
    );
    sub.unsubscribe();
    restarted.destroy();
    expect(await stored(db)).toEqual(history(6).map(eventId));
  });

  it("saves the cursor on clean close during a sparse backfill and resumes past empty pages", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    let pageCount = 0;
    const adapter: EventAdapter = {
      fetchEvents: async () => {
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
    const sub = await storeEvents(first, store, swaps, { name: "swaps", fromVersion: 0n, pollInterval: 5 });
    for (let i = 0; i < 200 && pageCount < 5; i++) await new Promise((r) => setTimeout(r, 2));
    await (sub as unknown as { [Symbol.asyncDispose]: () => Promise<void> })[Symbol.asyncDispose]();
    first.destroy();
    expect(await store.readCursor("swaps")).toBe("40");

    const resumedCursors: (string | undefined)[] = [];
    const second = new StreamdClient({
      adapter: {
        fetchEvents: async (_d, o) => {
          resumedCursors.push(o.cursor);
          return { events: [], nextCursor: "50", hasMore: false };
        },
      },
    });
    const sub2 = await caughtUp((onCaughtUp) => storeEvents(second, store, swaps, { name: "swaps", fromVersion: 0n, onCaughtUp }));
    sub2.unsubscribe();
    second.destroy();
    expect(resumedCursors[0]).toBe("40");
  });

  it("stores an event holding a NUL character and carries on", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const events = history(3);
    events[1] = { ...events[1]!, data: { s: "a\u0000b" } };
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    const errors: Error[] = [];
    const sub = await caughtUp((onCaughtUp) =>
      storeEvents(client, store, swaps, { name: "swaps", batchSize: 1, onCaughtUp, onError: (e) => errors.push(e) }),
    );
    sub.unsubscribe();
    client.destroy();
    expect(errors).toEqual([]);
    expect(await stored(db)).toEqual(events.map(eventId));
  });

  it("sends a throwing onCaughtUp to onError", async () => {
    const store = new PostgresEventStore(new PGlite());
    const client = new StreamdClient({ adapter: new MemoryAdapter(history(2)) });
    const reached = new Promise<Error>((resolve) => {
      void storeEvents(client, store, swaps, {
        name: "swaps",
        onCaughtUp: () => {
          throw new Error("boom");
        },
        onError: resolve,
      });
    });
    const error = await reached;
    expect(error.message).toBe("onCaughtUp threw");
    expect((error.cause as Error).message).toBe("boom");
    client.destroy();
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

/** A chain client's transfers(), over a MemoryAdapter whose events already are records. */
function fake(records: StreamdEvent<Transfer>[]): TransferClient<SubscriptionOptions & { asset?: string }> & { destroy(): void } {
  const client = new StreamdClient({ adapter: new MemoryAdapter(records) });
  return {
    transfers: (callback, options) => {
      const { asset: _asset, ...rest } = options ?? {};
      return client.on({ kind: "module", eventType: DEPOSIT }, callback as EventCallback, rest);
    },
    destroy: () => client.destroy(),
  };
}

async function storedTransfers(db: PGlite): Promise<string[]> {
  const { rows } = await db.query<{ id: string }>(
    "SELECT version || ':' || event_index || ':' || amount AS id FROM streamd_transfers ORDER BY version",
  );
  return rows.map((r) => r.id);
}

const ids = (records: StreamdEvent<Transfer>[]) => records.map((r) => `${r.version}:${r.eventIndex}:${r.data.amount}`);

describe("storeEvents over a resource descriptor", () => {
  it("stores each change with the descriptor's address, which PostgresStoreAdapter then serves", async () => {
    const COIN = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
    const coin: EventDescriptor = { kind: "resource", accountAddress: "0xa", resourceType: COIN };
    const changes = history(5).map((e) => ({ ...e, type: COIN, eventIndex: 2 }));
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const client = new StreamdClient({ adapter: new MemoryAdapter([...changes, ...history(5)]) });
    const options = { batchSize: 2, pollInterval: 5 };
    const subs = await Promise.all([
      caughtUp((onCaughtUp) => storeEvents(client, store, coin, { ...options, name: "coin", onCaughtUp })),
      caughtUp((onCaughtUp) => storeEvents(client, store, swaps, { ...options, name: "swaps", onCaughtUp })),
    ]);
    for (const sub of subs) sub.unsubscribe();
    client.destroy();

    const { rows } = await db.query<{ type: string; account_address: string | null }>(
      "SELECT type, account_address FROM streamd_events ORDER BY version, event_index",
    );
    expect(rows).toHaveLength(10);
    const A = "0x" + "0".repeat(63) + "a";
    for (const row of rows) expect(row.account_address).toBe(row.type === COIN ? A : null);
    const served = await new StreamdClient({ adapter: new PostgresStoreAdapter(db) }).getEvents(coin, { limit: 100 });
    expect(served).toEqual(changes);
  });
});

describe("storeTransfers", () => {
  it("resumes with every record once under its own cursor name, and absorbs a replayed range", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.write([], { name: "t", cursor: "event-cursor" });
    const options = { name: "t", batchSize: 2, pollInterval: 5, asset: "0xa" };
    const run = async (count: number, seen: string[] = []) => {
      const client = fake(transfers(count));
      const sub = await caughtUp((onCaughtUp) =>
        storeTransfers(client, store, { ...options, onCaughtUp, where: (e) => (seen.push(eventId(e)), true) }),
      );
      sub.unsubscribe();
      client.destroy();
      return seen;
    };

    await run(5);
    expect(await storedTransfers(db)).toEqual(ids(transfers(5)));
    expect(await store.readCursor("transfers:t")).toBe("5");
    expect(await store.readCursor("t")).toBe("event-cursor");

    expect((await run(9))[0]).toBe("6:3");
    expect(await storedTransfers(db)).toEqual(ids(transfers(9)));

    // As after a crash between a record's write and the cursor saved after its page.
    await store.write([], { name: "transfers:t", cursor: "0" });
    expect(await run(9)).toEqual(transfers(9).map(eventId));
    expect(await storedTransfers(db)).toEqual(ids(transfers(9)));
  });

  it("ends on a failed write without moving the saved cursor past it", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeTransfers([]);
    await db.exec(`
      CREATE FUNCTION full() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.version >= 3 THEN RAISE EXCEPTION 'disk full'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER boom BEFORE INSERT ON streamd_transfers FOR EACH ROW EXECUTE FUNCTION full();
    `);
    const client = fake(transfers(6));
    const error = await new Promise<Error>((resolve) => {
      void storeTransfers(client, store, { name: "t", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    client.destroy();
    expect((error.cause as Error).message).toContain("disk full");
    expect(await storedTransfers(db)).toEqual(ids(transfers(2)));
    expect(await store.readCursor("transfers:t")).toBe("2");
    // @ts-expect-error asset is a string on this client
    await expect(storeTransfers(client, store, { name: "t", asset: 5 }).then((s) => s.unsubscribe())).resolves.toBeUndefined();
    // @ts-expect-error name is required
    await expect(storeTransfers(client, store, {})).rejects.toThrow("needs a name");
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

async function keys(db: PGlite, table: string): Promise<string[]> {
  const { rows } = await db.query<{ version: string; event_index: number }>(
    `SELECT version::text, event_index FROM ${table} ORDER BY version`,
  );
  return rows.map((r) => `${r.version}:${r.event_index}`);
}

describe("storeSwaps and storeSales", () => {
  it("resume after a restart with every record stored once, each under its own cursor name", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const options = { name: "r", batchSize: 2, pollInterval: 5 };
    const first = recordClient(swapRecords(3), saleRecords(4));
    const subs = await Promise.all([
      caughtUp((onCaughtUp) => storeSwaps(first, store, { ...options, protocol: ["hyperion"], onCaughtUp })),
      caughtUp((onCaughtUp) => storeSales(first, store, { ...options, token: "0xt", onCaughtUp })),
    ]);
    for (const sub of subs) sub.unsubscribe();
    first.destroy();
    expect(await keys(db, "streamd_swaps")).toEqual(swapRecords(3).map(eventId));
    expect(await keys(db, "streamd_sales")).toEqual(saleRecords(4).map(eventId));
    expect(await store.readCursor("swaps:r")).toBe("3");
    expect(await store.readCursor("sales:r")).toBe("4");

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
    expect(await keys(db, "streamd_swaps")).toEqual(swapRecords(5).map(eventId));
    expect(await keys(db, "streamd_sales")).toEqual(saleRecords(6).map(eventId));
  });

  it("ends on a failed write without moving the saved cursor past it, and needs a name", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writeSales([]);
    await db.exec(`
      CREATE FUNCTION full_sales() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.version >= 3 THEN RAISE EXCEPTION 'disk full'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER boom BEFORE INSERT ON streamd_sales FOR EACH ROW EXECUTE FUNCTION full_sales();
    `);
    const client = recordClient([], saleRecords(6));
    const error = await new Promise<Error>((resolve) => {
      void storeSales(client, store, { name: "s", batchSize: 2, pollInterval: 5, onError: resolve });
    });
    await new Promise((r) => setTimeout(r, 20));
    client.destroy();
    expect((error.cause as Error).message).toContain("disk full");
    expect(await keys(db, "streamd_sales")).toEqual(["1:2", "2:2"]);
    expect(await store.readCursor("sales:s")).toBe("2");
    // @ts-expect-error name is required
    await expect(storeSwaps(client, store, {})).rejects.toThrow("needs a name");
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
  const rows = async (db: PGlite) =>
    (await db.query(`SELECT name, address, points::text, version::text FROM streamd_points ORDER BY address`)).rows;
  const failFrom = (db: PGlite, version: number) =>
    db.exec(`
      CREATE FUNCTION full() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.version >= ${version} THEN RAISE EXCEPTION 'disk full'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER boom BEFORE INSERT ON streamd_points FOR EACH ROW EXECUTE FUNCTION full();
    `);

  it("writes and reads a leaderboard with its cursor in one statement", async () => {
    const store = new PostgresEventStore(new PGlite());
    expect(await store.readPoints("p")).toEqual(new Map());
    const f = board();
    const state = deeds.reduce(f, f.initial);
    await store.writePoints("p", state.values(), { name: "points:p", cursor: "c" });
    expect(await store.readPoints("p")).toEqual(state);
    expect(await store.readPoints("other")).toEqual(new Map());
    expect(await store.readCursor("points:p")).toBe("c");
    await expect(store.writePoints("", [])).rejects.toThrow("non-empty name");
  });

  it("folds the history into rows, then writes only a live event's row with the cursor", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const adapter = new MemoryAdapter(deeds);
    const client = new StreamdClient({ adapter });
    let atChange: string | undefined;
    let reduction: Reduction<PointsState> | undefined;
    const changed = new Promise<void>((resolve) => {
      void storePoints(client, store, swaps, board(), {
        name: "p",
        pollInterval: 5,
        onChange: () => {
          atChange = reduction!.cursor;
          resolve();
        },
      }).then((r) => (reduction = r));
    });
    while (reduction === undefined) await new Promise((r) => setTimeout(r, 1));
    await reduction.ready;
    expect(await rows(db)).toEqual([
      { name: "p", address: A, points: "25", version: "3" },
      { name: "p", address: B, points: "15", version: "2" },
    ]);
    expect(await store.readCursor("points:p")).toBe(reduction.cursor);
    const writes: [string, Score[], unknown][] = [];
    const write = store.writePoints.bind(store);
    store.writePoints = (name, scores, checkpoint) => (writes.push([name, [...scores], checkpoint]), write(name, scores, checkpoint));
    adapter.push(deed(4, "0xb"));
    await changed;
    for (let i = 0; i < 100 && (await store.readCursor("points:p")) !== atChange; i++) await new Promise((r) => setTimeout(r, 5));
    expect(writes.map(([n, s, c]) => [n, s.map((x) => x.address), c])).toEqual([["p", [B], { name: "points:p", cursor: atChange }]]);
    expect(await rows(db)).toEqual([
      { name: "p", address: A, points: "25", version: "3" },
      { name: "p", address: B, points: "25", version: "4" },
    ]);
    reduction.unsubscribe();
    client.destroy();
  });

  it("resumes from the saved rows and cursor without folding the history again", async () => {
    const store = new PostgresEventStore(new PGlite());
    const first = new StreamdClient({ adapter: new MemoryAdapter(deeds) });
    const r1 = await storePoints(first, store, swaps, board(), { name: "p" });
    await r1.ready;
    r1.unsubscribe();
    first.destroy();

    const seen: string[] = [];
    const later = [...deeds, deed(4, "0xb"), deed(5, "0xa")];
    const second = new StreamdClient({ adapter: new MemoryAdapter(later) });
    let done!: () => void;
    const reachedFive = new Promise<void>((resolve) => (done = resolve));
    const r2 = await storePoints(second, store, swaps, board(), {
      name: "p",
      pollInterval: 5,
      fromVersion: 0n,
      where: (e) => (seen.push(eventId(e)), true),
      onChange: (state) => state.get(A)?.version === 5n && done(),
    });
    await reachedFive;
    const f = board();
    const expected = later.reduce(f, f.initial);
    for (let i = 0; i < 100 && (await store.readPoints("p")).get(A)?.version !== 5n; i++) await new Promise((r) => setTimeout(r, 5));
    r2.unsubscribe();
    second.destroy();
    expect(seen).toEqual(["4:0", "5:0"]);
    expect(await store.readPoints("p")).toEqual(expected);
  });

  it("stops and reports a failed live write", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    const adapter = new MemoryAdapter(deeds);
    const client = new StreamdClient({ adapter });
    let reported!: (e: Error) => void;
    const failed = new Promise<Error>((resolve) => (reported = resolve));
    const reduction = await storePoints(client, store, swaps, board(), { name: "p", pollInterval: 5, onError: reported });
    await reduction.ready;
    await failFrom(db, 4);
    adapter.push(deed(4, "0xa"));
    const error = await failed;
    client.destroy();
    expect(error.message).toContain("disk full");
    expect(reduction.active).toBe(false);
    expect((await store.readPoints("p")).get(A)?.version).toBe(3n);
  });

  it("rejects ready and reports a failed backfill write", async () => {
    const db = new PGlite();
    const store = new PostgresEventStore(db);
    await store.writePoints("p", []);
    await failFrom(db, 0);
    const client = new StreamdClient({ adapter: new MemoryAdapter(deeds) });
    const reported: Error[] = [];
    const reduction = await storePoints(client, store, swaps, board(), { name: "p", onError: (e) => reported.push(e) });
    await expect(reduction.ready).rejects.toThrow("disk full");
    client.destroy();
    expect(reported.map((e) => e.message)).toEqual([expect.stringContaining("disk full")]);
    expect(reduction.active).toBe(false);
    expect(await store.readCursor("points:p")).toBeUndefined();
  });

  it("refuses a missing name or a cursor", async () => {
    const store = new PostgresEventStore(new PGlite());
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    await expect(storePoints(client, store, swaps, board(), { name: "" })).rejects.toThrow("needs a name");
    await expect(storePoints(client, store, swaps, board(), { name: "p", cursor: "x" } as never)).rejects.toThrow("from the store");
    client.destroy();
  });
});
