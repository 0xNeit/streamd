import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, eventId } from "@streamd/core";
import type { EventCallback, EventDescriptor, StreamdEvent, Subscription, SubscriptionOptions, Transfer } from "@streamd/core";
import { SqliteEventStore, StoreAdapter, storeEvents, storeTransfers } from "../src/index.js";
import type { TransferClient } from "../src/index.js";

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
function fake(records: StreamdEvent<Transfer>[]): TransferClient<FakeOptions> & { destroy(): void } {
  const client = new StreamdClient({ adapter: new MemoryAdapter(records) });
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
