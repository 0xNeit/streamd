import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent, Subscription } from "@streamd/core";
import { SqliteEventStore, StoreAdapter, storeEvents } from "../src/index.js";

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
