import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, eventId } from "@streamd/core";
import type { EventDescriptor, StreamdEvent, Subscription } from "@streamd/core";
import { PostgresEventStore, PostgresStoreAdapter, storeEvents } from "../src/index.js";

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
