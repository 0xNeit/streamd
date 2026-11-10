import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import type { StoreOptions } from "../src/args.js";
import { openDatabase, runStore, type OpenDatabase } from "../src/store.js";

const TYPE = "0x1::coin::DepositEvent";
const OTHER = "0x1::coin::WithdrawEvent";

function ev(version: number, type = TYPE): StreamdEvent {
  return { type, data: { n: String(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

const options = (extra: Partial<StoreOptions> = {}): StoreOptions => ({
  descriptor: { kind: "module", eventType: [TYPE] },
  network: "mainnet",
  pollInterval: 1,
  fromVersion: 0n,
  db: "e.db",
  ...extra,
});

const stored = (db: Database) =>
  (db.query("SELECT version FROM streamd_events ORDER BY version").all() as { version: string }[]).map((r) => Number(r.version));
const cursors = (db: Database) => (db.query("SELECT name FROM streamd_cursors ORDER BY name").all() as { name: string }[]).map((r) => r.name);

/** Runs until `done` holds, then aborts, resolving to the exit code. */
async function run(adapter: EventAdapter, db: OpenDatabase, opts: StoreOptions, done: () => boolean) {
  // Each run would close the database a later assertion or restart reads.
  db.close = () => {};
  const errors: string[] = [];
  const controller = new AbortController();
  const client = new StreamdClient({ adapter });
  const running = runStore(opts, { client, open: () => db, error: (m) => errors.push(m), signal: controller.signal });
  // `open` is awaited, so the tables are not there on the first tick.
  await new Promise((r) => setTimeout(r, 2));
  for (let i = 0; i < 300 && !done(); i++) await new Promise((r) => setTimeout(r, 2));
  // Let the cursor save that follows a batch land before stopping.
  await new Promise((r) => setTimeout(r, 10));
  controller.abort();
  const code = await running;
  client.destroy();
  return { code, errors };
}

describe("runStore", () => {
  it("stores events and its cursor, and exits 0 on abort", async () => {
    const db = new Database(":memory:");
    const { code, errors } = await run(new MemoryAdapter([ev(1), ev(2)]), db, options(), () => stored(db).length === 2);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(stored(db)).toEqual([1, 2]);
    expect(cursors(db)).toEqual([`mainnet:${JSON.stringify(options().descriptor)}`]);
  });

  it("resumes from the saved cursor, ignoring --from-version, with no repeat", async () => {
    const db = new Database(":memory:");
    const events = [ev(1), ev(2)];
    await run(new MemoryAdapter(events), db, options(), () => stored(db).length === 2);
    const fetched: unknown[] = [];
    const memory = new MemoryAdapter([...events, ev(3)]);
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => (fetched.push(o.cursor), memory.fetchEvents(d, o)),
      getHeadVersion: () => memory.getHeadVersion(),
    };
    await run(adapter, db, options({ fromVersion: 1n }), () => stored(db).length === 3);
    expect(stored(db)).toEqual([1, 2, 3]);
    expect(fetched[0]).toBeDefined();
  });

  it("keeps separate cursors per descriptor and per --name", async () => {
    const db = new Database(":memory:");
    const adapter = new MemoryAdapter([ev(1), ev(2, OTHER)]);
    await run(adapter, db, options(), () => stored(db).length === 1);
    const other = options({ descriptor: { kind: "module", eventType: [OTHER] } });
    await run(adapter, db, other, () => stored(db).length === 2);
    await run(adapter, db, options({ name: "mine" }), () => cursors(db).length === 3);
    expect(cursors(db)).toHaveLength(3);
    expect(cursors(db)).toContain("mine");
  });

  it("exits 1 when polling gives up", async () => {
    const db = new Database(":memory:");
    const errors: string[] = [];
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdNetworkError("down", 503);
      },
    };
    const client = new StreamdClient({ adapter, defaults: { maxErrors: 2, maxBackoff: 1 } });
    const code = await runStore(options(), { client, open: () => db, error: (m) => errors.push(m), signal: new AbortController().signal });
    expect(code).toBe(1);
    expect(errors.length).toBeGreaterThan(0);
    client.destroy();
  });

  it("exits 1 when a write fails", async () => {
    const db = new Database(":memory:");
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const open = () => {
      db.exec("CREATE TABLE streamd_events (version TEXT, event_index INTEGER, type TEXT, sender TEXT, guid_address TEXT, guid_creation TEXT, event TEXT, PRIMARY KEY (version, event_index))");
      db.exec("CREATE TRIGGER no_writes BEFORE INSERT ON streamd_events BEGIN SELECT RAISE(ABORT, 'disk full'); END");
      return db;
    };
    const code = await runStore(options(), { client, open, error: (m) => errors.push(m), signal: new AbortController().signal });
    expect(code).toBe(1);
    expect(errors.join()).toContain("disk full");
    client.destroy();
  });

  it("exits 1, naming the file, when it cannot be opened", async () => {
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    const open = () => {
      throw new Error("unable to open");
    };
    expect(await runStore(options(), { client, open, error: (m) => errors.push(m), signal: new AbortController().signal })).toBe(1);
    expect(errors).toEqual(["streamd: cannot open e.db: unable to open"]);
    client.destroy();
  });

  it("exits 0 without subscribing when already aborted, and closes the database", async () => {
    let closed = 0;
    const db = new Database(":memory:") as unknown as OpenDatabase;
    db.close = () => void closed++;
    const controller = new AbortController();
    controller.abort();
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new Error("must not fetch");
      },
    };
    const client = new StreamdClient({ adapter });
    expect(await runStore(options(), { client, open: () => db, error: () => {}, signal: controller.signal })).toBe(0);
    expect(closed).toBe(1);
    client.destroy();
  });

  it("opens a real file under this runtime", async () => {
    const db = await openDatabase(":memory:");
    db.exec("CREATE TABLE t (x)");
    db.close?.();
  });
});
