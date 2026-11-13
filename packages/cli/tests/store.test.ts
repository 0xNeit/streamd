import { PGlite } from "@electric-sql/pglite";
import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import type { StoreOptions } from "../src/args.js";
import { openDatabase, redactUrl, runStore, type OpenDatabase, type PostgresConnection } from "../src/store.js";

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

/** The other backend's opener, which a run must not call. */
const refuse = (): never => {
  throw new Error("opened the wrong backend");
};

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
  const running = runStore(opts, { client, connect: refuse, open: () => db, error: (m) => errors.push(m), signal: controller.signal });
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

  it("prefixes the cursor name with sui under --chain sui", async () => {
    const db = new Database(":memory:");
    await run(new MemoryAdapter([ev(1)]), db, options({ chain: "sui" }), () => cursors(db).length === 1);
    expect(cursors(db)).toEqual([`sui:mainnet:${JSON.stringify(options().descriptor)}`]);
  });

  it("names the cursor for a handle by creation number", async () => {
    const db = new Database(":memory:");
    const descriptor = { kind: "handle", accountAddress: "0x1", creationNumber: 7n } as const;
    const adapter: EventAdapter = { fetchEvents: async () => ({ events: [ev(1)], nextCursor: "1", hasMore: false }) };
    const { code, errors } = await run(adapter, db, options({ descriptor }), () => cursors(db).length === 1);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(cursors(db)[0]).toContain('"creationNumber":"7"');
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
    const code = await runStore(options(), { client, connect: refuse, open: () => db, error: (m) => errors.push(m), signal: new AbortController().signal });
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
    const code = await runStore(options(), { client, connect: refuse, open, error: (m) => errors.push(m), signal: new AbortController().signal });
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
    expect(await runStore(options(), { client, connect: refuse, open, error: (m) => errors.push(m), signal: new AbortController().signal })).toBe(1);
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
    expect(await runStore(options(), { client, connect: refuse, open: () => db, error: () => {}, signal: controller.signal })).toBe(0);
    expect(closed).toBe(1);
    client.destroy();
  });

  it("opens a real file under this runtime", async () => {
    const db = await openDatabase(":memory:");
    db.exec("CREATE TABLE t (x)");
    db.close?.();
  });
});

describe("runStore filter", () => {
  it("stores only matching events, under a cursor name that carries the filter", async () => {
    const db = new Database(":memory:");
    const filter = { senders: [], functions: [], where: [{ path: "n", op: "==" as const, value: "2" }] };
    await run(new MemoryAdapter([ev(1), ev(2), ev(3)]), db, options({ filter }), () => stored(db).length === 1);
    expect(stored(db)).toEqual([2]);
    expect(cursors(db)).toEqual([`mainnet:${JSON.stringify(options().descriptor)}:${JSON.stringify(filter)}`]);
  });
});

describe("runStore decode", () => {
  it("stores rows as they arrive, under the undecoded cursor name", async () => {
    const db = new Database(":memory:");
    await run(new MemoryAdapter([ev(1), ev(2)]), db, options({ decode: ["n"] }), () => stored(db).length === 2);
    expect(stored(db)).toEqual([1, 2]);
    expect(cursors(db)).toEqual([`mainnet:${JSON.stringify(options().descriptor)}`]);
    const data = (db.query("SELECT * FROM streamd_events ORDER BY version").all() as Record<string, unknown>[]).map((r) => JSON.stringify(r));
    expect(data.join()).toContain('\\"n\\":\\"1\\"');
  });
});

const URL_WITH_PASSWORD = "postgres://streamd:s3cr%23t@db.example:5432/events?sslmode=require&password=other1";

const pgOptions = (extra: Partial<StoreOptions> = {}): StoreOptions => {
  const { db: _db, ...rest } = options(extra);
  return { ...rest, postgres: URL_WITH_PASSWORD };
};

/** A PGlite database as the connection, counting closes and leaving it open for read-back. */
function pglite(pg = new PGlite()) {
  const connection = { query: (text: string, params?: unknown[]) => pg.query<Record<string, unknown>>(text, params), closed: 0, close: () => void connection.closed++ };
  return { pg, connection };
}

const pgStored = async (pg: PGlite) =>
  (await pg.query<{ version: string }>("SELECT version::text FROM streamd_events ORDER BY version")).rows.map((r) => Number(r.version));
const pgCursors = async (pg: PGlite) =>
  (await pg.query<{ name: string }>("SELECT name FROM streamd_cursors ORDER BY name")).rows.map((r) => r.name);

/** Runs until `done` holds, then aborts, resolving to the exit code. */
async function pgRun(adapter: EventAdapter, connection: PostgresConnection, opts: StoreOptions, done: () => Promise<boolean>) {
  const errors: string[] = [];
  const connected: string[] = [];
  const controller = new AbortController();
  const client = new StreamdClient({ adapter });
  const running = runStore(opts, {
    client,
    open: refuse,
    connect: (url) => (connected.push(url), connection),
    error: (m) => errors.push(m),
    signal: controller.signal,
  });
  for (let i = 0; i < 300; i++) {
    await new Promise((r) => setTimeout(r, 5));
    if (await done().catch(() => false)) break;
  }
  await new Promise((r) => setTimeout(r, 20));
  controller.abort();
  return { code: await running, errors, connected };
}

describe("runStore --postgres", () => {
  it("stores events and its cursor, connects with the URL as given, and closes once on abort", async () => {
    const { pg, connection } = pglite();
    const result = await pgRun(new MemoryAdapter([ev(1), ev(2)]), connection, pgOptions(), async () => (await pgStored(pg)).length === 2);
    expect(result).toEqual({ code: 0, errors: [], connected: [URL_WITH_PASSWORD] });
    expect(await pgStored(pg)).toEqual([1, 2]);
    expect(await pgCursors(pg)).toEqual([`mainnet:${JSON.stringify(options().descriptor)}`]);
    expect(connection.closed).toBe(1);
  });

  it("resumes from the saved cursor, ignoring --from-version, with no repeat", async () => {
    const { pg, connection } = pglite();
    const events = [ev(1), ev(2)];
    await pgRun(new MemoryAdapter(events), connection, pgOptions(), async () => (await pgStored(pg)).length === 2);
    const fetched: unknown[] = [];
    const memory = new MemoryAdapter([...events, ev(3)]);
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => (fetched.push(o.cursor), memory.fetchEvents(d, o)),
      getHeadVersion: () => memory.getHeadVersion(),
    };
    await pgRun(adapter, connection, pgOptions({ fromVersion: 1n }), async () => (await pgStored(pg)).length === 3);
    expect(await pgStored(pg)).toEqual([1, 2, 3]);
    expect(fetched[0]).toBeDefined();
  });

  it("names cursors as --db does: --name, sui and the filter", async () => {
    const { pg, connection } = pglite();
    const adapter = new MemoryAdapter([ev(1), ev(2)]);
    const filter = { senders: [], functions: [], where: [{ path: "n", op: "==" as const, value: "2" }] };
    await pgRun(adapter, connection, pgOptions({ name: "mine" }), async () => (await pgCursors(pg)).length === 1);
    await pgRun(adapter, connection, pgOptions({ chain: "sui" }), async () => (await pgCursors(pg)).length === 2);
    await pgRun(adapter, connection, pgOptions({ filter }), async () => (await pgCursors(pg)).length === 3);
    const descriptor = JSON.stringify(options().descriptor);
    expect(await pgCursors(pg)).toEqual([`mainnet:${descriptor}:${JSON.stringify(filter)}`, "mine", `sui:mainnet:${descriptor}`]);
  });

  it("exits 1 when the server refuses, naming the redacted URL and never the password", async () => {
    const errors: string[] = [];
    let closed = 0;
    const connection: PostgresConnection = {
      query: async () => {
        throw new Error(`password authentication failed for ${URL_WITH_PASSWORD} (s3cr#t, other1)`);
      },
      close: () => void closed++,
    };
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    const code = await runStore(pgOptions(), { client, open: refuse, connect: () => connection, error: (m) => errors.push(m), signal: new AbortController().signal });
    expect(code).toBe(1);
    expect(closed).toBe(1);
    expect(errors).toEqual([
      "streamd: cannot connect to postgres://streamd:***@db.example:5432/events: password authentication failed for postgres://streamd:***@db.example:5432/events (***, ***)",
    ]);
    expect(errors.join()).not.toMatch(/s3cr|other1/);
    client.destroy();
  });

  it("exits 1 when the driver cannot be loaded, with its message", async () => {
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    const connect = () => Promise.reject(new Error("--postgres under Node needs pg installed: npm i pg (Bun has its own client)"));
    expect(await runStore(pgOptions(), { client, open: refuse, connect, error: (m) => errors.push(m), signal: new AbortController().signal })).toBe(1);
    expect(errors).toEqual([
      "streamd: cannot connect to postgres://streamd:***@db.example:5432/events: --postgres under Node needs pg installed: npm i pg (Bun has its own client)",
    ]);
  });

  it("exits 0 when aborted while still connecting, and closes the connection", async () => {
    let closed = 0;
    const connection: PostgresConnection = { query: () => new Promise(() => {}), close: () => void closed++ };
    const controller = new AbortController();
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const running = runStore(pgOptions(), { client, open: refuse, connect: () => connection, error: () => {}, signal: controller.signal });
    await new Promise((r) => setTimeout(r, 10));
    controller.abort();
    expect(await running).toBe(0);
    expect(closed).toBe(1);
  });

  it("exits 1 when a write fails, with the password scrubbed from the message", async () => {
    const { pg, connection } = pglite();
    await pg.exec(`CREATE TABLE streamd_events (version numeric(20, 0), event_index integer, type text, sender text,
      guid_address text, guid_creation numeric(39, 0), event text, PRIMARY KEY (version, event_index));
      CREATE FUNCTION refuse() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'disk full, s3cr#t'; END $$ LANGUAGE plpgsql;
      CREATE TRIGGER no_writes BEFORE INSERT ON streamd_events FOR EACH ROW EXECUTE FUNCTION refuse();`);
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const code = await runStore(pgOptions(), { client, open: refuse, connect: () => connection, error: (m) => errors.push(m), signal: new AbortController().signal });
    expect(code).toBe(1);
    expect(errors.join()).toContain("disk full, ***");
    expect(connection.closed).toBe(1);
    client.destroy();
  });
});

describe("redactUrl", () => {
  it("hides the password and drops the query string", () => {
    expect(redactUrl(URL_WITH_PASSWORD)).toBe("postgres://streamd:***@db.example:5432/events");
    expect(redactUrl("postgresql://me@localhost/db")).toBe("postgresql://me@localhost/db");
  });

  it("names no part of a URL it cannot parse", () => {
    expect(redactUrl("postgres://:::secret")).toBe("the Postgres URL");
  });
});
