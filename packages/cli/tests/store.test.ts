import { PGlite } from "@electric-sql/pglite";
import { Database } from "bun:sqlite";
import { afterEach, describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, EventCallback, StreamdEvent, SubscriptionOptions } from "@streamd/core";
import type { StoreOptions, StoreRecordsOptions } from "../src/args.js";
import { redactUrl } from "../src/format.js";
import { openDatabase, runStore, runStoreRecords, type OpenDatabase, type PostgresConnection } from "../src/store.js";

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

  it("saves the cursor across empty sparse pages on abort and resumes past them", async () => {
    const db = new Database(":memory:");
    let count = 0;
    const sparse: EventAdapter = {
      fetchEvents: async () => {
        count++;
        if (count <= 3) {
          return {
            events: [],
            nextCursor: String(count * 100),
            hasMore: true,
            throughVersion: BigInt(count * 100),
          };
        }
        return new Promise(() => {});
      },
    };
    const { code, errors } = await run(sparse, db, options(), () => count >= 4);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    const key = `mainnet:${JSON.stringify(options().descriptor)}`;
    expect(cursors(db)).toEqual([key]);
    const saved = (db.query("SELECT cursor FROM streamd_cursors WHERE name = ?").get(key) as { cursor: string }).cursor;
    expect(saved).toBe("300");

    const resumedFrom: (string | undefined)[] = [];
    const second: EventAdapter = {
      fetchEvents: async (_d, o) => {
        resumedFrom.push(o.cursor);
        return { events: [ev(305)], nextCursor: "306", hasMore: false };
      },
    };
    await run(second, db, options({ fromVersion: 0n }), () => stored(db).length === 1);
    expect(resumedFrom[0]).toBe("300");
    expect(stored(db)).toEqual([305]);
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

const openedPg: PGlite[] = [];

afterEach(async () => {
  await Promise.all(openedPg.splice(0).map((pg) => pg.close().catch(() => {})));
});

/** A PGlite database as the connection, counting closes and leaving it open for read-back. */
function pglite(pg = new PGlite()) {
  openedPg.push(pg);
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

  it("saves the cursor across empty sparse pages on abort for --postgres and resumes past them", async () => {
    const { pg, connection } = pglite();
    let count = 0;
    const sparse: EventAdapter = {
      fetchEvents: async () => {
        count++;
        if (count <= 3) {
          return {
            events: [],
            nextCursor: String(count * 100),
            hasMore: true,
            throughVersion: BigInt(count * 100),
          };
        }
        return new Promise(() => {});
      },
    };
    const result = await pgRun(sparse, connection, pgOptions(), async () => count >= 4);
    expect(result).toEqual({ code: 0, errors: [], connected: [URL_WITH_PASSWORD] });
    const key = `mainnet:${JSON.stringify(options().descriptor)}`;
    const row = (await pg.query<{ cursor: string }>("SELECT cursor FROM streamd_cursors WHERE name = $1", [key])).rows[0];
    expect(row?.cursor).toBe("300");
    expect(connection.closed).toBe(1);
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

  it("exits 0 when aborted while still connecting, not waiting for a close that hangs", async () => {
    let closed = 0;
    const hang = () => new Promise<never>(() => {});
    const connection: PostgresConnection = { query: hang, close: () => (closed++, hang()) };
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

const SWAP_TYPE = "0x1::pool_v3::SwapEventV3";

function swapRecord(version: number): StreamdEvent {
  return {
    type: SWAP_TYPE,
    data: { protocol: "hyperion", poolId: "0xp", assetIn: "0xa", amountIn: BigInt(version), assetOut: "0xb", amountOut: 1n },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 1,
  };
}

function saleRecord(version: number): StreamdEvent {
  return {
    type: SWAP_TYPE,
    data: { marketplace: "wapal", kind: "listing", token: "0xt", seller: "0xs", buyer: "0xb", price: BigInt(version) },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 2,
  };
}

function transferRecord(version: number): StreamdEvent {
  return { type: SWAP_TYPE, data: { asset: "0xa", amount: BigInt(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 3 };
}

/** A StreamdClient with `kind`'s record method over a MemoryAdapter of records, keeping the options each call got. */
function recordClient(kind: "transfers" | "swaps" | "sales", records: StreamdEvent[], refuse?: string) {
  const client = new StreamdClient({ adapter: new MemoryAdapter(records) });
  const calls: Record<string, unknown>[] = [];
  const method = (callback: EventCallback, options: SubscriptionOptions & Record<string, unknown> = {}) => {
    calls.push(options);
    if (refuse !== undefined) throw new StreamdError(refuse);
    const { asset: _a, account: _c, protocol: _p, marketplace: _m, token: _t, ...rest } = options;
    return client.on({ kind: "module", eventType: SWAP_TYPE }, callback, rest);
  };
  return { client: Object.assign(client, { [kind]: method }), calls };
}

const recordOptions = (records: StoreRecordsOptions["records"], extra: Partial<StoreRecordsOptions> = {}): StoreRecordsOptions => ({
  network: "mainnet",
  pollInterval: 1,
  fromVersion: 0n,
  db: "e.db",
  records,
  ...extra,
});

async function runRecords(client: StreamdClient, opts: StoreRecordsOptions, db: OpenDatabase | undefined, connection: PostgresConnection | undefined, done: () => Promise<boolean>) {
  if (db !== undefined) db.close = () => {};
  const errors: string[] = [];
  const controller = new AbortController();
  const running = runStoreRecords(opts, {
    client,
    open: db === undefined ? refuse : () => db,
    connect: connection === undefined ? refuse : () => connection,
    error: (m) => errors.push(m),
    signal: controller.signal,
  });
  for (let i = 0; i < 300; i++) {
    await new Promise((r) => setTimeout(r, 5));
    if (await done().catch(() => false)) break;
  }
  await new Promise((r) => setTimeout(r, 20));
  controller.abort();
  const code = await running;
  client.destroy();
  return { code, errors };
}

describe("runStoreRecords", () => {
  const rowsOf = (db: Database, table: string) =>
    (db.query(`SELECT version FROM ${table} ORDER BY version`).all() as { version: string }[]).map((r) => Number(r.version));

  it("stores swaps under a cursor named by network and narrowing, passing the narrowing to the client", async () => {
    const db = new Database(":memory:");
    const { client, calls } = recordClient("swaps", [swapRecord(1), swapRecord(2)]);
    const opts = recordOptions({ kind: "swaps", protocol: ["hyperion"], asset: "0xa" });
    const { code, errors } = await runRecords(client, opts, db, undefined, async () => rowsOf(db, "streamd_swaps").length === 2);
    expect({ code, errors }).toEqual({ code: 0, errors: [] });
    expect(rowsOf(db, "streamd_swaps")).toEqual([1, 2]);
    expect(calls[0]).toMatchObject({ protocol: ["hyperion"], asset: "0xa", fromVersion: 0n, pollInterval: 1 });
    expect(cursors(db)).toEqual([`swaps:mainnet:${JSON.stringify({ protocol: ["hyperion"], asset: "0xa" })}`]);
  });

  it("stores sales and transfers, named by --name or the network alone, with a sui prefix on Sui", async () => {
    const db = new Database(":memory:");
    const sales = recordClient("sales", [saleRecord(3)]);
    await runRecords(sales.client, recordOptions({ kind: "sales" }, { name: "nft" }), db, undefined, async () => rowsOf(db, "streamd_sales").length === 1);
    const transfers = recordClient("transfers", [transferRecord(4)]);
    await runRecords(transfers.client, recordOptions({ kind: "transfers" }, { chain: "sui" }), db, undefined, async () => rowsOf(db, "streamd_transfers").length === 1);
    expect(rowsOf(db, "streamd_sales")).toEqual([3]);
    expect(rowsOf(db, "streamd_transfers")).toEqual([4]);
    expect(cursors(db)).toEqual(["sales:nft", "transfers:sui:mainnet"]);
  });

  it("stores swaps into Postgres", async () => {
    const { pg, connection } = pglite();
    const { client } = recordClient("swaps", [swapRecord(1), swapRecord(2)]);
    const count = async () => (await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM streamd_swaps")).rows[0]!.n;
    const { code, errors } = await runRecords(client, recordOptions({ kind: "swaps" }, { db: undefined, postgres: URL_WITH_PASSWORD }), undefined, connection, async () => (await count()) === 2);
    expect({ code, errors }).toEqual({ code: 0, errors: [] });
    expect(await count()).toBe(2);
    expect(await pgCursors(pg)).toEqual(["swaps:mainnet"]);
  });

  it("exits 1 with the client's message when it refuses the narrowing, not blaming the database", async () => {
    const db = new Database(":memory:");
    const { client } = recordClient("swaps", [], "Unknown swap protocol: nope");
    const { code, errors } = await runRecords(client, recordOptions({ kind: "swaps", protocol: ["nope"] }), db, undefined, async () => false);
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: Unknown swap protocol: nope"]);
  });

  it("exits 1 when the client has no such method", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    const { code, errors } = await runRecords(client, recordOptions({ kind: "sales" }), new Database(":memory:"), undefined, async () => true);
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: this chain's client has no sales"]);
  });
});

describe("redactUrl", () => {
  it("hides the password and drops the query string", () => {
    expect(redactUrl(URL_WITH_PASSWORD)).toBe("postgres://streamd:***@db.example:5432/events");
    expect(redactUrl("postgresql://me@localhost/db")).toBe("postgresql://me@localhost/db");
  });

  it("names no part of a URL it cannot parse", () => {
    expect(redactUrl("postgres://:::secret")).toBe("the URL");
  });
});
