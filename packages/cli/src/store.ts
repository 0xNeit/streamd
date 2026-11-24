import { StreamdError } from "@streamd/core";
import type { StreamdClient, Subscription } from "@streamd/core";
import {
  PostgresEventStore,
  storeEvents as storeInPostgres,
  storeSales as salesInPostgres,
  storeSwaps as swapsInPostgres,
  storeTransfers as transfersInPostgres,
  type PostgresClient,
  type StoreEventsOptions,
} from "@streamd/store-postgres";
import {
  SqliteEventStore,
  storeEvents as storeInSqlite,
  storeSales as salesInSqlite,
  storeSwaps as swapsInSqlite,
  storeTransfers as transfersInSqlite,
  type SaleClient,
  type SqliteDatabase,
  type SwapClient,
  type TransferClient,
} from "@streamd/store-sqlite";
import type { Endpoint, StoreAdminOptions, StoreOptions, StoreRecords, StoreRecordsOptions } from "./args.js";
import { filterPredicate } from "./filter.js";
import { message, redactUrl, scrubber } from "./format.js";
import { isMissingModule } from "./sui.js";

export type OpenDatabase = SqliteDatabase & { close?: () => void };
export type PostgresConnection = PostgresClient & { close(): unknown };

export interface StoreIO {
  client: StreamdClient;
  open(path: string): OpenDatabase | Promise<OpenDatabase>;
  connect(url: string): PostgresConnection | Promise<PostgresConnection>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

/** How long a stop waits for a batch already being written. */
const CLOSE_TIMEOUT = 5_000;
/** How long it then waits for the connection to close: one still being made can hold it for the driver's whole connect timeout. */
const DISCONNECT_TIMEOUT = 1_000;

/** Opens `path` with the runtime's own SQLite, which is why neither is a dependency. */
export async function openDatabase(path: string): Promise<OpenDatabase> {
  if ("Bun" in globalThis) {
    const { Database } = await import("bun:sqlite");
    return new Database(path, { create: true }) as unknown as OpenDatabase;
  }
  let sqlite: typeof import("node:sqlite");
  try {
    sqlite = await import("node:sqlite");
  } catch {
    throw new Error("streamd store needs Node 22.5 or newer, or Bun");
  }
  return new sqlite.DatabaseSync(path) as unknown as OpenDatabase;
}

interface PgPool extends PostgresClient {
  on(event: "error", listener: () => void): unknown;
  end(): Promise<void>;
}
type PgModule = { Pool: new (config: { connectionString: string; connectionTimeoutMillis: number }) => PgPool };

// A variable, so neither the bundle nor the typecheck needs pg installed.
const PG = "pg";

/**
 * Connects with the runtime's own Postgres client, `Bun.SQL`, or under Node with
 * `pg` when the user installed it, so the CLI ships no driver. Nothing is sent
 * until the first query.
 */
export async function connectPostgres(url: string): Promise<PostgresConnection> {
  if ("Bun" in globalThis) {
    const sql = new Bun.SQL(url);
    return { query: async (text, params = []) => ({ rows: await sql.unsafe(text, params) }), close: () => sql.close() };
  }
  let pg: PgModule & { default?: PgModule };
  try {
    pg = await import(PG);
  } catch (err) {
    if (isMissingModule(err)) throw new Error("--postgres under Node needs pg installed: npm i pg (Bun has its own client)");
    throw err;
  }
  // Bun.SQL's default connect timeout; pg's own is to wait forever.
  const pool = new (pg.default ?? pg).Pool({ connectionString: url, connectionTimeoutMillis: 30_000 });
  // An idle connection dropping is emitted here, and unheard would crash the process; the next query reconnects.
  pool.on("error", () => {});
  return { query: (text, params) => pool.query(text, params), close: () => pool.end() };
}

interface Backend {
  start(options: StoreEventsOptions): Subscription | Promise<Subscription>;
  close(): unknown;
}

/** How a run follows into each kind of store; both get the cursor name and listener options. */
interface Starters {
  sqlite(store: SqliteEventStore, options: StoreEventsOptions): Subscription;
  postgres(store: PostgresEventStore, options: StoreEventsOptions): Promise<Subscription>;
}

interface Target {
  db?: string;
  postgres?: string;
}

async function openBackend({ db, postgres }: Target, starters: Starters, io: StoreIO): Promise<Backend> {
  if (postgres !== undefined) {
    const connection = await io.connect(postgres);
    const store = new PostgresEventStore(connection);
    return { start: (options) => starters.postgres(store, options), close: () => connection.close() };
  }
  const opened = await io.open(db!);
  // WAL lets the file be queried while it is written; the timeout stops a busy reader failing a write.
  opened.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
  const store = new SqliteEventStore(opened);
  return { start: (options) => starters.sqlite(store, options), close: () => opened.close?.() };
}

/** Prefixed for Sui, so a database shared by both chains never resumes from the other's cursor. */
const networkName = (options: Endpoint) => (options.chain === "sui" ? `sui:${options.network}` : options.network);

/** The cursor name: `--name`, or one derived from the chain, network, descriptor and filter. */
function cursorName(options: StoreOptions): string {
  if (options.name !== undefined) return options.name;
  const descriptorName = JSON.stringify(options.descriptor, (_key, v: unknown) =>
    typeof v === "bigint" ? v.toString() : v,
  );
  // A filtered run gets its own cursor, so it never resumes from one that skipped events it wants.
  const filterName = options.filter === undefined ? "" : `:${JSON.stringify(options.filter)}`;
  return `${networkName(options)}:${descriptorName}${filterName}`;
}

/**
 * Follows `options.descriptor` into the SQLite file or Postgres database,
 * resuming from the cursor saved there, and resolves to the process exit code:
 * 0 when the signal aborts, 1 when polling gives up, a write fails, or the
 * database cannot be opened or reached. A Postgres URL is never printed as given.
 */
export function runStore(options: StoreOptions, io: StoreIO): Promise<number> {
  const { descriptor, fromVersion, pollInterval, filter, decode } = options;
  return follow(
    options,
    {
      sqlite: (store, listener) => storeInSqlite(io.client, store, descriptor, listener),
      postgres: (store, listener) => storeInPostgres(io.client, store, descriptor, listener),
    },
    {
      name: cursorName(options),
      // Only seeds the first run: the poller prefers the saved cursor storeEvents loads.
      ...(fromVersion !== undefined && { fromVersion }),
      ...(pollInterval !== undefined && { pollInterval }),
      ...(decode !== undefined && { decode }),
      ...(filter !== undefined && { where: filterPredicate(filter) }),
    },
    io,
  );
}

/** The narrowing a `--records` kind passes to its client method. */
function narrowing(records: StoreRecords): Record<string, unknown> {
  const { kind: _kind, ...rest } = records;
  return rest;
}

/**
 * The cursor name: `--name`, or the chain and network plus the narrowing,
 * so a narrowed run never resumes from one that skipped records it wants.
 * The sink prefixes it with the record kind.
 */
function recordCursorName(options: StoreRecordsOptions): string {
  if (options.name !== undefined) return options.name;
  const narrowed = narrowing(options.records);
  return Object.keys(narrowed).length === 0 ? networkName(options) : `${networkName(options)}:${JSON.stringify(narrowed)}`;
}

/** A store sink over one record kind, named once for both backends. */
function recordStarters(client: StreamdClient, { kind }: StoreRecords): Starters {
  // The parser refuses swaps and sales on Sui, whose client has only transfers.
  if (typeof (client as unknown as Record<string, unknown>)[kind] !== "function") {
    throw new Error(`this chain's client has no ${kind}`);
  }
  const records = client as unknown as TransferClient & SwapClient & SaleClient;
  if (kind === "transfers") {
    return { sqlite: (s, o) => transfersInSqlite(records, s, o), postgres: (s, o) => transfersInPostgres(records, s, o) };
  }
  if (kind === "swaps") {
    return { sqlite: (s, o) => swapsInSqlite(records, s, o), postgres: (s, o) => swapsInPostgres(records, s, o) };
  }
  return { sqlite: (s, o) => salesInSqlite(records, s, o), postgres: (s, o) => salesInPostgres(records, s, o) };
}

/**
 * Follows the client's `transfers`, `swaps` or `sales` into the SQLite file
 * or Postgres database, resuming from the cursor saved there, and resolves to
 * the exit code as `runStore` does. The narrowing values are the client's to
 * check; a refused one exits 1 with the client's message.
 */
export function runStoreRecords(options: StoreRecordsOptions, io: StoreIO): Promise<number> {
  const { fromVersion, pollInterval } = options;
  let starters: Starters;
  try {
    starters = recordStarters(io.client, options.records);
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return Promise.resolve(1);
  }
  return follow(
    options,
    starters,
    {
      name: recordCursorName(options),
      ...narrowing(options.records),
      ...(fromVersion !== undefined && { fromVersion }),
      ...(pollInterval !== undefined && { pollInterval }),
    },
    io,
  );
}

async function follow(target: Target, starters: Starters, listener: StoreEventsOptions, io: StoreIO): Promise<number> {
  const { postgres } = target;
  const scrub = postgres === undefined ? (text: string) => text : scrubber(postgres);
  const report = (text: string) => io.error(scrub(text));
  const failure = postgres === undefined ? `cannot open ${target.db}` : `cannot connect to ${redactUrl(postgres)}`;

  let backend: Backend;
  try {
    backend = await openBackend(target, starters, io);
  } catch (err) {
    report(`streamd: ${failure}: ${message(err)}`);
    return 1;
  }
  if (io.signal.aborted) {
    await backend.close();
    return 0;
  }

  let done = false;
  let sub: Subscription | undefined;
  let settle!: (code: number) => void;
  const ended = new Promise<number>((resolve) => (settle = resolve));
  const onAbort = () => settle(0);
  io.signal.addEventListener("abort", onAbort, { once: true });

  // Postgres reads the saved cursor, creating the tables, first, so an unreachable server fails here.
  Promise.resolve()
    .then(() =>
      backend.start({
        ...listener,
        onError: (err) => {
          report(`streamd: ${message(err)}`);
          // A failed write ends the subscription on a microtask, after this runs.
          setTimeout(() => {
            if (sub !== undefined && !sub.active) settle(1);
          }, 0);
        },
      }),
    )
    .then(
      (started) => {
        sub = started;
        // Aborted while connecting: the stop below has already run.
        if (done) started.unsubscribe();
      },
      (err: unknown) => {
        if (done) return;
        // A refusal from the client, such as an unknown protocol, is not the database's.
        report(err instanceof StreamdError ? `streamd: ${message(err)}` : `streamd: ${failure}: ${message(err)}`);
        settle(1);
      },
    );

  const code = await ended;
  done = true;
  io.signal.removeEventListener("abort", onAbort);
  // Stops the subscription and lets a batch already being written finish before the connection goes.
  await io.client.close({ timeout: CLOSE_TIMEOUT });
  // Persist any advanced sparse cursor (even if no batch arrived) before closing the database.
  if (code === 0 && sub !== undefined) {
    const asyncDispose = Symbol.asyncDispose ?? Symbol.for("Symbol.asyncDispose");
    await (sub as unknown as Record<symbol, (() => Promise<void>) | undefined>)[asyncDispose]?.();
  }
  await Promise.race([backend.close(), new Promise((resolve) => setTimeout(resolve, DISCONNECT_TIMEOUT).unref())]);
  return code;
}

export interface StoreAdminIO {
  open(path: string): OpenDatabase | Promise<OpenDatabase>;
  connect(url: string): PostgresConnection | Promise<PostgresConnection>;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborted when stdout closes, which ends the command with exit code 0. */
  signal: AbortSignal;
}

/** The store methods upkeep needs; SQLite answers them synchronously, Postgres asynchronously. */
type MaintainedStore = Pick<SqliteEventStore | PostgresEventStore, "stats" | "prune" | "cursors" | "deleteCursor">;

const line = (record: object) => `${JSON.stringify(record, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v))}\n`;

async function maintain(options: StoreAdminOptions, store: MaintainedStore, write: StoreAdminIO["write"]): Promise<number> {
  if (options.action === "stats") {
    await write(line(await store.stats({ byType: options.byType === true })));
    return 0;
  }
  if (options.action === "cursors") {
    if (options.delete === undefined) {
      for (const saved of await store.cursors()) await write(line(saved));
      return 0;
    }
    const deleted = await store.deleteCursor(options.delete);
    await write(line({ name: options.delete, deleted }));
    return deleted ? 0 : 1;
  }
  const beforeVersion = options.before!;
  const { prunedBefore, lowestVersion, highestVersion } = await store.stats();
  const noop = beforeVersion <= prunedBefore;
  if (options.yes !== true) {
    // The stores count no range, so the rows a prune would delete are not reported.
    await write(line({ dryRun: true, beforeVersion, prunedBefore, lowestVersion, highestVersion, noop, rows: "not counted" }));
    return 0;
  }
  if (!noop) await store.prune({ beforeVersion });
  await write(line({ dryRun: false, beforeVersion, noop, ...(await store.stats()) }));
  return 0;
}

/**
 * Runs `store stats`, `store prune` or `store cursors` against the SQLite file
 * or Postgres database and resolves to the exit code: 0 when done or stdout
 * closed, 1 when the database cannot be opened or reached, a read or write
 * fails, or `--delete` named no saved cursor. A Postgres URL is never printed as given.
 */
export async function runStoreAdmin(options: StoreAdminOptions, io: StoreAdminIO): Promise<number> {
  const { postgres } = options;
  const scrub = postgres === undefined ? (text: string) => text : scrubber(postgres);
  const report = (text: string) => io.error(scrub(text));
  let store: MaintainedStore;
  let close: () => unknown;
  try {
    if (postgres === undefined) {
      const db = await io.open(options.db!);
      store = new SqliteEventStore(db);
      close = () => db.close?.();
    } else {
      const connection = await io.connect(postgres);
      store = new PostgresEventStore(connection);
      close = () => connection.close();
    }
  } catch (err) {
    report(`streamd: ${postgres === undefined ? `cannot open ${options.db}` : `cannot connect to ${redactUrl(postgres)}`}: ${message(err)}`);
    return 1;
  }
  try {
    return await maintain(options, store, io.write);
  } catch (err) {
    if (io.signal.aborted) return 0;
    report(`streamd: ${message(err)}`);
    return 1;
  } finally {
    await Promise.race([close(), new Promise((resolve) => setTimeout(resolve, DISCONNECT_TIMEOUT).unref())]);
  }
}
