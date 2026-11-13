import type { EventDescriptor, StreamdClient, Subscription } from "@streamd/core";
import { PostgresEventStore, storeEvents as storeInPostgres, type PostgresClient, type StoreEventsOptions } from "@streamd/store-postgres";
import { SqliteEventStore, storeEvents as storeInSqlite, type SqliteDatabase } from "@streamd/store-sqlite";
import type { StoreOptions } from "./args.js";
import { filterPredicate } from "./filter.js";
import { message } from "./format.js";
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

/** `url` with its password shown as `***` and its query string dropped, safe to print. */
export function redactUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.password !== "") parsed.password = "***";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return "the Postgres URL";
  }
}

/** Replaces `url` in a message with its redacted form, and each secret it carries with `***`. */
function scrubber(url: string): (text: string) => string {
  const secrets = new Set<string>();
  try {
    const parsed = new URL(url);
    secrets.add(parsed.password).add(decodeURIComponent(parsed.password));
    for (const [key, value] of parsed.searchParams) {
      if (/pass/i.test(key)) secrets.add(value).add(encodeURIComponent(value));
    }
  } catch {
    // An unparsed URL still has its whole text replaced below.
  }
  secrets.delete("");
  const redacted = redactUrl(url);
  return (text) => {
    let out = text.split(url).join(redacted);
    for (const secret of secrets) out = out.split(secret).join("***");
    return out;
  };
}

interface Backend {
  start(options: StoreEventsOptions): Subscription | Promise<Subscription>;
  close(): unknown;
}

async function sqliteBackend(path: string, descriptor: EventDescriptor, io: StoreIO): Promise<Backend> {
  const db = await io.open(path);
  // WAL lets the file be queried while it is written; the timeout stops a busy reader failing a write.
  db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
  const store = new SqliteEventStore(db);
  return { start: (options) => storeInSqlite(io.client, store, descriptor, options), close: () => db.close?.() };
}

async function postgresBackend(url: string, descriptor: EventDescriptor, io: StoreIO): Promise<Backend> {
  const connection = await io.connect(url);
  const store = new PostgresEventStore(connection);
  return { start: (options) => storeInPostgres(io.client, store, descriptor, options), close: () => connection.close() };
}

/** The cursor name: `--name`, or one derived from the chain, network, descriptor and filter. */
function cursorName(options: StoreOptions): string {
  if (options.name !== undefined) return options.name;
  const descriptorName = JSON.stringify(options.descriptor, (_key, v: unknown) =>
    typeof v === "bigint" ? v.toString() : v,
  );
  // A filtered run gets its own cursor, so it never resumes from one that skipped events it wants.
  const filterName = options.filter === undefined ? "" : `:${JSON.stringify(options.filter)}`;
  // Prefixed for Sui, so a database shared by both chains never resumes from the other's cursor.
  const network = options.chain === "sui" ? `sui:${options.network}` : options.network;
  return `${network}:${descriptorName}${filterName}`;
}

/**
 * Follows `options.descriptor` into the SQLite file or Postgres database,
 * resuming from the cursor saved there, and resolves to the process exit code:
 * 0 when the signal aborts, 1 when polling gives up, a write fails, or the
 * database cannot be opened or reached. A Postgres URL is never printed as given.
 */
export async function runStore(options: StoreOptions, io: StoreIO): Promise<number> {
  const { postgres } = options;
  const scrub = postgres === undefined ? (text: string) => text : scrubber(postgres);
  const report = (text: string) => io.error(scrub(text));
  const failure = postgres === undefined ? `cannot open ${options.db}` : `cannot connect to ${redactUrl(postgres)}`;

  let backend: Backend;
  try {
    backend =
      postgres === undefined
        ? await sqliteBackend(options.db!, options.descriptor, io)
        : await postgresBackend(postgres, options.descriptor, io);
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

  const { fromVersion, pollInterval, filter, decode } = options;
  // Postgres reads the saved cursor, creating the tables, first, so an unreachable server fails here.
  Promise.resolve()
    .then(() =>
      backend.start({
        name: cursorName(options),
        // Only seeds the first run: the poller prefers the saved cursor storeEvents loads.
        ...(fromVersion !== undefined && { fromVersion }),
        ...(pollInterval !== undefined && { pollInterval }),
        ...(decode !== undefined && { decode }),
        ...(filter !== undefined && { where: filterPredicate(filter) }),
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
        report(`streamd: ${failure}: ${message(err)}`);
        settle(1);
      },
    );

  const code = await ended;
  done = true;
  io.signal.removeEventListener("abort", onAbort);
  // Stops the subscription and lets a batch already being written finish before the connection goes.
  await io.client.close({ timeout: CLOSE_TIMEOUT });
  await backend.close();
  return code;
}
