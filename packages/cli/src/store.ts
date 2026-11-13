import type { StreamdClient, Subscription } from "@streamd/core";
import { SqliteEventStore, storeEvents, type SqliteDatabase } from "@streamd/store-sqlite";
import type { StoreOptions } from "./args.js";
import { filterPredicate } from "./filter.js";
import { message } from "./format.js";

export type OpenDatabase = SqliteDatabase & { close?: () => void };

export interface StoreIO {
  client: StreamdClient;
  open(path: string): OpenDatabase | Promise<OpenDatabase>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

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

/**
 * Follows `options.descriptor` into the SQLite file, resuming from the cursor
 * saved there, and resolves to the process exit code: 0 when the signal
 * aborts, 1 when polling gives up, a write fails, or the file cannot be opened.
 */
export async function runStore(options: StoreOptions, io: StoreIO): Promise<number> {
  let db: OpenDatabase;
  let store: SqliteEventStore;
  try {
    db = await io.open(options.db);
    // WAL lets the file be queried while it is written; the timeout stops a busy reader failing a write.
    db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
    store = new SqliteEventStore(db);
  } catch (err) {
    io.error(`streamd: cannot open ${options.db}: ${message(err)}`);
    return 1;
  }

  const code = await new Promise<number>((resolve) => {
    if (io.signal.aborted) return resolve(0);
    let sub: Subscription | undefined;
    const finish = (result: number) => {
      io.signal.removeEventListener("abort", onAbort);
      sub?.unsubscribe();
      resolve(result);
    };
    const onAbort = () => finish(0);
    io.signal.addEventListener("abort", onAbort, { once: true });

    const { fromVersion, pollInterval, filter, decode } = options;
    const descriptorName = JSON.stringify(options.descriptor, (_key, v: unknown) =>
      typeof v === "bigint" ? v.toString() : v,
    );
    // A filtered run gets its own cursor, so it never resumes from one that skipped events it wants.
    const filterName = filter === undefined ? "" : `:${JSON.stringify(filter)}`;
    // Prefixed for Sui, so a database shared by both chains never resumes from the other's cursor.
    const network = options.chain === "sui" ? `sui:${options.network}` : options.network;
    const name = options.name ?? `${network}:${descriptorName}${filterName}`;
    sub = storeEvents(io.client, store, options.descriptor, {
      name,
      // Only seeds the first run: the poller prefers the saved cursor storeEvents loads.
      ...(fromVersion !== undefined && { fromVersion }),
      ...(pollInterval !== undefined && { pollInterval }),
      ...(decode !== undefined && { decode }),
      ...(filter !== undefined && { where: filterPredicate(filter) }),
      onError: (err) => {
        io.error(`streamd: ${message(err)}`);
        // A failed write ends the subscription on a microtask, after this runs.
        setTimeout(() => {
          if (sub !== undefined && !sub.active) finish(1);
        }, 0);
      },
    });
  });
  db.close?.();
  return code;
}
