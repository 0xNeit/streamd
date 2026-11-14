import { StreamdError } from "@streamd/core";

/** One prepared statement, with positional parameters. */
export interface SqliteStatement {
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
}

/**
 * The part of a SQLite connection the store uses. `bun:sqlite`'s `Database`,
 * `better-sqlite3`'s `Database` and `node:sqlite`'s `DatabaseSync` all fit it,
 * so the caller brings whichever driver their runtime has.
 */
export interface SqliteDatabase {
  exec(sql: string): unknown;
  prepare(sql: string): SqliteStatement;
}

const U64_MAX = 2n ** 64n - 1n;
const VERSION_DIGITS = 20;

// Zero-padded text sorts numerically for every u64, and no driver ever sees
// an integer it would round to a double.
export function versionKey(version: bigint): string {
  if (typeof version !== "bigint" || version < 0n || version > U64_MAX) {
    throw new StreamdError(`Not a valid ledger version: ${String(version)} (expected a u64 bigint)`);
  }
  return version.toString().padStart(VERSION_DIGITS, "0");
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS streamd_events (
  version TEXT NOT NULL,
  event_index INTEGER NOT NULL,
  type TEXT NOT NULL,
  sender TEXT,
  guid_address TEXT,
  guid_creation TEXT,
  event TEXT NOT NULL,
  PRIMARY KEY (version, event_index)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS streamd_events_type ON streamd_events (type, version, event_index);
CREATE INDEX IF NOT EXISTS streamd_events_sender ON streamd_events (sender, version, event_index)
  WHERE sender IS NOT NULL;
CREATE INDEX IF NOT EXISTS streamd_events_guid
  ON streamd_events (guid_address, guid_creation, version, event_index)
  WHERE guid_address IS NOT NULL;
CREATE TABLE IF NOT EXISTS streamd_cursors (
  name TEXT PRIMARY KEY,
  cursor TEXT NOT NULL
);
`;

// Its own schema, created only by what writes transfers: a read-only
// connection to a database made before it existed must still open.
export const TRANSFER_SCHEMA = `
CREATE TABLE IF NOT EXISTS streamd_transfers (
  version TEXT NOT NULL,
  event_index INTEGER NOT NULL,
  asset TEXT NOT NULL,
  amount TEXT NOT NULL,
  from_address TEXT,
  to_address TEXT,
  symbol TEXT,
  decimals INTEGER,
  transfer TEXT NOT NULL,
  PRIMARY KEY (version, event_index)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS streamd_transfers_asset ON streamd_transfers (asset, version, event_index);
CREATE INDEX IF NOT EXISTS streamd_transfers_from ON streamd_transfers (from_address, version, event_index)
  WHERE from_address IS NOT NULL;
CREATE INDEX IF NOT EXISTS streamd_transfers_to ON streamd_transfers (to_address, version, event_index)
  WHERE to_address IS NOT NULL;
`;

// Kept among the cursors so that a database written before pruning existed
// needs no new table, and stays readable on a read-only connection.
export const PRUNED_BEFORE = "streamd:pruned-before";

/** Raises the pruned edge; a lower `key` leaves it where it is. */
export function raisePrunedBefore(db: SqliteDatabase, key: string): void {
  db.prepare(
    `INSERT INTO streamd_cursors (name, cursor) VALUES (?, ?)
     ON CONFLICT (name) DO UPDATE SET cursor = max(cursor, excluded.cursor)`,
  ).run(PRUNED_BEFORE, key);
}

/** The lowest version still readable: `0n` until something is pruned. */
export function prunedBefore(db: SqliteDatabase): bigint {
  const row = db.prepare("SELECT cursor FROM streamd_cursors WHERE name = ?").get(PRUNED_BEFORE) as
    | { cursor: string }
    | null
    | undefined;
  return row == null ? 0n : BigInt(row.cursor);
}

export function requireDatabase(db: SqliteDatabase): void {
  if (typeof db?.exec !== "function" || typeof db.prepare !== "function") {
    throw new StreamdError("Expected a SQLite database with exec() and prepare()");
  }
}

export function ensureSchema(db: SqliteDatabase): void {
  requireDatabase(db);
  db.exec(SCHEMA);
}

/**
 * Runs `work` atomically, rolled back if it throws. A savepoint, so it also
 * works inside a transaction the caller already holds.
 */
export function transaction(db: SqliteDatabase, work: () => void): void {
  db.exec("SAVEPOINT streamd_write");
  try {
    work();
    db.exec("RELEASE streamd_write");
  } catch (err) {
    try {
      db.exec("ROLLBACK TO streamd_write");
      db.exec("RELEASE streamd_write");
    } catch {
      // SQLite may already have rolled back on its own; the write's error is the one to report.
    }
    throw err;
  }
}
