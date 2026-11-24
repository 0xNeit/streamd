import { StreamdError } from "@streamd/core";
import { requireDatabase, transaction, type SqliteDatabase, type SqliteStatement } from "./schema.js";

// No AUTOINCREMENT: the newest row is never evicted, so a rowid is never reused.
const SCHEMA = "CREATE TABLE IF NOT EXISTS streamd_dedup (id TEXT PRIMARY KEY);";

const DEFAULT_MAX_ENTRIES = 100_000;

export interface SqliteDedupStoreOptions {
  /** How many ids to remember, across the table, before forgetting the oldest. Default 100,000. */
  maxEntries?: number;
}

function requireMaxEntries(max: number): number {
  if (!Number.isSafeInteger(max) || max <= 0) {
    throw new StreamdError(`Invalid maxEntries: ${String(max)} (expected a positive integer)`);
  }
  return max;
}

function requireIds(ids: readonly string[]): void {
  if (!Array.isArray(ids) || ids.some((id) => typeof id !== "string")) {
    throw new StreamdError("Expected an array of string ids");
  }
}

/**
 * A `DedupStore` for `@streamd/receiver` over a SQLite database the caller opens,
 * so handled ids survive a restart and every receiver on the database shares them.
 */
export class SqliteDedupStore {
  private readonly db: SqliteDatabase;
  private readonly has: SqliteStatement;
  private readonly insert: SqliteStatement;
  private readonly evict: SqliteStatement;
  private readonly maxEntries: number;

  constructor(db: SqliteDatabase, options: SqliteDedupStoreOptions = {}) {
    requireDatabase(db);
    this.maxEntries = requireMaxEntries(options?.maxEntries ?? DEFAULT_MAX_ENTRIES);
    db.exec(SCHEMA);
    this.db = db;
    this.has = db.prepare("SELECT 1 AS found FROM streamd_dedup WHERE id = ?");
    // IGNORE keeps a held id's rowid, so it is forgotten at its first add's turn.
    this.insert = db.prepare("INSERT OR IGNORE INTO streamd_dedup (id) VALUES (?)");
    this.evict = db.prepare(
      "DELETE FROM streamd_dedup WHERE rowid <= (SELECT rowid FROM streamd_dedup ORDER BY rowid DESC LIMIT 1 OFFSET ?)",
    );
  }

  seen(ids: readonly string[]): boolean[] {
    requireIds(ids);
    return ids.map((id) => this.has.get(id) != null);
  }

  add(ids: readonly string[]): void {
    requireIds(ids);
    if (ids.length === 0) return;
    transaction(this.db, () => {
      for (const id of ids) this.insert.run(id);
      this.evict.run(this.maxEntries);
    });
  }
}
