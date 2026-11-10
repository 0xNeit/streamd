import { StreamdError, type PageStore } from "@streamd/core";
import { requireDatabase, type SqliteDatabase, type SqliteStatement } from "./schema.js";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS streamd_pages (
  key TEXT PRIMARY KEY,
  page TEXT NOT NULL
);
`;

export interface SqlitePageStoreOptions {
  /**
   * Keeps one network's pages from answering another's, since page keys name
   * only the request. Use one per network or source, for example `"mainnet"`.
   */
  namespace: string;
  /** Drop the oldest written past this many pages, across the database (default: no limit). */
  maxEntries?: number;
}

/** A `PageStore` for `CachingAdapter`, over a SQLite database the caller opens. */
export class SqlitePageStore implements PageStore {
  private readonly namespace: string;
  private readonly select: SqliteStatement;
  private readonly upsert: SqliteStatement;
  private readonly evict?: SqliteStatement;
  private readonly maxEntries?: number;

  constructor(db: SqliteDatabase, options: SqlitePageStoreOptions) {
    requireDatabase(db);
    const { namespace, maxEntries } = options ?? {};
    if (typeof namespace !== "string" || namespace === "") {
      throw new StreamdError("SqlitePageStore needs a namespace, one per network or source");
    }
    if (maxEntries !== undefined && (!Number.isSafeInteger(maxEntries) || maxEntries < 1)) {
      throw new StreamdError(`Invalid maxEntries: ${String(maxEntries)} (expected a positive integer)`);
    }
    db.exec(SCHEMA);
    this.namespace = namespace;
    this.maxEntries = maxEntries;
    this.select = db.prepare("SELECT page FROM streamd_pages WHERE key = ?");
    // REPLACE gives the row a new rowid, so rowid order is write order.
    this.upsert = db.prepare("INSERT OR REPLACE INTO streamd_pages (key, page) VALUES (?, ?)");
    if (maxEntries !== undefined) {
      this.evict = db.prepare(
        "DELETE FROM streamd_pages WHERE rowid <= (SELECT rowid FROM streamd_pages ORDER BY rowid DESC LIMIT 1 OFFSET ?)",
      );
    }
  }

  get(key: string): string | undefined {
    const row = this.select.get(this.scoped(key)) as { page: string } | null | undefined;
    return row?.page;
  }

  set(key: string, page: string): void {
    this.upsert.run(this.scoped(key), page);
    this.evict?.run(this.maxEntries);
  }

  private scoped(key: string): string {
    return `${this.namespace}\n${key}`;
  }
}
