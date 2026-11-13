import { StreamdError, type PageStore } from "@streamd/core";
import { ensureSchema, type PostgresClient } from "./schema.js";

// `seq` is the write order: an upsert renews it, as SQLite's REPLACE renews a rowid.
const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS streamd_pages (
    key text PRIMARY KEY,
    page text NOT NULL,
    seq bigserial NOT NULL
  )`,
  "CREATE INDEX IF NOT EXISTS streamd_pages_seq ON streamd_pages (seq)",
];

const UPSERT = `
INSERT INTO streamd_pages (key, page) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET page = excluded.page, seq = DEFAULT`;

// The eviction is a CTE of the upsert, so both land together on any pooled
// connection. It sees the table before the write, so it keeps the
// maxEntries - 1 newest other keys, and the row written makes maxEntries. It
// never touches that key: Postgres does not say which of two changes to one
// row in one statement wins.
const UPSERT_EVICTING = `
WITH evicted AS (
  DELETE FROM streamd_pages WHERE key <> $1 AND seq <= (
    SELECT seq FROM streamd_pages WHERE key <> $1 ORDER BY seq DESC LIMIT 1 OFFSET ($3::bigint - 1)
  )
)${UPSERT}`;

export interface PostgresPageStoreOptions {
  /**
   * Keeps one network's pages from answering another's, since page keys name
   * only the request. Use one per network or source, for example `"mainnet"`.
   */
  namespace: string;
  /** Drop the oldest written past this many pages, across the table (default: no limit). */
  maxEntries?: number;
}

/** A `PageStore` for `CachingAdapter`, over a Postgres client the caller supplies. */
export class PostgresPageStore implements PageStore {
  private readonly client: PostgresClient;
  private readonly namespace: string;
  private readonly maxEntries?: number;

  constructor(client: PostgresClient, options: PostgresPageStoreOptions) {
    if (typeof client?.query !== "function") throw new StreamdError("Expected a Postgres client with query()");
    const { namespace, maxEntries } = options ?? {};
    if (typeof namespace !== "string" || namespace === "") {
      throw new StreamdError("PostgresPageStore needs a namespace, one per network or source");
    }
    if (namespace.includes("\0")) throw new StreamdError("A namespace cannot hold a NUL character in Postgres");
    if (maxEntries !== undefined && (!Number.isSafeInteger(maxEntries) || maxEntries < 1)) {
      throw new StreamdError(`Invalid maxEntries: ${String(maxEntries)} (expected a positive integer)`);
    }
    this.client = client;
    this.namespace = namespace;
    this.maxEntries = maxEntries;
  }

  async get(key: string): Promise<string | undefined> {
    await ensureSchema(this.client, SCHEMA);
    const { rows } = await this.client.query("SELECT page FROM streamd_pages WHERE key = $1", [this.scoped(key)]);
    const page = rows[0]?.page;
    return typeof page === "string" ? page : undefined;
  }

  async set(key: string, page: string): Promise<void> {
    await ensureSchema(this.client, SCHEMA);
    if (this.maxEntries === undefined) {
      await this.client.query(UPSERT, [this.scoped(key), page]);
    } else {
      await this.client.query(UPSERT_EVICTING, [this.scoped(key), page, this.maxEntries]);
    }
  }

  private scoped(key: string): string {
    return `${this.namespace}\n${key}`;
  }
}
