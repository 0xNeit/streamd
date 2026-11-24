import { StreamdError } from "@streamd/core";
import { ensureSchema, type PostgresClient } from "./schema.js";

// `seq` is first-add order: a held id is never rewritten, so it keeps its place.
const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS streamd_dedup (
    id text PRIMARY KEY,
    seq bigserial NOT NULL
  )`,
  "CREATE INDEX IF NOT EXISTS streamd_dedup_seq ON streamd_dedup (seq)",
];

const SEEN = "SELECT id FROM streamd_dedup WHERE id = ANY($1::text[])";

// One statement, so it is atomic on any pooled connection. The DELETE sees the
// table before the INSERT, so it cannot remove rows the INSERT adds: the INSERT
// keeps only the newest $2 fresh ids, and the DELETE keeps $2 minus that many
// of the old ones. Ids are one array parameter, so no parameter limit applies.
const ADD = `
WITH input AS (
  SELECT id, min(pos) AS pos FROM unnest($1::text[]) WITH ORDINALITY AS t(id, pos) GROUP BY id
), fresh AS (
  SELECT id, pos FROM (
    SELECT id, pos, row_number() OVER (ORDER BY pos DESC) AS back FROM input
    WHERE NOT EXISTS (SELECT 1 FROM streamd_dedup d WHERE d.id = input.id)
  ) ranked WHERE back <= $2
), evicted AS (
  DELETE FROM streamd_dedup WHERE seq <= (
    SELECT seq FROM streamd_dedup ORDER BY seq DESC LIMIT 1
    OFFSET ($2::bigint - (SELECT count(*) FROM fresh))
  )
)
INSERT INTO streamd_dedup (id) SELECT id FROM fresh ORDER BY pos ON CONFLICT (id) DO NOTHING`;

const DEFAULT_MAX_ENTRIES = 100_000;

export interface PostgresDedupStoreOptions {
  /** How many ids to remember, across the table, before forgetting the oldest. Default 100,000. */
  maxEntries?: number;
}

function requireIds(ids: readonly string[]): void {
  if (!Array.isArray(ids) || ids.some((id) => typeof id !== "string")) {
    throw new StreamdError("Expected an array of string ids");
  }
  if (ids.some((id) => id.includes("\0"))) throw new StreamdError("An id cannot hold a NUL character in Postgres");
}

/**
 * A `DedupStore` for `@streamd/receiver` over a Postgres client the caller supplies,
 * so handled ids survive a restart and every receiver on the database shares them.
 */
export class PostgresDedupStore {
  private readonly client: PostgresClient;
  private readonly maxEntries: number;

  constructor(client: PostgresClient, options: PostgresDedupStoreOptions = {}) {
    if (typeof client?.query !== "function") throw new StreamdError("Expected a Postgres client with query()");
    const max = options?.maxEntries ?? DEFAULT_MAX_ENTRIES;
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new StreamdError(`Invalid maxEntries: ${String(max)} (expected a positive integer)`);
    }
    this.client = client;
    this.maxEntries = max;
  }

  async seen(ids: readonly string[]): Promise<boolean[]> {
    requireIds(ids);
    if (ids.length === 0) return [];
    await ensureSchema(this.client, SCHEMA);
    const { rows } = await this.client.query(SEEN, [[...ids]]);
    const held = new Set(rows.map((r) => r.id));
    return ids.map((id) => held.has(id));
  }

  async add(ids: readonly string[]): Promise<void> {
    requireIds(ids);
    if (ids.length === 0) return;
    await ensureSchema(this.client, SCHEMA);
    await this.client.query(ADD, [[...ids], this.maxEntries]);
  }
}
