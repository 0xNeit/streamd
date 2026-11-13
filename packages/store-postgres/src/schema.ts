import { StreamdError } from "@streamd/core";

/**
 * The part of a Postgres client the store uses: one parameterised query that
 * resolves with its rows. `pg`'s `Client` and `Pool` and PGlite fit it as they
 * are, so the caller brings whichever driver their runtime has.
 */
export interface PostgresClient {
  query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

const U64_MAX = 2n ** 64n - 1n;

/** A ledger version as the decimal string Postgres reads into `numeric`. */
export function versionParam(version: bigint): string {
  if (typeof version !== "bigint" || version < 0n || version > U64_MAX) {
    throw new StreamdError(`Not a valid ledger version: ${String(version)} (expected a u64 bigint)`);
  }
  return version.toString();
}

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS streamd_events (
    version numeric(20, 0) NOT NULL,
    event_index integer NOT NULL,
    type text NOT NULL,
    sender text,
    guid_address text,
    guid_creation numeric(39, 0),
    event text NOT NULL,
    PRIMARY KEY (version, event_index)
  )`,
  "CREATE INDEX IF NOT EXISTS streamd_events_type ON streamd_events (type, version, event_index)",
  `CREATE INDEX IF NOT EXISTS streamd_events_sender ON streamd_events (sender, version, event_index)
    WHERE sender IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS streamd_events_guid
    ON streamd_events (guid_address, guid_creation, version, event_index)
    WHERE guid_address IS NOT NULL`,
  "CREATE TABLE IF NOT EXISTS streamd_cursors (name text PRIMARY KEY, cursor text NOT NULL)",
];

// Kept among the cursors, as the SQLite store does, so a database created
// before pruning existed needs no migration: the row appears on the first prune.
export const PRUNED_BEFORE = "streamd:pruned-before";

/** The pruned edge as a `numeric` expression: `0` until something is pruned. */
export const EDGE = `coalesce((SELECT cursor::numeric FROM streamd_cursors WHERE name = '${PRUNED_BEFORE}'), 0)`;

const ready = new WeakMap<PostgresClient, Promise<void>>();

/**
 * Creates the tables once per client. A failure is not remembered, so the
 * next call tries again.
 */
export function ensureSchema(client: PostgresClient): Promise<void> {
  if (typeof client?.query !== "function") {
    throw new StreamdError("Expected a Postgres client with query()");
  }
  let pending = ready.get(client);
  if (pending === undefined) {
    pending = (async () => {
      // One statement per query: drivers using the extended protocol refuse several.
      for (const statement of SCHEMA) await client.query(statement);
    })();
    pending.catch(() => ready.delete(client));
    ready.set(client, pending);
  }
  return pending;
}
