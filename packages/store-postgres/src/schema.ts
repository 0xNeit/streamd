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
  // The column resource changes are served by, added to new and older tables
  // alike. Checked first, since the ALTER locks the whole table even when it
  // has nothing to do. One statement, so one transaction on any connection: a
  // second connection migrating at once waits on the first's lock, then finds
  // both already there.
  `DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_attribute
      WHERE attrelid = 'streamd_events'::regclass AND attname = 'account_address' AND NOT attisdropped
    ) THEN
      ALTER TABLE streamd_events ADD COLUMN IF NOT EXISTS account_address text;
      CREATE INDEX IF NOT EXISTS streamd_events_resource
        ON streamd_events (account_address, type, version, event_index)
        WHERE account_address IS NOT NULL;
    END IF;
  END $$`,
];

// Its own schema, created only by what writes or prunes transfers, so the
// event tables stay all a reader needs.
export const TRANSFER_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS streamd_transfers (
    version numeric(20, 0) NOT NULL,
    event_index integer NOT NULL,
    asset text NOT NULL,
    amount numeric(78, 0) NOT NULL,
    from_address text,
    to_address text,
    symbol text,
    decimals integer,
    transfer text NOT NULL,
    PRIMARY KEY (version, event_index)
  )`,
  "CREATE INDEX IF NOT EXISTS streamd_transfers_asset ON streamd_transfers (asset, version, event_index)",
  `CREATE INDEX IF NOT EXISTS streamd_transfers_from ON streamd_transfers (from_address, version, event_index)
    WHERE from_address IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS streamd_transfers_to ON streamd_transfers (to_address, version, event_index)
    WHERE to_address IS NOT NULL`,
];

// Kept among the cursors, as the SQLite store does, so a database created
// before pruning existed needs no migration: the row appears on the first prune.
export const PRUNED_BEFORE = "streamd:pruned-before";

/** The pruned edge as a `numeric` expression: `0` until something is pruned. */
export const EDGE = `coalesce((SELECT cursor::numeric FROM streamd_cursors WHERE name = '${PRUNED_BEFORE}'), 0)`;

// Per schema, then per client, so the event and page tables are each created
// only by what uses them.
const ready = new WeakMap<readonly string[], WeakMap<PostgresClient, Promise<void>>>();

/**
 * Runs `schema`'s statements once per client and schema. A failure is not
 * remembered, so the next call tries again.
 */
export function ensureSchema(client: PostgresClient, schema: readonly string[] = SCHEMA): Promise<void> {
  if (typeof client?.query !== "function") {
    throw new StreamdError("Expected a Postgres client with query()");
  }
  let clients = ready.get(schema);
  if (clients === undefined) {
    clients = new WeakMap();
    ready.set(schema, clients);
  }
  let pending = clients.get(client);
  if (pending === undefined) {
    pending = (async () => {
      // One statement per query: drivers using the extended protocol refuse several.
      for (const statement of schema) await client.query(statement);
    })();
    pending.catch(() => clients.delete(client));
    clients.set(client, pending);
  }
  return pending;
}
