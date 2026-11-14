import { StreamdError, eventId, normalizeAddress, parseEvent, serializeEvent } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";
import { PRUNED_BEFORE, TRANSFER_SCHEMA, ensureSchema, versionParam } from "./schema.js";
import type { PostgresClient } from "./schema.js";

/** A source cursor to save under `name` alongside a write. */
export interface StoreCheckpoint {
  name: string;
  /** `undefined` leaves the saved cursor as it is. */
  cursor: string | undefined;
}

/** A cursor saved under a name, as `cursors()` lists it. */
export interface SavedCursor {
  name: string;
  cursor: string;
}

type Row = [string, number, string, string | null, string | null, string | null, string];

function rowOf(event: StreamdEvent): Row {
  if (!Number.isSafeInteger(event.eventIndex) || event.eventIndex < 0 || event.eventIndex > 2 ** 31 - 1) {
    throw new StreamdError(`Not a valid eventIndex: ${String(event.eventIndex)}`);
  }
  const json = serializeEvent(event);
  // Refused now rather than stored, where it would fail every read of its range.
  parseEvent(json);
  const sender = event.sender === undefined ? null : (normalizeAddress(event.sender) ?? null);
  const guidAddress = event.guid && (normalizeAddress(event.guid.accountAddress) ?? null);
  return [
    versionParam(event.version),
    event.eventIndex,
    event.type,
    sender,
    guidAddress ?? null,
    guidAddress ? event.guid!.creationNumber.toString() : null,
    json,
  ];
}

type TransferRow = [string, number, string, string, string | null, string | null, string | null, number | null, string];

const U256_MAX = 2n ** 256n - 1n;

// `text` cannot hold NUL, which an asset's symbol may carry; the record JSON
// keeps the exact value, so the column takes a replacement character instead
// of failing the write and, with it, every retry of the sink.
function columnText(value: string): string {
  return value.replaceAll("\0", "\uFFFD");
}

function optionalText(value: unknown, field: string): string | null {
  if (value === undefined) return null;
  if (typeof value !== "string") throw new StreamdError(`Not a valid transfer ${field}: ${String(value)}`);
  return columnText(value);
}

function transferRowOf(record: StreamdEvent<Transfer>): TransferRow {
  const [version, eventIndex, , , , , json] = rowOf(record);
  const data = record.data as Partial<Transfer> | null;
  if (typeof data !== "object" || data === null) throw new StreamdError("A transfer record needs a Transfer as data");
  const { asset, amount, decimals } = data;
  if (typeof asset !== "string" || asset === "") throw new StreamdError("A transfer needs a non-empty asset");
  if (typeof amount !== "bigint" || amount < 0n || amount > U256_MAX) {
    throw new StreamdError(`Not a valid transfer amount: ${String(amount)} (expected a u256 bigint)`);
  }
  if (decimals !== undefined && (!Number.isSafeInteger(decimals) || decimals < 0 || decimals > 2 ** 31 - 1)) {
    throw new StreamdError(`Not a valid transfer decimals: ${String(decimals)}`);
  }
  return [
    version,
    eventIndex,
    columnText(asset),
    amount.toString(),
    optionalText(data.from, "from"),
    optionalText(data.to, "to"),
    optionalText(data.symbol, "symbol"),
    decimals ?? null,
    json,
  ];
}

function requireName(name: unknown): string {
  if (typeof name !== "string" || name === "") {
    throw new StreamdError("A checkpoint name must be a non-empty string");
  }
  if (name === PRUNED_BEFORE) throw new StreamdError(`"${name}" is reserved for the store`);
  return name;
}

// Both writes are data-modifying CTEs of one statement, so they land together
// without a transaction, which a pool could not hold on one connection. Rows
// arrive as one JSON parameter: drivers disagree on how to send arrays, and
// the parameter count stays fixed whatever the batch size. It is typed text,
// since a driver told the parameter is jsonb may encode the string as a JSON string.
const WRITE = `
WITH incoming AS (
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, r->>3, r->>4, (r->>5)::numeric, r->>6
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_events (version, event_index, type, sender, guid_address, guid_creation, event)
  SELECT * FROM incoming
  ON CONFLICT (version, event_index) DO UPDATE SET
    type = excluded.type, sender = excluded.sender, guid_address = excluded.guid_address,
    guid_creation = excluded.guid_creation, event = excluded.event
)
INSERT INTO streamd_cursors (name, cursor)
SELECT $2::text, $3::text WHERE $2::text IS NOT NULL AND $3::text IS NOT NULL
ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`;

// As WRITE, over the transfers table.
const WRITE_TRANSFERS = `
WITH incoming AS (
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, (r->>3)::numeric, r->>4, r->>5, r->>6, (r->>7)::integer, r->>8
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_transfers
    (version, event_index, asset, amount, from_address, to_address, symbol, decimals, transfer)
  SELECT * FROM incoming
  ON CONFLICT (version, event_index) DO UPDATE SET
    asset = excluded.asset, amount = excluded.amount, from_address = excluded.from_address,
    to_address = excluded.to_address, symbol = excluded.symbol, decimals = excluded.decimals,
    transfer = excluded.transfer
)
INSERT INTO streamd_cursors (name, cursor)
SELECT $2::text, $3::text WHERE $2::text IS NOT NULL AND $3::text IS NOT NULL
ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`;

// One statement, so the delete and the edge land together on any connection.
// The edge only rises: it is compared as numeric, since it is stored as text.
const PRUNE = `
WITH deleted AS (DELETE FROM streamd_events WHERE version < $1::numeric),
transfers AS (DELETE FROM streamd_transfers WHERE version < $1::numeric)
INSERT INTO streamd_cursors (name, cursor) VALUES ('${PRUNED_BEFORE}', $1::text)
ON CONFLICT (name) DO UPDATE SET cursor = GREATEST(streamd_cursors.cursor::numeric, excluded.cursor::numeric)::text`;

/**
 * Persists events to a Postgres database through a client the caller
 * supplies. Writes are idempotent on `(version, eventIndex)`: a redelivered
 * event replaces its row rather than adding one, so at-least-once delivery
 * stores each event once. The tables are created on first use.
 */
export class PostgresEventStore {
  private readonly client: PostgresClient;

  constructor(client: PostgresClient) {
    if (typeof client?.query !== "function") throw new StreamdError("Expected a Postgres client with query()");
    this.client = client;
  }

  /**
   * Upserts `events` and, given a checkpoint, saves its cursor, in one
   * statement: either everything lands or nothing does.
   */
  async write(events: readonly StreamdEvent[], checkpoint?: StoreCheckpoint): Promise<void> {
    // Validated before anything is sent, so a bad event writes nothing. One
    // statement cannot update a row twice, so a repeated id keeps its last copy.
    const rows = [...new Map(events.map((e) => [eventId(e), rowOf(e)])).values()];
    await this.upsertAll(WRITE, rows, checkpoint);
  }

  /**
   * Upserts transfer records, as `transfers()` delivers them, into
   * `streamd_transfers` and, given a checkpoint, saves its cursor, in one
   * statement. Idempotent on `(version, eventIndex)`, as `write` is.
   */
  async writeTransfers(records: readonly StreamdEvent<Transfer>[], checkpoint?: StoreCheckpoint): Promise<void> {
    const rows = [...new Map(records.map((r) => [eventId(r), transferRowOf(r)])).values()];
    await this.upsertAll(WRITE_TRANSFERS, rows, checkpoint, TRANSFER_SCHEMA);
  }

  private async upsertAll(
    statement: string,
    rows: unknown[],
    checkpoint: StoreCheckpoint | undefined,
    table?: readonly string[],
  ): Promise<void> {
    const name = checkpoint === undefined ? null : requireName(checkpoint.name);
    const cursor = checkpoint?.cursor;
    if (cursor !== undefined && typeof cursor !== "string") {
      throw new StreamdError("A checkpoint cursor must be a string or undefined");
    }
    await ensureSchema(this.client);
    if (table !== undefined) await ensureSchema(this.client, table);
    await this.client.query(statement, [JSON.stringify(rows), name, cursor ?? null]);
  }

  /**
   * Deletes every event and stored transfer below `beforeVersion` and records
   * that edge, so a `PostgresStoreAdapter` read below it fails instead of
   * returning a short history. Saved cursors stay. A lower edge than one already set stays put.
   */
  async prune(options: { beforeVersion: bigint }): Promise<void> {
    const edge = versionParam(options?.beforeVersion);
    await ensureSchema(this.client);
    await ensureSchema(this.client, TRANSFER_SCHEMA);
    await this.client.query(PRUNE, [edge]);
  }

  /** The cursor last saved under `name`, or `undefined` when none was. */
  async readCursor(name: string): Promise<string | undefined> {
    const key = requireName(name);
    await ensureSchema(this.client);
    const { rows } = await this.client.query("SELECT cursor FROM streamd_cursors WHERE name = $1", [key]);
    const cursor = rows[0]?.cursor;
    return typeof cursor === "string" ? cursor : undefined;
  }

  /**
   * Every saved cursor, sorted by name in byte order whatever the database's
   * collation, as the SQLite store sorts. The pruned edge is not one.
   */
  async cursors(): Promise<SavedCursor[]> {
    await ensureSchema(this.client);
    const { rows } = await this.client.query(
      `SELECT name, cursor FROM streamd_cursors WHERE name <> $1 ORDER BY name COLLATE "C"`,
      [PRUNED_BEFORE],
    );
    return rows.map((row) => ({ name: String(row.name), cursor: String(row.cursor) }));
  }

  /**
   * Removes the cursor saved under `name`, leaving every event, and reports
   * whether there was one. A subscription still saving under `name` writes it again.
   */
  async deleteCursor(name: string): Promise<boolean> {
    const key = requireName(name);
    await ensureSchema(this.client);
    const { rows } = await this.client.query("DELETE FROM streamd_cursors WHERE name = $1 RETURNING name", [key]);
    return rows.length > 0;
  }
}
