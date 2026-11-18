import {
  StreamdError,
  eventId,
  eventTableRows,
  normalizeAddress,
  parseEvent,
  requireAddress,
  requireEventTables,
  serializeEvent,
} from "@streamd/core";
import type { EventTable, StreamdEvent, Transfer } from "@streamd/core";
import { EDGE, PRUNED_BEFORE, TRANSFER_SCHEMA, ensureSchema, forgetSchema, hashKey, versionParam } from "./schema.js";
import type { PostgresClient } from "./schema.js";
import { eventTableSchema, eventTablesWrite } from "./tables.js";

/** A source cursor to save under `name` alongside a write. */
export interface StoreCheckpoint {
  name: string;
  /** `undefined` leaves the saved cursor as it is. */
  cursor: string | undefined;
}

/** What a write knows about its events that they do not carry themselves. */
export interface StoreWriteOptions {
  /**
   * The address holding the resource these events are changes of, as a
   * `resource` descriptor names it. A change carries only the resource type,
   * so this is what lets a store adapter serve the descriptor back.
   */
  accountAddress?: string;
}

/** A cursor saved under a name, as `cursors()` lists it. */
export interface SavedCursor {
  name: string;
  cursor: string;
}

/** What `stats()` reads beyond the totals. */
export interface StoreStatsOptions {
  /** Also count events per type. */
  byType?: boolean;
}

/** How many stored events have one type. */
export interface StoreTypeCount {
  type: string;
  count: bigint;
}

/** What a store holds, as `stats()` reports it. */
export interface StoreStats {
  /** Stored events: every row of `streamd_events` not written as a resource change. */
  events: bigint;
  /** Rows written as resource changes, with an `accountAddress`. */
  resourceChanges: bigint;
  /** Rows of `streamd_transfers`. */
  transfers: bigint;
  /** The lowest version of any stored event or resource change; absent when there are none. */
  lowestVersion?: bigint;
  /** The highest version of any stored event or resource change; absent when there are none. */
  highestVersion?: bigint;
  /** The pruned edge: `0n` until the first prune. */
  prunedBefore: bigint;
  /** Events per type, most first, then by type in byte order; only when asked. */
  byType?: StoreTypeCount[];
}

/** Reads `byType` from `stats()` options, refusing anything but a boolean. */
function wantsByType(options: StoreStatsOptions | undefined): boolean {
  if (options !== undefined && (typeof options !== "object" || options === null)) {
    throw new StreamdError("stats() options must be an object");
  }
  const byType = options?.byType;
  if (byType !== undefined && typeof byType !== "boolean") {
    throw new StreamdError(`Not a valid byType: ${String(byType)} (expected a boolean)`);
  }
  return byType === true;
}

function isMissingTable(err: unknown): boolean {
  if ((err as { code?: unknown } | null)?.code === "42P01") return true;
  const message = (err as { message?: unknown } | null)?.message;
  return typeof message === "string" && /relation "streamd_\w+" does not exist/i.test(message);
}

type Row = [string, number, string, string | null, string | null, string | null, string, string | null, string | null];

function rowOf(event: StreamdEvent, accountAddress: string | null = null): Row {
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
    accountAddress,
    typeof event.transactionHash === "string" ? hashKey(event.transactionHash) : null,
  ];
}

type TransferRow = [string, number, string, string, string | null, string | null, string | null, number | null, string];

const U256_MAX = 2n ** 256n - 1n;
// Postgres btree entries fail above ~2704 bytes; keep indexed transfer columns within 2048 UTF-8 bytes
// while preserving the full strings inside the `transfer` JSON text.
const MAX_INDEXED_BYTES = 2048;
const UTF8_ENCODE = new TextEncoder();
const UTF8_DECODE = new TextDecoder("utf-8");

// `text` cannot hold NUL, which an asset's symbol may carry; the record JSON
// keeps the exact value, so the column takes a replacement character instead
// of failing the write and, with it, every retry of the sink.
function columnText(value: string): string {
  return value.replaceAll("\0", "\uFFFD");
}

function indexText(value: string): string {
  const clean = columnText(value);
  const encoded = UTF8_ENCODE.encode(clean);
  if (encoded.byteLength <= MAX_INDEXED_BYTES) return clean;
  let end = MAX_INDEXED_BYTES;
  // Back off any incomplete UTF-8 multi-byte sequence at the byte cap.
  while (end > 0 && (encoded[end]! & 0xc0) === 0x80) end--;
  return UTF8_DECODE.decode(encoded.subarray(0, end));
}

// Nor can `jsonb` parse a lone surrogate, which a JavaScript string from a custom adapter may hold.
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;
const tableText = (value: string) => columnText(value).replace(LONE_SURROGATE, "\uFFFD");

function optionalText(value: unknown, field: string, cap = false): string | null {
  if (value === undefined) return null;
  if (typeof value !== "string") throw new StreamdError(`Not a valid transfer ${field}: ${String(value)}`);
  return cap ? indexText(value) : columnText(value);
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
    indexText(asset),
    amount.toString(),
    optionalText(data.from, "from", true),
    optionalText(data.to, "to", true),
    optionalText(data.symbol, "symbol"),
    decimals ?? null,
    json,
  ];
}

function checkpointParams(checkpoint: StoreCheckpoint | undefined): [string | null, string | null] {
  const name = checkpoint === undefined ? null : requireName(checkpoint.name);
  const cursor = checkpoint?.cursor;
  if (cursor !== undefined && typeof cursor !== "string") {
    throw new StreamdError("A checkpoint cursor must be a string or undefined");
  }
  return [name, cursor ?? null];
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
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, r->>3, r->>4, (r->>5)::numeric, r->>6, r->>7, r->>8
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_events
    (version, event_index, type, sender, guid_address, guid_creation, event, account_address, transaction_hash)
  SELECT * FROM incoming
  ON CONFLICT (version, event_index) DO UPDATE SET
    type = excluded.type, sender = excluded.sender, guid_address = excluded.guid_address,
    guid_creation = excluded.guid_creation, event = excluded.event,
    account_address = coalesce(excluded.account_address, streamd_events.account_address),
    transaction_hash = excluded.transaction_hash
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

// One statement, so every count reads one snapshot. Numbers come back as text,
// which every driver returns as it is. The per-type list is JSON text, sorted
// in byte order as the SQLite store sorts.
const STATS = `
WITH totals AS (
  SELECT count(*) AS rows, count(account_address) AS changes, min(version) AS lowest, max(version) AS highest
  FROM streamd_events
)
SELECT rows::text AS rows, changes::text AS changes, lowest::text AS lowest, highest::text AS highest,
  (SELECT count(*) FROM streamd_transfers)::text AS transfers,
  ${EDGE}::text AS edge,
  CASE WHEN $1::boolean THEN (
    SELECT coalesce(json_agg(json_build_array(type, n::text) ORDER BY n DESC, type COLLATE "C"), '[]'::json)::text
    FROM (SELECT type, count(*) AS n FROM streamd_events WHERE account_address IS NULL GROUP BY type) AS counted
  ) END AS by_type
FROM totals`;

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
   * statement: either everything lands or nothing does. A rewrite without
   * `accountAddress` keeps the one the row was stored with.
   */
  async write(events: readonly StreamdEvent[], checkpoint?: StoreCheckpoint, options?: StoreWriteOptions): Promise<void> {
    // Validated before anything is sent, so a bad event writes nothing. One
    // statement cannot update a row twice, so a repeated id keeps its last copy.
    const address = options?.accountAddress === undefined ? null : requireAddress(options.accountAddress);
    const rows = [...new Map(events.map((e) => [eventId(e), rowOf(e, address)])).values()];
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
    const [name, cursor] = checkpointParams(checkpoint);
    const prepare = async () => {
      await ensureSchema(this.client);
      if (table !== undefined) await ensureSchema(this.client, table);
    };
    await prepare();
    try {
      await this.client.query(statement, [JSON.stringify(rows), name, cursor]);
    } catch (err) {
      if (!isMissingTable(err)) throw err;
      forgetSchema(this.client);
      if (table !== undefined) forgetSchema(this.client, table);
      await prepare();
      await this.client.query(statement, [JSON.stringify(rows), name, cursor]);
    }
  }

  /**
   * Upserts each event into its table of `tables`, creating them on first
   * use, and, given a checkpoint, saves its cursor, in one statement.
   * Idempotent on `(version, eventIndex)`. An event with no table, or a value
   * its column refuses, writes nothing.
   */
  async writeTables(tables: readonly EventTable[], events: readonly StreamdEvent[], checkpoint?: StoreCheckpoint): Promise<void> {
    const rows = eventTableRows(tables, events, tableText);
    const [name, cursor] = checkpointParams(checkpoint);
    const spec = requireEventTables(tables);
    // One statement cannot update a row twice, so a repeated id keeps its last copy.
    const byTable = new Map<EventTable, Map<string, unknown[]>>();
    rows.forEach(({ table, values }, i) => {
      if (!byTable.has(table)) byTable.set(table, new Map());
      byTable.get(table)!.set(eventId(events[i]!), values);
    });
    await ensureSchema(this.client);
    await ensureSchema(this.client, eventTableSchema(spec));
    const present = spec.filter((t) => byTable.has(t));
    if (present.length === 0 && (name === null || cursor === null)) return;
    const params = present.map((t) =>
      JSON.stringify([...byTable.get(t)!.values()], (_, v: unknown) => (typeof v === "bigint" ? v.toString() : v)),
    );
    await this.client.query(eventTablesWrite(present), [...params, name, cursor]);
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

  /**
   * Counts what the store holds, in one statement so every count reads one
   * snapshot. Like every read, it creates and migrates the event tables on
   * first use, and creates `streamd_transfers` as `prune` does. Each count
   * scans its table or an index.
   */
  async stats(options?: StoreStatsOptions): Promise<StoreStats> {
    const byType = wantsByType(options);
    await ensureSchema(this.client);
    await ensureSchema(this.client, TRANSFER_SCHEMA);
    const { rows } = await this.client.query(STATS, [byType]);
    const row = rows[0]!;
    const stats: StoreStats = {
      events: BigInt(String(row.rows)) - BigInt(String(row.changes)),
      resourceChanges: BigInt(String(row.changes)),
      transfers: BigInt(String(row.transfers)),
      ...(row.lowest != null && { lowestVersion: BigInt(String(row.lowest)), highestVersion: BigInt(String(row.highest)) }),
      prunedBefore: BigInt(String(row.edge)),
    };
    if (byType) {
      const counted = JSON.parse(String(row.by_type)) as [string, string][];
      stats.byType = counted.map(([type, count]) => ({ type, count: BigInt(count) }));
    }
    return stats;
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
