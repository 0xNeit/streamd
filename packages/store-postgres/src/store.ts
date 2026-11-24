import {
  StreamdError,
  eventId,
  eventTableRows,
  normalizeAddress,
  parseEvent,
  requireAddress,
  requireEventTables,
  scoreFromJson,
  scoreToJson,
  serializeEvent,
} from "@streamd/core";
import type { EventTable, PointsState, Score, StreamdEvent, Transfer } from "@streamd/core";
import { EDGE, POINTS_SCHEMA, PRUNED_BEFORE, RECORD_SCHEMA, ensureSchema, forgetSchema, hashKey, versionParam } from "./schema.js";
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

/** A swap record's data as `writeSwaps` stores it; `@streamd/aptos`'s `Swap` fits it. */
export interface StoredSwap {
  protocol: string;
  poolId: string;
  trader?: string;
  assetIn: string;
  amountIn: bigint;
  assetOut: string;
  amountOut: bigint;
}

/** A sale record's data as `writeSales` stores it; `@streamd/aptos`'s `Sale` fits it. */
export interface StoredSale {
  marketplace: string;
  kind: string;
  token: string;
  collection?: string;
  seller: string;
  buyer: string;
  price: bigint;
  royalty?: bigint;
  fee?: bigint;
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
  /** Rows of `streamd_swaps`. */
  swaps: bigint;
  /** Rows of `streamd_sales`. */
  sales: bigint;
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
  return typeof message === "string" && /relation "[^"]+" does not exist/i.test(message);
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

function optionalText(value: unknown, field: string, cap = false, record = "transfer"): string | null {
  if (value === undefined) return null;
  if (typeof value !== "string") throw new StreamdError(`Not a valid ${record} ${field}: ${String(value)}`);
  return cap ? indexText(value) : columnText(value);
}

function requiredText(value: unknown, field: string, record: string, cap = false): string {
  if (typeof value !== "string" || value === "") throw new StreamdError(`A ${record} needs a non-empty ${field}`);
  return cap ? indexText(value) : columnText(value);
}

function u256Text(value: unknown, field: string, record: string): string {
  if (typeof value !== "bigint" || value < 0n || value > U256_MAX) {
    throw new StreamdError(`Not a valid ${record} ${field}: ${String(value)} (expected a u256 bigint)`);
  }
  return value.toString();
}

function optionalU256Text(value: unknown, field: string, record: string): string | null {
  return value === undefined ? null : u256Text(value, field, record);
}

/** The record's version, eventIndex and JSON, and its data, refused unless an object. */
function recordOf<T>(record: StreamdEvent<T>, kind: string): [[string, number, string], Partial<T>] {
  const [version, eventIndex, , , , , json] = rowOf(record);
  const data = record.data as Partial<T> | null;
  if (typeof data !== "object" || data === null) throw new StreamdError(`A ${kind} record needs an object as data`);
  return [[version, eventIndex, json], data];
}

function swapRowOf(record: StreamdEvent<StoredSwap>): unknown[] {
  const [[version, eventIndex, json], d] = recordOf(record, "swap");
  return [
    version,
    eventIndex,
    requiredText(d.protocol, "protocol", "swap"),
    requiredText(d.poolId, "poolId", "swap", true),
    optionalText(d.trader, "trader", true, "swap"),
    requiredText(d.assetIn, "assetIn", "swap", true),
    u256Text(d.amountIn, "amountIn", "swap"),
    requiredText(d.assetOut, "assetOut", "swap", true),
    u256Text(d.amountOut, "amountOut", "swap"),
    json,
  ];
}

function saleRowOf(record: StreamdEvent<StoredSale>): unknown[] {
  const [[version, eventIndex, json], d] = recordOf(record, "sale");
  return [
    version,
    eventIndex,
    requiredText(d.marketplace, "marketplace", "sale"),
    requiredText(d.kind, "kind", "sale"),
    requiredText(d.token, "token", "sale", true),
    optionalText(d.collection, "collection", true, "sale"),
    requiredText(d.seller, "seller", "sale", true),
    requiredText(d.buyer, "buyer", "sale", true),
    u256Text(d.price, "price", "sale"),
    optionalU256Text(d.royalty, "royalty", "sale"),
    optionalU256Text(d.fee, "fee", "sale"),
    json,
  ];
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

// As WRITE, over the swaps table.
const WRITE_SWAPS = `
WITH incoming AS (
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, r->>3, r->>4, r->>5, (r->>6)::numeric, r->>7, (r->>8)::numeric, r->>9
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_swaps
    (version, event_index, protocol, pool_id, trader, asset_in, amount_in, asset_out, amount_out, swap)
  SELECT * FROM incoming
  ON CONFLICT (version, event_index) DO UPDATE SET
    protocol = excluded.protocol, pool_id = excluded.pool_id, trader = excluded.trader,
    asset_in = excluded.asset_in, amount_in = excluded.amount_in, asset_out = excluded.asset_out,
    amount_out = excluded.amount_out, swap = excluded.swap
)
INSERT INTO streamd_cursors (name, cursor)
SELECT $2::text, $3::text WHERE $2::text IS NOT NULL AND $3::text IS NOT NULL
ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`;

// As WRITE, over the sales table.
const WRITE_SALES = `
WITH incoming AS (
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, r->>3, r->>4, r->>5, r->>6, r->>7,
    (r->>8)::numeric, (r->>9)::numeric, (r->>10)::numeric, r->>11
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_sales
    (version, event_index, marketplace, kind, token, collection, seller, buyer, price, royalty, fee, sale)
  SELECT * FROM incoming
  ON CONFLICT (version, event_index) DO UPDATE SET
    marketplace = excluded.marketplace, kind = excluded.kind, token = excluded.token,
    collection = excluded.collection, seller = excluded.seller, buyer = excluded.buyer,
    price = excluded.price, royalty = excluded.royalty, fee = excluded.fee, sale = excluded.sale
)
INSERT INTO streamd_cursors (name, cursor)
SELECT $2::text, $3::text WHERE $2::text IS NOT NULL AND $3::text IS NOT NULL
ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`;

// As WRITE, over the points table.
const WRITE_POINTS = `
WITH incoming AS (
  SELECT r->>0, r->>1, (r->>2)::numeric, (r->>3)::numeric, r->>4
  FROM jsonb_array_elements($1::text::jsonb) AS r
), written AS (
  INSERT INTO streamd_points (name, address, points, version, score)
  SELECT * FROM incoming
  ON CONFLICT (name, address) DO UPDATE SET
    points = excluded.points, version = excluded.version, score = excluded.score
)
INSERT INTO streamd_cursors (name, cursor)
SELECT $2::text, $3::text WHERE $2::text IS NOT NULL AND $3::text IS NOT NULL
ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`;

function requirePointsName(name: string): string {
  if (typeof name !== "string" || name === "") throw new StreamdError("A points leaderboard needs a non-empty name");
  return name;
}

// One statement, so the delete and the edge land together on any connection.
// The edge only rises: it is compared as numeric, since it is stored as text.
const PRUNE = `
WITH deleted AS (DELETE FROM streamd_events WHERE version < $1::numeric),
transfers AS (DELETE FROM streamd_transfers WHERE version < $1::numeric),
swaps AS (DELETE FROM streamd_swaps WHERE version < $1::numeric),
sales AS (DELETE FROM streamd_sales WHERE version < $1::numeric)
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
  (SELECT count(*) FROM streamd_swaps)::text AS swaps,
  (SELECT count(*) FROM streamd_sales)::text AS sales,
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
    await this.upsertAll(WRITE_TRANSFERS, rows, checkpoint, RECORD_SCHEMA);
  }

  /**
   * Upserts swap records, as `swaps()` delivers them, into `streamd_swaps`
   * and, given a checkpoint, saves its cursor, in one statement. Idempotent
   * on `(version, eventIndex)`, as `write` is.
   */
  async writeSwaps(records: readonly StreamdEvent<StoredSwap>[], checkpoint?: StoreCheckpoint): Promise<void> {
    const rows = [...new Map(records.map((r) => [eventId(r), swapRowOf(r)])).values()];
    await this.upsertAll(WRITE_SWAPS, rows, checkpoint, RECORD_SCHEMA);
  }

  /**
   * Upserts sale records, as `sales()` delivers them, into `streamd_sales`
   * and, given a checkpoint, saves its cursor, in one statement. Idempotent
   * on `(version, eventIndex)`, as `write` is.
   */
  async writeSales(records: readonly StreamdEvent<StoredSale>[], checkpoint?: StoreCheckpoint): Promise<void> {
    const rows = [...new Map(records.map((r) => [eventId(r), saleRowOf(r)])).values()];
    await this.upsertAll(WRITE_SALES, rows, checkpoint, RECORD_SCHEMA);
  }

  /**
   * Upserts `scores` as the rows of the leaderboard `name` in `streamd_points`
   * and, given a checkpoint, saves its cursor, in one statement.
   */
  async writePoints(name: string, scores: Iterable<Score>, checkpoint?: StoreCheckpoint): Promise<void> {
    const board = requirePointsName(name);
    // One statement cannot update a row twice, so a repeated address keeps its last copy.
    const rows = new Map<string, unknown[]>();
    for (const score of scores) {
      rows.set(score.address, [board, score.address, score.points.toString(), versionParam(score.version), scoreToJson(score)]);
    }
    await this.upsertAll(WRITE_POINTS, [...rows.values()], checkpoint, POINTS_SCHEMA);
  }

  /** The leaderboard `name` as `writePoints` left it, keyed by long-form address; empty when none. */
  async readPoints(name: string): Promise<PointsState> {
    const board = requirePointsName(name);
    await ensureSchema(this.client, POINTS_SCHEMA);
    const { rows } = await this.client.query(
      `SELECT score FROM streamd_points WHERE name = $1 ORDER BY address COLLATE "C"`,
      [board],
    );
    return new Map(rows.map((row) => {
      const score = scoreFromJson(String(row.score));
      return [score.address, score];
    }));
  }

  private async runWrite(run: () => Promise<void>, table?: readonly string[]): Promise<void> {
    const prepare = async () => {
      await ensureSchema(this.client);
      if (table !== undefined) await ensureSchema(this.client, table);
    };
    await prepare();
    try {
      await run();
    } catch (err) {
      if (!isMissingTable(err)) throw err;
      forgetSchema(this.client);
      if (table !== undefined) forgetSchema(this.client, table);
      await prepare();
      await run();
    }
  }

  private async upsertAll(
    statement: string,
    rows: unknown[],
    checkpoint: StoreCheckpoint | undefined,
    table?: readonly string[],
  ): Promise<void> {
    const [name, cursor] = checkpointParams(checkpoint);
    await this.runWrite(() => this.client.query(statement, [JSON.stringify(rows), name, cursor]).then(() => {}), table);
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
    const present = spec.filter((t) => byTable.has(t));
    const tableSchema = eventTableSchema(spec);
    if (present.length === 0 && (name === null || cursor === null)) {
      await ensureSchema(this.client);
      await ensureSchema(this.client, tableSchema);
      return;
    }
    const params = present.map((t) =>
      JSON.stringify([...byTable.get(t)!.values()], (_, v: unknown) => (typeof v === "bigint" ? v.toString() : v)),
    );
    await this.runWrite(() => this.client.query(eventTablesWrite(present), [...params, name, cursor]).then(() => {}), tableSchema);
  }

  /**
   * Deletes every event and stored transfer, swap and sale below `beforeVersion` and records
   * that edge, so a `PostgresStoreAdapter` read below it fails instead of
   * returning a short history. Saved cursors stay. A lower edge than one already set stays put.
   */
  async prune(options: { beforeVersion: bigint }): Promise<void> {
    const edge = versionParam(options?.beforeVersion);
    await ensureSchema(this.client);
    await ensureSchema(this.client, RECORD_SCHEMA);
    await this.client.query(PRUNE, [edge]);
  }

  /**
   * Counts what the store holds, in one statement so every count reads one
   * snapshot. Like every read, it creates and migrates the event tables on
   * first use, and creates the record tables as `prune` does. Each count
   * scans its table or an index.
   */
  async stats(options?: StoreStatsOptions): Promise<StoreStats> {
    const byType = wantsByType(options);
    await ensureSchema(this.client);
    await ensureSchema(this.client, RECORD_SCHEMA);
    const { rows } = await this.client.query(STATS, [byType]);
    const row = rows[0]!;
    const stats: StoreStats = {
      events: BigInt(String(row.rows)) - BigInt(String(row.changes)),
      resourceChanges: BigInt(String(row.changes)),
      transfers: BigInt(String(row.transfers)),
      swaps: BigInt(String(row.swaps)),
      sales: BigInt(String(row.sales)),
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
