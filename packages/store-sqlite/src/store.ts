import { StreamdError, normalizeAddress, parseEvent, requireAddress, serializeEvent } from "@streamd/core";
import type { StreamdEvent, Transfer } from "@streamd/core";
import {
  PRUNED_BEFORE,
  TRANSFER_SCHEMA,
  ensureSchema,
  migrateSchema,
  raisePrunedBefore,
  transaction,
  versionKey,
} from "./schema.js";
import type { SqliteDatabase, SqliteStatement } from "./schema.js";

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

function rowOf(event: StreamdEvent, accountAddress: string | null = null): unknown[] {
  if (!Number.isSafeInteger(event.eventIndex) || event.eventIndex < 0) {
    throw new StreamdError(`Not a valid eventIndex: ${String(event.eventIndex)}`);
  }
  const json = serializeEvent(event);
  // Refused now rather than stored, where it would fail every read of its range.
  parseEvent(json);
  const sender = event.sender === undefined ? null : (normalizeAddress(event.sender) ?? null);
  const guidAddress = event.guid && (normalizeAddress(event.guid.accountAddress) ?? null);
  return [
    versionKey(event.version),
    event.eventIndex,
    event.type,
    sender,
    guidAddress ?? null,
    guidAddress ? event.guid!.creationNumber.toString() : null,
    json,
    accountAddress,
  ];
}

const U256_MAX = 2n ** 256n - 1n;

function optionalText(value: unknown, field: string): string | null {
  if (value === undefined) return null;
  if (typeof value !== "string") throw new StreamdError(`Not a valid transfer ${field}: ${String(value)}`);
  return value;
}

function transferRowOf(record: StreamdEvent<Transfer>): unknown[] {
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
    asset,
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

/**
 * Persists events to a SQLite database the caller supplies. Writes are
 * idempotent on `(version, eventIndex)`: a redelivered event replaces its row
 * rather than adding one, so at-least-once delivery stores each event once.
 */
export class SqliteEventStore {
  private readonly db: SqliteDatabase;
  private upsert?: SqliteStatement;
  private readonly saveCursor: SqliteStatement;
  private readonly loadCursor: SqliteStatement;
  private readonly listCursors: SqliteStatement;
  private readonly dropCursor: SqliteStatement;
  private upsertTransfer?: SqliteStatement;

  constructor(db: SqliteDatabase) {
    ensureSchema(db);
    this.db = db;
    this.saveCursor = db.prepare(
      `INSERT INTO streamd_cursors (name, cursor) VALUES (?, ?)
       ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`,
    );
    this.loadCursor = db.prepare("SELECT cursor FROM streamd_cursors WHERE name = ?");
    this.listCursors = db.prepare("SELECT name, cursor FROM streamd_cursors WHERE name <> ? ORDER BY name");
    this.dropCursor = db.prepare("DELETE FROM streamd_cursors WHERE name = ?");
  }

  /**
   * Upserts `events` and, given a checkpoint, saves its cursor, all in one
   * transaction: either everything lands or nothing does. A rewrite without
   * `accountAddress` keeps the one the row was stored with.
   */
  write(events: readonly StreamdEvent[], checkpoint?: StoreCheckpoint, options?: StoreWriteOptions): void {
    // Validated before the transaction opens, so a bad event writes nothing.
    const address = options?.accountAddress === undefined ? null : requireAddress(options.accountAddress);
    const rows = events.map((event) => rowOf(event, address));
    // Prepared on first write, which migrates an older database: a read-only
    // connection that never writes can still read its cursors.
    if (this.upsert === undefined) migrateSchema(this.db);
    this.upsert ??= this.db.prepare(
      `INSERT INTO streamd_events
         (version, event_index, type, sender, guid_address, guid_creation, event, account_address)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (version, event_index) DO UPDATE SET
         type = excluded.type, sender = excluded.sender, guid_address = excluded.guid_address,
         guid_creation = excluded.guid_creation, event = excluded.event,
         account_address = coalesce(excluded.account_address, account_address)`,
    );
    this.upsertAll(this.upsert, rows, checkpoint);
  }

  private upsertAll(upsert: SqliteStatement, rows: unknown[][], checkpoint: StoreCheckpoint | undefined): void {
    const name = checkpoint === undefined ? undefined : requireName(checkpoint.name);
    const cursor = checkpoint?.cursor;
    if (cursor !== undefined && typeof cursor !== "string") {
      throw new StreamdError("A checkpoint cursor must be a string or undefined");
    }
    transaction(this.db, () => {
      for (const row of rows) upsert.run(...row);
      if (name !== undefined && cursor !== undefined) this.saveCursor.run(name, cursor);
    });
  }

  /**
   * Upserts transfer records, as `transfers()` delivers them, into
   * `streamd_transfers` and, given a checkpoint, saves its cursor, all in one
   * transaction. Idempotent on `(version, eventIndex)`, as `write` is.
   */
  writeTransfers(records: readonly StreamdEvent<Transfer>[], checkpoint?: StoreCheckpoint): void {
    const rows = records.map(transferRowOf);
    if (this.upsertTransfer === undefined) this.db.exec(TRANSFER_SCHEMA);
    this.upsertTransfer ??= this.db.prepare(
      `INSERT INTO streamd_transfers
         (version, event_index, asset, amount, from_address, to_address, symbol, decimals, transfer)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (version, event_index) DO UPDATE SET
         asset = excluded.asset, amount = excluded.amount, from_address = excluded.from_address,
         to_address = excluded.to_address, symbol = excluded.symbol, decimals = excluded.decimals,
         transfer = excluded.transfer`,
    );
    this.upsertAll(this.upsertTransfer, rows, checkpoint);
  }

  /**
   * Deletes every event and stored transfer below `beforeVersion` and records
   * that edge, so a `StoreAdapter` read below it fails instead of returning a
   * short history. Saved cursors stay. A lower edge than one already set
   * deletes nothing new.
   */
  prune(options: { beforeVersion: bigint }): void {
    const key = versionKey(options?.beforeVersion);
    this.db.exec(TRANSFER_SCHEMA);
    transaction(this.db, () => {
      this.db.prepare("DELETE FROM streamd_events WHERE version < ?").run(key);
      this.db.prepare("DELETE FROM streamd_transfers WHERE version < ?").run(key);
      raisePrunedBefore(this.db, key);
    });
  }

  /** The cursor last saved under `name`, or `undefined` when none was. */
  readCursor(name: string): string | undefined {
    const row = this.loadCursor.get(requireName(name)) as { cursor: string } | null | undefined;
    return row?.cursor ?? undefined;
  }

  /** Every saved cursor, sorted by name in the database encoding's byte order. The pruned edge is not one. */
  cursors(): SavedCursor[] {
    return (this.listCursors.all(PRUNED_BEFORE) as SavedCursor[]).map(({ name, cursor }) => ({ name, cursor }));
  }

  /**
   * Removes the cursor saved under `name`, leaving every event, and reports
   * whether there was one. A subscription still saving under `name` writes it again.
   */
  deleteCursor(name: string): boolean {
    const { changes } = this.dropCursor.run(requireName(name)) as { changes: number | bigint };
    return Number(changes) > 0;
  }
}
