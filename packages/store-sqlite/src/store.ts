import { StreamdError, normalizeAddress, parseEvent, serializeEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { ensureSchema, raisePrunedBefore, transaction, versionKey } from "./schema.js";
import type { SqliteDatabase, SqliteStatement } from "./schema.js";

/** A source cursor to save under `name` alongside a write. */
export interface StoreCheckpoint {
  name: string;
  /** `undefined` leaves the saved cursor as it is. */
  cursor: string | undefined;
}

function rowOf(event: StreamdEvent): unknown[] {
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
  ];
}

function requireName(name: unknown): string {
  if (typeof name !== "string" || name === "") {
    throw new StreamdError("A checkpoint name must be a non-empty string");
  }
  return name;
}

/**
 * Persists events to a SQLite database the caller supplies. Writes are
 * idempotent on `(version, eventIndex)`: a redelivered event replaces its row
 * rather than adding one, so at-least-once delivery stores each event once.
 */
export class SqliteEventStore {
  private readonly db: SqliteDatabase;
  private readonly upsert: SqliteStatement;
  private readonly saveCursor: SqliteStatement;
  private readonly loadCursor: SqliteStatement;

  constructor(db: SqliteDatabase) {
    ensureSchema(db);
    this.db = db;
    this.upsert = db.prepare(
      `INSERT INTO streamd_events (version, event_index, type, sender, guid_address, guid_creation, event)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (version, event_index) DO UPDATE SET
         type = excluded.type, sender = excluded.sender, guid_address = excluded.guid_address,
         guid_creation = excluded.guid_creation, event = excluded.event`,
    );
    this.saveCursor = db.prepare(
      `INSERT INTO streamd_cursors (name, cursor) VALUES (?, ?)
       ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor`,
    );
    this.loadCursor = db.prepare("SELECT cursor FROM streamd_cursors WHERE name = ?");
  }

  /**
   * Upserts `events` and, given a checkpoint, saves its cursor, all in one
   * transaction: either everything lands or nothing does.
   */
  write(events: readonly StreamdEvent[], checkpoint?: StoreCheckpoint): void {
    // Validated before the transaction opens, so a bad event writes nothing.
    const rows = events.map(rowOf);
    const name = checkpoint === undefined ? undefined : requireName(checkpoint.name);
    const cursor = checkpoint?.cursor;
    if (cursor !== undefined && typeof cursor !== "string") {
      throw new StreamdError("A checkpoint cursor must be a string or undefined");
    }
    transaction(this.db, () => {
      for (const row of rows) this.upsert.run(...row);
      if (name !== undefined && cursor !== undefined) this.saveCursor.run(name, cursor);
    });
  }

  /**
   * Deletes every event below `beforeVersion` and records that edge, so a
   * `StoreAdapter` read below it fails instead of returning a short history.
   * Saved cursors stay. A lower edge than one already set deletes nothing new.
   */
  prune(options: { beforeVersion: bigint }): void {
    const key = versionKey(options?.beforeVersion);
    transaction(this.db, () => {
      this.db.prepare("DELETE FROM streamd_events WHERE version < ?").run(key);
      raisePrunedBefore(this.db, key);
    });
  }

  /** The cursor last saved under `name`, or `undefined` when none was. */
  readCursor(name: string): string | undefined {
    const row = this.loadCursor.get(requireName(name)) as { cursor: string } | null | undefined;
    return row?.cursor ?? undefined;
  }
}
