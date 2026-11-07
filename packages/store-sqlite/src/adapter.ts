import { StreamdError, parseEvent, requireAddress, requireEventType } from "@streamd/core";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "@streamd/core";
import { ensureSchema, versionKey } from "./schema.js";
import type { SqliteDatabase } from "./schema.js";

const DEFAULT_LIMIT = 25;
const CURSOR = /^(\d{1,20}):(-1|\d+)$/;

interface Position {
  version: bigint;
  eventIndex: number;
}

interface Where {
  sql: string;
  params: unknown[];
}

function typeList(types: readonly string[]): Where {
  return { sql: `type IN (${types.map(() => "?").join(", ")})`, params: types.map(requireEventType) };
}

function descriptorWhere(descriptor: EventDescriptor): Where {
  switch (descriptor?.kind) {
    case "module": {
      const { eventType } = descriptor;
      if (Array.isArray(eventType) && eventType.length === 0) {
        throw new StreamdError("Module descriptor requires at least one eventType");
      }
      return typeList(Array.isArray(eventType) ? eventType : [eventType as string]);
    }
    case "account": {
      const sender: Where = { sql: "sender = ?", params: [requireAddress(descriptor.accountAddress)] };
      if (descriptor.eventTypes === undefined) return sender;
      if (descriptor.eventTypes.length === 0) return { sql: "0", params: [] };
      const types = typeList(descriptor.eventTypes);
      return { sql: `${sender.sql} AND ${types.sql}`, params: [...sender.params, ...types.params] };
    }
    case "handle": {
      const { creationNumber } = descriptor as { creationNumber?: unknown };
      if (typeof creationNumber !== "bigint" || creationNumber < 0n) {
        throw new StreamdError(
          "StoreAdapter serves handle descriptors by creationNumber only; listEventHandles returns it",
        );
      }
      return {
        sql: "guid_address = ? AND guid_creation = ?",
        params: [requireAddress(descriptor.accountAddress), creationNumber.toString()],
      };
    }
    default:
      throw new StreamdError(
        `Unknown descriptor kind: ${String((descriptor as { kind?: unknown })?.kind)}`,
      );
  }
}

function parseCursor(cursor: string): Position {
  const match = typeof cursor === "string" ? CURSOR.exec(cursor) : null;
  if (match === null) throw new StreamdError(`Not a cursor this adapter issued: ${String(cursor)}`);
  const eventIndex = Number(match[2]);
  if (!Number.isSafeInteger(eventIndex)) {
    throw new StreamdError(`Not a cursor this adapter issued: ${cursor}`);
  }
  return { version: BigInt(match[1]!), eventIndex };
}

function startOf(options: FetchOptions): Position {
  if (options.cursor !== undefined) return parseCursor(options.cursor);
  return { version: options.fromVersion ?? 0n, eventIndex: -1 };
}

function count(value: number | undefined, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < 0) throw new StreamdError(`Not a valid ${label}: ${value}`);
  return value;
}

/**
 * Serves events a `SqliteEventStore` wrote, as an `EventAdapter`. It answers
 * from what is stored, so it sees only what the store's subscriptions caught:
 * `account` matches events stored with that `sender`, and `handle` matches by
 * `creationNumber` against each event's stored `guid`.
 */
export class StoreAdapter implements EventAdapter {
  private readonly db: SqliteDatabase;

  constructor(db: SqliteDatabase) {
    ensureSchema(db);
    this.db = db;
  }

  /** The highest stored version across the whole store, or `0n` when it is empty. */
  async getHeadVersion(): Promise<bigint> {
    return this.bound("MAX");
  }

  /** The lowest stored version across the whole store, or `0n` when it is empty. */
  async getEarliestVersion(): Promise<bigint> {
    return this.bound("MIN");
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    const where = descriptorWhere(descriptor);
    const start = startOf(options);
    const limit = count(options.limit, "limit", DEFAULT_LIMIT);
    const offset = options.cursor === undefined ? count(options.offset, "offset", 0) : 0;
    const startCursor = `${start.version}:${start.eventIndex}`;
    if (limit === 0) return { events: [], nextCursor: startCursor, hasMore: false };

    const params: unknown[] = [...where.params, versionKey(start.version), start.eventIndex];
    let sql = `SELECT event FROM streamd_events WHERE ${where.sql} AND (version, event_index) > (?, ?)`;
    if (options.toVersion !== undefined) {
      sql += " AND version <= ?";
      params.push(versionKey(options.toVersion));
    }
    // One row past the page says whether another is waiting.
    sql += " ORDER BY version, event_index LIMIT ? OFFSET ?";
    params.push(limit + 1, offset);

    const rows = this.db.prepare(sql).all(...params) as { event: string }[];
    const events: StreamdEvent[] = rows.slice(0, limit).map((r) => parseEvent(r.event));
    const last = events.at(-1);
    const page: EventPage = {
      events,
      nextCursor: last === undefined ? startCursor : `${last.version}:${last.eventIndex}`,
      hasMore: rows.length > limit,
    };
    // No throughVersion: another writer may still add rows below the cursor.
    if (options.orderBy === "desc") events.reverse();
    return page;
  }

  private bound(fn: "MAX" | "MIN"): bigint {
    const row = this.db.prepare(`SELECT ${fn}(version) AS v FROM streamd_events`).get() as
      | { v: string | null }
      | null
      | undefined;
    return row?.v == null ? 0n : BigInt(row.v);
  }
}
