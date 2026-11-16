import {
  StreamdError,
  StreamdNetworkError,
  parseEvent,
  requireAddress,
  requireEventType,
  requireResourceType,
} from "@streamd/core";
import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
  TransactionEvents,
} from "@streamd/core";
import { ensureSchema, eventColumns, hashKey, prunedBefore, versionKey } from "./schema.js";
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

/**
 * A `module` descriptor's types: each exactly, and one written without type
 * arguments also with any. The column's BINARY collation orders by bytes, so
 * `T<...` lies in [`T<`, `T=`). The range is added only when a probe finds a
 * row in it, so a non-generic type keeps the plan that seeks the type index.
 */
function moduleTypes(db: SqliteDatabase, types: readonly string[]): Where {
  const exact = typeList(types);
  const probe = db.prepare("SELECT 1 FROM streamd_events WHERE type >= ? AND type < ? LIMIT 1");
  const generic = (exact.params as string[]).filter((t) => !t.includes("<") && probe.get(`${t}<`, `${t}=`) != null);
  if (generic.length === 0) return exact;
  const ranges = generic.map(() => " OR (type >= ? AND type < ?)").join("");
  return { sql: `(${exact.sql}${ranges})`, params: [...exact.params, ...generic.flatMap((t) => [`${t}<`, `${t}=`])] };
}

function descriptorWhere(db: SqliteDatabase, descriptor: EventDescriptor): Where {
  switch (descriptor?.kind) {
    case "module": {
      const { eventType } = descriptor;
      if (Array.isArray(eventType) && eventType.length === 0) {
        throw new StreamdError("Module descriptor requires at least one eventType");
      }
      return moduleTypes(db, Array.isArray(eventType) ? eventType : [eventType as string]);
    }
    case "account": {
      if (descriptor.received === true) throw new StreamdError("StoreAdapter keys account events by sender, so it cannot serve received ones");
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
    case "resource":
      return {
        sql: "account_address = ? AND type = ?",
        params: [requireAddress(descriptor.accountAddress), requireResourceType(descriptor.resourceType)],
      };
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

/**
 * No start position means the oldest version kept; only asking for pruned history fails.
 * An empty page leaves `nextCursor` at the last match, so pruning past a quiet
 * follower's cursor triggers a 410 `StreamdNetworkError` even when nothing matching was
 * deleted; restart such a follower with `fromVersion: "earliest"`.
 */
function startOf(options: FetchOptions, edge: bigint): Position {
  const start =
    options.cursor !== undefined
      ? parseCursor(options.cursor)
      : { version: options.fromVersion ?? edge, eventIndex: -1 };
  // A cursor at edge - 1 fails too: it cannot show whether that version's later events went.
  if (start.version < edge || (options.toVersion !== undefined && options.toVersion < edge)) {
    throw new StreamdNetworkError(`Version pruned from this store; the oldest kept is ${edge}`, 410);
  }
  return start;
}

function requireHash(hash: unknown): string {
  if (typeof hash !== "string" || hash === "") throw new StreamdError("A transaction hash must be a non-empty string");
  return hashKey(hash);
}

/**
 * Stored rows as one transaction. A store keeps no status, so even a failed
 * transaction's stored fee statement reads as a success.
 */
function transactionOf(rows: { event: string }[]): TransactionEvents | undefined {
  if (rows.length === 0) return undefined;
  const events = rows.map((r) => parseEvent(r.event));
  return { version: events[0]!.version, success: true, vmStatus: "", events };
}

function count(value: number | undefined, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < 0) throw new StreamdError(`Not a valid ${label}: ${value}`);
  return value;
}

/**
 * Serves events a `SqliteEventStore` wrote, as an `EventAdapter`. It answers
 * from what is stored, so it sees only what the store's subscriptions caught:
 * `account` matches events stored with that `sender` (and refuses `received: true`,
 * so received transactions written from `IndexerAccountAdapter` must be read by
 * `module`, `handle`, or transaction lookup), `handle` matches by
 * `creationNumber` against each event's stored `guid`, and `resource` matches
 * changes `storeEvents` stored with that address, so none an older release
 * stored without one. Changes stored with an address match no other kind.
 *
 * Empty fetches keep `nextCursor` at the last match, so a quiet follower fails
 * with status 410 `StreamdNetworkError` once `prune` passes that cursor and can
 * restart from `fromVersion: "earliest"`.
 */
export class StoreAdapter implements EventAdapter {
  private readonly db: SqliteDatabase;

  constructor(db: SqliteDatabase) {
    ensureSchema(db);
    this.db = db;
  }

  /** The highest stored version, never below the pruned edge; `0n` for a new store. */
  async getHeadVersion(): Promise<bigint> {
    const edge = prunedBefore(this.db);
    const highest = this.bound("MAX") ?? 0n;
    return highest > edge ? highest : edge;
  }

  /** The lowest readable version: the lowest stored, or the pruned edge if that is higher. */
  async getEarliestVersion(): Promise<bigint> {
    const edge = prunedBefore(this.db);
    const lowest = this.bound("MIN") ?? 0n;
    return lowest > edge ? lowest : edge;
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    let where = descriptorWhere(this.db, descriptor);
    // Never migrates, since it may be read-only: a database no write has
    // migrated yet holds no resource rows, to serve or to leave out.
    const migrated = eventColumns(this.db).has("account_address");
    if (descriptor.kind === "resource") {
      if (!migrated) where = { sql: "0", params: [] };
    } else if (migrated) {
      where = { sql: `${where.sql} AND account_address IS NULL`, params: where.params };
    }
    const start = startOf(options, prunedBefore(this.db));
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

  /**
   * The stored events of the transaction at the version a row with this hash
   * is stored at, in transaction order, so rows stored there without a hash
   * (handle events, older rows) come back too, as `getTransactionEventsByVersion`
   * returns them; `undefined` when none is stored, which `eventsOf` waits on.
   * Only what the store's subscriptions caught, and no resource changes.
   */
  async getTransactionEvents(transactionHash: string): Promise<TransactionEvents | undefined> {
    const hash = requireHash(transactionHash);
    if (!eventColumns(this.db).has("transaction_hash")) return undefined;
    const rows = this.db
      .prepare(
        `SELECT event FROM streamd_events
         WHERE version = (SELECT version FROM streamd_events WHERE transaction_hash = ? LIMIT 1)
           AND account_address IS NULL
         ORDER BY event_index`,
      )
      .all(hash) as { event: string }[];
    return transactionOf(rows);
  }

  /** Every stored event at `version`, resource changes left out; below the pruned edge, a 410. */
  async getTransactionEventsByVersion(version: bigint): Promise<TransactionEvents | undefined> {
    const key = versionKey(version);
    startOf({ fromVersion: version }, prunedBefore(this.db));
    const onlyEvents = eventColumns(this.db).has("account_address") ? " AND account_address IS NULL" : "";
    const rows = this.db
      .prepare(`SELECT event FROM streamd_events WHERE version = ?${onlyEvents} ORDER BY event_index`)
      .all(key) as { event: string }[];
    return transactionOf(rows);
  }

  private bound(fn: "MAX" | "MIN"): bigint | undefined {
    const row = this.db.prepare(`SELECT ${fn}(version) AS v FROM streamd_events`).get() as
      | { v: string | null }
      | null
      | undefined;
    return row?.v == null ? undefined : BigInt(row.v);
  }
}
