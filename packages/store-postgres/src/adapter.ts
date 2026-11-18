import {
  StreamdError,
  StreamdNetworkError,
  parseEvent,
  requireAddress,
  requireEventType,
  requireResourceType,
} from "@streamd/core";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions, ModuleEventDescriptor, TransactionEvents } from "@streamd/core";
import { EDGE, ensureSchema, hashKey, versionParam } from "./schema.js";
import type { PostgresClient } from "./schema.js";

const DEFAULT_LIMIT = 25;
const CURSOR = /^(\d{1,20}):(-1|\d+)$/;

interface Position {
  version: bigint;
  eventIndex: number;
}

/** A condition on stored rows; `bind` adds a parameter and returns its placeholder. */
type Where = (bind: (value: unknown) => string) => string;

function types(list: readonly string[]): Where {
  if (list.length === 0) return () => "false";
  const checked = list.map(requireEventType);
  return (bind) => `type IN (${checked.map((t) => bind(t)).join(", ")})`;
}

/**
 * A `module` read over `streamd_events_type_c` (`type COLLATE "C", version,
 * event_index WHERE account_address IS NULL`): finds any distinct generic
 * instantiations `[T<, T=)` for bare types with a skip scan, then pulls one
 * indexed top-N arm per matched type.
 */
function moduleRead(
  descriptor: ModuleEventDescriptor,
  bind: (value: unknown) => string,
  after: string,
  toVersion: bigint | undefined,
  limit: number,
  offset: number,
): string {
  const { eventType } = descriptor;
  if (Array.isArray(eventType) && eventType.length === 0) {
    throw new StreamdError("Module descriptor requires at least one eventType");
  }
  const unique = [...new Set((Array.isArray(eventType) ? eventType : [eventType as string]).map(requireEventType))];
  const exactSql = unique.map((t) => bind(t)).join(", ");
  const bare = unique.filter((t) => !t.includes("<"));
  const base = bare
    .map((t) => {
      const lo = bind(`${t}<`);
      const hi = bind(`${t}=`);
      return `SELECT (SELECT type COLLATE "C" FROM streamd_events WHERE type COLLATE "C" >= ${lo}::text COLLATE "C" AND type COLLATE "C" < ${hi}::text COLLATE "C" AND account_address IS NULL ORDER BY type COLLATE "C" LIMIT 1), ${hi}::text`;
    })
    .join(" UNION ALL ");
  const cte =
    bare.length === 0
      ? `WITH wanted(t) AS (SELECT unnest(ARRAY[${exactSql}]::text[]))`
      : `WITH RECURSIVE generic(t, hi) AS (
          ${base}
          UNION ALL
          SELECT (
            SELECT type COLLATE "C" FROM streamd_events
            WHERE type COLLATE "C" > g.t COLLATE "C" AND type COLLATE "C" < g.hi COLLATE "C" AND account_address IS NULL
            ORDER BY type COLLATE "C" LIMIT 1
          ), g.hi
          FROM generic g WHERE g.t IS NOT NULL
        ), wanted(t) AS (
          SELECT unnest(ARRAY[${exactSql}]::text[])
          UNION
          SELECT t FROM generic WHERE t IS NOT NULL
        )`;
  const until = toVersion === undefined ? "" : ` AND version <= ${bind(versionParam(toVersion))}::numeric`;
  const armLimit = bind(limit + 1 + offset);
  return `${cte}
    SELECT matched.version, matched.event_index, matched.event
    FROM wanted CROSS JOIN LATERAL (
      SELECT version, event_index, event FROM streamd_events
      WHERE type COLLATE "C" = wanted.t COLLATE "C" AND account_address IS NULL AND (version, event_index) > ${after}${until}
      ORDER BY type COLLATE "C", version, event_index LIMIT ${armLimit}
    ) matched
    ORDER BY matched.version, matched.event_index LIMIT ${bind(limit + 1)} OFFSET ${bind(offset)}`;
}

function descriptorWhere(descriptor: EventDescriptor): Where {
  switch (descriptor?.kind) {
    case "account": {
      if (descriptor.received === true) {
        throw new StreamdError("PostgresStoreAdapter keys account events by sender, so it cannot serve received ones");
      }
      const sender = requireAddress(descriptor.accountAddress);
      if (descriptor.eventTypes === undefined) return (bind) => `sender = ${bind(sender)}`;
      const typed = types(descriptor.eventTypes);
      return (bind) => `sender = ${bind(sender)} AND ${typed(bind)}`;
    }
    case "handle": {
      const { creationNumber } = descriptor as { creationNumber?: unknown };
      if (typeof creationNumber !== "bigint" || creationNumber < 0n) {
        throw new StreamdError(
          "PostgresStoreAdapter serves handle descriptors by creationNumber only; listEventHandles returns it",
        );
      }
      const address = requireAddress(descriptor.accountAddress);
      return (bind) =>
        `guid_address = ${bind(address)} AND guid_creation = ${bind(creationNumber.toString())}::numeric`;
    }
    case "resource": {
      const address = requireAddress(descriptor.accountAddress);
      const type = requireResourceType(descriptor.resourceType);
      return (bind) => `account_address = ${bind(address)} AND type = ${bind(type)}`;
    }
    default:
      throw new StreamdError(`Unknown descriptor kind: ${String((descriptor as { kind?: unknown })?.kind)}`);
  }
}

function parseCursor(cursor: string): Position {
  const match = typeof cursor === "string" ? CURSOR.exec(cursor) : null;
  const eventIndex = Number(match?.[2]);
  if (match === null || !Number.isSafeInteger(eventIndex)) {
    throw new StreamdError(`Not a cursor this adapter issued: ${String(cursor)}`);
  }
  return { version: BigInt(match[1]!), eventIndex };
}

function requireHash(hash: unknown): string {
  if (typeof hash !== "string" || hash === "") throw new StreamdError("A transaction hash must be a non-empty string");
  return hashKey(hash);
}

/**
 * Stored rows as one transaction. A store keeps no status, so even a failed
 * transaction's stored fee statement reads as a success.
 */
function transactionOf(rows: Record<string, unknown>[]): TransactionEvents | undefined {
  if (rows.length === 0) return undefined;
  const events = rows.map((r) => parseEvent(String(r.event)));
  return { version: events[0]!.version, success: true, vmStatus: "", events };
}

function count(value: number | undefined, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < 0) throw new StreamdError(`Not a valid ${label}: ${value}`);
  return value;
}

/** Fails a read that asks for pruned history; a read with no start begins at the edge instead. */
function checkEdge(start: Position, toVersion: bigint | undefined, edge: bigint): void {
  // A cursor at edge - 1 fails too: it cannot show whether that version's later events went.
  if (start.version < edge || (toVersion !== undefined && toVersion < edge)) {
    throw new StreamdNetworkError(`Version pruned from this store; the oldest kept is ${edge}`, 410);
  }
}

/**
 * Serves events a `PostgresEventStore` wrote, as an `EventAdapter`. It
 * answers from what is stored, so it sees only what the store's subscriptions
 * caught: `account` matches events stored with that `sender`, `handle`
 * matches by `creationNumber` against each event's stored `guid`, and
 * `resource` matches changes `storeEvents` stored with that address, so none
 * an older release stored without one. Changes stored with an
 * address match no other kind.
 */
export class PostgresStoreAdapter implements EventAdapter {
  private readonly client: PostgresClient;

  constructor(client: PostgresClient) {
    if (typeof client?.query !== "function") throw new StreamdError("Expected a Postgres client with query()");
    this.client = client;
  }

  /** The highest stored version, never below the pruned edge; `0n` for a new store. */
  async getHeadVersion(): Promise<bigint> {
    return this.bound("max");
  }

  /** The lowest readable version: the lowest stored, or the pruned edge if that is higher. */
  async getEarliestVersion(): Promise<bigint> {
    return this.bound("min");
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    const cursor = options.cursor === undefined ? undefined : parseCursor(options.cursor);
    const limit = count(options.limit, "limit", DEFAULT_LIMIT);
    const offset = cursor === undefined ? count(options.offset, "offset", 0) : 0;
    const fromVersion = cursor?.version ?? options.fromVersion;

    const params: unknown[] = [];
    const bind = (value: unknown) => "$" + params.push(value);
    const from = bind(fromVersion === undefined ? null : versionParam(fromVersion));
    const after = `(coalesce(${from}::numeric, edge.v), ${bind(cursor?.eventIndex ?? -1)}::integer)`;
    let read: string;
    if (descriptor?.kind === "module") {
      read = moduleRead(descriptor, bind, after, options.toVersion, limit, offset);
    } else {
      const matches = descriptorWhere(descriptor);
      const where: Where =
        descriptor.kind === "resource" ? matches : (nextBind) => `${matches(nextBind)} AND account_address IS NULL`;
      read = `SELECT version, event_index, event FROM streamd_events
        WHERE ${where(bind)} AND (version, event_index) > ${after}`;
      if (options.toVersion !== undefined) read += ` AND version <= ${bind(versionParam(options.toVersion))}::numeric`;
      // One row past the page says whether another is waiting.
      read += ` ORDER BY version, event_index LIMIT ${bind(limit + 1)} OFFSET ${bind(offset)}`;
    }
    // The edge is read in the same statement as the page, so a prune on another
    // connection cannot land between the check and the read.
    const sql = `WITH edge AS (SELECT ${EDGE} AS v)
      SELECT edge.v::text AS edge, page.event FROM edge LEFT JOIN LATERAL (${read}) page ON true
      ORDER BY page.version, page.event_index`;

    await ensureSchema(this.client);
    const { rows } = await this.client.query(sql, params);
    const edge = BigInt(String(rows[0]!.edge));
    const start = cursor ?? { version: fromVersion ?? edge, eventIndex: -1 };
    checkEdge(start, options.toVersion, edge);
    const startCursor = `${start.version}:${start.eventIndex}`;
    if (limit === 0) return { events: [], nextCursor: startCursor, hasMore: false };
    const found = rows.filter((r) => r.event != null);
    const events = found.slice(0, limit).map((r) => parseEvent(String(r.event)));
    const last = events.at(-1);
    const page: EventPage = {
      events,
      nextCursor: last === undefined ? startCursor : `${last.version}:${last.eventIndex}`,
      hasMore: found.length > limit,
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
    await ensureSchema(this.client);
    const { rows } = await this.client.query(
      `SELECT event FROM streamd_events
       WHERE version = (SELECT version FROM streamd_events WHERE transaction_hash = $1 LIMIT 1)
         AND account_address IS NULL
       ORDER BY event_index`,
      [hash],
    );
    return transactionOf(rows);
  }

  /** Every stored event at `version`, resource changes left out; below the pruned edge, a 410. */
  async getTransactionEventsByVersion(version: bigint): Promise<TransactionEvents | undefined> {
    const at = versionParam(version);
    await ensureSchema(this.client);
    // The edge and the rows in one statement, as fetchEvents reads them.
    const { rows } = await this.client.query(
      `WITH edge AS (SELECT ${EDGE} AS v)
       SELECT edge.v::text AS edge, found.event FROM edge LEFT JOIN LATERAL (
         SELECT event_index, event FROM streamd_events WHERE version = $1::numeric AND account_address IS NULL
       ) found ON true
       ORDER BY found.event_index`,
      [at],
    );
    checkEdge({ version, eventIndex: -1 }, undefined, BigInt(String(rows[0]!.edge)));
    return transactionOf(rows.filter((r) => r.event != null));
  }

  private async bound(fn: "max" | "min"): Promise<bigint> {
    await ensureSchema(this.client);
    const { rows } = await this.client.query(
      `SELECT GREATEST(coalesce(${fn}(version), 0), ${EDGE})::text AS v FROM streamd_events`,
    );
    return BigInt(String(rows[0]!.v));
  }
}
