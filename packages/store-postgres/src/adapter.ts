import { StreamdError, StreamdNetworkError, parseEvent, requireAddress, requireEventType } from "@streamd/core";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "@streamd/core";
import { EDGE, ensureSchema, versionParam } from "./schema.js";
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
  const checked = list.map(requireEventType);
  // One JSON parameter, since drivers disagree on how to send arrays.
  return (bind) => `type IN (SELECT jsonb_array_elements_text(${bind(JSON.stringify(checked))}::text::jsonb))`;
}

function descriptorWhere(descriptor: EventDescriptor): Where {
  switch (descriptor?.kind) {
    case "module": {
      const { eventType } = descriptor;
      if (Array.isArray(eventType) && eventType.length === 0) {
        throw new StreamdError("Module descriptor requires at least one eventType");
      }
      return types(Array.isArray(eventType) ? eventType : [eventType as string]);
    }
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

function count(value: number | undefined, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || value < 0) throw new StreamdError(`Not a valid ${label}: ${value}`);
  return value;
}

/** No start position means the oldest version kept; only asking for pruned history fails. */
function checkEdge(start: Position, toVersion: bigint | undefined, edge: bigint): void {
  // A cursor at edge - 1 fails too: it cannot show whether that version's later events went.
  if (start.version < edge || (toVersion !== undefined && toVersion < edge)) {
    throw new StreamdNetworkError(`Version pruned from this store; the oldest kept is ${edge}`, 410);
  }
}

/**
 * Serves events a `PostgresEventStore` wrote, as an `EventAdapter`. It
 * answers from what is stored, so it sees only what the store's subscriptions
 * caught: `account` matches events stored with that `sender`, and `handle`
 * matches by `creationNumber` against each event's stored `guid`.
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
    const where = descriptorWhere(descriptor);
    const cursor = options.cursor === undefined ? undefined : parseCursor(options.cursor);
    const limit = count(options.limit, "limit", DEFAULT_LIMIT);
    const offset = cursor === undefined ? count(options.offset, "offset", 0) : 0;
    const fromVersion = cursor?.version ?? options.fromVersion;

    const params: unknown[] = [];
    const bind = (value: unknown) => "$" + params.push(value);
    const from = bind(fromVersion === undefined ? null : versionParam(fromVersion));
    const after = `(coalesce(${from}::numeric, edge.v), ${bind(cursor?.eventIndex ?? -1)}::integer)`;
    let read = `SELECT version, event_index, event FROM streamd_events
      WHERE ${where(bind)} AND (version, event_index) > ${after}`;
    if (options.toVersion !== undefined) read += ` AND version <= ${bind(versionParam(options.toVersion))}::numeric`;
    // One row past the page says whether another is waiting.
    read += ` ORDER BY version, event_index LIMIT ${bind(limit + 1)} OFFSET ${bind(offset)}`;
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

  private async bound(fn: "max" | "min"): Promise<bigint> {
    await ensureSchema(this.client);
    const { rows } = await this.client.query(
      `SELECT GREATEST(coalesce(${fn}(version), 0), ${EDGE})::text AS v FROM streamd_events`,
    );
    return BigInt(String(rows[0]!.v));
  }
}
