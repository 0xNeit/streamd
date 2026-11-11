import { StreamdError, parseEvent, requireAddress, requireEventType } from "@streamd/core";
import type { EventAdapter, EventDescriptor, EventPage, FetchOptions, StreamdEvent } from "@streamd/core";
import { ensureSchema, versionParam } from "./schema.js";
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
  return (bind) => `type = ANY(${bind(checked)}::text[])`;
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
      const sender = requireAddress(descriptor.accountAddress);
      if (descriptor.eventTypes === undefined) return (bind) => `sender = ${bind(sender)}`;
      if (descriptor.eventTypes.length === 0) return () => "FALSE";
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

// jsonb comes back parsed from most drivers and as text from some.
const eventOf = (value: unknown): StreamdEvent =>
  parseEvent(typeof value === "string" ? value : JSON.stringify(value));

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

  /** The highest stored version; `0n` for an empty store. */
  async getHeadVersion(): Promise<bigint> {
    return this.bound("max");
  }

  /** The lowest stored version; `0n` for an empty store. */
  async getEarliestVersion(): Promise<bigint> {
    return this.bound("min");
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    const where = descriptorWhere(descriptor);
    const start =
      options.cursor !== undefined
        ? parseCursor(options.cursor)
        : { version: options.fromVersion ?? 0n, eventIndex: -1 };
    const limit = count(options.limit, "limit", DEFAULT_LIMIT);
    const offset = options.cursor === undefined ? count(options.offset, "offset", 0) : 0;
    const startCursor = `${start.version}:${start.eventIndex}`;
    if (limit === 0) return { events: [], nextCursor: startCursor, hasMore: false };

    const params: unknown[] = [];
    const bind = (value: unknown) => "$" + params.push(value);
    const after = `(${bind(versionParam(start.version))}::numeric, ${bind(start.eventIndex)}::integer)`;
    let sql = `SELECT event FROM streamd_events WHERE ${where(bind)} AND (version, event_index) > ${after}`;
    if (options.toVersion !== undefined) sql += ` AND version <= ${bind(versionParam(options.toVersion))}::numeric`;
    // One row past the page says whether another is waiting.
    sql += ` ORDER BY version, event_index LIMIT ${bind(limit + 1)} OFFSET ${bind(offset)}`;

    await ensureSchema(this.client);
    const { rows } = await this.client.query(sql, params);
    const events = rows.slice(0, limit).map((r) => eventOf(r.event));
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

  private async bound(fn: "max" | "min"): Promise<bigint> {
    await ensureSchema(this.client);
    const { rows } = await this.client.query(`SELECT ${fn}(version)::text AS v FROM streamd_events`);
    const v = rows[0]?.v;
    return typeof v === "string" ? BigInt(v) : 0n;
  }
}
