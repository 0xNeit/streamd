import { StreamdError, eventId, normalizeAddress, parseEvent, serializeEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { ensureSchema, versionParam } from "./schema.js";
import type { PostgresClient } from "./schema.js";

/** A source cursor to save under `name` alongside a write. */
export interface StoreCheckpoint {
  name: string;
  /** `undefined` leaves the saved cursor as it is. */
  cursor: string | undefined;
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

function requireName(name: unknown): string {
  if (typeof name !== "string" || name === "") {
    throw new StreamdError("A checkpoint name must be a non-empty string");
  }
  return name;
}

// Both writes are data-modifying CTEs of one statement, so they land together
// without a transaction, which a pool could not hold on one connection. Rows
// arrive as one JSON parameter: drivers disagree on how to send arrays, and
// the parameter count stays fixed whatever the batch size. It is typed text,
// since a driver told the parameter is jsonb may encode the string as a JSON string.
const WRITE = `
WITH incoming AS (
  SELECT (r->>0)::numeric, (r->>1)::integer, r->>2, r->>3, r->>4, (r->>5)::numeric, (r->>6)::jsonb
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
    const name = checkpoint === undefined ? null : requireName(checkpoint.name);
    const cursor = checkpoint?.cursor;
    if (cursor !== undefined && typeof cursor !== "string") {
      throw new StreamdError("A checkpoint cursor must be a string or undefined");
    }
    await ensureSchema(this.client);
    await this.client.query(WRITE, [JSON.stringify(rows), name, cursor ?? null]);
  }

  /** The cursor last saved under `name`, or `undefined` when none was. */
  async readCursor(name: string): Promise<string | undefined> {
    const key = requireName(name);
    await ensureSchema(this.client);
    const { rows } = await this.client.query("SELECT cursor FROM streamd_cursors WHERE name = $1", [key]);
    const cursor = rows[0]?.cursor;
    return typeof cursor === "string" ? cursor : undefined;
  }
}
