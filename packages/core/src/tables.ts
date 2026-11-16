import { eventId } from "./cursor.js";
import { StreamdError } from "./errors.js";
import { MISSING, normalizeAddress, readField } from "./filter.js";
import type { EventColumn, EventColumnType, EventTable, StreamdEvent } from "./types.js";

/** The columns every event table starts with, in order; `type_arguments` only on a generic table. */
export const STANDARD_COLUMNS = ["version", "event_index", "transaction_hash", "timestamp", "sender", "type_arguments"] as const;

/** One event as a row of its table: the standard columns, then the table's own, in order. */
export interface EventTableRow {
  table: EventTable;
  values: unknown[];
}

// Lowercase only, so SQLite's case-insensitive names and Postgres's quoted
// case-sensitive ones agree, and at most Postgres's 63-byte identifier limit.
const IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/;
const EVENT_TYPE = /^(0x[0-9a-fA-F]{1,64})::([A-Za-z_][A-Za-z0-9_]*)::([A-Za-z_][A-Za-z0-9_]*)$/;
const RESERVED_TABLE = /^(streamd_|sqlite_)/;
const DECIMAL = /^-?\d+$/;
const I64_MAX = 2n ** 63n - 1n;
const COLUMN_TYPES: ReadonlySet<string> = new Set<EventColumnType>([
  "u8", "u16", "u32", "u64", "u128", "u256", "i8", "i16", "i32", "i64", "i128", "i256",
  "bool", "address", "text", "json",
]);

interface Spec {
  tables: readonly EventTable[];
  byType: ReadonlyMap<string, EventTable>;
}

const specs = new WeakMap<readonly EventTable[], Spec>();

const invalid = (message: string): never => {
  throw new StreamdError(`Invalid event tables: ${message}`);
};

function typeKey(eventType: string): string | undefined {
  const match = EVENT_TYPE.exec(eventType);
  return match === null ? undefined : `${normalizeAddress(match[1]!)}::${match[2]}::${match[3]}`;
}

function column(raw: EventColumn, table: string, names: Set<string>): EventColumn {
  const { name, type, path, nullable } = (raw ?? {}) as Partial<EventColumn>;
  if (typeof name !== "string" || !IDENTIFIER.test(name)) invalid(`table ${table} has a bad column name ${JSON.stringify(name)}`);
  if ((STANDARD_COLUMNS as readonly string[]).includes(name!) || names.has(name!)) {
    invalid(`table ${table} has column ${name} twice or as a standard column`);
  }
  names.add(name!);
  if (typeof type !== "string" || !COLUMN_TYPES.has(type)) invalid(`column ${table}.${name} has a bad type ${JSON.stringify(type)}`);
  if (typeof nullable !== "boolean") invalid(`column ${table}.${name} needs a boolean nullable`);
  const ok = Array.isArray(path) && path.every((s) => (typeof s === "string" && s !== "") || (Number.isSafeInteger(s) && (s as number) >= 0));
  if (!ok) invalid(`column ${table}.${name} has a bad path`);
  return Object.freeze({ name: name!, type: type!, path: Object.freeze([...path!]), nullable: nullable! });
}

function spec(tables: readonly EventTable[]): Spec {
  const cached = specs.get(tables);
  if (cached !== undefined) return cached;
  if (!Array.isArray(tables) || tables.length === 0) invalid("expected a non-empty list");
  const byType = new Map<string, EventTable>();
  const names = new Set<string>();
  const copies = tables.map((raw) => {
    const { eventType, table, generic, columns } = (raw ?? {}) as Partial<EventTable>;
    if (typeof table !== "string" || !IDENTIFIER.test(table) || RESERVED_TABLE.test(table)) {
      invalid(`bad table name ${JSON.stringify(table)}`);
    }
    if (names.has(table!)) invalid(`table ${table} appears twice`);
    names.add(table!);
    const key = typeof eventType === "string" ? typeKey(eventType) : undefined;
    if (key === undefined) invalid(`table ${table} has a bad event type ${JSON.stringify(eventType)}`);
    if (byType.has(key!)) invalid(`event type ${eventType} has two tables`);
    if (typeof generic !== "boolean") invalid(`table ${table} needs a boolean generic`);
    if (!Array.isArray(columns)) invalid(`table ${table} has no column list`);
    const own = new Set<string>();
    const copy = Object.freeze({
      eventType: eventType!,
      table: table!,
      generic: generic!,
      columns: Object.freeze(columns!.map((c) => column(c, table!, own))),
    });
    byType.set(key!, copy);
    return copy;
  });
  const result = { tables: Object.freeze(copies), byType };
  specs.set(tables, result);
  return result;
}

/**
 * Validates `tables` and returns a frozen copy, the same one for the same
 * list: names a SQL statement is built from are checked here, then quoted.
 */
export function requireEventTables(tables: readonly EventTable[]): readonly EventTable[] {
  return spec(tables).tables;
}

function integer(raw: unknown, type: EventColumnType): bigint | undefined {
  let value: bigint | undefined;
  if (typeof raw === "bigint") value = raw;
  else if (typeof raw === "number" && Number.isSafeInteger(raw)) value = BigInt(raw);
  else if (typeof raw === "string" && DECIMAL.test(raw)) value = BigInt(raw);
  if (value === undefined) return undefined;
  const bits = BigInt(type.slice(1));
  const [min, max] = type.startsWith("u") ? [0n, 2n ** bits - 1n] : [-(2n ** (bits - 1n)), 2n ** (bits - 1n) - 1n];
  return value >= min && value <= max ? value : undefined;
}

const describe = (raw: unknown) =>
  typeof raw === "string" ? JSON.stringify(raw.length > 80 ? `${raw.slice(0, 80)}...` : raw) : typeof raw;

function value(col: EventColumn, raw: unknown, text: (s: string) => string): unknown {
  switch (col.type) {
    case "bool":
      return typeof raw === "boolean" ? raw : undefined;
    case "address":
      return typeof raw === "string" ? normalizeAddress(raw) : undefined;
    case "text":
      return typeof raw === "string" ? text(raw) : undefined;
    case "json":
      return JSON.stringify(raw, (_, v: unknown) => (typeof v === "bigint" ? v.toString() : typeof v === "string" ? text(v) : v));
    default: {
      const n = integer(raw, col.type);
      if (n === undefined) return undefined;
      // Up to 32 bits a number; wider, a decimal string, so no driver rounds it through a double.
      return Number(col.type.slice(1)) <= 32 ? Number(n) : n.toString();
    }
  }
}

function standard(event: StreamdEvent, table: EventTable, id: string, text: (s: string) => string): unknown[] {
  const { version, eventIndex, transactionHash, timestamp, sender } = event;
  if (typeof version !== "bigint" || version < 0n || version > I64_MAX) {
    throw new StreamdError(`Event ${id} has a version outside 0 to 2^63 - 1`);
  }
  if (!Number.isSafeInteger(eventIndex) || eventIndex < 0 || eventIndex > 2 ** 31 - 1) {
    throw new StreamdError(`Event ${id} has a bad eventIndex`);
  }
  if (timestamp !== undefined && (typeof timestamp !== "bigint" || timestamp < 0n || timestamp > I64_MAX)) {
    throw new StreamdError(`Event ${id} has a timestamp outside 0 to 2^63 - 1`);
  }
  const hash = typeof transactionHash === "string" ? transactionHash : null;
  const values = [
    version,
    eventIndex,
    hash !== null && /^0x[0-9a-fA-F]+$/.test(hash) ? hash.toLowerCase() : hash === null ? null : text(hash),
    timestamp ?? null,
    typeof sender === "string" ? (normalizeAddress(sender) ?? text(sender)) : null,
  ];
  if (table.generic) {
    const open = event.type.indexOf("<");
    values.push(open === -1 ? null : text(event.type.slice(open + 1, -1)));
  }
  return values;
}

/**
 * Each event as a row of its table, its values checked against the column
 * types: integers of 32 bits or fewer as numbers, wider ones as canonical
 * decimal strings, addresses in long form, JSON as text with bigints as
 * decimal strings, `null` for a missing nullable value. `text` rewrites every
 * stored string, for a database that cannot hold some characters. Throws a
 * `StreamdError` for an event with no table or a value its column refuses.
 */
export function eventTableRows(
  tables: readonly EventTable[],
  events: readonly StreamdEvent[],
  text: (s: string) => string = (s) => s,
): EventTableRow[] {
  const { byType } = spec(tables);
  return events.map((event) => {
    const id = eventId(event);
    const type = typeof event.type === "string" ? event.type : "";
    const open = type.indexOf("<");
    const key = typeKey(open === -1 ? type : type.slice(0, open));
    const table = key === undefined ? undefined : byType.get(key);
    if (table === undefined) throw new StreamdError(`No event table for ${JSON.stringify(type)} (event ${id})`);
    const values = standard(event, table, id, text);
    for (const col of table.columns) {
      const raw = col.path.length === 0 ? event.data : readField(event.data, col.path);
      if (raw === MISSING || raw === null || raw === undefined) {
        if (!col.nullable) throw new StreamdError(`Column ${table.table}.${col.name} of event ${id} has no value`);
        values.push(null);
        continue;
      }
      const stored = value(col, raw, text);
      if (stored === undefined) {
        throw new StreamdError(`Column ${table.table}.${col.name} of event ${id} expects a ${col.type}, got ${describe(raw)}`);
      }
      values.push(stored);
    }
    return { table, values };
  });
}
