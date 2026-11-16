import { createHash } from "node:crypto";
import { normalizeAddress, StreamdError } from "@streamd/core";
import type { EventColumn, EventColumnType, EventTable, MoveEventType } from "@streamd/core";
import { eventTableSchema as postgresSchema } from "@streamd/store-postgres";
import { eventTableSchema as sqliteSchema } from "@streamd/store-sqlite";
import type { MoveType, StructAbi } from "./abi.js";
import type { Dialect } from "./codegen.js";

/** The SQL dialects `--sql` writes DDL for. */
export type SqlDialect = "sqlite" | "postgres";

/** Where a value of one Move type is stored, relative to the value. */
export interface ColumnMapping {
  type: EventColumnType;
  path: (string | number)[];
  nullable: boolean;
}

/** What a framework struct's column mapping can call on. */
export interface ColumnContext {
  args: MoveType[];
  /** The mapping of a type argument. */
  inner: (t: MoveType) => ColumnMapping;
}

/** An event struct to make a table for. */
export interface TableTarget {
  address: string;
  module: string;
  struct: StructAbi;
}

const MAX_NAME = 63;
const STANDARD = new Set(["version", "event_index", "transaction_hash", "timestamp", "sender", "type_arguments"]);
const RESERVED = /^(streamd_|sqlite_)/;
const SCHEMA: Record<SqlDialect, (tables: readonly EventTable[]) => string[]> = { sqlite: sqliteSchema, postgres: postgresSchema };

const json = (): ColumnMapping => ({ type: "json", path: [], nullable: false });

/** How a field of type `t` is stored: a scalar column where the type has one, else JSON. */
export function columnMapping(t: MoveType, d: Dialect): ColumnMapping {
  switch (t.kind) {
    case "primitive":
      if (t.name === "bool") return { type: "bool", path: [], nullable: false };
      if (t.name === "address" || t.name === "signer") return { type: "address", path: [], nullable: false };
      return { type: t.name, path: [], nullable: false };
    case "vector":
      return t.of.kind === "primitive" && t.of.name === "u8" ? { type: "text", path: [], nullable: false } : json();
    case "struct": {
      const column = d.builtin(t)?.column;
      return column === undefined ? json() : column({ args: t.args, inner: (a) => columnMapping(a, d) });
    }
    case "param":
      return json();
    case "unsupported":
      throw new StreamdError(`Unsupported Move type: ${t.text}`);
  }
}

const hash = (text: string) => createHash("sha256").update(text).digest("hex").slice(0, 8);

// CamelCase to snake_case, keeping acronyms together: SwapEventV3 is swap_event_v3, FAEvent fa_event.
const snake = (name: string) =>
  name.replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2").replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();

const shortened = (name: string, key: string) => `${name.slice(0, MAX_NAME - 9)}_${hash(key)}`;

const qualified = (t: TableTarget) => `${normalizeAddress(t.address) ?? t.address}::${t.module}::${t.struct.name}`;

/**
 * `<module>_<struct>` in snake case, whatever else is generated, so a table
 * keeps its name as targets come and go; `move_` in front of a prefix the
 * stores reserve. A name two structs share or too long for Postgres is cut
 * and suffixed with a hash of the qualified type instead.
 */
function tableNames(targets: TableTarget[]): string[] {
  const plain = targets.map((t) => {
    const name = `${snake(t.module)}_${snake(t.struct.name)}`;
    return RESERVED.test(name) ? `move_${name}` : name;
  });
  const counts = new Map<string, number>();
  for (const name of plain) counts.set(name, (counts.get(name) ?? 0) + 1);
  const names = plain.map((name, i) =>
    counts.get(name) === 1 && name.length <= MAX_NAME ? name : shortened(name, qualified(targets[i]!)),
  );
  if (new Set(names).size !== names.length) throw new StreamdError("Could not give every event table a distinct name");
  return names;
}

/** The field name lowercased, past the standard columns and earlier fields, within 63 bytes. */
function columnName(field: string, taken: Set<string>): string {
  let name = field.toLowerCase();
  if (name.length > MAX_NAME) name = shortened(name, field);
  while (taken.has(name)) name += "_";
  if (name.length > MAX_NAME) throw new StreamdError(`Could not give field ${field} a distinct column name`);
  taken.add(name);
  return name;
}

function columns(struct: StructAbi, d: Dialect): EventColumn[] {
  if (struct.isEnum) {
    return [
      { name: "variant", type: "text", path: [d.variantKey], nullable: false },
      { name: "data", type: "json", path: [], nullable: false },
    ];
  }
  const taken = new Set(STANDARD);
  return struct.fields.map((f) => {
    const m = columnMapping(f.type, d);
    return { name: columnName(f.name, taken), type: m.type, path: [f.name, ...m.path], nullable: m.nullable };
  });
}

/** One table per event struct, in the order given. */
export function eventTables(targets: TableTarget[], d: Dialect): EventTable[] {
  const names = tableNames(targets);
  return targets.map((t, i) => ({
    eventType: `${t.address}::${t.module}::${t.struct.name}` as MoveEventType,
    table: names[i]!,
    generic: t.struct.typeParams > 0,
    columns: columns(t.struct, d),
  }));
}

function renderTable(t: EventTable): string {
  const cols = t.columns.map(
    (c) =>
      `      { name: ${JSON.stringify(c.name)}, type: ${JSON.stringify(c.type)}, path: [${c.path.map((k) => JSON.stringify(k)).join(", ")}], nullable: ${c.nullable} },`,
  );
  return [
    "  {",
    `    eventType: ${JSON.stringify(t.eventType)},`,
    `    table: ${JSON.stringify(t.table)},`,
    `    generic: ${t.generic},`,
    ...(cols.length === 0 ? ["    columns: [],"] : ["    columns: [", ...cols, "    ],"]),
    "  },",
  ].join("\n");
}

/** The import line `renderTables` needs. */
export const TABLES_IMPORT = 'import type { EventTable as StreamdEventTable } from "@streamd/core";';

/** `streamdTables` and the dialect's `streamdSchema`, as generated source. */
export function renderTables(tables: EventTable[], sql: SqlDialect): string {
  const store = sql === "sqlite" ? "@streamd/store-sqlite" : "@streamd/store-postgres";
  const schema = tables.length === 0 ? [] : SCHEMA[sql](tables);
  return [
    `/** One table per event type, a column per field, for \`storeTables\` in \`${store}\`. */`,
    `export const streamdTables: readonly StreamdEventTable[] = [${tables.length === 0 ? "];" : ""}`,
    ...(tables.length === 0 ? [] : [...tables.map(renderTable), "];"]),
    "",
    `/** The tables' ${sql === "sqlite" ? "SQLite" : "Postgres"} DDL, one statement each, which \`storeTables\` also runs. */`,
    `export const streamdSchema: readonly string[] = [${schema.length === 0 ? "];" : ""}`,
    // Template literals stay readable; validated names and fixed types hold no backtick or `${`.
    ...(schema.length === 0 ? [] : [...schema.map((s) => `  \`${s}\`,`), "];"]),
  ].join("\n");
}
