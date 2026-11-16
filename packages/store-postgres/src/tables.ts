import { requireEventTables } from "@streamd/core";
import type { EventColumnType, EventTable } from "@streamd/core";

const SQL_TYPES: Record<EventColumnType, string> = {
  u8: "integer", u16: "integer", i8: "integer", i16: "integer", i32: "integer",
  u32: "bigint", i64: "bigint",
  u64: "numeric(20, 0)", u128: "numeric(39, 0)", i128: "numeric(39, 0)", u256: "numeric(78, 0)", i256: "numeric(78, 0)",
  bool: "boolean", address: "text", text: "text", json: "jsonb",
};

// Names are validated by requireEventTables, then quoted, so no on-chain name is ever SQL.
const quote = (name: string) => `"${name}"`;

type Column = [name: string, type: string, notNull: boolean];

const allColumns = (table: EventTable): Column[] => [
  ["version", "bigint", true],
  ["event_index", "integer", true],
  ["transaction_hash", "text", false],
  ["timestamp", "bigint", false],
  ["sender", "text", false],
  ...(table.generic ? ([["type_arguments", "text", false]] as Column[]) : []),
  ...table.columns.map((c): Column => [c.name, SQL_TYPES[c.type], !c.nullable]),
];

const schemas = new WeakMap<readonly EventTable[], string[]>();

/** One `CREATE TABLE IF NOT EXISTS` per event table, keyed on `(version, event_index)`; the same array for the same list. */
export function eventTableSchema(tables: readonly EventTable[]): string[] {
  const spec = requireEventTables(tables);
  let schema = schemas.get(spec);
  if (schema === undefined) {
    schema = spec.map((table) =>
      [
        `CREATE TABLE IF NOT EXISTS ${quote(table.table)} (`,
        ...allColumns(table).map(([name, type, notNull]) => `  ${quote(name)} ${type}${notNull ? " NOT NULL" : ""},`),
        '  PRIMARY KEY ("version", "event_index")',
        ")",
      ].join("\n"),
    );
    schemas.set(spec, schema);
  }
  return schema;
}

/**
 * One statement upserting each table's rows, `$1` to `$n` a JSON array of
 * rows per table in order, then saving the cursor from `$n+1` and `$n+2`:
 * data-modifying CTEs, so it all lands together on any pooled connection.
 */
export function eventTablesWrite(tables: readonly EventTable[]): string {
  const ctes = tables.map((table, i) => {
    const columns = allColumns(table);
    const names = columns.map(([name]) => quote(name));
    const values = columns.map(([, type], k) => (type === "text" ? `r->>${k}` : `(r->>${k})::${type}`));
    const updated = names.filter((n) => n !== '"version"' && n !== '"event_index"');
    return [
      `w${i} AS (`,
      `  INSERT INTO ${quote(table.table)} (${names.join(", ")})`,
      `  SELECT ${values.join(", ")} FROM jsonb_array_elements($${i + 1}::text::jsonb) AS r`,
      `  ON CONFLICT ("version", "event_index") DO UPDATE SET ${updated.map((n) => `${n} = excluded.${n}`).join(", ")}`,
      ")",
    ].join("\n");
  });
  const name = `$${tables.length + 1}::text`;
  const cursor = `$${tables.length + 2}::text`;
  return [
    ...(ctes.length > 0 ? [`WITH ${ctes.join(",\n")}`] : []),
    "INSERT INTO streamd_cursors (name, cursor)",
    `SELECT ${name}, ${cursor} WHERE ${name} IS NOT NULL AND ${cursor} IS NOT NULL`,
    "ON CONFLICT (name) DO UPDATE SET cursor = excluded.cursor",
  ].join("\n");
}
