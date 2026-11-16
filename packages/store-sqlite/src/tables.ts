import { requireEventTables } from "@streamd/core";
import type { EventColumnType, EventTable } from "@streamd/core";

// Integers wider than 32 bits are exact decimal text: INTEGER is a signed
// 64-bit value, and most drivers read it back as a double.
const SQL_TYPES: Record<EventColumnType, string> = {
  u8: "INTEGER", u16: "INTEGER", u32: "INTEGER", i8: "INTEGER", i16: "INTEGER", i32: "INTEGER",
  u64: "TEXT", u128: "TEXT", u256: "TEXT", i64: "TEXT", i128: "TEXT", i256: "TEXT",
  bool: "INTEGER", address: "TEXT", text: "TEXT", json: "TEXT",
};

// Names are validated by requireEventTables, then quoted, so no on-chain name is ever SQL.
const quote = (name: string) => `"${name}"`;

const standardColumns = (table: EventTable): [string, string][] => [
  ["version", "INTEGER NOT NULL"],
  ["event_index", "INTEGER NOT NULL"],
  ["transaction_hash", "TEXT"],
  ["timestamp", "INTEGER"],
  ["sender", "TEXT"],
  ...(table.generic ? ([["type_arguments", "TEXT"]] as [string, string][]) : []),
];

const allColumns = (table: EventTable): [string, string][] => [
  ...standardColumns(table),
  ...table.columns.map((c): [string, string] => [c.name, `${SQL_TYPES[c.type]}${c.nullable ? "" : " NOT NULL"}`]),
];

/** One `CREATE TABLE IF NOT EXISTS` per event table, keyed on `(version, event_index)`. */
export function eventTableSchema(tables: readonly EventTable[]): string[] {
  return requireEventTables(tables).map((table) =>
    [
      `CREATE TABLE IF NOT EXISTS ${quote(table.table)} (`,
      ...allColumns(table).map(([name, type]) => `  ${quote(name)} ${type},`),
      '  PRIMARY KEY ("version", "event_index")',
      ") WITHOUT ROWID",
    ].join("\n"),
  );
}

/** The upsert of one row of `table`, replacing every column on redelivery. */
export function eventTableUpsert(table: EventTable): string {
  const names = allColumns(table).map(([name]) => name);
  const updated = names.filter((n) => n !== "version" && n !== "event_index");
  return [
    `INSERT INTO ${quote(table.table)} (${names.map(quote).join(", ")})`,
    `VALUES (${names.map(() => "?").join(", ")})`,
    `ON CONFLICT ("version", "event_index") DO UPDATE SET ${updated.map((n) => `${quote(n)} = excluded.${quote(n)}`).join(", ")}`,
  ].join("\n");
}
