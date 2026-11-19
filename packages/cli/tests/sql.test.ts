import { PGlite } from "@electric-sql/pglite";
import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateEventTypes, parseModuleAbi } from "@streamd/codegen";
import type { GenerateOptions, ModuleAbi } from "@streamd/codegen";
import { normalizeAddress, StreamdError } from "@streamd/core";
import type { EventColumn, EventTable, StreamdEvent } from "@streamd/core";
import { PostgresEventStore } from "@streamd/store-postgres";
import { SqliteEventStore } from "@streamd/store-sqlite";
import { sqlTables } from "../src/sql.js";
import { SUI_DIALECT } from "../src/sui-abi.js";

const DEX = "0x00000000000000000000000000000000000000000000000000000000000cafe0";
const OTHER = "0x00000000000000000000000000000000000000000000000000000000000beef1";

const struct = (name: string, fields: [string, string][], isEvent = false, typeParams = 0) => ({
  name,
  is_event: isEvent,
  is_enum: false,
  generic_type_params: Array.from({ length: typeParams }, () => ({ constraints: [] })),
  fields: fields.map(([n, type]) => ({ name: n, type })),
  variants: [],
});

const module = (address: string, name: string, structs: unknown[]) => parseModuleAbi({ abi: { address, name, structs } });

const shapes = module(DEX, "shapes", [
  struct("Everything", [
    ["small", "u8"], ["mid", "u16"], ["wide", "u32"], ["signed", "i32"], ["tiny", "i8"], ["big", "u128"], ["huge", "u256"],
    ["amount", "u64"], ["neg", "i64"], ["neg_big", "i128"], ["neg_huge", "i256"], ["flag", "bool"], ["who", "address"],
    ["bytes", "vector<u8>"], ["name", "0x1::string::String"], ["maybe", "0x1::option::Option<u64>"],
    ["obj", "0x1::object::Object<0x1::object::ObjectCore>"], ["maybe_obj", "0x1::option::Option<0x1::object::Object<0x1::object::ObjectCore>>"],
    ["maybe_inner", `0x1::option::Option<${DEX}::shapes::Inner>`], ["list", "vector<vector<u16>>"], ["inner", `${DEX}::shapes::Inner`],
    ["pick", `${DEX}::shapes::Pick`],
  ], true),
  struct("Inner", [["x", "u64"], ["names", "vector<0x1::string::String>"]]),
  struct("Boxed", [["item", "T0"], ["count", "u64"]], true, 1),
  struct("Clash", [
    ["version", "u64"], ["Timestamp", "u64"], ["timestamp_", "u8"], ["Amount", "u8"], ["amount", "u8"], ["xmin", "u8"], ["TIMESTAMP", "u8"],
  ], true),
  {
    name: "Pick", is_event: true, is_enum: true, generic_type_params: [], fields: [],
    variants: [{ name: "None", fields: [] }, { name: "Some", fields: [{ name: "amount", type: "u64" }] }],
  },
]);

function loader(abis: ModuleAbi[]) {
  return async (address: string, name: string) => {
    const found = abis.find((a) => normalizeAddress(a.address) === normalizeAddress(address) && a.name === name);
    if (found === undefined) throw new StreamdError(`Module ${address}::${name} not found`);
    return found;
  };
}

async function tablesOf(abis: ModuleAbi[], options: GenerateOptions = { tables: sqlTables("sqlite") }): Promise<EventTable[]> {
  const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".sql-"));
  try {
    const source = await generateEventTypes(abis.map((a) => ({ address: a.address, module: a.name })), loader(abis), options);
    writeFileSync(join(dir, "gen.ts"), source);
    return (await import(join(dir, "gen.ts"))).streamdTables;
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const columnsOf = (tables: EventTable[], table: string) =>
  Object.fromEntries(tables.find((t) => t.table === table)!.columns.map((c) => [c.name, [c.type, c.path.join("."), c.nullable]]));

describe("generateEventTypes sql", () => {
  it("maps every field type to its column, one table per event struct", async () => {
    const tables = await tablesOf([shapes]);
    expect(tables.map((t) => [t.eventType, t.table, t.generic])).toEqual([
      [`${DEX}::shapes::Boxed`, "shapes_boxed", true],
      [`${DEX}::shapes::Clash`, "shapes_clash", false],
      [`${DEX}::shapes::Everything`, "shapes_everything", false],
      [`${DEX}::shapes::Pick`, "shapes_pick", false],
    ]);
    expect(columnsOf(tables, "shapes_everything")).toEqual({
      small: ["u8", "small", false], mid: ["u16", "mid", false], wide: ["u32", "wide", false], signed: ["i32", "signed", false],
      tiny: ["i8", "tiny", false], big: ["u128", "big", false], huge: ["u256", "huge", false], amount: ["u64", "amount", false],
      neg: ["i64", "neg", false], neg_big: ["i128", "neg_big", false], neg_huge: ["i256", "neg_huge", false],
      flag: ["bool", "flag", false], who: ["address", "who", false], bytes: ["text", "bytes", false], name: ["text", "name", false],
      maybe: ["u64", "maybe.vec.0", true], obj: ["address", "obj.inner", false], maybe_obj: ["address", "maybe_obj.vec.0.inner", true],
      maybe_inner: ["json", "maybe_inner.vec.0", true], list: ["json", "list", false], inner: ["json", "inner", false],
      pick: ["json", "pick", false],
    });
    expect(columnsOf(tables, "shapes_boxed")).toEqual({ item: ["json", "item", false], count: ["u64", "count", false] });
    expect(columnsOf(tables, "shapes_pick")).toEqual({ variant: ["text", "__variant__", false], data: ["json", "", false] });
  });

  it("steps field columns around standard columns and each other", async () => {
    const tables = await tablesOf([shapes]);
    expect(Object.keys(columnsOf(tables, "shapes_clash"))).toEqual([
      "version_", "timestamp_", "timestamp__", "amount", "amount_", "xmin_", "timestamp___",
    ]);
    expect(tables.find((t) => t.table === "shapes_clash")!.columns.map((c) => c.path[0])).toEqual([
      "version", "Timestamp", "timestamp_", "Amount", "amount", "xmin", "TIMESTAMP",
    ]);
  });

  it("names tables <module>_<struct> in snake case, unchanged by generating other names", async () => {
    const one = module(DEX, "pool_v3", [struct("SwapEventV3", [], true), struct("FAEvent", [], true)]);
    expect((await tablesOf([one])).map((t) => t.table)).toEqual(["pool_v3_fa_event", "pool_v3_swap_event_v3"]);
    const more = await tablesOf([one, module(OTHER, "router", [struct("Swapped", [], true)])]);
    expect(more.map((t) => t.table).sort()).toEqual(["pool_v3_fa_event", "pool_v3_swap_event_v3", "router_swapped"]);
  });

  it("hashes names that collide or run past 63 bytes, and steps around reserved prefixes", async () => {
    const long = `m${"x".repeat(40)}`;
    const tables = await tablesOf([
      module(DEX, "dex", [struct("Swap", [], true)]),
      module(OTHER, "dex", [struct("Swap", [], true)]),
      module(DEX, "streamd", [struct("Events", [], true)]),
      module(DEX, long, [struct(`Event${"Y".repeat(30)}`, [[`f${"z".repeat(70)}`, "u8"]], true)]),
    ]);
    const name = (address: string, mod: string) => tables.find((t) => t.eventType.startsWith(`${address}::${mod}::`))!.table;
    const names = [name(DEX, "dex"), name(OTHER, "dex"), name(DEX, "streamd"), name(DEX, long)];
    expect(names[0]).toMatch(/^dex_swap_[0-9a-f]{8}$/);
    expect(names[1]).toMatch(/^dex_swap_[0-9a-f]{8}$/);
    expect(names[0]).not.toBe(names[1]);
    expect(names[2]).toBe("move_streamd_events");
    expect(names[3]).toHaveLength(63);
    expect(names[3]).toMatch(/_[0-9a-f]{8}$/);
    expect(tables.find((t) => t.table === names[3])!.columns[0]!.name).toHaveLength(63);
    expect(tables.find((t) => t.table === names[3])!.columns[0]!.name).toMatch(/_[0-9a-f]{8}$/);
    // Pinned, since a renamed table loses its rows: the hash is of the long-form type.
    expect(names[0]).toBe("dex_swap_b77a794e");
    // Hashed names are stable across runs.
    const again = await tablesOf([module(OTHER, "dex", [struct("Swap", [], true)]), module("0xcafe0", "dex", [struct("Swap", [], true)])]);
    expect(again.map((t) => t.table).sort()).toEqual(names.slice(0, 2).sort());
  });

  it("hashes again when stepping a 63-byte column name would pass the limit", async () => {
    const long = "a".repeat(63);
    // The second field takes the name a key that never changes between attempts would hash to, which looped forever.
    const trap = `${"a".repeat(54)}_0c9f8b61`;
    const tables = await tablesOf([
      module(DEX, "w", [struct("Wide", [[long, "u8"], [trap, "u8"], [long.toUpperCase(), "u8"], [`${long.slice(0, 62)}B`, "u8"]], true)]),
    ]);
    const names = tables[0]!.columns.map((c) => c.name);
    expect(names.slice(0, 3)).toEqual([long, trap, `${"a".repeat(54)}_c915b29c`]);
    expect(new Set(names).size).toBe(4);
    for (const name of names) expect(name.length).toBeLessThanOrEqual(63);
  });

  it("steps around Postgres's pg_ prefix and writes empty lists for a module with no events", async () => {
    expect((await tablesOf([module(DEX, "pg", [struct("Class", [], true)])])).map((t) => t.table)).toEqual(["move_pg_class"]);
    const views = parseModuleAbi({
      abi: { address: DEX, name: "v", structs: [], exposed_functions: [{ name: "f", is_view: true, generic_type_params: [], params: [] }] },
    });
    const source = await generateEventTypes([{ address: DEX, module: "v" }], loader([views]), { tables: sqlTables("sqlite") });
    expect(source).toContain("export const streamdTables: readonly StreamdEventTable[] = [];");
    expect(source).toContain("export const streamdSchema: readonly string[] = [];");
  });

  it("maps Sui's builtins to their columns", async () => {
    const sui = module(DEX, "pool", [
      struct("Moved", [
        ["id", "0x2::object::ID"], ["uid", "0x2::object::UID"], ["coin", "0x2::balance::Balance<T0>"], ["link", "0x2::url::Url"],
        ["label", "0x1::ascii::String"], ["kind", "0x1::type_name::TypeName"], ["maybe", "0x1::option::Option<u64>"],
        ["bytes", "vector<u8>"], ["name", "0x1::string::String"],
      ], true, 1),
      { name: "Kind", is_event: true, is_enum: true, generic_type_params: [], fields: [], variants: [{ name: "A", fields: [] }] },
    ]);
    const tables = await tablesOf([sui], { tables: sqlTables("postgres"), dialect: SUI_DIALECT });
    expect(columnsOf(tables, "pool_kind")).toEqual({ variant: ["text", "@variant", false], data: ["json", "", false] });
    expect(columnsOf(tables, "pool_moved")).toEqual({
      id: ["address", "id", false], uid: ["address", "uid", false], coin: ["u64", "coin", false], link: ["text", "link", false],
      label: ["text", "label", false], kind: ["text", "kind", false], maybe: ["u64", "maybe", true], bytes: ["text", "bytes", false],
      name: ["text", "name", false],
    });
  });

  it("writes nothing new without the option and refuses a struct named StreamdEventTable", async () => {
    const load = loader([shapes]);
    const plain = await generateEventTypes([{ address: DEX, module: "shapes" }], load);
    expect(plain).not.toContain("streamdTables");
    expect(plain).not.toContain("StreamdEventTable");
    const clash = module(DEX, "c", [struct("StreamdEventTable", [], true)]);
    await expect(generateEventTypes([{ address: DEX, module: "c" }], loader([clash]))).resolves.toBeString();
    await expect(generateEventTypes([{ address: DEX, module: "c" }], loader([clash]), { tables: sqlTables("sqlite") })).rejects.toThrow(/StreamdEventTable/);
  });
});

type Mod = Record<string, any>;

async function withGenerated(sql: "sqlite" | "postgres", run: (mod: Mod, dir: string, source: string) => Promise<void>) {
  // Inside the package, so the generated import of @streamd/core resolves at runtime.
  const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".sql-"));
  try {
    const source = await generateEventTypes([{ address: DEX, module: "shapes" }], loader([shapes]), { tables: sqlTables(sql), arbitraries: true });
    writeFileSync(join(dir, "gen.ts"), source);
    await run(await import(join(dir, "gen.ts")), dir, source);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const RUNS = 300;

function payloads(mod: Mod): StreamdEvent[] {
  const rng = mod.streamdRng(7);
  const events: StreamdEvent[] = [];
  let version = 1n;
  for (let i = 0; i < RUNS; i++) {
    events.push(mod.streamdEvent(mod.Everything, mod.arbitraryEverything(rng), version++));
    events.push(mod.streamdEvent(mod.Boxed("vector<u8>"), mod.arbitraryBoxed(rng, (r: () => number) => [r() % 9, "x"]), version++));
    events.push(mod.streamdEvent(mod.Clash, mod.arbitraryClash(rng), version++));
    events.push(mod.streamdEvent(mod.Pick, mod.arbitraryPick(rng), version++));
  }
  return events.map((e, i) => ({ ...e, transactionHash: `0x${i.toString(16)}`, timestamp: BigInt(i) * 1000n, sender: "0xA" }));
}

function at(data: unknown, path: readonly (string | number)[]): unknown {
  let value = data;
  for (const key of path) {
    if (value === null || typeof value !== "object" || !Object.hasOwn(value, key)) return null;
    value = (value as Record<string, unknown>)[key];
  }
  return value ?? null;
}

// What each column should hold, worked out from the payload without the mapping code.
function expected(col: EventColumn, data: unknown, sql: "sqlite" | "postgres"): unknown {
  const raw = at(data, col.path);
  if (raw === null) return null;
  if (col.type === "bool") return sql === "sqlite" ? Number(raw) : String(raw);
  if (col.type === "address") return normalizeAddress(raw as string);
  if (col.type === "json") return JSON.parse(JSON.stringify(raw));
  if (col.type === "text") return raw;
  const narrow = Number(col.type.slice(1)) <= 32;
  return sql === "sqlite" && narrow ? Number(raw) : BigInt(raw as string).toString();
}

function compare(tables: EventTable[], events: StreamdEvent[], read: (table: EventTable) => Record<string, unknown>[], sql: "sqlite" | "postgres") {
  let checked = 0;
  for (const table of tables) {
    const mine = events.filter((e) => e.type.split("<")[0] === table.eventType);
    const rows = read(table);
    expect(rows).toHaveLength(mine.length);
    rows.forEach((row, i) => {
      const event = mine[i]!;
      expect(String(row.version)).toBe(String(event.version));
      expect(row.sender).toBe(normalizeAddress("0xA"));
      if (table.generic) expect(row.type_arguments).toBe("vector<u8>");
      for (const col of table.columns) {
        const got = col.type === "json" && row[col.name] !== null ? JSON.parse(String(row[col.name])) : row[col.name];
        expect([col.name, got]).toEqual([col.name, expected(col, event.data, sql)]);
        checked++;
      }
    });
  }
  return checked;
}

describe("generated tables, filled", () => {
  it("land random payloads in SQLite with every column equal to the payload's value", async () => {
    await withGenerated("sqlite", async (mod) => {
      const db = new Database(":memory:");
      for (const statement of mod.streamdSchema) db.exec(statement);
      const store = new SqliteEventStore(db);
      const events = payloads(mod);
      store.writeTables(mod.streamdTables, events, { name: "t", cursor: "c" });
      const checked = compare(mod.streamdTables, events, (t) => db.prepare(`SELECT * FROM "${t.table}" ORDER BY version`).all() as never, "sqlite");
      expect(checked).toBeGreaterThan(RUNS * 20);
    });
  });

  it("land random payloads in Postgres with every column equal to the payload's value", async () => {
    await withGenerated("postgres", async (mod) => {
      const db = new PGlite();
      try {
        for (const statement of mod.streamdSchema) await db.query(statement);
        const store = new PostgresEventStore(db);
        const events = payloads(mod);
        await store.writeTables(mod.streamdTables, events, { name: "t", cursor: "c" });
        const read = async (t: EventTable) => {
          const names = ["version", "sender", ...(t.generic ? ["type_arguments"] : []), ...t.columns.map((c) => c.name)];
          const { rows } = await db.query(`SELECT ${names.map((n) => `"${n}"::text AS "${n}"`).join(", ")} FROM "${t.table}" AS src ORDER BY src.version`);
          return rows as Record<string, unknown>[];
        };
        const all = new Map<EventTable, Record<string, unknown>[]>();
        for (const t of mod.streamdTables as EventTable[]) all.set(t, await read(t));
        const checked = compare(mod.streamdTables, events, (t) => all.get(t)!, "postgres");
        expect(checked).toBeGreaterThan(RUNS * 20);
      } finally {
        await db.close();
      }
    });
  }, 60_000);

  it("typechecks under strict settings", async () => {
    await withGenerated("postgres", async (_mod, dir) => {
      writeFileSync(
        join(dir, "use.ts"),
        [
          'import type { EventTable } from "@streamd/core";',
          'import { streamdSchema, streamdTables } from "./gen.js";',
          "const tables: readonly EventTable[] = streamdTables;",
          "const first: string | undefined = streamdSchema[0];",
          "void tables; void first;",
        ].join("\n"),
      );
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022", module: "ESNext", moduleResolution: "bundler", strict: true, noUnusedLocals: true,
            noUnusedParameters: true, noPropertyAccessFromIndexSignature: true, noUncheckedIndexedAccess: true,
            noEmit: true, skipLibCheck: true, types: [], lib: ["ES2022"],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["gen.ts", "use.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
    });
  }, 60_000);
});
