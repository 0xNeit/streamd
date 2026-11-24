import { PGlite } from "@electric-sql/pglite";
import { Database } from "bun:sqlite";
import { afterEach, describe, expect, it } from "bun:test";
import type { StreamdEvent } from "@streamd/core";
import { PostgresEventStore } from "@streamd/store-postgres";
import { SqliteEventStore } from "@streamd/store-sqlite";
import type { StoreAdminOptions } from "../src/args.js";
import { runStoreAdmin, type PostgresConnection } from "../src/store.js";

const TYPE = "0x1::coin::DepositEvent";
const OTHER = "0x1::coin::WithdrawEvent";
const URL = "postgres://me:hunter2@db.example/events";

function ev(version: number, type = TYPE): StreamdEvent {
  return { type, data: { n: String(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

const opened: PGlite[] = [];
afterEach(async () => {
  await Promise.all(opened.splice(0).map((pg) => pg.close().catch(() => {})));
});

interface Backend {
  name: string;
  /** Options selecting this backend, and a store over the same database for setup and read-back. */
  make(): Promise<{ target: Pick<StoreAdminOptions, "db" | "postgres">; io: { open: () => Database; connect: () => PostgresConnection }; store: SqliteEventStore | PostgresEventStore }>;
}

const refuse = (): never => {
  throw new Error("opened the wrong backend");
};

const backends: Backend[] = [
  {
    name: "sqlite",
    async make() {
      const db = new Database(":memory:");
      const store = new SqliteEventStore(db);
      // The run's close would end the database the assertions read.
      return { target: { db: "e.db" }, io: { open: () => Object.assign(db, { close: () => {} }), connect: refuse }, store };
    },
  },
  {
    name: "postgres",
    async make() {
      const pg = new PGlite();
      opened.push(pg);
      const connection = { query: (text: string, params?: unknown[]) => pg.query<Record<string, unknown>>(text, params), close: () => {} };
      return { target: { postgres: URL }, io: { open: refuse, connect: () => connection }, store: new PostgresEventStore(connection) };
    },
  },
];

async function run(backend: Awaited<ReturnType<Backend["make"]>>, options: Omit<StoreAdminOptions, "db" | "postgres">) {
  const lines: string[] = [];
  const errors: string[] = [];
  const code = await runStoreAdmin(
    { ...options, ...backend.target },
    { ...backend.io, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal: new AbortController().signal },
  );
  return { code, lines: lines.map((l) => JSON.parse(l) as Record<string, unknown>), raw: lines, errors };
}

async function filled(backend: Backend) {
  const made = await backend.make();
  await made.store.write([ev(100), ev(200), ev(300, OTHER)], { name: "swaps", cursor: "c1" });
  await made.store.write([], { name: "fees", cursor: "c2" });
  return made;
}

describe.each(backends)("runStoreAdmin over $name", (backend) => {
  it("prints the stats as one line, bigints as strings, by type when asked", async () => {
    const made = await filled(backend);
    const plain = await run(made, { action: "stats" });
    expect(plain.code).toBe(0);
    expect(plain.raw).toHaveLength(1);
    expect(plain.raw[0]!.endsWith("\n")).toBe(true);
    expect(plain.lines[0]).toEqual({ events: "3", resourceChanges: "0", transfers: "0", lowestVersion: "100", highestVersion: "300", prunedBefore: "0" });
    const typed = await run(made, { action: "stats", byType: true });
    expect(typed.lines[0]!.byType).toEqual([
      { type: TYPE, count: "2" },
      { type: OTHER, count: "1" },
    ]);
  });

  it("prints stats of an empty store without a version range", async () => {
    const { lines, code } = await run(await backend.make(), { action: "stats" });
    expect(code).toBe(0);
    expect(lines).toEqual([{ events: "0", resourceChanges: "0", transfers: "0", prunedBefore: "0" }]);
  });

  it("reports a prune without --yes and changes nothing", async () => {
    const made = await filled(backend);
    const { code, lines } = await run(made, { action: "prune", before: 250n });
    expect(code).toBe(0);
    expect(lines).toEqual([
      { dryRun: true, beforeVersion: "250", prunedBefore: "0", lowestVersion: "100", highestVersion: "300", noop: false, rows: "not counted" },
    ]);
    expect((await made.store.stats()).events).toBe(3n);
  });

  it("prunes with --yes and prints the new edge and what remains", async () => {
    const made = await filled(backend);
    const { code, lines } = await run(made, { action: "prune", before: 250n, yes: true });
    expect(code).toBe(0);
    expect(lines).toEqual([
      { dryRun: false, beforeVersion: "250", noop: false, events: "1", resourceChanges: "0", transfers: "0", lowestVersion: "300", highestVersion: "300", prunedBefore: "250" },
    ]);
    expect((await made.store.cursors()).map((c) => c.name)).toEqual(["fees", "swaps"]);
  });

  it("reports a --before at or below the edge as a no-op, without pruning", async () => {
    const made = await filled(backend);
    await made.store.prune({ beforeVersion: 150n });
    for (const before of [150n, 120n]) {
      const dry = await run(made, { action: "prune", before });
      expect(dry.lines[0]).toMatchObject({ dryRun: true, prunedBefore: "150", noop: true });
      const real = await run(made, { action: "prune", before, yes: true });
      expect(real.code).toBe(0);
      expect(real.lines[0]).toMatchObject({ dryRun: false, noop: true, prunedBefore: "150", events: "2" });
    }
  });

  it("lists cursors one per line in name order, and none as nothing", async () => {
    const listed = await run(await filled(backend), { action: "cursors" });
    expect(listed.code).toBe(0);
    expect(listed.lines).toEqual([
      { name: "fees", cursor: "c2" },
      { name: "swaps", cursor: "c1" },
    ]);
    const empty = await run(await backend.make(), { action: "cursors" });
    expect(empty).toMatchObject({ code: 0, lines: [] });
  });

  it("deletes a cursor, exiting 1 when there was none", async () => {
    const made = await filled(backend);
    expect(await run(made, { action: "cursors", delete: "swaps" })).toMatchObject({ code: 0, lines: [{ name: "swaps", deleted: true }] });
    expect(await run(made, { action: "cursors", delete: "swaps" })).toMatchObject({ code: 1, lines: [{ name: "swaps", deleted: false }] });
    expect((await made.store.cursors()).map((c) => c.name)).toEqual(["fees"]);
  });

  it("refuses to delete the reserved edge row with the store's error, exit 1", async () => {
    const made = await filled(backend);
    await made.store.prune({ beforeVersion: 150n });
    const { code, lines, errors } = await run(made, { action: "cursors", delete: "streamd:pruned-before" });
    expect(code).toBe(1);
    expect(lines).toEqual([]);
    expect(errors.join("\n")).toContain("reserved");
    expect((await made.store.stats()).prunedBefore).toBe(150n);
  });
});

describe("runStoreAdmin opening", () => {
  const io = { write: () => {}, signal: new AbortController().signal };

  it("exits 1 naming the file when SQLite cannot be opened", async () => {
    const errors: string[] = [];
    const code = await runStoreAdmin(
      { action: "stats", db: "missing.db" },
      { ...io, open: () => { throw new Error("no such file"); }, connect: refuse, error: (m) => errors.push(m) },
    );
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: cannot open missing.db: no such file"]);
  });

  it("never prints the Postgres password, connecting or querying", async () => {
    const errors: string[] = [];
    const unreachable = await runStoreAdmin(
      { action: "stats", postgres: URL },
      { ...io, open: refuse, connect: () => { throw new Error(`refused ${URL}`); }, error: (m) => errors.push(m) },
    );
    const failing = await runStoreAdmin(
      { action: "cursors", postgres: URL },
      {
        ...io,
        open: refuse,
        connect: () => ({ query: () => Promise.reject(new Error("password hunter2 rejected")), close: () => {} }),
        error: (m) => errors.push(m),
      },
    );
    expect([unreachable, failing]).toEqual([1, 1]);
    expect(errors[0]).toContain("cannot connect to postgres://me:***@db.example/events");
    expect(errors.join("\n")).not.toContain("hunter2");
  });

  it("closes the database after a run", async () => {
    const db = new Database(":memory:");
    let closed = 0;
    const code = await runStoreAdmin(
      { action: "cursors", db: "e.db" },
      { ...io, open: () => Object.assign(db, { close: () => void closed++ }), connect: refuse, error: () => {} },
    );
    expect([code, closed]).toEqual([0, 1]);
  });
});
