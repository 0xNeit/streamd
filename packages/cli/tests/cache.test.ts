import { Database } from "bun:sqlite";
import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamdClient, recordingAdapter, replayAdapter } from "@streamd/core";
import type { EventDescriptor } from "@streamd/core";
import type { Endpoint } from "../src/args.js";
import { cacheNamespace, openCache } from "../src/cache.js";
import { chainFor } from "../src/chain.js";
import { openDatabase } from "../src/store.js";

const TYPE = "0x1::coin::DepositEvent";
const DESCRIPTOR: EventDescriptor = { kind: "module", eventType: TYPE };
const HEAD = 1000;

/** The paths a local fake fullnode answered, at `HEAD` with one event per transaction. */
const requests: string[] = [];
const server = Bun.serve({
  port: 0,
  hostname: "127.0.0.1",
  fetch(request) {
    const url = new URL(request.url);
    requests.push(url.pathname);
    if (url.pathname.endsWith("/transactions")) {
      const start = Number(url.searchParams.get("start"));
      const limit = Number(url.searchParams.get("limit"));
      const txns = [];
      for (let v = start; v < Math.min(start + limit, HEAD + 1); v++) {
        txns.push({
          type: "user_transaction",
          version: String(v),
          hash: `0x${v.toString(16)}`,
          sender: "0x1",
          timestamp: String(v * 1000),
          events: [{ type: TYPE, data: { amount: String(v) }, sequence_number: "0", guid: { creation_number: "0", account_address: "0x0" } }],
        });
      }
      return Response.json(txns);
    }
    return Response.json({
      chain_id: 1,
      epoch: "1",
      ledger_version: String(HEAD),
      oldest_ledger_version: "0",
      ledger_timestamp: "1",
      node_role: "full_node",
      oldest_block_height: "0",
      block_height: "10",
      git_hash: "",
    });
  },
});
afterAll(() => server.stop(true));

const ENDPOINT: Endpoint = { network: "mainnet", nodeUrl: `http://127.0.0.1:${server.port}/v1` };
const fakeNode = () => {
  requests.length = 0;
  return { requests };
};

const memory = () => new Database(":memory:");
const quiet = () => {};

/** A cache over `db`, as a fresh process opening the file would have it. */
const cacheOver = (db: Database, report: (text: string) => void = quiet) =>
  openCache("pages.db", ENDPOINT, () => db as never, report);

async function query(db: Database, range: { fromVersion: bigint; toVersion: bigint }, report?: (text: string) => void) {
  const chain = await chainFor(ENDPOINT, await cacheOver(db, report));
  const client = chain.client();
  try {
    return await client.getEvents(DESCRIPTOR, range);
  } finally {
    client.destroy();
  }
}

describe("cacheNamespace", () => {
  it("names the network, then a hash of the sorted node URLs and an archive URL", () => {
    expect(cacheNamespace({ network: "mainnet" })).toBe("aptos:mainnet");
    expect(cacheNamespace({ network: "testnet", archive: "default" })).toBe("aptos:testnet");
    const pool = cacheNamespace({ network: "mainnet", nodeUrl: "https://b/SECRET/v1", extraNodeUrls: ["https://a/v1"] });
    expect(pool).toMatch(/^aptos:mainnet [0-9a-f]{64}$/);
    expect(pool).not.toContain("SECRET");
    expect(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1", extraNodeUrls: ["https://b/SECRET/v1"] })).toBe(pool);
    expect(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1" })).not.toBe(pool);
    expect(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1", archive: "https://archive/v1" }))
      .not.toBe(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1" }));
    expect(cacheNamespace({ network: "testnet", nodeUrl: "https://a/v1" })).toStartWith("aptos:testnet ");
    expect(cacheNamespace({ network: "mainnet", archive: "https://z/v1" })).not.toBe(cacheNamespace({ network: "mainnet", nodeUrl: "https://z/v1" }));
    expect(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1", archive: "https://b/v1" }))
      .not.toBe(cacheNamespace({ network: "mainnet", nodeUrl: "https://a/v1", extraNodeUrls: ["https://b/v1"] }));
  });
});

describe("--cache through chainFor", () => {
  const SETTLED = { fromVersion: 100n, toVersion: 400n };

  it("answers a second run of a settled range from the file, with no request", async () => {
    const db = memory();
    const node = fakeNode();
    const first = await query(db, SETTLED);
    expect(first).toHaveLength(301);
    expect(node.requests.length).toBeGreaterThan(0);
    node.requests.length = 0;
    const second = await query(db, SETTLED);
    expect(node.requests).toEqual([]);
    expect(second).toEqual(first);
  });

  it("refetches a page that is not final, such as one reaching the head", async () => {
    const db = memory();
    const node = fakeNode();
    const range = { fromVersion: 900n, toVersion: BigInt(HEAD) };
    const first = await query(db, range);
    node.requests.length = 0;
    expect(await query(db, range)).toEqual(first);
    expect(node.requests.length).toBeGreaterThan(0);
  });

  it("keeps a different node's pages apart", async () => {
    const db = memory();
    await query(db, SETTLED);
    // The same server under another name, so only the namespace differs.
    const endpoint = { ...ENDPOINT, nodeUrl: ENDPOINT.nodeUrl!.replace("127.0.0.1", "localhost") };
    const node = fakeNode();
    const other = await chainFor(endpoint, await openCache("pages.db", endpoint, () => db as never, quiet));
    await other.adapter().fetchEvents(DESCRIPTOR, SETTLED);
    expect(node.requests.length).toBeGreaterThan(0);
  });

  it("records every call a replay needs when the pages come from the file", async () => {
    const db = memory();
    const node = fakeNode();
    const expected = await query(db, SETTLED);
    node.requests.length = 0;
    const recorder = recordingAdapter((await chainFor(ENDPOINT, await cacheOver(db))).adapter());
    await new StreamdClient({ adapter: recorder }).getEvents(DESCRIPTOR, SETTLED);
    expect(node.requests).toEqual([]);
    const replay = new StreamdClient({ adapter: replayAdapter(JSON.parse(JSON.stringify(recorder))) });
    expect(await replay.getEvents(DESCRIPTOR, SETTLED)).toEqual(expected);
  });

  it("reads a broken entry as a miss, reported once", async () => {
    const db = memory();
    const node = fakeNode();
    const first = await query(db, SETTLED);
    db.exec("UPDATE streamd_pages SET page = 'not json'");
    node.requests.length = 0;
    const reports: string[] = [];
    expect(await query(db, SETTLED, (text) => reports.push(text))).toEqual(first);
    expect(node.requests.length).toBeGreaterThan(0);
    expect(reports).toHaveLength(1);
    expect(reports[0]).toStartWith("streamd: --cache: ");
    expect(reports[0]).toEndWith("; reading from the node");
  });
});

describe("openCache", () => {
  it("rejects a path that cannot be opened, and a file that is not SQLite", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-cache-"));
    try {
      await expect(openCache(join(dir, "missing", "pages.db"), ENDPOINT, openDatabase, quiet)).rejects.toThrow();
      const garbage = join(dir, "garbage.db");
      writeFileSync(garbage, "this is not a database, it is long enough to have a header ".repeat(10));
      await expect(openCache(garbage, ENDPOINT, openDatabase, quiet)).rejects.toThrow();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("creates a missing file and keeps pages in it across opens", async () => {
    const dir = mkdtempSync(join(tmpdir(), "streamd-cache-"));
    try {
      const path = join(dir, "pages.db");
      const first = await openCache(path, ENDPOINT, openDatabase, quiet);
      await first.store.set("k", "v");
      first.close();
      const second = await openCache(path, ENDPOINT, openDatabase, quiet);
      expect(await second.store.get("k")).toBe("v");
      second.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
