import { Database } from "bun:sqlite";
import { afterAll, describe, expect, it } from "bun:test";
import type { EventDescriptor } from "@streamd/core";
import type { Endpoint } from "../src/args.js";
import { openCache } from "../src/cache.js";
import { chainFor } from "../src/chain.js";
import { RequestStats, onStatsSignal } from "../src/stats.js";

const TYPE = "0x1::coin::DepositEvent";
const DESCRIPTOR: EventDescriptor = { kind: "module", eventType: TYPE };
const HEAD = 100;

/** A fullnode at every `/<name>/v1`, at `HEAD` with one event per transaction. */
const server = Bun.serve({
  port: 0,
  hostname: "127.0.0.1",
  fetch(request) {
    const url = new URL(request.url);
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

const url = (name: string) => `http://127.0.0.1:${server.port}/${name}/v1`;

type Methods = Record<string, { requests: number; errors: number; rateLimited: number; latency?: Record<string, number> }>;
const parsed = (stats: RequestStats) => JSON.parse(stats.line()) as { stats: { nodes: Methods[]; archive?: Methods } };

describe("RequestStats through chainFor", () => {
  it("counts a client and adapter() of one chain as one node", async () => {
    const stats = new RequestStats();
    const chain = await chainFor({ network: "mainnet", nodeUrl: url("one") }, undefined, stats);
    const client = chain.client();
    try {
      await client.getEvents(DESCRIPTOR, { fromVersion: 0n, toVersion: 20n });
      await chain.adapter().getHeadVersion();
    } finally {
      client.destroy();
    }
    const { nodes, archive } = parsed(stats).stats;
    expect(nodes).toHaveLength(1);
    expect(archive).toBeUndefined();
    expect(nodes[0]!.fetchEvents!.requests).toBeGreaterThan(0);
    expect(nodes[0]!.getHeadVersion!.requests).toBe(1);
    expect(nodes[0]!.fetchEvents!.errors).toBe(0);
  });

  it("gives each pooled node its own entry, in --node-url order, and the archive one", async () => {
    const stats = new RequestStats();
    const endpoint: Endpoint = { network: "mainnet", nodeUrl: url("a"), extraNodeUrls: [url("b")], archive: url("c") };
    const adapter = (await chainFor(endpoint, undefined, stats)).adapter();
    // A pool rotates new walks, so each node takes one.
    await adapter.fetchEvents(DESCRIPTOR, { fromVersion: 0n, limit: 5 });
    await adapter.fetchEvents(DESCRIPTOR, { fromVersion: 0n, limit: 5 });
    // The archive answers for the oldest version.
    await adapter.getEarliestVersion!(DESCRIPTOR);
    const { nodes, archive } = parsed(stats).stats;
    expect(nodes.map((n) => n.fetchEvents?.requests)).toEqual([1, 1]);
    expect(archive).toEqual({ getEarliestVersion: expect.objectContaining({ requests: 1, errors: 0 }) });
  });

  it("does not count pages the cache answers", async () => {
    const db = new Database(":memory:");
    const endpoint: Endpoint = { network: "mainnet", nodeUrl: url("cached") };
    const walk = async () => {
      const stats = new RequestStats();
      const cache = await openCache("pages.db", endpoint, () => db as never, () => {});
      const client = (await chainFor(endpoint, cache, stats)).client();
      try {
        await client.getEvents(DESCRIPTOR, { fromVersion: 0n, toVersion: 20n });
      } finally {
        client.destroy();
      }
      return parsed(stats).stats.nodes[0]!;
    };
    expect((await walk()).fetchEvents!.requests).toBeGreaterThan(0);
    expect((await walk()).fetchEvents).toBeUndefined();
  });

  it("tracks each Sui node once, however many clients and adapters are made", async () => {
    const stats = new RequestStats();
    const chain = await chainFor({ chain: "sui", network: "testnet", nodeUrl: "https://a.invalid:443", extraNodeUrls: ["https://b.invalid:443"] }, undefined, stats);
    chain.client().destroy();
    chain.client().destroy();
    chain.adapter();
    expect(parsed(stats).stats.nodes).toEqual([{}, {}]);
  });
});

describe("RequestStats.line", () => {
  it("is one JSON line with whole-millisecond latencies and no URL", async () => {
    const stats = new RequestStats();
    const node = { fetchEvents: async (_d: EventDescriptor, _o: object) => ({ events: [], nextCursor: "x", hasMore: false }) };
    const tracked = stats.track(node, "node");
    await tracked.fetchEvents(DESCRIPTOR, {});
    const line = stats.line();
    expect(line.endsWith("\n")).toBe(true);
    expect(line.trimEnd()).not.toContain("\n");
    const { latency } = parsed(stats).stats.nodes[0]!.fetchEvents!;
    for (const value of Object.values(latency!)) expect(Number.isInteger(value)).toBe(true);
    expect(line).not.toContain("http");
    expect(JSON.parse(new RequestStats().line())).toEqual({ stats: { nodes: [] } });
  });
});

describe("onStatsSignal", () => {
  it("installs a SIGUSR1 listener and removes it, and installs none on Windows", () => {
    const before = process.listenerCount("SIGUSR1");
    const print = () => {};
    const remove = onStatsSignal(print, "darwin");
    expect(process.listenerCount("SIGUSR1")).toBe(before + 1);
    remove();
    expect(process.listenerCount("SIGUSR1")).toBe(before);
    onStatsSignal(print, "win32")();
    expect(process.listenerCount("SIGUSR1")).toBe(before);
  });

  it("prints on the signal", async () => {
    let printed = 0;
    const remove = onStatsSignal(() => printed++);
    try {
      process.kill(process.pid, "SIGUSR1");
      await Bun.sleep(50);
      expect(printed).toBe(1);
    } finally {
      remove();
    }
  });
});
