import { describe, expect, it } from "bun:test";
import { StreamdNetworkError } from "@streamd/core";
import type { EventAdapter } from "@streamd/core";
import { runMonitor } from "../src/monitor.js";
import type { MonitorOptions } from "../src/args.js";

const TYPE = "0x1::transaction_fee::FeeStatement";
const OPTIONS: MonitorOptions = {
  descriptor: { kind: "module", eventType: [TYPE] },
  network: "mainnet",
  nodeUrl: "https://a.example/v1",
  extraNodeUrls: ["https://b.example/v1"],
  interval: 1000,
  window: 3000,
  range: 10,
  format: "json",
};

type Head = bigint | Error | (() => Promise<bigint>);

/** A node whose head and head-block time follow the given lists, one entry per round. */
function node(heads: Head[], times: (bigint | undefined)[] = []): EventAdapter & { calls: number } {
  let round = 0;
  const fake = {
    calls: 0,
    fetchEvents: async () => ({ events: [], hasMore: false }),
    getHeadVersion: async () => {
      fake.calls++;
      const head = heads[Math.min(round++, heads.length - 1)]!;
      if (head instanceof Error) throw head;
      return typeof head === "function" ? head() : head;
    },
    getBlockHeights: async () => ({ oldest: 0n, newest: BigInt(round) }),
    getBlock: async () => {
      const time = times.length === 0 ? 0n : times[Math.min(round - 1, times.length - 1)];
      if (time === undefined) throw new StreamdNetworkError("no block", 500);
      return { height: BigInt(round), firstVersion: 0n, lastVersion: 0n, timestamp: time };
    },
  };
  return fake;
}

async function run(nodes: EventAdapter[], options: Partial<MonitorOptions> = {}, signal = new AbortController().signal) {
  let clock = 1_000_000;
  const lines: Record<string, any>[] = [];
  const code = await runMonitor({ ...OPTIONS, limit: 3, ...options }, {
    nodes,
    write: (line) => void lines.push(JSON.parse(line)),
    signal,
    now: () => clock,
    sleep: async (ms) => void (clock += ms),
  });
  return { code, lines };
}

describe("runMonitor", () => {
  it("reports each node's lag behind the round's best, in versions and chain time", async () => {
    const { code, lines } = await run([node([100n], [5_000_000n]), node([90n], [4_750_000n])], { limit: 1 });
    expect(code).toBe(0);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ round: 1, time: new Date(1_000_000).toISOString() });
    expect(lines[0]!.nodes).toEqual([
      {
        node: 0, up: true, error: null, head: "100", lagVersions: "0", lagMs: 0,
        window: { rounds: 1, up: 1, requests: 3, errors: 0, rateLimited: 0, errorRate: 0, maxLagVersions: "0", maxLagMs: 0 },
      },
      {
        node: 1, up: true, error: null, head: "90", lagVersions: "10", lagMs: 250,
        window: { rounds: 1, up: 1, requests: 3, errors: 0, rateLimited: 0, errorRate: 0, maxLagVersions: "10", maxLagMs: 250 },
      },
    ]);
  });

  it("gives a down node a code, never its message, and leaves its head and lag null", async () => {
    const secret = "https://user:key@a.example/v1";
    const { lines } = await run(
      [node([100n]), node([new StreamdNetworkError(`503 from ${secret}`, 503)]), node([new Error(`boom ${secret}`)])],
      { limit: 1 },
    );
    expect(JSON.stringify(lines)).not.toContain("a.example");
    expect(lines[0]!.nodes.map((n: any) => [n.up, n.error, n.head, n.lagVersions, n.lagMs])).toEqual([
      [true, null, "100", "0", 0],
      [false, "503", null, null, null],
      [false, "failed", null, null, null],
    ]);
    expect(lines[0]!.nodes[1].window).toMatchObject({ requests: 1, errors: 1, errorRate: 1, up: 0, maxLagVersions: null });
  });

  it("times out a node that does not answer within the interval", async () => {
    const silent = node([() => new Promise<bigint>(() => {})]);
    const { lines } = await run([node([5n]), silent], { limit: 1, interval: 20 });
    expect(lines[0]!.nodes[1]).toMatchObject({ up: false, error: "timeout" });
    expect(lines[0]!.nodes[1].window).toMatchObject({ requests: 1, errors: 1 });
  });

  it("leaves lagMs null when a node cannot read its head block, counting the failed read", async () => {
    const { lines } = await run([node([10n], [1_000n]), node([10n], [undefined])], { limit: 1 });
    expect(lines[0]!.nodes[1]).toMatchObject({ up: true, lagVersions: "0", lagMs: null });
    expect(lines[0]!.nodes[1].window).toMatchObject({ requests: 3, errors: 1, errorRate: 0.3333 });
  });

  it("sets a node aside after a 429 for its retryAfter, then reads it again", async () => {
    const limited = node([new StreamdNetworkError("slow down", 429, undefined, 2500), 7n]);
    const { lines } = await run([node([7n]), limited], { limit: 5, window: 60_000 });
    expect(lines.map((l) => l.nodes[1].error)).toEqual(["429", "429", "429", null, null]);
    expect(limited.calls).toBe(3);
    expect(lines[4]!.nodes[1].window).toMatchObject({ rounds: 5, up: 2, requests: 7, errors: 1, rateLimited: 1 });
  });

  it("waits one interval after a 429 that names no wait", async () => {
    const limited = node([new StreamdNetworkError("slow down", 429), 7n]);
    const { lines } = await run([node([7n]), limited], { limit: 3 });
    expect(lines.map((l) => l.nodes[1].error)).toEqual(["429", "429", null]);
    expect(limited.calls).toBe(2);
  });

  it("sums the window over the rounds inside it and drops older ones", async () => {
    const { lines } = await run([node([10n, 20n, 30n, 40n]), node([10n, 5n, 30n, 40n])], { limit: 4, window: 1500 });
    expect(lines.map((l) => l.nodes[1].window.rounds)).toEqual([1, 2, 2, 2]);
    expect(lines.map((l) => l.nodes[1].lagVersions)).toEqual(["0", "15", "0", "0"]);
    expect(lines.map((l) => l.nodes[1].window.maxLagVersions)).toEqual(["0", "15", "15", "0"]);
    expect(lines[3]!.nodes[1].window.requests).toBe(6);
  });

  it("starts rounds one interval apart, at once after a slow round", async () => {
    let clock = 0;
    const waits: number[] = [];
    let reads = 0;
    const slow = { getHeadVersion: async () => ((clock += ++reads === 2 ? 1500 : 200), 1n), fetchEvents: async () => ({ events: [], hasMore: false }) };
    await runMonitor({ ...OPTIONS, limit: 3 }, {
      nodes: [node([1n]), slow],
      write: () => {},
      signal: new AbortController().signal,
      now: () => clock,
      sleep: async (ms) => void (waits.push(ms), (clock += ms)),
    });
    expect(waits).toEqual([800, 0]);
  });

  it("stops with 0 when the signal aborts, mid-round included", async () => {
    const controller = new AbortController();
    const hanging = node([() => new Promise<bigint>(() => {})]);
    const done = run([node([1n]), hanging], { limit: undefined, interval: 200 }, controller.signal);
    setTimeout(() => controller.abort(), 10);
    const { code, lines } = await done;
    expect(code).toBe(0);
    expect(lines).toHaveLength(0);
  });
});
