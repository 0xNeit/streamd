import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter } from "@streamd/core";
import { runMonitor, serveMetrics } from "../src/monitor.js";
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
        node: 0, up: true, error: null, head: "100", lagVersions: "0", lagMs: 0, compare: null,
        window: { rounds: 1, up: 1, requests: 4, errors: 0, rateLimited: 0, errorRate: 0, maxLagVersions: "0", maxLagMs: 0 },
      },
      {
        node: 1, up: true, error: null, head: "90", lagVersions: "10", lagMs: 250,
        compare: { fromVersion: "81", toVersion: "90", window: null, matched: 0, missing: 0, extra: 0, different: 0 },
        window: {
          rounds: 1, up: 1, requests: 4, errors: 0, rateLimited: 0, errorRate: 0, maxLagVersions: "10", maxLagMs: 250,
          matched: 0, disagreements: 0,
        },
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
    expect(lines[0]!.nodes[1].window).toMatchObject({ requests: 4, errors: 1, errorRate: 0.25 });
  });

  it("sets a node aside after a 429 for its retryAfter, then reads it again", async () => {
    const limited = node([new StreamdNetworkError("slow down", 429, undefined, 2500), 7n]);
    const { lines } = await run([node([7n]), limited], { limit: 5, window: 60_000 });
    expect(lines.map((l) => l.nodes[1].error)).toEqual(["429", "429", "429", null, null]);
    expect(limited.calls).toBe(3);
    expect(lines[4]!.nodes[1].window).toMatchObject({ rounds: 5, up: 2, requests: 9, errors: 1, rateLimited: 1 });
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
    expect(lines[3]!.nodes[1].window.requests).toBe(8);
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

const ev = (v: number, amount = "1", eventIndex = 0) => ({
  type: TYPE, data: { amount }, version: BigInt(v), sequenceNumber: 0n, eventIndex,
});

/** A `MemoryAdapter` that reports `head` as its head, holding events up to it. */
function memory(head: bigint, events: ReturnType<typeof ev>[]): EventAdapter {
  const adapter = new MemoryAdapter(events);
  return {
    fetchEvents: (d, o) => adapter.fetchEvents(d, o),
    getHeadVersion: async () => head,
  };
}

describe("runMonitor disagreement", () => {
  const all = Array.from({ length: 30 }, (_, i) => ev(i + 1));

  it("compares each up node with node 0 over the newest range both have, counting the window", async () => {
    const a = memory(30n, all);
    const same = memory(30n, all);
    const behind = memory(25n, all.filter((e) => e.version <= 25n && e.version !== 22n).map((e) => (e.version === 24n ? ev(24, "9") : e)));
    const { lines } = await run([a, same, behind], { limit: 2, range: 10 });
    const [, b, c] = lines[1]!.nodes;
    expect(b.compare).toEqual({ fromVersion: "21", toVersion: "30", window: { fromVersion: "21", toVersion: "30" }, matched: 10, missing: 0, extra: 0, different: 0 });
    expect(c.compare).toEqual({ fromVersion: "16", toVersion: "25", window: { fromVersion: "16", toVersion: "25" }, matched: 8, missing: 1, extra: 0, different: 1 });
    expect(b.window).toMatchObject({ matched: 20, disagreements: 0 });
    expect(c.window).toMatchObject({ matched: 16, disagreements: 4 });
    expect(lines[1]!.nodes[0].compare).toBeNull();
    expect("matched" in lines[1]!.nodes[0].window).toBe(false);
  });

  it("starts a range at version 0 when the head is younger than it", async () => {
    const { lines } = await run([memory(3n, all.slice(0, 3)), memory(3n, all.slice(0, 3))], { limit: 1 });
    expect(lines[0]!.nodes[1].compare).toMatchObject({ fromVersion: "0", toVersion: "3", matched: 3 });
  });

  it("compares nothing when node 0 is down, and skips a node that is down", async () => {
    const down = node([new StreamdNetworkError("down", 503)]);
    const first = await run([down, memory(30n, all)], { limit: 1 });
    expect(first.lines[0]!.nodes[1].compare).toBeNull();
    const second = await run([memory(30n, all), down, memory(30n, all)], { limit: 1 });
    expect(second.lines[0]!.nodes.map((n: any) => n.compare?.matched ?? null)).toEqual([null, null, 10]);
  });

  it("counts a failing compare walk as an error on the node that failed", async () => {
    const failing: EventAdapter = {
      fetchEvents: async () => Promise.reject(new StreamdNetworkError("down", 500)),
      getHeadVersion: async () => 30n,
    };
    const { lines } = await run([memory(30n, all), failing], { limit: 1, range: 10 });
    expect(lines[0]!.nodes[1]).toMatchObject({ up: true, compare: { window: null, matched: 0 } });
    expect(lines[0]!.nodes[1].window).toMatchObject({ requests: 2, errors: 1, disagreements: 0 });
    expect(lines[0]!.nodes[0].window).toMatchObject({ requests: 2, errors: 0 });
  });
});

describe("runMonitor prometheus", () => {
  async function text(nodes: EventAdapter[], limit = 1) {
    let clock = 0;
    const out: string[] = [];
    const published: string[] = [];
    await runMonitor({ ...OPTIONS, format: "prometheus", limit }, {
      nodes,
      write: (t) => void out.push(t),
      publish: (t) => void published.push(t),
      signal: new AbortController().signal,
      now: () => clock,
      sleep: async (ms) => void (clock += ms),
    });
    return { out, published };
  }

  it("prints the monitor's gauges per node, then each node's adapter counters, leaving unknown series out", async () => {
    const all = Array.from({ length: 10 }, (_, i) => ev(i + 1));
    const timed: EventAdapter = {
      ...memory(10n, all),
      getBlockHeights: async () => ({ oldest: 0n, newest: 4n }),
      getBlock: async () => ({ height: 4n, firstVersion: 10n, lastVersion: 10n, timestamp: 3_000_000n }),
    };
    const { out, published } = await text([timed, memory(10n, all.map((e) => (e.version === 5n ? ev(5, "9") : e))), node([new StreamdNetworkError("down", 503)])]);
    const [round] = out;
    expect(round!.endsWith("\n\n")).toBe(true);
    expect(published).toEqual([round!.slice(0, -1)]);
    const lines = round!.split("\n");
    for (const sample of [
      '# TYPE streamd_monitor_up gauge',
      'streamd_monitor_up{node="0"} 1',
      'streamd_monitor_up{node="1"} 1',
      'streamd_monitor_up{node="2"} 0',
      'streamd_monitor_head_version{node="0"} 10',
      'streamd_monitor_lag_versions{node="1"} 0',
      'streamd_monitor_lag_seconds{node="0"} 0',
      'streamd_monitor_window_requests{node="2"} 1',
      'streamd_monitor_window_errors{node="2"} 1',
      'streamd_monitor_window_rate_limited{node="2"} 0',
      'streamd_monitor_window_matched{node="1"} 9',
      'streamd_monitor_window_disagreements{node="1"} 1',
      'streamd_adapter_requests_total{node="0",method="getHeadVersion"} 1',
      'streamd_adapter_errors_total{node="2",method="getHeadVersion"} 1',
    ]) expect(lines).toContain(sample);
    expect(round).not.toContain('head_version{node="2"}');
    expect(round).not.toContain('lag_seconds{node="1"}');
    expect(round).not.toContain('window_matched{node="0"}');
    expect(lines.filter((l) => l === "# TYPE streamd_monitor_up gauge")).toHaveLength(1);
  });

  it("separates rounds with a blank line", async () => {
    const { out } = await text([node([1n]), node([1n])], 2);
    expect(out.join("").split("\n\n").filter((s) => s !== "")).toHaveLength(2);
  });
});

describe("serveMetrics", () => {
  it("serves the latest text at /metrics, 404 elsewhere, until closed", async () => {
    let latest = "";
    const server = await serveMetrics({ host: "127.0.0.1", port: 0 }, () => latest);
    const { port } = server.address() as { port: number };
    const url = `http://127.0.0.1:${port}`;
    try {
      const empty = await fetch(`${url}/metrics`);
      expect(empty.status).toBe(200);
      expect(empty.headers.get("content-type")).toBe("text/plain; version=0.0.4; charset=utf-8");
      expect(await empty.text()).toBe("");
      latest = 'streamd_monitor_up{node="0"} 1\n';
      expect(await (await fetch(`${url}/metrics?x=1`)).text()).toBe(latest);
      expect((await fetch(`${url}/`)).status).toBe(404);
    } finally {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    }
    expect(await fetch(`${url}/metrics`).then(() => "open", () => "closed")).toBe("closed");
  });

  it("rejects when the port is taken", async () => {
    const first = await serveMetrics({ host: "127.0.0.1", port: 0 }, () => "");
    const { port } = first.address() as { port: number };
    try {
      await expect(serveMetrics({ host: "127.0.0.1", port }, () => "")).rejects.toMatchObject({ code: "EADDRINUSE" });
    } finally {
      await new Promise((resolve) => first.close(resolve));
    }
  });
});

describe("runMonitor repairs", () => {
  it("sets a node aside when a compare read answers 429", async () => {
    const all = Array.from({ length: 30 }, (_, i) => ev(i + 1));
    let heads = 0;
    const throttled: EventAdapter = {
      fetchEvents: async () => Promise.reject(new StreamdNetworkError("slow down", 429, undefined, 1500)),
      getHeadVersion: async () => (heads++, 30n),
    };
    const { lines } = await run([memory(30n, all), throttled], { limit: 3 });
    expect(lines.map((l) => l.nodes[1].error)).toEqual([null, "429", null]);
    expect(heads).toBe(2);
    expect(lines[2]!.nodes[1].window).toMatchObject({ rateLimited: 2 });
  });

  it("removes its abort listener after every round", async () => {
    const controller = new AbortController();
    let listening = 0;
    const add = controller.signal.addEventListener.bind(controller.signal);
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.addEventListener = ((...args: Parameters<typeof add>) => (listening++, add(...args))) as typeof add;
    controller.signal.removeEventListener = ((...args: Parameters<typeof remove>) => (listening--, remove(...args))) as typeof remove;
    await run([node([1n]), node([1n])], { limit: 50 }, controller.signal);
    expect(listening).toBe(0);
  });

  it("renders lag in seconds, and publishes Prometheus text under --format json", async () => {
    let clock = 0;
    const published: string[] = [];
    await runMonitor({ ...OPTIONS, limit: 1 }, {
      nodes: [node([10n], [3_000_000n]), node([10n], [2_750_000n])],
      write: () => {},
      publish: (t) => void published.push(t),
      signal: new AbortController().signal,
      now: () => clock,
      sleep: async (ms) => void (clock += ms),
    });
    expect(published).toHaveLength(1);
    expect(published[0]!.split("\n")).toContain('streamd_monitor_lag_seconds{node="1"} 0.25');
  });
});
