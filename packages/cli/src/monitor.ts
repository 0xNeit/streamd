import { createServer } from "node:http";
import type { Server } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { StreamdNetworkError, adapterMetrics, compareAdapters, toPrometheus } from "@streamd/core";
import type { AdapterComparison, AdapterMetrics, EventAdapter } from "@streamd/core";
import type { MonitorOptions } from "./args.js";

export interface MonitorIO {
  /** One per `--node-url`, in order; node 0 is the reference. Each needs `getHeadVersion`. */
  nodes: EventAdapter[];
  write(text: string): Promise<void> | void;
  signal: AbortSignal;
  /** Wall-clock milliseconds; `Date.now` unless a test drives the clock. */
  now?(): number;
  /** Waits `ms`, or less when `signal` aborts; real timers unless a test replaces them. */
  sleep?(ms: number, signal: AbortSignal): Promise<void>;
  /** Receives each round's Prometheus text, whatever `format` prints. */
  publish?(text: string): void;
}

/** The most rounds the window keeps, however long it is, so a long window cannot grow without bound. */
export const MAX_WINDOW_ROUNDS = 10_000;

// One page covers the default range of a type emitted once per transaction.
const COMPARE_PAGE = 100;
const COMPARE_PAGES = 10;

const CALLS = ["getHeadVersion", "getBlockHeights", "getBlock", "fetchEvents"] as const;

class Timeout extends StreamdNetworkError {}

/**
 * `adapter` with each call the monitor makes rejected after `ms`, so one
 * silent node cannot stall a round, and every failure, compare reads
 * included, passed to `failed` before it is rethrown.
 */
function timed(adapter: EventAdapter, ms: number, failed: (err: unknown) => void): EventAdapter {
  const out = {} as EventAdapter;
  for (const method of CALLS) {
    const call = adapter[method] as ((...args: unknown[]) => Promise<unknown>) | undefined;
    if (typeof call !== "function") continue;
    Object.assign(out, {
      [method]: async (...args: unknown[]) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Timeout(`${method} timed out after ${ms}ms`)), ms);
        });
        try {
          return await Promise.race([call.apply(adapter, args), timeout]);
        } catch (err) {
          failed(err);
          throw err;
        } finally {
          clearTimeout(timer);
        }
      },
    });
  }
  return out;
}

interface Node {
  metrics: AdapterMetrics;
  /** Wall-clock time before which the node is not read, after a 429. */
  until: number;
  /** Lifetime totals at the end of the previous round. */
  totals: Totals;
}

interface Totals {
  requests: number;
  errors: number;
  rateLimited: number;
}

/** What one round learned from one node. */
interface Reading {
  error: string | null;
  head?: bigint;
  /** The head block's time, in microseconds. */
  timestamp?: bigint;
  /** This round's comparison with node 0, over `fromVersion` to `toVersion`. */
  compare?: { fromVersion: bigint; toVersion: bigint; report: AdapterComparison };
}

/** One node's share of one round, kept for the window. */
interface Row extends Totals {
  up: boolean;
  lagVersions?: bigint;
  lagMs?: number;
  matched: number;
  disagreements: number;
}

interface Round {
  at: number;
  rows: Row[];
}

/** The code a record carries for a failure: never the message, which can hold the node's URL. */
function code(err: unknown): string {
  if (err instanceof Timeout) return "timeout";
  if (err instanceof StreamdNetworkError && err.statusCode !== undefined) return String(err.statusCode);
  return "failed";
}

function totals(metrics: AdapterMetrics): Totals {
  const sum: Totals = { requests: 0, errors: 0, rateLimited: 0 };
  for (const m of Object.values(metrics.snapshot())) {
    sum.requests += m.requests;
    sum.errors += m.errors;
    sum.rateLimited += m.rateLimited;
  }
  return sum;
}

const disagreements = (report: AdapterComparison) => report.missing.length + report.extra.length + report.different.length;

function comparison(compare: Reading["compare"]) {
  if (compare === undefined) return null;
  const { fromVersion, toVersion, report } = compare;
  return {
    fromVersion: fromVersion.toString(),
    toVersion: toVersion.toString(),
    window: report.window === undefined
      ? null
      : { fromVersion: report.window.fromVersion.toString(), toVersion: report.window.toVersion.toString() },
    matched: report.matched,
    missing: report.missing.length,
    extra: report.extra.length,
    different: report.different.length,
  };
}

class Monitor {
  private readonly nodes: Node[];
  private readonly now: () => number;
  private history: Round[] = [];

  constructor(
    private readonly options: MonitorOptions,
    adapters: EventAdapter[],
    now: () => number,
  ) {
    this.now = now;
    this.nodes = adapters.map((adapter) => {
      const node = { until: -Infinity, totals: { requests: 0, errors: 0, rateLimited: 0 } } as Node;
      node.metrics = adapterMetrics(timed(adapter, options.interval, (err) => this.failed(node, err)));
      return node;
    });
  }

  /** A 429 sets the node aside for its `retryAfter`, or until after the next round. */
  private failed(node: Node, err: unknown): void {
    if (err instanceof StreamdNetworkError && err.statusCode === 429) {
      node.until = this.now() + (err.retryAfter ?? this.options.interval);
    }
  }

  private async headTime(node: Node): Promise<bigint | undefined> {
    const { adapter } = node.metrics;
    if (adapter.getBlockHeights === undefined || adapter.getBlock === undefined) return undefined;
    try {
      const { newest } = await adapter.getBlockHeights(this.options.descriptor);
      return (await adapter.getBlock(this.options.descriptor, newest)).timestamp;
    } catch {
      return undefined;
    }
  }

  private async read(node: Node, at: number): Promise<Reading> {
    if (at <= node.until) return { error: "429" };
    try {
      const head = await node.metrics.adapter.getHeadVersion!(this.options.descriptor);
      const timestamp = await this.headTime(node);
      return timestamp === undefined ? { error: null, head } : { error: null, head, timestamp };
    } catch (err) {
      return { error: code(err) };
    }
  }

  /**
   * Each up node against node 0, over the newest `range` versions both have
   * committed, so a slow node narrows only its own comparison.
   */
  private async compare(readings: Reading[]): Promise<void> {
    const reference = readings[0]!.head;
    if (reference === undefined) return;
    const { descriptor, range } = this.options;
    await Promise.all(readings.map(async (reading, i) => {
      if (i === 0 || reading.head === undefined) return;
      const toVersion = reading.head < reference ? reading.head : reference;
      const start = toVersion - BigInt(range) + 1n;
      const fromVersion = start > 0n ? start : 0n;
      const report = await compareAdapters(this.nodes[0]!.metrics.adapter, this.nodes[i]!.metrics.adapter, descriptor, {
        fromVersion,
        toVersion,
        limit: COMPARE_PAGE,
        maxPages: COMPARE_PAGES,
        // Only the counts are reported, so none is cut.
        maxReported: Number.MAX_SAFE_INTEGER,
      });
      reading.compare = { fromVersion, toVersion, report };
    }));
  }

  /** Reads every node once and returns the round as a JSON line and as Prometheus text. */
  async round(index: number): Promise<{ json: string; prometheus: string }> {
    const at = this.now();
    const readings = await Promise.all(this.nodes.map((node) => this.read(node, at)));
    await this.compare(readings);
    const heads = readings.flatMap((r) => (r.head === undefined ? [] : [r.head]));
    const times = readings.flatMap((r) => (r.timestamp === undefined ? [] : [r.timestamp]));
    const top = heads.reduce((a, b) => (b > a ? b : a), -1n);
    const newest = times.reduce((a, b) => (b > a ? b : a), -1n);
    const rows = this.nodes.map((node, i): Row => {
      const now = totals(node.metrics);
      const r = readings[i]!;
      const row: Row = {
        up: r.head !== undefined,
        requests: now.requests - node.totals.requests,
        errors: now.errors - node.totals.errors,
        rateLimited: now.rateLimited - node.totals.rateLimited,
        matched: r.compare?.report.matched ?? 0,
        disagreements: r.compare === undefined ? 0 : disagreements(r.compare.report),
      };
      node.totals = now;
      if (r.head !== undefined) row.lagVersions = top - r.head;
      if (r.timestamp !== undefined) row.lagMs = Number((newest - r.timestamp) / 1000n);
      return row;
    });
    this.history.push({ at, rows });
    this.history = this.history.filter((h) => at - h.at < this.options.window).slice(-MAX_WINDOW_ROUNDS);
    return { json: this.json(index, at, readings, rows), prometheus: this.prometheus(readings, rows) };
  }

  private prometheus(readings: Reading[], rows: Row[]): string {
    const windows = rows.map((_, i) => this.window(i));
    const values = (pick: (i: number) => bigint | number | undefined, from = 0) =>
      rows.flatMap((_, i) => {
        const value = i < from ? undefined : pick(i);
        return value === undefined ? [] : [`{node="${i}"} ${value}`];
      });
    const families = [
      gauge("up", "1 when the node answered its head read this round, else 0.", values((i) => (rows[i]!.up ? 1 : 0))),
      gauge("head_version", "The node's ledger version this round.", values((i) => readings[i]!.head)),
      gauge("lag_versions", "Versions behind the highest head read this round.", values((i) => rows[i]!.lagVersions)),
      gauge("lag_seconds", "Chain time behind the newest head block read this round.", values((i) => (rows[i]!.lagMs === undefined ? undefined : rows[i]!.lagMs! / 1000))),
      gauge("window_requests", "Calls made to the node over the window.", values((i) => windows[i]!.requests)),
      gauge("window_errors", "Failed calls to the node over the window.", values((i) => windows[i]!.errors)),
      gauge("window_rate_limited", "Calls the node refused with 429 over the window.", values((i) => windows[i]!.rateLimited)),
      gauge("window_matched", "Events that matched node 0's over the window.", values((i) => windows[i]!.matched, 1)),
      gauge("window_disagreements", "Events missing, extra or different from node 0's over the window.", values((i) => windows[i]!.disagreements, 1)),
    ];
    const adapters = toPrometheus(this.nodes.map((node, i) => ({ snapshot: node.metrics.snapshot(), labels: { node: String(i) } })));
    return families.join("") + adapters;
  }

  private json(index: number, at: number, readings: Reading[], rows: Row[]): string {
    const nodes = rows.map((row, i) => ({
      node: i,
      up: row.up,
      error: readings[i]!.error,
      head: readings[i]!.head?.toString() ?? null,
      lagVersions: row.lagVersions?.toString() ?? null,
      lagMs: row.lagMs ?? null,
      compare: comparison(readings[i]!.compare),
      window: this.window(i),
    }));
    return `${JSON.stringify({ round: index, time: new Date(at).toISOString(), nodes })}\n`;
  }

  /** Node `i`'s sums over the rounds in the window. */
  private window(i: number) {
    const rows = this.history.map((h) => h.rows[i]!);
    const sum = (field: keyof Totals | "matched" | "disagreements") => rows.reduce((total, row) => total + row[field], 0);
    const requests = sum("requests");
    const errors = sum("errors");
    const lags = rows.flatMap((row) => (row.lagVersions === undefined ? [] : [row.lagVersions]));
    const ms = rows.flatMap((row) => (row.lagMs === undefined ? [] : [row.lagMs]));
    return {
      rounds: rows.length,
      up: rows.filter((row) => row.up).length,
      requests,
      errors,
      rateLimited: sum("rateLimited"),
      errorRate: requests === 0 ? 0 : Math.round((errors / requests) * 10_000) / 10_000,
      maxLagVersions: lags.length === 0 ? null : lags.reduce((a, b) => (b > a ? b : a)).toString(),
      maxLagMs: ms.length === 0 ? null : Math.max(...ms),
      ...(i > 0 && { matched: sum("matched"), disagreements: sum("disagreements") }),
    };
  }
}

function gauge(name: string, help: string, samples: string[]): string {
  const metric = `streamd_monitor_${name}`;
  if (samples.length === 0) return "";
  return `# HELP ${metric} ${help}\n# TYPE ${metric} gauge\n${samples.map((s) => `${metric}${s}\n`).join("")}`;
}

/**
 * Serves the text `latest` returns at `GET /metrics`, for a Prometheus scrape;
 * every other path is 404. Resolves once listening, rejects if it cannot bind.
 */
export function serveMetrics(listen: { host: string; port: number }, latest: () => string): Promise<Server> {
  const server = createServer((request, response) => {
    if (new URL(request.url ?? "/", "http://localhost").pathname !== "/metrics") {
      response.writeHead(404).end();
      return;
    }
    response.writeHead(200, { "content-type": "text/plain; version=0.0.4; charset=utf-8" }).end(latest());
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(listen.port, listen.host, () => {
      server.off("error", reject);
      resolve(server);
    });
  });
}

/** `round`, or undefined as soon as `signal` aborts; the listener goes when the round settles. */
function untilAborted<T>(round: Promise<T>, signal: AbortSignal): Promise<T | undefined> {
  return new Promise((resolve, reject) => {
    const stop = () => resolve(undefined);
    signal.addEventListener("abort", stop, { once: true });
    round.then(resolve, reject).finally(() => signal.removeEventListener("abort", stop));
  });
}

const sleep = (ms: number, signal: AbortSignal) => delay(ms, undefined, { signal }).catch(() => {});

/**
 * Reads every node each `interval`, printing one record per round, until
 * `limit` rounds or the signal. Resolves to 0: a node that is down is what
 * the monitor reports, not a reason to stop.
 */
export async function runMonitor(options: MonitorOptions, io: MonitorIO): Promise<number> {
  const now = io.now ?? Date.now;
  const wait = io.sleep ?? sleep;
  const monitor = new Monitor(options, io.nodes, now);
  for (let index = 1; !io.signal.aborted; index++) {
    const started = now();
    const round = await untilAborted(monitor.round(index), io.signal);
    if (round === undefined || io.signal.aborted) break;
    io.publish?.(round.prometheus);
    try {
      await io.write(options.format === "json" ? round.json : `${round.prometheus}\n`);
    } catch (err) {
      if (io.signal.aborted) break;
      throw err;
    }
    if (options.limit !== undefined && index >= options.limit) break;
    await wait(Math.max(0, started + options.interval - now()), io.signal);
  }
  return 0;
}
