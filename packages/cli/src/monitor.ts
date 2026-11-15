import { setTimeout as delay } from "node:timers/promises";
import { StreamdNetworkError, adapterMetrics } from "@streamd/core";
import type { AdapterMetrics, EventAdapter } from "@streamd/core";
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
}

/** The most rounds the window keeps, however long it is, so a long window cannot grow without bound. */
export const MAX_WINDOW_ROUNDS = 10_000;

const CALLS = ["getHeadVersion", "getBlockHeights", "getBlock", "fetchEvents"] as const;

class Timeout extends StreamdNetworkError {}

/** `adapter` with each call the monitor makes rejected after `ms`, so one silent node cannot stall a round. */
function timed(adapter: EventAdapter, ms: number): EventAdapter {
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
}

/** One node's share of one round, kept for the window. */
interface Row extends Totals {
  up: boolean;
  lagVersions?: bigint;
  lagMs?: number;
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
    this.nodes = adapters.map((adapter) => ({
      metrics: adapterMetrics(timed(adapter, options.interval)),
      until: -Infinity,
      totals: { requests: 0, errors: 0, rateLimited: 0 },
    }));
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
    } catch (err) {
      this.failed(node, err);
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
      this.failed(node, err);
      return { error: code(err) };
    }
  }

  /** Reads every node once and returns the round's record. */
  async round(index: number): Promise<string> {
    const at = this.now();
    const readings = await Promise.all(this.nodes.map((node) => this.read(node, at)));
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
      };
      node.totals = now;
      if (r.head !== undefined) row.lagVersions = top - r.head;
      if (r.timestamp !== undefined) row.lagMs = Number((newest - r.timestamp) / 1000n);
      return row;
    });
    this.history.push({ at, rows });
    this.history = this.history.filter((h) => at - h.at < this.options.window).slice(-MAX_WINDOW_ROUNDS);
    return this.json(index, at, readings, rows);
  }

  private json(index: number, at: number, readings: Reading[], rows: Row[]): string {
    const nodes = rows.map((row, i) => ({
      node: i,
      up: row.up,
      error: readings[i]!.error,
      head: readings[i]!.head?.toString() ?? null,
      lagVersions: row.lagVersions?.toString() ?? null,
      lagMs: row.lagMs ?? null,
      window: this.window(i),
    }));
    return `${JSON.stringify({ round: index, time: new Date(at).toISOString(), nodes })}\n`;
  }

  /** Node `i`'s sums over the rounds in the window. */
  private window(i: number) {
    const rows = this.history.map((h) => h.rows[i]!);
    const sum = (field: keyof Totals) => rows.reduce((total, row) => total + row[field], 0);
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
    };
  }
}

function aborted(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve();
    else signal.addEventListener("abort", () => resolve(), { once: true });
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
  const stopped = aborted(io.signal);
  for (let index = 1; !io.signal.aborted; index++) {
    const started = now();
    const record = await Promise.race([monitor.round(index), stopped]);
    if (record === undefined || io.signal.aborted) break;
    try {
      await io.write(record);
    } catch (err) {
      if (io.signal.aborted) break;
      throw err;
    }
    if (options.limit !== undefined && index >= options.limit) break;
    await wait(Math.max(0, started + options.interval - now()), io.signal);
  }
  return 0;
}
