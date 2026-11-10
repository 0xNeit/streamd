import type { EventAdapter } from "../types.js";
import { StreamdNetworkError } from "../errors.js";
import { requireAdapter } from "./validate.js";

const METHODS = [
  "fetchEvents",
  "getHeadVersion",
  "getEarliestVersion",
  "getBlock",
  "getBlockHeights",
  "getTransactionEvents",
  "simulateTransactionEvents",
  "view",
] as const satisfies readonly (keyof EventAdapter)[];

// Enough for stable percentiles, and it bounds a process that runs for weeks.
const SAMPLES = 1024;

export type AdapterMethod = (typeof METHODS)[number];

/** Milliseconds, nearest-rank over the most recent settled calls. */
export interface LatencyPercentiles {
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export interface MethodMetrics {
  /** Calls started, including those still in flight. */
  requests: number;
  /** Calls that failed, rate-limited ones included. */
  errors: number;
  /** Failures with status 429 or a `retryAfter` hint. */
  rateLimited: number;
  /** Absent until a call has settled. */
  latency?: LatencyPercentiles;
}

export interface AdapterMetrics {
  /** `adapter` with every call counted; pass this to `StreamdClient`. */
  adapter: EventAdapter;
  /** Numbers for each method called so far. */
  snapshot(): Partial<Record<AdapterMethod, MethodMetrics>>;
  /** Forgets everything counted so far. */
  reset(): void;
}

interface Counter {
  requests: number;
  errors: number;
  rateLimited: number;
  durations: number[];
  /** Where the next sample goes once `durations` is full. */
  next: number;
}

function record(counter: Counter, ms: number): void {
  if (counter.durations.length < SAMPLES) counter.durations.push(ms);
  else counter.durations[counter.next] = ms;
  counter.next = (counter.next + 1) % SAMPLES;
}

function percentile(sorted: readonly number[], p: number): number {
  return sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

function isRateLimited(err: unknown): boolean {
  return err instanceof StreamdNetworkError && (err.statusCode === 429 || err.retryAfter !== undefined);
}

/**
 * Wraps `adapter` to count every call per method, for dashboards and for
 * seeing what a rate limit is being spent on. Calls, results and errors pass
 * through unchanged, and a method the adapter lacks stays absent.
 */
export function adapterMetrics(adapter: EventAdapter): AdapterMetrics {
  const source = requireAdapter(adapter, "adapterMetrics");
  const counters = new Map<AdapterMethod, Counter>();

  const counterFor = (method: AdapterMethod): Counter => {
    let counter = counters.get(method);
    if (counter === undefined) {
      counter = { requests: 0, errors: 0, rateLimited: 0, durations: [], next: 0 };
      counters.set(method, counter);
    }
    return counter;
  };

  const wrapped = {} as EventAdapter;
  for (const method of METHODS) {
    if (typeof source[method] !== "function") continue;
    const call = (source[method] as (...args: unknown[]) => Promise<unknown>).bind(source);
    Object.assign(wrapped, {
      [method]: async (...args: unknown[]) => {
        // Taken per call, so a reset during a call cannot resurrect old counts.
        const counter = counterFor(method);
        counter.requests++;
        const started = performance.now();
        try {
          return await call(...args);
        } catch (err) {
          counter.errors++;
          if (isRateLimited(err)) counter.rateLimited++;
          throw err;
        } finally {
          record(counter, performance.now() - started);
        }
      },
    });
  }

  return {
    adapter: wrapped,
    snapshot() {
      const out: Partial<Record<AdapterMethod, MethodMetrics>> = {};
      for (const [method, { requests, errors, rateLimited, durations }] of counters) {
        const metrics: MethodMetrics = { requests, errors, rateLimited };
        if (durations.length > 0) {
          const sorted = [...durations].sort((a, b) => a - b);
          metrics.latency = {
            p50: percentile(sorted, 50),
            p95: percentile(sorted, 95),
            p99: percentile(sorted, 99),
            max: sorted[sorted.length - 1]!,
          };
        }
        out[method] = metrics;
      }
      return out;
    },
    reset: () => counters.clear(),
  };
}
