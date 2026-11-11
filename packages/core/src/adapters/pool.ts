import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { requireAdapter } from "./validate.js";
import { descriptorKey } from "../utils.js";

const DEFAULT_COOLDOWN = 30_000;
const REMEMBERED_CURSORS = 1024;

export interface PoolAdapterOptions {
  /** How long a node that answered 429 without `retryAfter` is set aside, in ms (default 30000). */
  cooldown?: number;
}

type Optional = Exclude<keyof EventAdapter, "fetchEvents">;
const OPTIONAL: readonly Optional[] = [
  "getHeadVersion",
  "getEarliestVersion",
  "getBlock",
  "getBlockHeights",
  "getTransactionEvents",
  "simulateTransactionEvents",
  "view",
];

const limited = (err: unknown): err is StreamdNetworkError =>
  err instanceof StreamdNetworkError && err.statusCode === 429;

/**
 * Spreads reads across several fullnodes of one chain. A walk (a `fetchEvents`
 * without a cursor, then the cursors its pages return) stays on the node it
 * started on, since nodes lag one another and a cursor can outrun a slower
 * one; new walks and other calls rotate. A node that answers 429 is set aside
 * for its `retryAfter`, or `cooldown`, and the call moves to the next one;
 * with every node set aside the 429 is rethrown, so backoff applies. Any
 * other failure is rethrown as it is.
 */
export class PoolAdapter implements EventAdapter {
  private readonly nodes: readonly EventAdapter[];
  private readonly cooldown: number;
  private readonly until: number[];
  private readonly owners = new Map<string, number>();
  private next = 0;

  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];

  constructor(adapters: readonly EventAdapter[], options: PoolAdapterOptions = {}) {
    if (!Array.isArray(adapters) || adapters.length === 0) {
      throw new StreamdError("PoolAdapter requires at least one adapter");
    }
    const cooldown = options.cooldown ?? DEFAULT_COOLDOWN;
    if (!Number.isFinite(cooldown) || cooldown < 0) {
      throw new StreamdError(`Invalid cooldown: ${String(cooldown)} (expected a non-negative number of ms)`);
    }
    this.nodes = adapters.map((a, i) => requireAdapter(a, `PoolAdapter[${i}]`));
    this.cooldown = cooldown;
    this.until = this.nodes.map(() => 0);
    for (const name of OPTIONAL) {
      if (this.nodes.every((n) => n[name] !== undefined)) {
        // Each optional method takes its own arguments; they pass through untouched.
        (this as Record<string, unknown>)[name] = async (...args: unknown[]) =>
          (await this.call(this.rotate(), (n) => (n[name] as (...a: unknown[]) => Promise<unknown>)(...args))).result;
      }
    }
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    // Cursors are only unique per descriptor; two walks at one position may share one, and either node serves it.
    const key = (cursor: string) => `${descriptorKey(descriptor)} ${cursor}`;
    const owner = options.cursor === undefined ? undefined : this.owners.get(key(options.cursor));
    const { node, result } = await this.call(owner ?? this.rotate(), (n) => n.fetchEvents(descriptor, { ...options }));
    if (result.nextCursor !== undefined) this.remember(key(result.nextCursor), node);
    return result;
  }

  /** The next node in rotation, starting after the last one handed out. */
  private rotate(): number {
    const node = this.next;
    this.next = (this.next + 1) % this.nodes.length;
    return node;
  }

  /**
   * Runs `run` on `first`, or on the next node not set aside while each
   * answers 429. With every node set aside, `first` is asked anyway.
   */
  private async call<T>(first: number, run: (node: EventAdapter) => Promise<T>): Promise<{ node: number; result: T }> {
    const now = Date.now();
    const order = this.nodes.map((_, step) => (first + step) % this.nodes.length);
    const open = order.filter((node) => this.until[node]! <= now);
    let last: StreamdNetworkError | undefined;
    for (const node of open.length > 0 ? open : [first]) {
      try {
        return { node, result: await run(this.nodes[node]!) };
      } catch (err) {
        if (!limited(err)) throw err;
        last = err;
        this.until[node] = Date.now() + (err.retryAfter ?? this.cooldown);
      }
    }
    throw last;
  }

  private remember(cursor: string, node: number): void {
    this.owners.delete(cursor);
    this.owners.set(cursor, node);
    if (this.owners.size > REMEMBERED_CURSORS) this.owners.delete(this.owners.keys().next().value!);
  }
}
