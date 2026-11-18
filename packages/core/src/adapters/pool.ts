import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { hasArchiveHeights, isPrimaryOnly, markHasArchive, withPrimaryOnly } from "../time.js";
import { requireAdapter, requireBlockHeights } from "./validate.js";
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
  "getTransactionEventsByVersion",
  "simulateTransactionEvents",
  "view",
  "readObjects",
];

const limited = (err: unknown): err is StreamdNetworkError =>
  err instanceof StreamdNetworkError && err.statusCode === 429;

/**
 * Spreads reads across several fullnodes of one chain. A walk (a `fetchEvents`
 * without a cursor, then the cursors its pages return) stays on the node it
 * started on, since nodes lag one another and a cursor can outrun a slower
 * one; new walks and other calls rotate, while `getEarliestVersion` and
 * `getBlockHeights` query all open nodes and return the most conservative
 * bounds. A node that answers 429 is set aside
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
  declare getTransactionEventsByVersion?: EventAdapter["getTransactionEventsByVersion"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];
  declare readObjects?: EventAdapter["readObjects"];

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
    if (this.getEarliestVersion) {
      this.getEarliestVersion = async (descriptor) => {
        const versions = await this.allOpen((n) => n.getEarliestVersion!(descriptor));
        return versions.reduce((a, b) => (a > b ? a : b));
      };
    }
    if (this.getBlockHeights) {
      this.getBlockHeights = async (descriptor) => {
        const ranges = await this.allOpen((n) => n.getBlockHeights!(descriptor));
        const checked = ranges.map(requireBlockHeights);
        const oldest = checked.reduce((a, b) => (a > b.oldest ? a : b.oldest), checked[0]!.oldest);
        const minNewest = checked.reduce((a, b) => (a < b.newest ? a : b.newest), checked[0]!.newest);
        const combined = { oldest, newest: minNewest < oldest ? oldest : minNewest };
        return ranges.some(hasArchiveHeights) ? markHasArchive(combined) : combined;
      };
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

  private openOrder(first: number): number[] {
    const now = Date.now();
    const order = this.nodes.map((_, step) => (first + step) % this.nodes.length);
    const open = order.filter((node) => this.until[node]! <= now);
    return open.length > 0 ? open : [first];
  }

  private recordLimit(node: number, err: unknown): StreamdNetworkError {
    if (!limited(err)) throw err;
    this.until[node] = Date.now() + (err.retryAfter ?? this.cooldown);
    return err;
  }

  /**
   * Runs `run` on `first`, or on the next node not set aside while each
   * answers 429. With every node set aside, `first` is asked anyway.
   */
  private async call<T>(first: number, run: (node: EventAdapter) => Promise<T>): Promise<{ node: number; result: T }> {
    const primaryOnly = isPrimaryOnly();
    let last: StreamdNetworkError | undefined;
    for (const node of this.openOrder(first)) {
      try {
        return {
          node,
          result: await (primaryOnly ? withPrimaryOnly(() => run(this.nodes[node]!)) : run(this.nodes[node]!)),
        };
      } catch (err) {
        last = this.recordLimit(node, err);
      }
    }
    throw last;
  }

  private async allOpen<T>(run: (node: EventAdapter) => Promise<T>): Promise<T[]> {
    const primaryOnly = isPrimaryOnly();
    let last: StreamdNetworkError | undefined;
    const results: T[] = [];
    await Promise.all(
      this.openOrder(this.rotate()).map(async (node) => {
        try {
          results.push(await (primaryOnly ? withPrimaryOnly(() => run(this.nodes[node]!)) : run(this.nodes[node]!)));
        } catch (err) {
          last = this.recordLimit(node, err);
        }
      }),
    );
    if (results.length > 0) return results;
    throw last;
  }

  private remember(cursor: string, node: number): void {
    this.owners.set(cursor, node);
    if (this.owners.size > REMEMBERED_CURSORS) this.owners.delete(this.owners.keys().next().value!);
  }
}
