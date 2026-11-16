import { StreamdError } from "@streamd/core";

/**
 * Where a receiver records the events it has handled, by `eventId`
 * (`version:eventIndex`, prefixed `<namespace>/` when the receiver has one). Implement it over any database to keep the record
 * across restarts and across instances behind one webhook URL.
 */
export interface DedupStore {
  /** Whether each id was added before, in the order given. */
  seen(ids: readonly string[]): readonly boolean[] | PromiseLike<readonly boolean[]>;
  /** Records each id as handled. */
  add(ids: readonly string[]): void | PromiseLike<void>;
}

export interface MemoryDedupStoreOptions {
  /** How many ids to remember before forgetting the oldest. Default 100,000. */
  maxEntries?: number;
}

const DEFAULT_MAX_ENTRIES = 100_000;

/**
 * The default `DedupStore`: ids in process memory, the oldest forgotten past
 * `maxEntries`, and nothing kept across a restart.
 */
export class MemoryDedupStore implements DedupStore {
  private readonly ids = new Set<string>();
  // Insertion order as a ring, since walking a Set past its deleted entries slows every add once full.
  private readonly order: string[] = [];
  private next = 0;
  private readonly maxEntries: number;

  constructor(options: MemoryDedupStoreOptions = {}) {
    const max = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new StreamdError(`Invalid maxEntries: ${String(max)} (expected a positive integer)`);
    }
    this.maxEntries = max;
  }

  seen(ids: readonly string[]): boolean[] {
    return ids.map((id) => this.ids.has(id));
  }

  /** Adding an id already held changes nothing, so it is forgotten at its first add's turn. */
  add(ids: readonly string[]): void {
    for (const id of ids) {
      if (this.ids.has(id)) continue;
      if (this.order.length === this.maxEntries) this.ids.delete(this.order[this.next]!);
      this.order[this.next] = id;
      this.next = (this.next + 1) % this.maxEntries;
      this.ids.add(id);
    }
  }
}
