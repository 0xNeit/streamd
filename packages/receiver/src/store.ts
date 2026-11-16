import { StreamdError } from "@streamd/core";

/**
 * Where a receiver records the events it has handled, by `eventId`
 * (`version:eventIndex`). Implement it over any database to keep the record
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

  add(ids: readonly string[]): void {
    for (const id of ids) {
      this.ids.delete(id);
      this.ids.add(id);
    }
    // A Set iterates in insertion order, so the first entries are the oldest.
    for (const id of this.ids) {
      if (this.ids.size <= this.maxEntries) break;
      this.ids.delete(id);
    }
  }
}
