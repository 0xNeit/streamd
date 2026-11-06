import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
} from "../types.js";
import { StreamdError } from "../errors.js";
import { fetchKey } from "../utils.js";
import { requireAdapter } from "./validate.js";

const DEFAULT_MAX_ENTRIES = 1000;

export interface CachingAdapterOptions {
  /** Pages kept before the least recently used is dropped (default: 1000). */
  maxEntries?: number;
}

function copy(page: EventPage): EventPage {
  return { ...page, events: [...page.events] };
}

/**
 * Wraps `adapter` and reuses the pages it marks `final`, so re-reading settled
 * history skips the source. Every other page, and every failure, passes
 * through unstored. A cached page keeps being served after the source prunes
 * that history. `getHeadVersion`, `getEarliestVersion`, `getBlock`,
 * `getBlockHeights`, `getTransactionEvents`, `simulateTransactionEvents` and
 * `view` pass through uncached, only when the wrapped adapter has them.
 * (A view at a past version never changes, so it could be cached; it is not.)
 */
export class CachingAdapter implements EventAdapter {
  private readonly source: EventAdapter;
  private readonly maxEntries: number;
  // Map order is recency order: a hit is moved to the end, the front is evicted.
  private readonly pages = new Map<string, EventPage>();

  getHeadVersion?: EventAdapter["getHeadVersion"];
  getEarliestVersion?: EventAdapter["getEarliestVersion"];
  getBlock?: EventAdapter["getBlock"];
  getBlockHeights?: EventAdapter["getBlockHeights"];
  getTransactionEvents?: EventAdapter["getTransactionEvents"];
  simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  view?: EventAdapter["view"];

  constructor(adapter: EventAdapter, options: CachingAdapterOptions = {}) {
    this.source = requireAdapter(adapter, "CachingAdapter");
    const max = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new StreamdError(
        `Invalid maxEntries: ${String(max)} (expected a positive integer)`,
      );
    }
    this.maxEntries = max;

    const source = this.source;
    if (source.getHeadVersion !== undefined) {
      this.getHeadVersion = (descriptor) => source.getHeadVersion!(descriptor);
    }
    if (source.getEarliestVersion !== undefined) {
      this.getEarliestVersion = (descriptor) => source.getEarliestVersion!(descriptor);
    }
    if (source.getBlock !== undefined) {
      this.getBlock = (descriptor, height) => source.getBlock!(descriptor, height);
    }
    if (source.getBlockHeights !== undefined) {
      this.getBlockHeights = (descriptor) => source.getBlockHeights!(descriptor);
    }
    if (source.getTransactionEvents !== undefined) {
      this.getTransactionEvents = (hash, descriptor) =>
        source.getTransactionEvents!(hash, descriptor);
    }
    if (source.simulateTransactionEvents !== undefined) {
      this.simulateTransactionEvents = (simulation, descriptor) =>
        source.simulateTransactionEvents!(simulation, descriptor);
    }
    if (source.view !== undefined) {
      this.view = (request, ledgerVersion, descriptor) =>
        source.view!(request, ledgerVersion, descriptor);
    }
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    const key = fetchKey(descriptor, options);
    if (key === undefined) return this.source.fetchEvents(descriptor, options);

    const cached = this.pages.get(key);
    if (cached !== undefined) {
      this.pages.delete(key);
      this.pages.set(key, cached);
      return copy(cached);
    }

    const page = await this.source.fetchEvents(descriptor, options);
    if (page.final !== true) return page;
    this.pages.set(key, copy(page));
    if (this.pages.size > this.maxEntries) {
      this.pages.delete(this.pages.keys().next().value!);
    }
    return page;
  }
}
