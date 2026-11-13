import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
} from "../types.js";
import { StreamdError } from "../errors.js";
import { fetchKey } from "../utils.js";
import { pageJson, parsePage } from "./recording.js";
import { requireAdapter } from "./validate.js";

const DEFAULT_MAX_ENTRIES = 1000;

/**
 * Somewhere `CachingAdapter` can keep pages past a restart. Values are opaque
 * JSON text. Keys name the request and not the network, so keep one store per
 * network.
 */
export interface PageStore {
  get(key: string): string | undefined | null | Promise<string | undefined | null>;
  set(key: string, page: string): void | Promise<void>;
}

export interface CachingAdapterOptions {
  /** Pages kept in memory before the least recently used is dropped (default: 1000). */
  maxEntries?: number;
  /** Also keep final pages here, read when memory misses. */
  store?: PageStore;
  /**
   * Called when `store` fails or holds an unreadable entry. A broken store
   * never fails a read: the page comes from the source instead.
   */
  onStoreError?: (error: unknown) => void;
}

function copy(page: EventPage): EventPage {
  return { ...page, events: [...page.events] };
}

/**
 * Wraps `adapter` and reuses the pages it marks `final`, so re-reading settled
 * history skips the source. Every other page, and every failure, passes
 * through unstored. A cached page keeps being served after the source prunes
 * that history. `getHeadVersion`, `getEarliestVersion`, `getBlock`,
 * `getBlockHeights`, `getTransactionEvents`, `simulateTransactionEvents`,
 * `view` and `readObjects` pass through uncached, only when the wrapped adapter has them.
 * (A view at a past version never changes, so it could be cached; it is not.)
 *
 * With a `store`, final pages also survive a restart. A `bigint` inside an
 * event's `data` comes back from the store as a decimal string, as
 * `serializeEvent` writes it.
 */
export class CachingAdapter implements EventAdapter {
  private readonly source: EventAdapter;
  private readonly maxEntries: number;
  private readonly store?: PageStore;
  private readonly onStoreError?: (error: unknown) => void;
  // Map order is recency order: a hit is moved to the end, the front is evicted.
  private readonly pages = new Map<string, EventPage>();

  getHeadVersion?: EventAdapter["getHeadVersion"];
  getEarliestVersion?: EventAdapter["getEarliestVersion"];
  getBlock?: EventAdapter["getBlock"];
  getBlockHeights?: EventAdapter["getBlockHeights"];
  getTransactionEvents?: EventAdapter["getTransactionEvents"];
  simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  view?: EventAdapter["view"];
  readObjects?: EventAdapter["readObjects"];

  constructor(adapter: EventAdapter, options: CachingAdapterOptions = {}) {
    this.source = requireAdapter(adapter, "CachingAdapter");
    const max = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new StreamdError(
        `Invalid maxEntries: ${String(max)} (expected a positive integer)`,
      );
    }
    this.maxEntries = max;
    if (options.store !== undefined && (typeof options.store.get !== "function" || typeof options.store.set !== "function")) {
      throw new StreamdError("store must have get() and set()");
    }
    this.store = options.store;
    this.onStoreError = options.onStoreError;

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
    if (source.readObjects !== undefined) {
      this.readObjects = (request, at, descriptor) => source.readObjects!(request, at, descriptor);
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

    const stored = await this.readStored(key);
    if (stored !== undefined) {
      this.remember(key, stored);
      return copy(stored);
    }

    const page = await this.source.fetchEvents(descriptor, options);
    if (page.final !== true) return page;
    this.remember(key, page);
    await this.writeStored(key, page);
    return page;
  }

  private remember(key: string, page: EventPage): void {
    this.pages.set(key, copy(page));
    if (this.pages.size > this.maxEntries) {
      this.pages.delete(this.pages.keys().next().value!);
    }
  }

  private async readStored(key: string): Promise<EventPage | undefined> {
    if (this.store === undefined) return undefined;
    try {
      const text = await this.store.get(key);
      if (text === undefined || text === null) return undefined;
      const page = parsePage(JSON.parse(text));
      if (page.final !== true) throw new StreamdError("A stored page is not final");
      return page;
    } catch (err) {
      this.onStoreError?.(err);
      return undefined;
    }
  }

  private async writeStored(key: string, page: EventPage): Promise<void> {
    if (this.store === undefined) return;
    try {
      await this.store.set(key, JSON.stringify(pageJson(page)));
    } catch (err) {
      this.onStoreError?.(err);
    }
  }
}
