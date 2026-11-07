import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "./types.js";
import { fetchKey } from "./utils.js";

interface Entry {
  at: number;
  ttl: number;
  page: Promise<EventPage>;
  settled: boolean;
}

const MAX_DELAY = 2_147_483_647;

const PASS_THROUGH = [
  "getHeadVersion",
  "getEarliestVersion",
  "getBlock",
  "getBlockHeights",
  "view",
] as const;

/**
 * Serves identical `fetchEvents` calls from one adapter request: a call joins
 * one in flight with the same key, or reuses its page if that was requested
 * less than the caller's poll interval ago. A start at the head (no cursor, no
 * `fromVersion`) is never shared, since its answer depends on when it is asked.
 * Only pages without `hasMore` are kept after they settle: a backfill's pages
 * would otherwise pile up for a whole interval.
 */
export class SharedFetches {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly adapter: EventAdapter) {}

  /** The adapter as one subscription polling every `pollInterval` ms sees it. */
  view(pollInterval: number): EventAdapter {
    const view: EventAdapter = {
      fetchEvents: (descriptor, options) => this.fetch(descriptor, options, pollInterval),
    };
    for (const name of PASS_THROUGH) {
      const method = this.adapter[name];
      if (method !== undefined) Object.assign(view, { [name]: method.bind(this.adapter) });
    }
    return view;
  }

  private fetch(
    descriptor: EventDescriptor,
    options: FetchOptions,
    ttl: number,
  ): Promise<EventPage> {
    const request = () =>
      new Promise<EventPage>((r) => r(this.adapter.fetchEvents(descriptor, options)));
    const key = shareKey(descriptor, options, ttl);
    if (key === undefined) return request();

    // Monotonic, so a wall clock stepped backwards cannot keep a page alive.
    const now = performance.now();
    for (const [k, e] of this.entries) {
      if (e.settled && now - e.at >= e.ttl) this.entries.delete(k);
    }
    const hit = this.entries.get(key);
    if (hit !== undefined) return hit.page;

    const entry: Entry = { at: now, ttl, page: request(), settled: false };
    const forget = () => {
      if (this.entries.get(key) === entry) this.entries.delete(key);
    };
    this.entries.set(key, entry);
    entry.page.then(
      (page) => {
        entry.settled = true;
        if (page.hasMore) forget();
      },
      forget,
    );
    return entry.page;
  }

  /** Drop every kept page, so a destroyed client holds none. */
  clear(): void {
    this.entries.clear();
  }
}

function shareKey(
  descriptor: EventDescriptor,
  options: FetchOptions,
  ttl: number,
): string | undefined {
  // An interval setTimeout cannot honour would keep a page alive indefinitely.
  if (!(ttl > 0 && ttl <= MAX_DELAY)) return undefined;
  if (options.cursor === undefined && options.fromVersion === undefined) return undefined;
  const request = fetchKey(descriptor, options);
  return request === undefined ? undefined : `${ttl}|${request}`;
}
