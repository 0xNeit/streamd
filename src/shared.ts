import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "./types.js";
import { descriptorKey } from "./utils.js";

interface Entry {
  at: number;
  ttl: number;
  page: Promise<EventPage>;
}

const PASS_THROUGH = [
  "getHeadVersion",
  "getEarliestVersion",
  "getBlock",
  "getBlockHeights",
] as const;

/**
 * Serves identical `fetchEvents` calls from one adapter request: a call joins
 * one in flight with the same key, or reuses its page if that was requested
 * less than the caller's poll interval ago. A start at the head (no cursor, no
 * `fromVersion`) is never shared, since its answer depends on when it is asked.
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

    const now = Date.now();
    for (const [k, e] of this.entries) if (now - e.at >= e.ttl) this.entries.delete(k);
    const hit = this.entries.get(key);
    if (hit !== undefined) return hit.page;

    const entry: Entry = { at: now, ttl, page: request() };
    this.entries.set(key, entry);
    entry.page.catch(() => {
      if (this.entries.get(key) === entry) this.entries.delete(key);
    });
    return entry.page;
  }
}

function shareKey(
  descriptor: EventDescriptor,
  options: FetchOptions,
  ttl: number,
): string | undefined {
  const descriptorPart = descriptorKey(descriptor);
  if (descriptorPart === undefined) return undefined;
  if (options.cursor === undefined && options.fromVersion === undefined) return undefined;
  // Every option is part of the request, so every option is part of the key.
  const fields = Object.entries(options)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => [k, typeof v === "bigint" ? `${v}n` : v]);
  return JSON.stringify([descriptorPart, ttl, fields]);
}
