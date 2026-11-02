import type { StreamdEvent } from "./types.js";

const MAX_SEEN_SIZE = 1000;

/**
 * Tracks polling position and deduplicates events using a sliding window.
 *
 * `token` is the adapter's opaque resume position. Dedup is a safety net on
 * top of it, for adapters whose pages can overlap at the boundary.
 */
export class CursorManager {
  /** Opaque adapter-defined resume position; undefined before the first page. */
  token: string | undefined;

  private seen = new Set<string>();
  private seenOrder: string[] = [];

  constructor(token?: string) {
    this.token = token;
  }

  /**
   * Identity is (version, eventIndex). `sequenceNumber` is not usable here:
   * module events commonly report 0, so several events in one transaction
   * would collide and all but the first would be dropped as duplicates.
   */
  private key(event: StreamdEvent): string {
    return `${event.version}:${event.eventIndex}`;
  }

  hasSeen(event: StreamdEvent): boolean {
    return this.seen.has(this.key(event));
  }

  mark(event: StreamdEvent): void {
    const k = this.key(event);
    if (this.seen.has(k)) return;

    this.seen.add(k);
    this.seenOrder.push(k);

    // Evict oldest entries when the window exceeds the limit
    while (this.seenOrder.length > MAX_SEEN_SIZE) {
      const oldest = this.seenOrder.shift()!;
      this.seen.delete(oldest);
    }
  }
}
