import { StreamdError } from "./errors.js";
import type { StreamdEvent } from "./types.js";

const DEFAULT_DEDUP_WINDOW = 1000;

function validateDedupWindow(dedupWindow: number): number {
  if (!Number.isSafeInteger(dedupWindow) || dedupWindow < 0) {
    throw new StreamdError(
      `Invalid dedupWindow: ${String(dedupWindow)} (expected a non-negative integer)`,
    );
  }
  return dedupWindow;
}

/**
 * The `version:eventIndex` key that identifies an event, the same one the client
 * deduplicates on. `sequenceNumber` is not part of it: module events commonly
 * report 0, so several events in one transaction would share it. `handle` events
 * from `FullnodeScanAdapter` carry the handle's sequence number as `eventIndex`,
 * because that endpoint omits the event's position in its transaction.
 */
export function eventId(event: Pick<StreamdEvent, "version" | "eventIndex">): string {
  return `${event.version}:${event.eventIndex}`;
}

/**
 * Tracks polling position and deduplicates events using a sliding window.
 *
 * `token` is the adapter's opaque resume position. Dedup is a safety net on
 * top of it, for adapters whose pages can overlap at the boundary.
 */
export class CursorManager {
  /** Opaque adapter-defined resume position; undefined before the first page. */
  token: string | undefined;

  private readonly dedupWindow: number;
  private readonly seen = new Set<string>();

  constructor(token?: string, dedupWindow: number = DEFAULT_DEDUP_WINDOW) {
    this.token = token;
    this.dedupWindow = validateDedupWindow(dedupWindow);
  }

  hasSeen(event: StreamdEvent): boolean {
    if (this.dedupWindow === 0) return false;
    return this.seen.has(eventId(event));
  }

  mark(event: StreamdEvent): void {
    if (this.dedupWindow === 0) return;
    const k = eventId(event);
    if (this.seen.has(k)) return;

    this.seen.add(k);

    // Evict oldest entry when the window exceeds the limit (Set preserves insertion order).
    if (this.seen.size > this.dedupWindow) {
      const oldest = this.seen.values().next().value!;
      this.seen.delete(oldest);
    }
  }
}
