import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "../types.js";
import { StreamdError } from "../errors.js";
import {
  DEFAULT_LIMIT,
  moduleTypeFilter,
  parseCursor,
  requireAddress,
  validateOffset,
} from "./validate.js";

function compareEvents(a: StreamdEvent, b: StreamdEvent): number {
  if (a.version < b.version) return -1;
  if (a.version > b.version) return 1;
  return a.eventIndex < b.eventIndex ? -1 : a.eventIndex > b.eventIndex ? 1 : 0;
}

function descriptorFilter(
  descriptor: EventDescriptor,
): (type: string) => boolean {
  switch (descriptor.kind) {
    case "module":
      return moduleTypeFilter(descriptor.eventType);
    case "account": {
      requireAddress(descriptor.accountAddress);
      if (descriptor.eventTypes === undefined) return () => true;
      const types = new Set(descriptor.eventTypes);
      return (type) => types.has(type);
    }
    case "handle":
      requireAddress(descriptor.accountAddress);
      return () => true;
    default: {
      const unknown: never = descriptor;
      throw new StreamdError(
        `Unknown descriptor kind: ${String((unknown as { kind?: unknown })?.kind)}`,
      );
    }
  }
}

function validateLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new StreamdError(`Not a valid limit: ${limit}`);
  }
  return limit;
}

/**
 * In-memory `EventAdapter` seeded with a static list of events, for testing
 * `on()`, `once()`, `stream()`, `getEvents()`, and `getEventPages()` handlers
 * and predicates offline without a fullnode.
 */
export class MemoryAdapter implements EventAdapter {
  private readonly events: readonly StreamdEvent[];

  constructor(events: readonly StreamdEvent[] = []) {
    this.events = [...events].sort(compareEvents);
  }

  /** The highest seeded version, or `0n` when empty. */
  async getHeadVersion(): Promise<bigint> {
    return this.events.at(-1)?.version ?? 0n;
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    const keep = descriptorFilter(descriptor);
    const cursorIndex =
      options.cursor !== undefined ? parseCursor(options.cursor) : undefined;
    validateOffset(options.offset);
    const wanted = validateLimit(options.limit);

    const startBig = cursorIndex ?? BigInt(options.offset ?? 0);
    if (wanted === 0 || startBig >= BigInt(this.events.length)) {
      return {
        events: [],
        nextCursor: startBig.toString(),
        hasMore: false,
      };
    }

    // Cursor takes precedence over fromVersion when both are present.
    const minVersion =
      cursorIndex === undefined ? options.fromVersion : undefined;
    const maxVersion = options.toVersion;
    const events: StreamdEvent[] = [];
    let i = Number(startBig);

    while (i < this.events.length && events.length < wanted) {
      const event = this.events[i]!;
      if (maxVersion !== undefined && event.version > maxVersion) break;
      i++;
      if (minVersion !== undefined && event.version < minVersion) continue;
      if (keep(event.type)) events.push(event);
    }

    const hasMore =
      i < this.events.length &&
      (maxVersion === undefined || this.events[i]!.version <= maxVersion);
    if (options.orderBy === "desc") events.reverse();

    return {
      events,
      nextCursor: String(i),
      hasMore,
    };
  }
}
