import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
  TransactionEvents,
} from "../types.js";
import { eventId } from "../cursor.js";
import { StreamdError } from "../errors.js";
import { parseEvent, type ParseEventOptions } from "../serialize.js";
import {
  DEFAULT_LIMIT,
  handlePath,
  moduleTypeFilter,
  parseCursor,
  requireAddress,
  requireResourceType,
  requireTransactionHash,
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
      handlePath(descriptor);
      return () => true;
    case "resource": {
      requireAddress(descriptor.accountAddress);
      const resourceType = requireResourceType(descriptor.resourceType);
      return (type) => type === resourceType;
    }
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

function parseLine(line: string, number: number, options: ParseEventOptions): StreamdEvent {
  try {
    return parseEvent(line, options);
  } catch (err) {
    throw new StreamdError(`Line ${number}: ${(err as Error).message}`, err);
  }
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

  /**
   * An adapter over saved `streamd tail` output, one `serializeEvent` line per
   * event. A resumed tail repeats what it printed since its last save, so each
   * event is kept once. Blank lines are ignored; any other unreadable line
   * throws, naming its line number.
   */
  static fromJsonLines(text: string, options: ParseEventOptions = {}): MemoryAdapter {
    if (typeof text !== "string") {
      throw new StreamdError("JSON lines must be a string");
    }
    const events = new Map<string, StreamdEvent>();
    text.split("\n").forEach((line, index) => {
      if (line.trim() === "") return;
      const event = parseLine(line, index + 1, options);
      const id = eventId(event);
      if (!events.has(id)) events.set(id, event);
    });
    return new MemoryAdapter([...events.values()]);
  }

  /** The highest seeded version, or `0n` when empty. */
  async getHeadVersion(): Promise<bigint> {
    return this.events.at(-1)?.version ?? 0n;
  }

  /** The lowest seeded version, or `0n` when empty. */
  async getEarliestVersion(): Promise<bigint> {
    return this.events[0]?.version ?? 0n;
  }

  /**
   * The seeded events carrying this `transactionHash`, as one successful
   * transaction at their version, or `undefined` when none do.
   */
  async getTransactionEvents(transactionHash: string): Promise<TransactionEvents | undefined> {
    const hash = requireTransactionHash(transactionHash).toLowerCase();
    const events = this.events.filter((e) => e.transactionHash?.toLowerCase() === hash);
    if (events.length === 0) return undefined;
    return { version: events[0]!.version, success: true, vmStatus: "Executed successfully", events };
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

    const page: EventPage = { events, nextCursor: String(i), hasMore };
    const through = this.throughVersion(Number(startBig), i, maxVersion);
    if (through !== undefined) page.throughVersion = through;
    return page;
  }

  /** Settled below the next unexamined event, which may share a version with the last one examined. */
  private throughVersion(start: number, i: number, maxVersion: bigint | undefined) {
    if (i === start) return undefined;
    const settled =
      i < this.events.length ? this.events[i]!.version - 1n : this.events[i - 1]!.version;
    const through = maxVersion !== undefined && maxVersion < settled ? maxVersion : settled;
    return through >= 0n ? through : undefined;
  }
}
