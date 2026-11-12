import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "./types.js";
import { StreamdError } from "./errors.js";
import { DEFAULT_LIMIT, isObject, requireAdapter } from "./adapters/validate.js";
import { describe, pageShapeError, requireCount } from "./conformance.js";
import { eventId } from "./cursor.js";
import { normalizeAddress } from "./filter.js";

export interface AdapterCompareRange {
  /** First version compared (inclusive). */
  fromVersion: bigint;
  /** Last version compared (inclusive); pick one both sources have settled. */
  toVersion: bigint;
  /** Page size asked for on every fetch (default 25). */
  limit?: number;
  /** Most pages fetched from each adapter (default 10), which bounds the work against a real source. */
  maxPages?: number;
  /** Most entries kept in each of `missing`, `extra` and `different` (default 100). */
  maxReported?: number;
}

/** A field two sources disagree on for the same `eventId`. */
export type EventDifferenceField =
  | "type"
  | "data"
  | "sequenceNumber"
  | "guid"
  | "transactionHash"
  | "timestamp"
  | "sender"
  | "entryFunction";

export interface EventDifference {
  /** The `eventId` both sources returned. */
  id: string;
  fields: EventDifferenceField[];
  a: StreamdEvent;
  b: StreamdEvent;
}

export interface AdapterComparison {
  /** True when a non-empty window was compared, nothing disagreed and neither walk errored. */
  ok: boolean;
  /** The versions compared: both sources covered them. Undefined when they share none. */
  window?: { fromVersion: bigint; toVersion: bigint };
  /** The highest version each walk is known to have finished, `fromVersion - 1` for none. */
  coverage: { a: bigint; b: bigint };
  /** In the window from `a` and not from `b`. */
  missing: StreamdEvent[];
  /** In the window from `b` and not from `a`. */
  extra: StreamdEvent[];
  different: EventDifference[];
  /** Events in the window both sources returned identically. */
  matched: number;
  pagesRead: { a: number; b: number };
  /** Why a walk stopped early: a rejected fetch or a malformed page. */
  errors: { a?: string; b?: string };
  /** A list held more than `maxReported` entries and was cut. */
  truncated: boolean;
}

const DEFAULT_MAX_PAGES = 10;
const DEFAULT_MAX_REPORTED = 100;

interface Settings {
  fromVersion: bigint;
  toVersion: bigint;
  limit: number;
  maxPages: number;
  maxReported: number;
}

interface Walk {
  events: StreamdEvent[];
  pages: number;
  covered: bigint;
  error?: string;
}

function requireVersion(value: unknown, label: string): bigint {
  if (typeof value !== "bigint" || value < 0n) {
    throw new StreamdError(`compareAdapters ${label} must be a non-negative bigint`);
  }
  return value;
}

function settings(range: AdapterCompareRange): Settings {
  if (!isObject(range)) throw new StreamdError("compareAdapters range must be an object");
  const fromVersion = requireVersion(range.fromVersion, "fromVersion");
  const toVersion = requireVersion(range.toVersion, "toVersion");
  if (toVersion < fromVersion) {
    throw new StreamdError("compareAdapters toVersion must not be below fromVersion");
  }
  return {
    fromVersion,
    toVersion,
    limit: requireCount(range.limit, "compareAdapters limit", DEFAULT_LIMIT),
    maxPages: requireCount(range.maxPages, "compareAdapters maxPages", DEFAULT_MAX_PAGES),
    maxReported: requireCount(range.maxReported, "compareAdapters maxReported", DEFAULT_MAX_REPORTED),
  };
}

function maxVersion(events: StreamdEvent[]): bigint | undefined {
  let max: bigint | undefined;
  for (const e of events) if (max === undefined || e.version > max) max = e.version;
  return max;
}

/**
 * Without `throughVersion`, a walk cut short may have more events at its last
 * version on the next page, so only the versions below it count as covered.
 */
function covered(events: StreamdEvent[], through: bigint | undefined, ended: boolean, s: Settings): bigint {
  const last = maxVersion(events);
  const none = s.fromVersion - 1n;
  const c = through ?? (last === undefined ? none : ended ? last : last - 1n);
  return c > s.toVersion ? s.toVersion : c < none ? none : c;
}

async function walk(adapter: EventAdapter, descriptor: EventDescriptor, s: Settings): Promise<Walk> {
  const events: StreamdEvent[] = [];
  let pages = 0;
  let cursor: string | undefined;
  let through: bigint | undefined;
  let ended = false;
  let error: string | undefined;
  while (pages < s.maxPages) {
    const request: FetchOptions =
      cursor === undefined
        ? { fromVersion: s.fromVersion, toVersion: s.toVersion, limit: s.limit }
        : { cursor, toVersion: s.toVersion, limit: s.limit };
    let page: unknown;
    try {
      page = await adapter.fetchEvents(descriptor, request);
    } catch (err) {
      error = `fetchEvents rejected: ${describe(err)}`;
      break;
    }
    const shapeError = pageShapeError(page);
    if (shapeError !== undefined) {
      error = `page ${pages + 1}: ${shapeError}`;
      break;
    }
    const valid = page as EventPage;
    pages++;
    events.push(...valid.events);
    const t = valid.throughVersion;
    if (t !== undefined && (through === undefined || t > through)) through = t;
    // An event past toVersion means everything up to it was seen, so a source
    // that ignores toVersion does not spend the rest of the page budget.
    const pastEnd = (through ?? -1n) >= s.toVersion || (maxVersion(valid.events) ?? -1n) > s.toVersion;
    if (!valid.hasMore || pastEnd) {
      ended = true;
      break;
    }
    if (valid.nextCursor === undefined || valid.nextCursor === cursor) break;
    cursor = valid.nextCursor;
  }
  return { events, pages, covered: covered(events, through, ended, s), error };
}

/** Deep equality over JSON-like values. */
export function sameValue(x: unknown, y: unknown): boolean {
  if (x === y) return true;
  if (!isObject(x) || !isObject(y) || Array.isArray(x) !== Array.isArray(y)) return false;
  const keys = Object.keys(x);
  return (
    keys.length === Object.keys(y).length &&
    keys.every((k) => Object.hasOwn(y, k) && sameValue(x[k], y[k]))
  );
}

function sameAddress(x: string, y: string): boolean {
  return (normalizeAddress(x) ?? x) === (normalizeAddress(y) ?? y);
}

const OPTIONAL_CHECKS: [EventDifferenceField, (a: StreamdEvent, b: StreamdEvent) => boolean][] = [
  [
    "guid",
    (a, b) =>
      a.guid!.creationNumber === b.guid!.creationNumber &&
      sameAddress(a.guid!.accountAddress, b.guid!.accountAddress),
  ],
  ["transactionHash", (a, b) => a.transactionHash!.toLowerCase() === b.transactionHash!.toLowerCase()],
  ["timestamp", (a, b) => a.timestamp === b.timestamp],
  ["sender", (a, b) => sameAddress(a.sender!, b.sender!)],
  ["entryFunction", (a, b) => sameValue(a.entryFunction, b.entryFunction)],
];

/** Optional fields count only when both sources set them: sources differ in what they have in hand. */
function differences(a: StreamdEvent, b: StreamdEvent): EventDifferenceField[] {
  const fields: EventDifferenceField[] = [];
  if (a.type !== b.type) fields.push("type");
  if (!sameValue(a.data, b.data)) fields.push("data");
  if (a.sequenceNumber !== b.sequenceNumber) fields.push("sequenceNumber");
  for (const [field, same] of OPTIONAL_CHECKS) {
    if (a[field] !== undefined && b[field] !== undefined && !same(a, b)) fields.push(field);
  }
  return fields;
}

/** The window's events by `eventId`, first occurrence kept, in `(version, eventIndex)` order. */
function windowed(events: StreamdEvent[], from: bigint, to: bigint): Map<string, StreamdEvent> {
  const inside = events
    .filter((e) => e.version >= from && e.version <= to)
    .sort((x, y) => (x.version === y.version ? x.eventIndex - y.eventIndex : x.version < y.version ? -1 : 1));
  const byId = new Map<string, StreamdEvent>();
  for (const e of inside) if (!byId.has(eventId(e))) byId.set(eventId(e), e);
  return byId;
}

function errors(a: Walk, b: Walk): AdapterComparison["errors"] {
  const out: AdapterComparison["errors"] = {};
  if (a.error !== undefined) out.a = a.error;
  if (b.error !== undefined) out.b = b.error;
  return out;
}

/**
 * Walk two sources over `range` and report, by `eventId`, where they disagree
 * inside the window both provably covered: each walk's `throughVersion`, or
 * the versions it saw when a source sets none. Disagreements and failing
 * sources are reported, never rejections; it rejects only for invalid
 * arguments. Against real sources it makes at most `maxPages` network reads
 * per adapter. Needs no test runner, so assert on `report.ok` from any of them.
 */
export async function compareAdapters(
  a: EventAdapter,
  b: EventAdapter,
  descriptor: EventDescriptor,
  range: AdapterCompareRange,
): Promise<AdapterComparison> {
  requireAdapter(a, "compareAdapters");
  requireAdapter(b, "compareAdapters");
  const s = settings(range);
  const [wa, wb] = await Promise.all([walk(a, descriptor, s), walk(b, descriptor, s)]);
  const end = wa.covered < wb.covered ? wa.covered : wb.covered;
  const report: AdapterComparison = {
    ok: false,
    coverage: { a: wa.covered, b: wb.covered },
    missing: [],
    extra: [],
    different: [],
    matched: 0,
    pagesRead: { a: wa.pages, b: wb.pages },
    errors: errors(wa, wb),
    truncated: false,
  };
  if (end < s.fromVersion) return report;
  report.window = { fromVersion: s.fromVersion, toVersion: end };
  const fromA = windowed(wa.events, s.fromVersion, end);
  const fromB = windowed(wb.events, s.fromVersion, end);
  const keep = <T>(list: T[], item: T) => {
    if (list.length < s.maxReported) list.push(item);
    else report.truncated = true;
  };
  for (const [id, ea] of fromA) {
    const eb = fromB.get(id);
    if (eb === undefined) keep(report.missing, ea);
    else {
      const fields = differences(ea, eb);
      if (fields.length === 0) report.matched++;
      else keep(report.different, { id, fields, a: ea, b: eb });
    }
  }
  for (const [id, eb] of fromB) if (!fromA.has(id)) keep(report.extra, eb);
  report.ok =
    report.missing.length + report.extra.length + report.different.length === 0 &&
    wa.error === undefined &&
    wb.error === undefined;
  return report;
}
