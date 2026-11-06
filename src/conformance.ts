import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
} from "./types.js";
import { StreamdError, StreamdNetworkError } from "./errors.js";
import { DEFAULT_LIMIT, requireAdapter } from "./adapters/validate.js";

/** The `EventAdapter` contract rules `checkAdapter` reports on, in report order. */
export type AdapterCheckName =
  | "fetch"
  | "page-shape"
  | "from-version"
  | "order"
  | "cursor-advance"
  | "cursor-precedence"
  | "foreign-cursor"
  | "head-version"
  | "earliest-version"
  | "block";

export interface AdapterCheckOptions {
  /** Where the walk starts; omitted leaves the start to the adapter (usually the head). */
  fromVersion?: bigint;
  /** Page size asked for on every fetch (default 25). */
  limit?: number;
  /** Most pages the walk fetches (default 3), which bounds the work against a real source. */
  maxPages?: number;
  /** Height to ask `getBlock` for; its rule is skipped without one. */
  blockHeight?: bigint;
}

export interface AdapterCheckResult {
  name: AdapterCheckName;
  status: "pass" | "fail" | "skip";
  /** Why the rule failed or was skipped. */
  message?: string;
}

export interface AdapterCheckReport {
  /** True when no result failed. */
  ok: boolean;
  results: AdapterCheckResult[];
  /** Walk pages fetched. */
  pagesFetched: number;
}

const DEFAULT_MAX_PAGES = 3;
const FOREIGN_CURSOR = "streamd-check:never-issued";

interface Settings {
  fromVersion?: bigint;
  limit: number;
  maxPages: number;
  blockHeight?: bigint;
}

interface WalkedPage {
  /** The cursor this page was asked for with; undefined for the first. */
  cursor?: string;
  page: EventPage;
}

interface Walk {
  pages: WalkedPage[];
  fetchError?: string;
  shapeError?: string;
}

function requireCount(value: unknown, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new StreamdError(`checkAdapter ${label} must be a positive integer`);
  }
  return value;
}

function requireVersion(value: unknown, label: string): bigint | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "bigint" || value < 0n) {
    throw new StreamdError(`checkAdapter ${label} must be a non-negative bigint`);
  }
  return value;
}

function settings(options: AdapterCheckOptions): Settings {
  if (typeof options !== "object" || options === null) {
    throw new StreamdError("checkAdapter options must be an object");
  }
  return {
    fromVersion: requireVersion(options.fromVersion, "fromVersion"),
    limit: requireCount(options.limit, "limit", DEFAULT_LIMIT),
    maxPages: requireCount(options.maxPages, "maxPages", DEFAULT_MAX_PAGES),
    blockHeight: requireVersion(options.blockHeight, "blockHeight"),
  };
}

function describe(err: unknown): string {
  return err instanceof Error ? `${err.name}: ${err.message}` : String(err);
}

function eventShapeError(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return "is not an object";
  const e = value as Record<string, unknown>;
  if (typeof e.type !== "string") return "type is not a string";
  if (typeof e.version !== "bigint" || e.version < 0n) {
    return "version is not a non-negative bigint";
  }
  if (typeof e.sequenceNumber !== "bigint") return "sequenceNumber is not a bigint";
  if (!Number.isSafeInteger(e.eventIndex) || (e.eventIndex as number) < 0) {
    return "eventIndex is not a non-negative integer";
  }
  for (const key of ["transactionHash", "sender"]) {
    if (e[key] !== undefined && typeof e[key] !== "string") return `${key} is not a string`;
  }
  if (e.timestamp !== undefined && typeof e.timestamp !== "bigint") {
    return "timestamp is not a bigint";
  }
  return undefined;
}

function pageShapeError(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return "page is not an object";
  const page = value as Record<string, unknown>;
  if (!Array.isArray(page.events)) return "events is not an array";
  if (typeof page.hasMore !== "boolean") return "hasMore is not a boolean";
  if (page.nextCursor !== undefined && typeof page.nextCursor !== "string") {
    return "nextCursor is not a string";
  }
  for (const [i, event] of page.events.entries()) {
    const error = eventShapeError(event);
    if (error !== undefined) return `events[${i}] ${error}`;
  }
  return undefined;
}

async function walk(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  s: Settings,
): Promise<Walk> {
  const pages: WalkedPage[] = [];
  let cursor: string | undefined;
  while (pages.length < s.maxPages) {
    const request: FetchOptions =
      cursor !== undefined
        ? { cursor, limit: s.limit }
        : s.fromVersion !== undefined
          ? { fromVersion: s.fromVersion, limit: s.limit }
          : { limit: s.limit };
    let page: unknown;
    try {
      page = await adapter.fetchEvents(descriptor, request);
    } catch (err) {
      return { pages, fetchError: describe(err) };
    }
    const shapeError = pageShapeError(page);
    const at = `page ${pages.length + 1}`;
    if (shapeError !== undefined) return { pages, shapeError: `${at}: ${shapeError}` };
    const valid = page as EventPage;
    pages.push({ cursor, page: valid });
    if (!valid.hasMore || valid.nextCursor === undefined || valid.nextCursor === cursor) break;
    cursor = valid.nextCursor;
  }
  return { pages };
}

function verdict(name: AdapterCheckName, failure: string | undefined): AdapterCheckResult {
  return failure === undefined ? { name, status: "pass" } : { name, status: "fail", message: failure };
}

function skip(name: AdapterCheckName, message: string): AdapterCheckResult {
  return { name, status: "skip", message };
}

function allEvents(w: Walk): StreamdEvent[] {
  return w.pages.flatMap(({ page }) => page.events);
}

function id(e: StreamdEvent): string {
  return `${e.version}:${e.eventIndex}`;
}

function orderError(events: StreamdEvent[]): string | undefined {
  for (let i = 1; i < events.length; i++) {
    const a = events[i - 1]!;
    const b = events[i]!;
    if (b.version < a.version || (b.version === a.version && b.eventIndex <= a.eventIndex)) {
      return `event ${id(b)} follows ${id(a)}; (version, eventIndex) must strictly ascend`;
    }
  }
  return undefined;
}

function cursorAdvanceError(w: Walk): string | undefined {
  for (const [i, { cursor, page }] of w.pages.entries()) {
    if (!page.hasMore) continue;
    if (page.nextCursor === undefined) return `page ${i + 1} has hasMore true and no nextCursor`;
    if (page.nextCursor === cursor) {
      return `page ${i + 1} has hasMore true and nextCursor did not move from ${cursor}`;
    }
  }
  return undefined;
}

function fromVersionError(events: StreamdEvent[], from: bigint): string | undefined {
  const below = events.find((e) => e.version < from);
  return below && `event ${id(below)} is below fromVersion ${from}`;
}

function walkResults(w: Walk, s: Settings): AdapterCheckResult[] {
  const judge = (name: AdapterCheckName, failure: string | undefined) =>
    w.pages.length === 0 ? skip(name, "no page fetched") : verdict(name, failure);
  const events = allEvents(w);
  return [
    verdict("fetch", w.fetchError === undefined ? undefined : `fetchEvents rejected: ${w.fetchError}`),
    w.shapeError === undefined ? judge("page-shape", undefined) : verdict("page-shape", w.shapeError),
    s.fromVersion === undefined
      ? skip("from-version", "fromVersion not given")
      : judge("from-version", fromVersionError(events, s.fromVersion)),
    judge("order", orderError(events)),
    judge("cursor-advance", cursorAdvanceError(w)),
  ];
}

async function cursorPrecedence(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  w: Walk,
  s: Settings,
): Promise<AdapterCheckResult> {
  const second = w.pages[1];
  if (second === undefined) return skip("cursor-precedence", "the walk had no second page");
  const request = { cursor: second.cursor, fromVersion: s.fromVersion ?? 0n, limit: s.limit };
  let page: unknown;
  try {
    page = await adapter.fetchEvents(descriptor, request);
  } catch (err) {
    return verdict("cursor-precedence", `fetchEvents with cursor and fromVersion rejected: ${describe(err)}`);
  }
  const shapeError = pageShapeError(page);
  if (shapeError !== undefined) return verdict("cursor-precedence", shapeError);
  const got = (page as EventPage).events.map(id).join(",");
  const want = second.page.events.map(id).join(",");
  return verdict(
    "cursor-precedence",
    got === want ? undefined : `cursor plus fromVersion returned [${got}], cursor alone [${want}]`,
  );
}

async function foreignCursor(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  s: Settings,
): Promise<AdapterCheckResult> {
  try {
    await adapter.fetchEvents(descriptor, { cursor: FOREIGN_CURSOR, limit: s.limit });
  } catch (err) {
    if (err instanceof StreamdNetworkError) {
      return verdict("foreign-cursor", "rejected as StreamdNetworkError, so callers would retry it");
    }
    if (err instanceof StreamdError) return verdict("foreign-cursor", undefined);
    return verdict("foreign-cursor", `rejected with ${describe(err)}, not a StreamdError`);
  }
  return verdict("foreign-cursor", `accepted cursor ${FOREIGN_CURSOR}, which it never issued`);
}

/** Calls an optional method; a string result is the failure message. */
async function readVersion(
  name: "getHeadVersion" | "getEarliestVersion",
  adapter: EventAdapter,
  descriptor: EventDescriptor,
): Promise<bigint | string> {
  try {
    const value: unknown = await adapter[name]!(descriptor);
    if (typeof value === "bigint" && value >= 0n) return value;
    return `${name} resolved ${String(value)}, not a non-negative bigint`;
  } catch (err) {
    return `${name} rejected: ${describe(err)}`;
  }
}

function headResult(head: bigint | string, events: StreamdEvent[]): AdapterCheckResult {
  if (typeof head === "string") return verdict("head-version", head);
  const above = events.find((e) => e.version > head);
  return verdict("head-version", above && `head ${head} is below seen event ${id(above)}`);
}

function earliestResult(
  earliest: bigint | string,
  head: bigint | string | undefined,
  events: StreamdEvent[],
): AdapterCheckResult {
  if (typeof earliest === "string") return verdict("earliest-version", earliest);
  const below = events.find((e) => e.version < earliest);
  if (below !== undefined) {
    return verdict("earliest-version", `earliest ${earliest} is above seen event ${id(below)}`);
  }
  const overHead = typeof head === "bigint" && earliest > head;
  return verdict("earliest-version", overHead ? `earliest ${earliest} is above head ${head}` : undefined);
}

async function versionResults(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  events: StreamdEvent[],
): Promise<AdapterCheckResult[]> {
  const absent = "not implemented (optional)";
  const head = adapter.getHeadVersion && (await readVersion("getHeadVersion", adapter, descriptor));
  const earliest =
    adapter.getEarliestVersion && (await readVersion("getEarliestVersion", adapter, descriptor));
  return [
    head === undefined ? skip("head-version", absent) : headResult(head, events),
    earliest === undefined
      ? skip("earliest-version", absent)
      : earliestResult(earliest, head, events),
  ];
}

function blockError(block: unknown, height: bigint): string | undefined {
  if (typeof block !== "object" || block === null) return "getBlock resolved a non-object";
  const b = block as Record<string, unknown>;
  for (const key of ["height", "firstVersion", "lastVersion", "timestamp"]) {
    if (typeof b[key] !== "bigint") return `getBlock ${key} is not a bigint`;
  }
  if (b.height !== height) return `getBlock(${height}) returned height ${String(b.height)}`;
  if ((b.firstVersion as bigint) > (b.lastVersion as bigint)) {
    return "getBlock firstVersion is above lastVersion";
  }
  return undefined;
}

async function blockResult(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  height: bigint | undefined,
): Promise<AdapterCheckResult> {
  if (adapter.getBlock === undefined) return skip("block", "not implemented (optional)");
  if (height === undefined) return skip("block", "blockHeight not given");
  try {
    return verdict("block", blockError(await adapter.getBlock(descriptor, height), height));
  } catch (err) {
    return verdict("block", `getBlock rejected: ${describe(err)}`);
  }
}

/**
 * Drive `adapter` through a short read-only walk over `descriptor` and report
 * which `EventAdapter` contract rules it keeps. Violations are `fail` results,
 * never rejections; it rejects only for invalid arguments. Against a real
 * source it makes network reads: at most `maxPages` walk fetches, two more
 * fetches, and one call per optional method present. Needs no test runner, so
 * assert on `report.ok` from any of them.
 */
export async function checkAdapter(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  options: AdapterCheckOptions = {},
): Promise<AdapterCheckReport> {
  requireAdapter(adapter, "checkAdapter");
  const s = settings(options);
  const w = await walk(adapter, descriptor, s);
  const results = [
    ...walkResults(w, s),
    await cursorPrecedence(adapter, descriptor, w, s),
    await foreignCursor(adapter, descriptor, s),
    ...(await versionResults(adapter, descriptor, allEvents(w))),
    await blockResult(adapter, descriptor, s.blockHeight),
  ];
  return {
    ok: results.every((r) => r.status !== "fail"),
    results,
    pagesFetched: w.pages.length,
  };
}
