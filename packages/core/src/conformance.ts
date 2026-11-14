import type {
  BlockHeights,
  BlockInfo,
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  StreamdEvent,
  TransactionEvents,
  ViewRequest,
} from "./types.js";
import { StreamdError, StreamdNetworkError } from "./errors.js";
import {
  DEFAULT_LIMIT,
  blockError,
  blockHeightsError,
  isObject,
  requireAdapter,
  requireViewRequest,
  versionError,
} from "./adapters/validate.js";
import { eventId } from "./cursor.js";

/** The `EventAdapter` contract rules `checkAdapter` reports on, in report order. */
export type AdapterCheckName =
  | "fetch"
  | "page-shape"
  | "from-version"
  | "order"
  | "cursor-advance"
  | "through-version"
  | "cursor-precedence"
  | "foreign-cursor"
  | "head-version"
  | "earliest-version"
  | "block"
  | "block-heights"
  | "block-sequence"
  | "transaction-events"
  | "transaction-events-by-version"
  | "view";

export interface AdapterCheckOptions {
  /** Where the walk starts; omitted leaves the start to the adapter (usually the head). */
  fromVersion?: bigint;
  /** Page size asked for on every fetch (default 25). */
  limit?: number;
  /** Most pages the walk fetches (default 3), which bounds the work against a real source. */
  maxPages?: number;
  /**
   * Height to ask `getBlock` for; omitted takes `newest` from
   * `getBlockHeights`, and the block rules skip when neither is there.
   */
  blockHeight?: bigint;
  /**
   * A view to read through `view`, at `version` or else the first walked
   * event's version or the head; its rule is skipped without one, since no
   * view function exists on every chain.
   */
  view?: { request: ViewRequest; version?: bigint };
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
const ABSENT = "not implemented (optional)";

interface Settings {
  fromVersion?: bigint;
  limit: number;
  maxPages: number;
  blockHeight?: bigint;
  view?: { request: ViewRequest; version?: bigint };
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

/** `label` names the caller and option, as in `"checkAdapter limit"`. */
export function requireCount(value: unknown, label: string, fallback: number): number {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new StreamdError(`${label} must be a positive integer`);
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
    limit: requireCount(options.limit, "checkAdapter limit", DEFAULT_LIMIT),
    maxPages: requireCount(options.maxPages, "checkAdapter maxPages", DEFAULT_MAX_PAGES),
    blockHeight: requireVersion(options.blockHeight, "blockHeight"),
    view: viewOption(options.view),
  };
}

function viewOption(value: unknown): Settings["view"] {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null) {
    throw new StreamdError("checkAdapter view must be an object");
  }
  const { request, version } = value as Record<string, unknown>;
  return { request: requireViewRequest(request), version: requireVersion(version, "view.version") };
}

export function describe(err: unknown): string {
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
  if (e.guid !== undefined) {
    const guid = e.guid as Record<string, unknown> | null;
    if (typeof guid?.creationNumber !== "bigint" || typeof guid.accountAddress !== "string") {
      return "guid is not { creationNumber: bigint, accountAddress: string }";
    }
  }
  if (e.entryFunction !== undefined) {
    const call = e.entryFunction as Record<string, unknown> | null;
    if (
      typeof call?.function !== "string" ||
      !Array.isArray(call.typeArguments) ||
      !call.typeArguments.every((t) => typeof t === "string") ||
      !Array.isArray(call.arguments)
    ) {
      return "entryFunction is not { function: string, typeArguments: string[], arguments: unknown[] }";
    }
  }
  return undefined;
}

export function pageShapeError(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null) return "page is not an object";
  const page = value as Record<string, unknown>;
  if (!Array.isArray(page.events)) return "events is not an array";
  if (typeof page.hasMore !== "boolean") return "hasMore is not a boolean";
  if (page.nextCursor !== undefined && typeof page.nextCursor !== "string") {
    return "nextCursor is not a string";
  }
  const through = page.throughVersion;
  const badThrough = through !== undefined && (typeof through !== "bigint" || through < 0n);
  if (badThrough) return "throughVersion is not a non-negative bigint";
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

function orderError(events: StreamdEvent[]): string | undefined {
  for (let i = 1; i < events.length; i++) {
    const a = events[i - 1]!;
    const b = events[i]!;
    if (b.version < a.version || (b.version === a.version && b.eventIndex <= a.eventIndex)) {
      return `event ${eventId(b)} follows ${eventId(a)}; (version, eventIndex) must strictly ascend`;
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

/** Each page's `throughVersion` must not fall, and later pages must stay above it. */
function throughVersionError(w: Walk): string | undefined {
  let settled = -1n;
  for (const [i, { page }] of w.pages.entries()) {
    const early = page.events.find((e) => e.version <= settled);
    if (early) return `page ${i + 1} has event ${eventId(early)} at or below throughVersion ${settled}`;
    const through = page.throughVersion ?? settled;
    if (through < settled) return `page ${i + 1} throughVersion ${through} is below an earlier ${settled}`;
    settled = through;
  }
  return undefined;
}

function fromVersionError(events: StreamdEvent[], from: bigint): string | undefined {
  const below = events.find((e) => e.version < from);
  return below && `event ${eventId(below)} is below fromVersion ${from}`;
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
    w.pages.some(({ page }) => page.throughVersion !== undefined)
      ? verdict("through-version", throughVersionError(w))
      : skip("through-version", "no page sets throughVersion (optional)"),
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
  const got = (page as EventPage).events.map(eventId).join(",");
  const want = second.page.events.map(eventId).join(",");
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
    return versionError(name, value) ?? (value as bigint);
  } catch (err) {
    return `${name} rejected: ${describe(err)}`;
  }
}

function headResult(head: bigint | string, events: StreamdEvent[]): AdapterCheckResult {
  if (typeof head === "string") return verdict("head-version", head);
  const above = events.find((e) => e.version > head);
  return verdict("head-version", above && `head ${head} is below seen event ${eventId(above)}`);
}

function earliestResult(
  earliest: bigint | string,
  head: bigint | string | undefined,
  events: StreamdEvent[],
): AdapterCheckResult {
  if (typeof earliest === "string") return verdict("earliest-version", earliest);
  const below = events.find((e) => e.version < earliest);
  if (below !== undefined) {
    return verdict("earliest-version", `earliest ${earliest} is above seen event ${eventId(below)}`);
  }
  const overHead = typeof head === "bigint" && earliest > head;
  return verdict("earliest-version", overHead ? `earliest ${earliest} is above head ${head}` : undefined);
}

async function versionResults(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  events: StreamdEvent[],
): Promise<{ results: AdapterCheckResult[]; head?: bigint }> {
  const head = adapter.getHeadVersion && (await readVersion("getHeadVersion", adapter, descriptor));
  const earliest =
    adapter.getEarliestVersion && (await readVersion("getEarliestVersion", adapter, descriptor));
  const results = [
    head === undefined ? skip("head-version", ABSENT) : headResult(head, events),
    earliest === undefined
      ? skip("earliest-version", ABSENT)
      : earliestResult(earliest, head, events),
  ];
  return { results, head: typeof head === "bigint" ? head : undefined };
}

type Outcome = { ok: true; value: unknown } | { ok: false; error: string };

/** Runs one optional-method call, turning a rejection or a synchronous throw into a failure message. */
async function attempt(method: string, run: () => unknown): Promise<Outcome> {
  try {
    return { ok: true, value: await run() };
  } catch (err) {
    return { ok: false, error: `${method} rejected: ${describe(err)}` };
  }
}

async function readBlock(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  height: bigint,
): Promise<{ block?: BlockInfo; error?: string }> {
  const got = await attempt("getBlock", () => adapter.getBlock!(descriptor, height));
  if (!got.ok) return { error: got.error };
  const error = blockError(got.value, height);
  return error === undefined ? { block: got.value as BlockInfo } : { error };
}

/** Block `height - 1` must end where `height` starts, and block time must not fall. */
async function sequenceResult(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  height: bigint,
  block: BlockInfo | undefined,
  oldest: bigint | undefined,
): Promise<AdapterCheckResult> {
  const name = "block-sequence";
  if (block === undefined) return skip(name, `block ${height} did not pass`);
  if (height === 0n) return skip(name, "no block before height 0");
  const before = height - 1n;
  if (oldest !== undefined && before < oldest) return skip(name, `block ${before} is below oldest ${oldest}`);
  const prev = await readBlock(adapter, descriptor, before);
  if (prev.block === undefined) return verdict(name, prev.error);
  if (prev.block.lastVersion + 1n !== block.firstVersion) {
    return verdict(
      name,
      `block ${before} ends at version ${prev.block.lastVersion} but block ${height} starts at ${block.firstVersion}`,
    );
  }
  const fell = prev.block.timestamp > block.timestamp;
  return verdict(
    name,
    fell ? `block ${height} timestamp ${block.timestamp} is below block ${before}'s ${prev.block.timestamp}` : undefined,
  );
}

/** `block`, `block-heights` and `block-sequence`, reading the heights first for the default height. */
async function blockResults(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  given: bigint | undefined,
): Promise<AdapterCheckResult[]> {
  const heights =
    adapter.getBlockHeights &&
    (await attempt("getBlockHeights", () => adapter.getBlockHeights!(descriptor)));
  const heightsError = heights && (heights.ok ? blockHeightsError(heights.value) : heights.error);
  const known = heights?.ok && heightsError === undefined ? (heights.value as BlockHeights) : undefined;
  const heightsResult =
    heights === undefined ? skip("block-heights", ABSENT) : verdict("block-heights", heightsError);
  const height = given ?? known?.newest;
  if (adapter.getBlock === undefined || height === undefined) {
    const reason =
      adapter.getBlock === undefined
        ? ABSENT
        : heights === undefined
          ? "blockHeight not given"
          : "blockHeight not given and getBlockHeights did not pass";
    return [skip("block", reason), heightsResult, skip("block-sequence", reason)];
  }
  const at = await readBlock(adapter, descriptor, height);
  return [
    verdict("block", at.error),
    heightsResult,
    await sequenceResult(adapter, descriptor, height, at.block, known?.oldest),
  ];
}

const LOOKUP_KINDS = new Set(["module", "account", "handle"]);

/** Why `value` is not a lookup result for `walked`'s transaction, or undefined when it is. */
function lookupError(
  label: string,
  value: unknown,
  walked: StreamdEvent,
  kind: EventDescriptor["kind"],
): string | undefined {
  if (value === undefined) return `${label} found nothing for walked event ${eventId(walked)}`;
  if (!isObject(value)) return `${label} resolved a non-object`;
  const { version, success, vmStatus, events } = value;
  if (typeof version !== "bigint" || version < 0n) return `${label} version is not a non-negative bigint`;
  if (typeof success !== "boolean") return `${label} success is not a boolean`;
  if (typeof vmStatus !== "string") return `${label} vmStatus is not a string`;
  if (!Array.isArray(events)) return `${label} events is not an array`;
  for (const [i, event] of events.entries()) {
    const error = eventShapeError(event);
    if (error !== undefined) return `${label} events[${i}] ${error}`;
  }
  if (version !== walked.version) {
    return `${label} returned version ${version}, but walked event ${eventId(walked)} is at ${walked.version}`;
  }
  const held = events as StreamdEvent[];
  const off = held.find((e) => e.version !== version);
  if (off) return `${label} event ${eventId(off)} is not at version ${version}`;
  for (let i = 1; i < held.length; i++) {
    if (held[i]!.eventIndex <= held[i - 1]!.eventIndex) {
      return `${label} eventIndex ${held[i]!.eventIndex} follows ${held[i - 1]!.eventIndex}; transaction order must strictly ascend`;
    }
  }
  // A handle source may index by the handle's sequence number, as FullnodeScanAdapter does.
  const same =
    kind === "handle"
      ? (e: StreamdEvent) => e.type === walked.type && e.sequenceNumber === walked.sequenceNumber
      : (e: StreamdEvent) => e.type === walked.type && e.eventIndex === walked.eventIndex;
  return held.some(same) ? undefined : `${label} does not hold walked event ${eventId(walked)} (${walked.type})`;
}

async function judgeLookup(
  name: AdapterCheckName,
  label: string,
  run: () => unknown,
  walked: StreamdEvent,
  kind: EventDescriptor["kind"],
): Promise<{ result: AdapterCheckResult; found?: TransactionEvents }> {
  const got = await attempt(label, run);
  if (!got.ok) return { result: verdict(name, got.error) };
  const error = lookupError(label, got.value, walked, kind);
  return { result: verdict(name, error), found: error === undefined ? (got.value as TransactionEvents) : undefined };
}

/** `transaction-events` and `transaction-events-by-version`, each looking up a walked event's transaction. */
async function lookupResults(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  events: StreamdEvent[],
): Promise<AdapterCheckResult[]> {
  const byHashName = "transaction-events";
  const byVersionName = "transaction-events-by-version";
  const reason = !LOOKUP_KINDS.has(descriptor.kind)
    ? `${descriptor.kind} changes are not transaction events`
    : events.length === 0
      ? "no walked event to look up"
      : undefined;
  const hashed = events.find((e) => e.transactionHash !== undefined);
  let byHash: { result: AdapterCheckResult; found?: TransactionEvents } | undefined;
  let hashResult: AdapterCheckResult;
  if (adapter.getTransactionEvents === undefined) hashResult = skip(byHashName, ABSENT);
  else if (reason !== undefined) hashResult = skip(byHashName, reason);
  else if (hashed === undefined) hashResult = skip(byHashName, "no walked event carries a transactionHash");
  else {
    const hash = hashed.transactionHash!;
    byHash = await judgeLookup(
      byHashName,
      `getTransactionEvents(${hash})`,
      () => adapter.getTransactionEvents!(hash, descriptor),
      hashed,
      descriptor.kind,
    );
    hashResult = byHash.result;
  }
  if (adapter.getTransactionEventsByVersion === undefined) return [hashResult, skip(byVersionName, ABSENT)];
  if (reason !== undefined) return [hashResult, skip(byVersionName, reason)];
  const walked = byHash !== undefined ? hashed! : events[0]!;
  const byVersion = await judgeLookup(
    byVersionName,
    `getTransactionEventsByVersion(${walked.version})`,
    () => adapter.getTransactionEventsByVersion!(walked.version, descriptor),
    walked,
    descriptor.kind,
  );
  if (byHash?.found === undefined || byVersion.found === undefined) return [hashResult, byVersion.result];
  const hashIds = byHash.found.events.map(eventId).join(",");
  const versionIds = byVersion.found.events.map(eventId).join(",");
  return [
    hashResult,
    verdict(
      byVersionName,
      hashIds === versionIds ? undefined : `disagrees with getTransactionEvents: [${versionIds}] vs [${hashIds}]`,
    ),
  ];
}

async function viewResult(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  view: Settings["view"],
  at: bigint | undefined,
): Promise<AdapterCheckResult> {
  if (adapter.view === undefined) return skip("view", ABSENT);
  if (view === undefined) return skip("view", "view request not given");
  const version = view.version ?? at;
  if (version === undefined) return skip("view", "no version to read at");
  const got = await attempt("view", () => adapter.view!(view.request, version, descriptor));
  if (!got.ok) return verdict("view", got.error);
  const label = `view(${view.request.function}) at ${version}`;
  return verdict("view", Array.isArray(got.value) ? undefined : `${label} resolved ${typeof got.value}, not an array`);
}

/**
 * Drive `adapter` through a short read-only walk over `descriptor` and report
 * which `EventAdapter` contract rules it keeps. Violations are `fail` results,
 * never rejections; it rejects only for invalid arguments. Against a real
 * source it makes network reads: at most `maxPages` walk fetches, two more
 * fetches, and one call to each optional method it checks that is present
 * (two to `getBlock`): at most `maxPages + 10` calls.
 * Needs no test runner, so assert on `report.ok` from any of them.
 */
export async function checkAdapter(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  options: AdapterCheckOptions = {},
): Promise<AdapterCheckReport> {
  requireAdapter(adapter, "checkAdapter");
  const s = settings(options);
  const w = await walk(adapter, descriptor, s);
  const events = allEvents(w);
  const precedence = await cursorPrecedence(adapter, descriptor, w, s);
  const foreign = await foreignCursor(adapter, descriptor, s);
  const versions = await versionResults(adapter, descriptor, events);
  const results = [
    ...walkResults(w, s),
    precedence,
    foreign,
    ...versions.results,
    ...(await blockResults(adapter, descriptor, s.blockHeight)),
    ...(await lookupResults(adapter, descriptor, events)),
    await viewResult(adapter, descriptor, s.view, events[0]?.version ?? versions.head),
  ];
  return {
    ok: results.every((r) => r.status !== "fail"),
    results,
    pagesFetched: w.pages.length,
  };
}
