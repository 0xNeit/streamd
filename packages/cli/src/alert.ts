import { count, serializeEvent, sum } from "@streamd/core";
import type { Fold, StreamdClient, StreamdEvent, Subscription } from "@streamd/core";
import type { AlertRule } from "./alert-rules.js";
import type { AlertOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
import { filterPredicate } from "./filter.js";
import { deliver } from "./forward.js";
import { message } from "./format.js";

export interface AlertIO {
  client: Pick<StreamdClient, "onBatch">;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborting abandons any in-flight POST and ends the command with exit code 0. */
  signal: AbortSignal;
  /** First retry wait in ms, doubling per attempt (default 1000). */
  retryDelay?: number;
}

/** One rule's open window: its start in microseconds, the count or sum so far, and whether it alerted. */
export interface AlertWindow {
  start: bigint;
  value: bigint;
  fired: boolean;
}

/** A cursor and the window as of the end of the page that cursor follows. */
interface Position {
  cursor: string;
  window?: AlertWindow;
}

interface Entry {
  rule: AlertRule;
  fold: Fold<bigint>;
  limit: bigint;
  /** The last position known to be consistent; what the cursor file holds for this rule. */
  confirmed?: Position;
  /** The window after the last page delivered, which the next page continues. */
  window?: AlertWindow;
  /** A delivered page's window, waiting for the cursor to move off the position it was fetched from. */
  pending?: { before: string | undefined; window: AlertWindow | undefined };
  sub?: Subscription;
}

const INTEGER = /^-?\d+$/;

function savedWindow(raw: unknown): AlertWindow | undefined {
  if (raw === undefined) return undefined;
  const { start, value, fired } = (raw ?? {}) as Record<string, unknown>;
  if (typeof start !== "string" || !INTEGER.test(start) || typeof value !== "string" || !INTEGER.test(value) || typeof fired !== "boolean") {
    throw new Error("malformed window");
  }
  return { start: BigInt(start), value: BigInt(value), fired };
}

/** Every saved entry by rule name, each still raw so entries for rules no longer in the file are kept as they are. */
function readState(path: string): Record<string, unknown> {
  // No prototype, so a rule named __proto__ is an ordinary key.
  const state: Record<string, unknown> = Object.create(null);
  const text = readCursor(path);
  if (text === undefined) return state;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`not a JSON object of rule name to saved position: ${path}`);
  }
  return Object.assign(state, parsed);
}

function savedPosition(raw: unknown, rule: AlertRule, path: string): Position {
  const { rule: fingerprint, cursor, window } = (raw ?? {}) as Record<string, unknown>;
  if (typeof cursor !== "string" || cursor === "" || typeof fingerprint !== "string") {
    throw new Error(`malformed entry for rule ${JSON.stringify(rule.name)}: ${path}`);
  }
  if (fingerprint !== rule.fingerprint) {
    throw new Error(
      `rule ${JSON.stringify(rule.name)} changed since its position was saved; remove its entry from ${path} or use another --cursor-file`,
    );
  }
  try {
    const saved = savedWindow(window);
    return saved === undefined ? { cursor } : { cursor, window: saved };
  } catch {
    throw new Error(`malformed entry for rule ${JSON.stringify(rule.name)}: ${path}`);
  }
}

function serialized(entry: Entry, position: Position) {
  const { window } = position;
  return {
    rule: entry.rule.fingerprint,
    cursor: position.cursor,
    ...(window !== undefined && { window: { start: `${window.start}`, value: `${window.value}`, fired: window.fired } }),
  };
}

/**
 * The window after `event`, and whether it crossed the threshold; throws when
 * the event has no timestamp, which places no window.
 */
export function advance(
  entry: Pick<Entry, "rule" | "fold" | "limit">,
  window: AlertWindow | undefined,
  event: StreamdEvent,
): { window: AlertWindow; fire: boolean } {
  const { timestamp } = event;
  if (timestamp === undefined) {
    throw new Error(`the event at version ${event.version} has no timestamp, so no window can be placed`);
  }
  const start = timestamp - (timestamp % entry.rule.window);
  const open = window?.start === start ? window : { start, value: entry.fold.initial, fired: false };
  const value = entry.fold(open.value, event);
  const fire = !open.fired && value >= entry.limit;
  return { window: { start, value, fired: open.fired || fire }, fire };
}

/** The JSON body POSTed for an alert; `id` is the same on every redelivery. */
export function alertBody(rule: AlertRule, window: AlertWindow, event: StreamdEvent): string {
  const { threshold } = rule;
  const head = JSON.stringify({
    id: `${rule.name}:${window.start}`,
    rule: rule.name,
    windowStart: `${window.start}`,
    windowEnd: `${window.start + rule.window}`,
    threshold: "count" in threshold ? { count: `${threshold.count}` } : { sum: threshold.sum, gte: `${threshold.gte}` },
    value: `${window.value}`,
  });
  return `${head.slice(0, -1)},"event":${serializeEvent(event)}}`;
}

/**
 * Follows every rule, POSTing an alert the first time a window crosses its
 * threshold, and resolves to the exit code: 0 when the signal aborts; 1 when
 * the cursor file cannot be read or written or names a changed rule, a
 * delivery fails, a rule's polling gives up, or an event has no timestamp.
 *
 * Delivery is at least once: the cursor file only ever pairs a cursor with
 * the window as of the end of that cursor's page, written after the page's
 * alerts were acknowledged.
 */
export function runAlert(options: AlertOptions, rules: readonly AlertRule[], io: AlertIO): Promise<number> {
  const report = (text: string) => io.error(`streamd: ${text}`);
  let raw: Record<string, unknown>;
  const entries: Entry[] = [];
  try {
    raw = readState(options.cursorFile);
    for (const rule of rules) {
      const fold = "count" in rule.threshold ? count() : sum(rule.threshold.sum);
      const limit = "count" in rule.threshold ? rule.threshold.count : rule.threshold.gte;
      const entry: Entry = { rule, fold, limit };
      if (Object.hasOwn(raw, rule.name)) {
        entry.confirmed = savedPosition(raw[rule.name], rule, options.cursorFile);
        if (entry.confirmed.window !== undefined) entry.window = entry.confirmed.window;
      }
      entries.push(entry);
    }
  } catch (err) {
    report(`cannot read cursor file: ${message(err)}`);
    return Promise.resolve(1);
  }
  if (io.signal.aborted) return Promise.resolve(0);

  let written = JSON.stringify(raw);
  const checkpoint = (): boolean => {
    for (const entry of entries) {
      const cursor = entry.sub?.cursor;
      if (cursor === undefined) continue;
      if (entry.pending !== undefined) {
        // The cursor leaves the position a page was fetched from only once the
        // poller has moved past that page, so its window is then consistent.
        if (cursor === entry.pending.before) continue;
        const { window } = entry.pending;
        entry.confirmed = window === undefined ? { cursor } : { cursor, window };
        entry.pending = undefined;
      } else if (cursor !== entry.confirmed?.cursor) {
        // Moved over pages with nothing matching: the window is unchanged.
        entry.confirmed = entry.window === undefined ? { cursor } : { cursor, window: entry.window };
      }
      raw[entry.rule.name] = serialized(entry, entry.confirmed!);
    }
    const text = JSON.stringify(raw);
    if (text === written) return true;
    try {
      writeCursor(options.cursorFile, text);
      written = text;
      return true;
    } catch (err) {
      report(`cannot write cursor file: ${message(err)}`);
      return false;
    }
  };

  return new Promise<number>((resolve) => {
    let done = false;
    const finish = (code: number) => {
      if (done) return;
      done = true;
      for (const entry of entries) entry.sub?.unsubscribe();
      io.signal.removeEventListener("abort", onAbort);
      resolve(checkpoint() ? code : 1);
    };
    const onAbort = () => finish(0);
    io.signal.addEventListener("abort", onAbort, { once: true });

    const { fromVersion, fromTime, pollInterval } = options;
    function subscribe(entry: Entry): void {
      const { rule } = entry;
      const name = JSON.stringify(rule.name);
      const handle = async (events: StreamdEvent[]) => {
        const before = entry.sub?.cursor;
        let window = entry.window;
        let ok = true;
        try {
          for (const event of events) {
            if (done) {
              ok = false;
              break;
            }
            let next: ReturnType<typeof advance>;
            try {
              next = advance(entry, window, event);
            } catch (err) {
              if (event.timestamp === undefined) throw err;
              report(`rule ${name}: ${message(err)}; skipping the event at version ${event.version}`);
              continue;
            }
            window = next.window;
            if (next.fire && !(await deliver(alertBody(rule, window, event), options, io))) {
              if (!io.signal.aborted) report(`rule ${name}: the alert for the window at ${window.start} was not delivered`);
              ok = false;
              break;
            }
          }
        } catch (err) {
          ok = false;
          report(`rule ${name}: ${message(err)}`);
        } finally {
          // Stopping before returning keeps the poller's cursor on this page.
          if (!ok) finish(io.signal.aborted ? 0 : 1);
        }
        if (!ok) return;
        entry.window = window;
        entry.pending = { before, window };
        // The poller moves its cursor once this callback settles, on a microtask; a timer runs after.
        setTimeout(() => {
          if (!done && !checkpoint()) finish(1);
        }, 0);
      };
      entry.sub = io.client.onBatch(rule.descriptor, handle, {
        ...(entry.confirmed !== undefined
          ? { cursor: entry.confirmed.cursor }
          : { ...(fromVersion !== undefined && { fromVersion }), ...(fromTime !== undefined && { fromTime }) }),
        ...(pollInterval !== undefined && { pollInterval }),
        ...(rule.decode !== undefined && { decode: rule.decode }),
        ...(rule.filter !== undefined && { where: filterPredicate(rule.filter) }),
        onError: (err) => {
          report(`rule ${name}: ${message(err)}`);
          if (!entry.sub?.active) finish(1);
        },
      });
    }

    try {
      for (const entry of entries) subscribe(entry);
    } catch (err) {
      report(message(err));
      finish(1);
    }
  });
}
