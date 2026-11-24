import { count, serializeAlert, sum } from "@streamd/core";
import type { AptosClient, SaleMarketplace, SwapProtocol } from "@streamd/aptos";
import type { AlertPayload, Fold, StreamdClient, StreamdEvent, Subscription, SubscriptionOptions } from "@streamd/core";
import type { AlertRule, AlertSource, Anomaly } from "./alert-rules.js";
import { band, decimal, outside } from "./anomaly.js";
import type { Band } from "./anomaly.js";
import type { AlertOptions } from "./args.js";
import { readCursor, writeCursor } from "./cursor-file.js";
import { filterPredicate } from "./filter.js";
import { deliver, webhookSink } from "./forward.js";
import { message } from "./format.js";

/** Record sources need an `AptosClient`; descriptor rules need only `onBatch`. */
export type AlertClient = Pick<StreamdClient, "onBatch"> & Partial<Pick<AptosClient, "transfers" | "swaps" | "sales">>;

export interface AlertIO {
  client: AlertClient;
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
  /** Anomaly rules only: the values of the closed windows before this one, oldest first. Replaced, never mutated. */
  history?: readonly bigint[];
  /** Anomaly rules only: the first window after a fresh start, which may have begun mid-window. */
  partial?: boolean;
}

/** One alert to send: the window it is about and, for an anomaly, how it left its band. */
export interface Fire {
  window: AlertWindow;
  anomaly?: { side: "above" | "below"; band: Band; baselineWindows: number };
}

/** A cursor and the window as of the end of the page that cursor follows. */
interface Position {
  cursor: string;
  window?: AlertWindow;
}

interface Entry {
  rule: AlertRule;
  fold: Fold<bigint>;
  /** The count or sum threshold; undefined for an anomaly rule. */
  limit: bigint | undefined;
  /** The last position known to be consistent; what the cursor file holds for this rule. */
  confirmed?: Position;
  /** The window after the last page delivered, which the next page continues. */
  window?: AlertWindow;
  /** A delivered page's window, waiting for the cursor to move off the position it was fetched from. */
  pending?: { before: string | undefined; window: AlertWindow | undefined };
  sub?: Subscription;
}

const INTEGER = /^-?\d+$/;

const isInteger = (value: unknown): value is string => typeof value === "string" && INTEGER.test(value);

function savedWindow(raw: unknown): AlertWindow | undefined {
  if (raw === undefined) return undefined;
  const { start, value, fired, history, partial } = (raw ?? {}) as Record<string, unknown>;
  if (!isInteger(start) || !isInteger(value) || typeof fired !== "boolean") throw new Error("malformed window");
  if (history !== undefined && !(Array.isArray(history) && history.every(isInteger))) throw new Error("malformed window");
  if (partial !== undefined && partial !== true) throw new Error("malformed window");
  return {
    start: BigInt(start),
    value: BigInt(value),
    fired,
    ...(history !== undefined && { history: history.map((v: string) => BigInt(v)) }),
    ...(partial === true && { partial }),
  };
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
    ...(window !== undefined && {
      window: {
        start: `${window.start}`,
        value: `${window.value}`,
        fired: window.fired,
        ...(window.history !== undefined && { history: window.history.map(String) }),
        ...(window.partial === true && { partial: true }),
      },
    }),
  };
}

// History arrays are never mutated, so a band computed once serves every event of its window.
const bands = new WeakMap<readonly bigint[], Band>();

/**
 * The alert for a window outside its band, marking it fired; undefined when it
 * fired already, is partial, has too short a baseline, lies inside the band,
 * or lies on a side the rule ignores. An open window can only fire `above`,
 * since a count that is low now may still grow.
 */
function judge(anomaly: Anomaly, window: AlertWindow, closed: boolean): Fire | undefined {
  const history = window.history ?? [];
  if (window.fired || window.partial === true || history.length < anomaly.minBaseline) return undefined;
  let b = bands.get(history);
  if (b === undefined) bands.set(history, (b = band(history, anomaly.k)));
  const side = outside(b, window.value);
  if (side === undefined || (!closed && side === "below") || (anomaly.direction !== "both" && anomaly.direction !== side)) return undefined;
  window.fired = true;
  return { window, anomaly: { side, band: b, baselineWindows: history.length } };
}

const appended = (history: readonly bigint[], value: bigint, max: number) => [...history, value].slice(-max);

/**
 * Closes `window` and every empty window before the one starting at `start`,
 * and returns the new open window with the baseline they leave. A run of
 * empty windows is judged once, so one outage sends one alert rather than
 * one per window.
 */
function close(anomaly: Anomaly, length: bigint, window: AlertWindow, start: bigint, fires: Fire[]): AlertWindow {
  const closing = { ...window };
  const fire = judge(anomaly, closing, true);
  if (fire !== undefined) fires.push(fire);
  let history = closing.partial === true ? (closing.history ?? []) : appended(closing.history ?? [], closing.value, anomaly.baseline);
  const gap = (start - closing.start) / length - 1n;
  if (gap > 0n) {
    // Past `baseline` empty windows the baseline is all zeros whatever the gap.
    const empty = gap < BigInt(anomaly.baseline) ? Number(gap) : anomaly.baseline;
    // The run is judged at its first window whose baseline is long enough, which may lie inside the gap.
    const judged = Math.max(0, anomaly.minBaseline - history.length);
    if (judged < empty) {
      const before = [...history, ...Array<bigint>(judged).fill(0n)];
      const at = closing.start + BigInt(judged + 1) * length;
      const zero = judge(anomaly, { start: at, value: 0n, fired: false, history: before }, true);
      if (zero !== undefined) fires.push(zero);
    }
    history = [...history, ...Array<bigint>(empty).fill(0n)].slice(-anomaly.baseline);
  }
  return { start, value: 0n, fired: false, history };
}

/**
 * The window after `event`, and the alerts it raised; throws when the event
 * has no timestamp, which places no window, or when the fold refuses it,
 * leaving the window as it was.
 */
export function advance(
  entry: Pick<Entry, "rule" | "fold" | "limit">,
  window: AlertWindow | undefined,
  event: StreamdEvent,
): { window: AlertWindow; fires: Fire[] } {
  const { timestamp } = event;
  if (timestamp === undefined) {
    throw new Error(`the event at version ${event.version} has no timestamp, so no window can be placed`);
  }
  const { rule, fold, limit } = entry;
  const start = timestamp - (timestamp % rule.window);
  if (!("anomaly" in rule.threshold)) {
    const open = window?.start === start ? window : { start, value: fold.initial, fired: false };
    const value = fold(open.value, event);
    const fire = !open.fired && value >= limit!;
    const next = { start, value, fired: open.fired || fire };
    return { window: next, fires: fire ? [{ window: next }] : [] };
  }
  const { anomaly } = rule.threshold;
  const fires: Fire[] = [];
  const open =
    window === undefined
      ? { start, value: fold.initial, fired: false, history: [], partial: true }
      : // Timestamps never decrease along an Aptos or Sui walk; an earlier one counts in the open window rather than reopening the past.
        start <= window.start
        ? window
        : close(anomaly, rule.window, window, start, fires);
  const next: AlertWindow = { ...open, value: fold(open.value, event) };
  const fire = judge(anomaly, next, false);
  if (fire !== undefined) fires.push(fire);
  return { window: next, fires };
}

/** Follows `source` one record at a time; the poller moves the cursor only once a page's records are all handled. */
function followSource(
  client: AlertClient,
  source: AlertSource,
  handle: (events: StreamdEvent[]) => Promise<void>,
  options: SubscriptionOptions,
): Subscription {
  const callback = (record: StreamdEvent) => handle([record]);
  const method = client[source.kind];
  if (method === undefined) throw new Error(`${source.kind} needs an Aptos client`);
  if (source.kind === "transfers") {
    const { asset, account } = source;
    return client.transfers!(callback, { ...options, ...(asset !== undefined && { asset }), ...(account !== undefined && { account }) });
  }
  if (source.kind === "swaps") {
    const { protocol, asset } = source;
    return client.swaps!(callback, {
      ...options,
      ...(protocol !== undefined && { protocol: protocol as SwapProtocol[] }),
      ...(asset !== undefined && { asset }),
    });
  }
  const { marketplace, token } = source;
  return client.sales!(callback, {
    ...options,
    ...(marketplace !== undefined && { marketplace: marketplace as SaleMarketplace[] }),
    ...(token !== undefined && { token }),
  });
}

/** The JSON body POSTed for an alert; `id` is the same on every redelivery. */
export function alertBody(rule: AlertRule, fire: Fire, event: StreamdEvent): string {
  const { window, anomaly } = fire;
  const alert: AlertPayload = {
    id: `${rule.name}:${window.start}`,
    rule: rule.name,
    windowStart: window.start,
    windowEnd: window.start + rule.window,
    threshold: rule.threshold,
    value: window.value,
    ...(anomaly !== undefined && {
      anomaly: {
        side: anomaly.side,
        median: decimal(anomaly.band.median),
        mad: decimal(anomaly.band.mad),
        low: decimal(anomaly.band.low),
        high: decimal(anomaly.band.high),
        baselineWindows: anomaly.baselineWindows,
      },
    }),
    event,
  };
  return serializeAlert(alert);
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
      const { threshold } = rule;
      const summed = "anomaly" in threshold ? threshold.anomaly.sum : "sum" in threshold ? threshold.sum : undefined;
      const fold = summed === undefined ? count() : sum(summed);
      const limit = "count" in threshold ? threshold.count : "gte" in threshold ? threshold.gte : undefined;
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

  const sink = webhookSink(options);
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
            for (const fire of next.fires) {
              if (!(await deliver(alertBody(rule, fire, event), sink, options.retries, io))) {
                if (!io.signal.aborted) report(`rule ${name}: the alert for the window at ${fire.window.start} was not delivered`);
                ok = false;
                break;
              }
            }
            if (!ok) break;
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
      const listener: SubscriptionOptions = {
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
      };
      try {
        entry.sub =
          rule.source === undefined ? io.client.onBatch(rule.descriptor!, handle, listener) : followSource(io.client, rule.source, handle, listener);
      } catch (err) {
        throw new Error(`rule ${name}: ${message(err)}`);
      }
    }

    // The first poll is a timer, so a rule refused here stops every earlier one before any read.
    try {
      for (const entry of entries) subscribe(entry);
    } catch (err) {
      report(message(err));
      finish(1);
    }
  });
}
