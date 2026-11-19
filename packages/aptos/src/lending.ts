import { RequestLimiter, StreamdError, markSelfTimed } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, ReduceOptions, Reducer, Reduction, StreamdEvent, Subscription } from "@streamd/core";
import type { ViewAt } from "./multisig.js";
import { addr, address, amount, malformed, pageSource } from "./records.js";
import type { Fields } from "./records.js";
import { abortReason, isViewAbort } from "./transfers.js";
import type { Fraction, LendingPosition, LendingProtocol, Positions, PositionsOptions, PositionAsset } from "./types.js";
import { fraction } from "./values.js";

export type { ViewAt };

/** Aave V3's pool package on Aptos mainnet, whose source is published with it. */
export const AAVE = "0x39ddcd9e1a39fa14f25e3f9ec8a86074d05cc0881cbf667df8a6ee70942016fb";
const WAD = 10n ** 18n;
const BPS = 10_000n;

const bad = (account: string, what: string) => new StreamdError(`aave position ${account}: ${what} is not what the view returns`);

function int(value: unknown, account: string, what: string): bigint {
  const n = amount(value);
  if (n === undefined) throw bad(account, what);
  return n;
}

/** The reserves `account` holds a supply or a debt in, from `get_user_reserves_data`'s scaled balances. */
function heldReserves(reply: unknown[], account: string): { asset: string; decimals: number }[] {
  const list = reply[0];
  if (!Array.isArray(list)) throw bad(account, "reserves");
  const held: { asset: string; decimals: number }[] = [];
  for (const entry of list as unknown[]) {
    const d = (entry ?? {}) as Fields;
    const asset = address(d.underlying_asset);
    const decimals = Number(int(d.decimals, account, "reserve decimals"));
    if (asset === undefined || decimals > 255) throw bad(account, "reserves");
    if (int(d.scaled_a_token_balance, account, "reserves") > 0n || int(d.scaled_variable_debt, account, "reserves") > 0n) {
      held.push({ asset, decimals });
    }
  }
  return held.sort((a, b) => (a.asset < b.asset ? -1 : a.asset > b.asset ? 1 : 0));
}

/**
 * `account`'s Aave position at `version`, from three views. The figures are
 * `user_logic::get_user_account_data`'s, which
 * `generic_logic::calculate_user_account_data` computes: collateral and debt
 * in USD with 18 decimals through Aave's oracle, the collateral-weighted
 * liquidation threshold in basis points, and the health factor as a WAD,
 * `u256::MAX` with no debt; `validation_logic` allows a liquidation only below
 * 1e18. When that view aborts (an oracle refusing a price aborts the whole
 * calculation) the balances, read without prices, are kept and `unknown`
 * says why. Any other failure rejects; a reply not shaped as the view
 * returns throws a `StreamdError`.
 */
export async function readAavePosition(view: ViewAt, account: string, version: bigint): Promise<LendingPosition> {
  const call = (fn: `${string}::${string}`, args: string[]) => view({ function: `${AAVE}::${fn}`, typeArguments: [], functionArguments: args }, version);
  const [data, reserves] = await Promise.all([
    call("user_logic::get_user_account_data", [account]).then(
      (reply) => ({ reply }),
      (err: unknown) => {
        if (isViewAbort(err)) return { unknown: abortReason(err) };
        throw err;
      },
    ),
    call("ui_pool_data_provider_v3::get_user_reserves_data", [account]),
  ]);
  const assets: PositionAsset[] = await Promise.all(
    heldReserves(reserves, account).map(async ({ asset, decimals }) => {
      const reply = await call("pool_data_provider::get_user_reserve_data", [asset, account]);
      if (typeof reply[4] !== "boolean") throw bad(account, "reserve data");
      return {
        asset,
        decimals,
        supplied: int(reply[0], account, "reserve data"),
        borrowed: int(reply[1], account, "reserve data"),
        collateral: reply[4],
      };
    }),
  );
  const position: LendingPosition = { protocol: "aave", account, version, assets };
  if ("unknown" in data) return { ...position, unknown: data.unknown };
  if (!Array.isArray(data.reply) || data.reply.length !== 6) throw bad(account, "account data");
  const [collateral, debt, , threshold, , health] = data.reply.map((v) => int(v, account, "account data"));
  return {
    ...position,
    collateralValue: fraction(collateral!, WAD),
    debtValue: fraction(debt!, WAD),
    liquidationThreshold: fraction(threshold!, BPS),
    ...(debt !== 0n && { health: fraction(health!, WAD) }),
  };
}

/** Whether `position`'s health is below `threshold`: false with no debt, `undefined` when unknown. */
export function isBelow(position: LendingPosition, threshold: Fraction): boolean | undefined {
  if (position.unknown !== undefined) return undefined;
  const { health } = position;
  if (health === undefined) return false;
  return health.numerator * threshold.denominator < threshold.numerator * health.denominator;
}

/** Each Aave event that changes a position, and the fields naming the accounts it changes. */
const AAVE_EVENTS: Record<string, readonly string[]> = {
  "supply_logic::Supply": ["on_behalf_of"],
  "supply_logic::Withdraw": ["user"],
  "borrow_logic::Borrow": ["on_behalf_of"],
  "borrow_logic::Repay": ["user"],
  "liquidation_logic::LiquidationCall": ["user"],
  "liquidation_logic::DeficitCreated": ["user"],
  "events::ReserveUsedAsCollateralEnabled": ["user"],
  "events::ReserveUsedAsCollateralDisabled": ["user"],
  "emode_logic::UserEModeSet": ["user"],
  // An aToken moving between accounts, a liquidator taking the collateral included.
  "events::BalanceTransfer": ["from", "to"],
};
/**
 * The one scan `positions("aave")` follows. Only the module defining an event
 * type can emit it, so every event here is Aave's own.
 */
export const AAVE_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: Object.keys(AAVE_EVENTS).map((name) => `${AAVE}::${name}`) as `${string}::${string}::${string}`[],
};

/** Reads one account's position at a version. */
export type PositionReader = (view: ViewAt, account: string, version: bigint) => Promise<LendingPosition>;

/** What `positions()` needs to follow one protocol. */
export interface LendingMarket {
  /** The package address, naming the head reads' records. */
  address: string;
  /** The scan over every event that changes a position. */
  descriptor: ModuleEventDescriptor;
  /** Each event type, in full, and the fields naming the accounts it changes. */
  fields: ReadonlyMap<string, readonly string[]>;
  /** A reader for one monitor, which may keep what never changes between reads. */
  reader: () => PositionReader;
}

const fieldsOf = (address: string, events: Record<string, readonly string[]>) =>
  new Map(Object.entries(events).map(([name, fields]) => [`${address}::${name}`, fields]));

/** Every protocol `positions()` reads. */
export const LENDING: Record<LendingProtocol, LendingMarket> = {
  aave: { address: AAVE, descriptor: AAVE_DESCRIPTOR, fields: fieldsOf(AAVE, AAVE_EVENTS), reader: () => readAavePosition },
};

const ACCOUNT_FIELDS = new Map(Object.values(LENDING).flatMap((m) => [...m.fields]));

const ZERO = `0x${"0".repeat(64)}`;

/** The accounts `event` changes, long form; throws for a known event without its fields. */
export function accountsOf(event: StreamdEvent): string[] {
  const fields = ACCOUNT_FIELDS.get(event.type);
  if (fields === undefined) return [];
  const d = event.data as Fields | null;
  if (typeof d !== "object" || d === null) throw malformed(event);
  const named = fields.map((f) => addr(d[f], event)).filter((a) => a !== ZERO);
  return [...new Set(named)];
}

/**
 * The scan's source: events naming no tracked account are dropped (every
 * named account is tracked when `only` is undefined), and each kept event
 * carries the positions of the accounts it names as `state`, read at its
 * version once per account and version, at most four at a time.
 */
export function positionSource(
  adapter: EventAdapter,
  read: PositionReader,
  only: ReadonlySet<string> | undefined,
  requestTimeout?: number,
  drop?: AbortSignal,
): EventAdapter {
  if (typeof adapter.view !== "function") throw new StreamdError("positions needs an adapter that implements view");
  // Outside the client's limiter, whose slot the page asking holds while its
  // views run; each read is timed here and the page marked self-timed.
  // `drop` sheds reads still waiting once the monitor stops.
  const timed = new RequestLimiter(4, Infinity, requestTimeout).wrap(adapter, drop);
  const view: ViewAt = (request, version) => timed.view!(request, version);
  return markSelfTimed(pageSource(timed, (events) => {
    const reads = new Map<string, Promise<LendingPosition>>();
    const kept: [StreamdEvent, string[]][] = [];
    for (const event of events) {
      const accounts = accountsOf(event).filter((a) => only === undefined || only.has(a));
      if (accounts.length === 0) continue;
      kept.push([event, accounts]);
      for (const a of accounts) {
        const key = `${a}@${event.version}`;
        if (!reads.has(key)) reads.set(key, read(view, a, event.version));
      }
    }
    return Promise.all(
      kept.map(async ([event, accounts]) => ({ ...event, state: await Promise.all(accounts.map((a) => reads.get(`${a}@${event.version}`)!)) })),
    );
  }));
}

/**
 * A source whose every fetch reads each `tracked` account at the head, at
 * most four at a time, and returns the positions as events at that version,
 * so `EventPoller` supplies the interval, backoff and error budget. A head not
 * past the last one read, or past `since` (where the start read them), returns
 * nothing. An account whose read fails goes to
 * `failed` and is read again at the next head; the fetch fails only when
 * every read did.
 */
export function refreshSource(
  adapter: EventAdapter,
  market: Pick<LendingMarket, "address">,
  read: PositionReader,
  tracked: () => Iterable<string>,
  failed: (err: Error) => void,
  since: bigint | undefined,
  requestTimeout?: number,
  drop?: AbortSignal,
): EventAdapter {
  if (typeof adapter.view !== "function" || typeof adapter.getHeadVersion !== "function") {
    throw new StreamdError("positions needs an adapter that implements view and getHeadVersion");
  }
  const timed = new RequestLimiter(4, Infinity, requestTimeout).wrap(adapter, drop);
  const view: ViewAt = (request, version) => timed.view!(request, version);
  let latest = since ?? -1n;
  return markSelfTimed({
    fetchEvents: async (descriptor) => {
      const head = await timed.getHeadVersion!(descriptor);
      if (head <= latest) return { events: [], nextCursor: String(latest), hasMore: false };
      const settled = await Promise.allSettled([...tracked()].map((a) => read(view, a, head)));
      const positions = settled.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
      const errors = settled.flatMap((r) => (r.status === "rejected" ? [r.reason as Error] : []));
      // Left unset when nothing was read, so the poller retries this head.
      if (positions.length === 0 && errors.length > 0) throw errors[0];
      errors.forEach(failed);
      latest = head;
      return {
        events: positions.map((data, eventIndex) => ({ type: `${market.address}::position`, data, version: head, sequenceNumber: 0n, eventIndex })),
        nextCursor: String(head),
        hasMore: false,
        throughVersion: head,
      };
    },
  });
}

/** `client.reduce()` over the scan's source narrowed to `only`, as `positions()` calls it. */
export type ReducePositions = (
  market: LendingMarket,
  read: PositionReader,
  only: ReadonlySet<string> | undefined,
  drop: AbortSignal,
  reducer: Reducer<null>,
  initial: null,
  options: ReduceOptions<null>,
) => Reduction<null>;

const ALLOWED: readonly string[] = [
  "account",
  "threshold",
  "refreshInterval",
  "fromVersion",
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "maxPollInterval",
  "retries",
  "signal",
  "onError",
  "onChange",
  "onCross",
] satisfies readonly (keyof PositionsOptions)[];

function accountSet(chosen: unknown): ReadonlySet<string> | undefined {
  if (chosen === undefined) return undefined;
  const list: unknown = typeof chosen === "string" ? [chosen] : chosen;
  const found = Array.isArray(list) ? list.map(address) : [];
  if (found.length === 0 || found.includes(undefined)) {
    throw new StreamdError("account must be an address or a non-empty list of addresses");
  }
  return new Set(found as string[]);
}

function checkThreshold(threshold: unknown): Fraction {
  if (threshold === undefined) return { numerator: 1n, denominator: 1n };
  const { numerator, denominator } = (threshold ?? {}) as Partial<Fraction>;
  if (typeof numerator !== "bigint" || typeof denominator !== "bigint" || numerator < 0n || denominator <= 0n) {
    throw new StreamdError("threshold must be { numerator, denominator } bigints, the numerator not negative and the denominator positive");
  }
  return { numerator, denominator };
}

/** What `startPositions` reads through besides the reduction. */
export interface PositionsHost {
  view: (request: Parameters<ViewAt>[0], version: bigint, drop: AbortSignal) => Promise<unknown[]>;
  head: (descriptor: ModuleEventDescriptor) => Promise<bigint>;
  track: (stop: () => void) => () => void;
  onError: ((error: Error) => void) | undefined;
  /** Follows `refreshSource` over `tracked`, delivering each fetch's positions. */
  refresh: (
    market: LendingMarket,
    read: PositionReader,
    tracked: () => Iterable<string>,
    deliver: (positions: LendingPosition[]) => void,
    options: Pick<PositionsOptions, "maxErrors" | "maxBackoff" | "signal"> & { pollInterval: number; onError: (error: Error) => void },
    since: bigint | undefined,
    drop: AbortSignal,
  ) => Subscription;
}

/** A position without its version, so two reads of the same figures compare equal. */
const figures = (p: LendingPosition) =>
  JSON.stringify({ ...p, version: undefined }, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v));

function checkInterval(interval: unknown, tracking: boolean): number {
  // ponytail: without `account` the map keeps every account any event names,
  // so a head read of all of them is off unless asked for; add eviction if
  // following the whole protocol for long becomes a use.
  if (interval === undefined) return tracking ? 60_000 : 0;
  if (typeof interval !== "number" || !Number.isFinite(interval) || interval < 0) {
    throw new StreamdError("refreshInterval must be a number of milliseconds, 0 for none");
  }
  return interval;
}

/**
 * `AptosClient.positions()`: the tracked accounts read at the start (the head,
 * or the version before `fromVersion`), then one reduction over the event
 * scan from the version after, each event's positions replacing those held
 * unless older.
 */
export function startPositions(reduce: ReducePositions, host: PositionsHost, protocol: unknown, options: PositionsOptions = {}): Positions {
  const protocols = Object.keys(LENDING);
  if (typeof protocol !== "string" || !protocols.includes(protocol)) {
    throw new StreamdError(`positions reads one of: ${protocols.join(", ")} (got ${String(protocol)})`);
  }
  const market = LENDING[protocol as LendingProtocol];
  const read = market.reader();
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported positions option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  const { account, threshold: given, refreshInterval, fromVersion, onChange, onCross, ...rest } = options;
  const only = accountSet(account);
  const threshold = checkThreshold(given);
  const interval = checkInterval(refreshInterval, only !== undefined);
  if (fromVersion !== undefined && (typeof fromVersion !== "bigint" || fromVersion < 1n)) {
    throw new StreamdError("positions fromVersion must be a bigint version of at least 1");
  }
  const positions = new Map<string, LendingPosition>();
  const sides = new Map<string, boolean>();

  /**
   * Holds `next` unless a newer read is held; returns whether any figure
   * changed (a first read included) and whether it crossed the threshold.
   */
  const apply = (next: LendingPosition): [boolean, boolean] => {
    const held = positions.get(next.account);
    if (held !== undefined && held.version > next.version) return [false, false];
    const below = isBelow(next, threshold);
    const position = below === undefined ? next : { ...next, below };
    positions.set(next.account, position);
    const changed = held === undefined || figures(held) !== figures(position);
    if (below === undefined) return [changed, false];
    const before = sides.get(next.account);
    sides.set(next.account, below);
    return [changed, before !== undefined && before !== below];
  };

  let walk: Reduction<null> | undefined;
  let refresh: Subscription | undefined;
  let stopped = false;
  const dropped = new AbortController();
  const stop = () => {
    stopped = true;
    dropped.abort(new StreamdError("positions was stopped"));
    untrack();
    walk?.unsubscribe();
    refresh?.unsubscribe();
  };
  // Tracked until the reduction exists and tracks itself, so destroy() also
  // stops a start read in flight.
  const untrack = host.track(stop);
  const halted = () => stopped || rest.signal?.aborted === true;
  const report = (err: Error) => (rest.onError ?? host.onError)?.(err);
  /** Each callback on its own, so one that throws cannot hide another's crossing. */
  const call = (handler: PositionsOptions["onChange"], p: LendingPosition, event?: StreamdEvent) => {
    try {
      handler?.(p, event);
    } catch (err) {
      report(new StreamdError(`positions handler threw for ${p.account} at ${p.version}`, err));
    }
  };
  const notifyAll = (changes: [LendingPosition, boolean][], event?: StreamdEvent) => {
    for (const [p, crossed] of changes) {
      call(onChange, p, event);
      if (crossed) call(onCross, p, event);
    }
  };
  const startRefresh = () => {
    const { maxErrors, maxBackoff, signal } = rest;
    refresh = host.refresh(
      market,
      read,
      // Health only stays live while both run, so one ending ends both.
      () => (walk?.active === true ? positions.keys() : (stop(), [])),
      (read) => {
        const changes: [LendingPosition, boolean][] = [];
        for (const p of read) {
          const [changed, crossed] = apply(p);
          if (changed) changes.push([positions.get(p.account)!, crossed]);
        }
        notifyAll(changes);
      },
      {
        pollInterval: interval,
        maxErrors,
        maxBackoff,
        signal,
        onError: (err) => {
          if (refresh !== undefined && !refresh.active) stop();
          report(err);
        },
      },
      startedAt,
      dropped.signal,
    );
  };

  let notify: [LendingPosition, boolean][] = [];
  let startedAt: bigint | undefined;
  const ready = (async () => {
    try {
      let from = fromVersion;
      if (only !== undefined) {
        const at = fromVersion === undefined ? await host.head(market.descriptor) : fromVersion - 1n;
        const view: ViewAt = (request, version) => host.view(request, version, dropped.signal);
        for (const p of await Promise.all([...only].map((a) => read(view, a, at)))) apply(p);
        startedAt = at;
        from = at + 1n;
      }
      if (halted()) throw new StreamdError("positions was stopped before it was ready");
      walk = reduce(
        market,
        read,
        only,
        dropped.signal,
        (s, event) => {
          notify = [];
          for (const p of event.state as LendingPosition[]) {
            const [changed, crossed] = apply(p);
            if (changed) notify.push([positions.get(p.account)!, crossed]);
          }
          return s;
        },
        null,
        {
          ...rest,
          ...(from !== undefined && { fromVersion: from }),
          onChange: (_, event: StreamdEvent) => notifyAll(notify, event),
        },
      );
      untrack();
      await walk.ready;
      if (interval > 0 && !halted()) startRefresh();
      return positions as ReadonlyMap<string, LendingPosition>;
    } catch (err) {
      stopped = true;
      untrack();
      throw err;
    }
  })();
  ready.catch(() => {});

  return {
    value: positions,
    get: (at) => {
      const key = address(at);
      return key === undefined ? undefined : positions.get(key);
    },
    ready,
    get active() {
      return !halted() && (walk === undefined || walk.active) && (refresh === undefined || refresh.active);
    },
    unsubscribe: stop,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: stop,
  } as Positions;
}
