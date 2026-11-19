import { RequestLimiter, StreamdError, markSelfTimed } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, ReduceOptions, Reducer, Reduction, StreamdEvent, Subscription, ViewRequest } from "@streamd/core";
import { addr, address, amount, malformed, pageSource } from "./records.js";
import type { Fields } from "./records.js";
import { isViewAbort } from "./transfers.js";
import type { Fraction, LendingPosition, Positions, PositionsOptions, PositionAsset } from "./types.js";
import { fraction } from "./values.js";

/** A view read at a version, as `EventAdapter.view` reads one. */
export type ViewAt = (request: ViewRequest, version: bigint) => Promise<unknown[]>;

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
        if (isViewAbort(err)) return { unknown: (err as Error).message };
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
const ACCOUNT_FIELDS = new Map(Object.entries(AAVE_EVENTS).map(([name, fields]) => [`${AAVE}::${name}`, fields]));

/**
 * The one scan `positions("aave")` follows. Only the module defining an event
 * type can emit it, so every event here is Aave's own.
 */
export const AAVE_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [...ACCOUNT_FIELDS.keys()] as `${string}::${string}::${string}`[],
};

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
export function positionSource(adapter: EventAdapter, only: ReadonlySet<string> | undefined, requestTimeout?: number): EventAdapter {
  if (typeof adapter.view !== "function") throw new StreamdError("positions needs an adapter that implements view");
  // Outside the client's limiter, whose slot the page asking holds while its
  // views run; each read is timed here and the page marked self-timed.
  const timed = new RequestLimiter(4, Infinity, requestTimeout).wrap(adapter);
  const view: ViewAt = (request, version) => timed.view!(request, version);
  return markSelfTimed(pageSource(timed, (events) => {
    const read = new Map<string, Promise<LendingPosition>>();
    const kept: [StreamdEvent, string[]][] = [];
    for (const event of events) {
      const accounts = accountsOf(event).filter((a) => only === undefined || only.has(a));
      if (accounts.length === 0) continue;
      kept.push([event, accounts]);
      for (const a of accounts) {
        const key = `${a}@${event.version}`;
        if (!read.has(key)) read.set(key, readAavePosition(view, a, event.version));
      }
    }
    return Promise.all(
      kept.map(async ([event, accounts]) => ({ ...event, state: await Promise.all(accounts.map((a) => read.get(`${a}@${event.version}`)!)) })),
    );
  }));
}

/**
 * A source whose every fetch reads each `tracked` account at the head, at
 * most four at a time, and returns the positions as events at that version,
 * so `EventPoller` supplies the interval, backoff and error budget. A head
 * not past the last one read returns nothing.
 */
export function refreshSource(adapter: EventAdapter, tracked: () => Iterable<string>, requestTimeout?: number): EventAdapter {
  if (typeof adapter.view !== "function" || typeof adapter.getHeadVersion !== "function") {
    throw new StreamdError("positions needs an adapter that implements view and getHeadVersion");
  }
  const timed = new RequestLimiter(4, Infinity, requestTimeout).wrap(adapter);
  const view: ViewAt = (request, version) => timed.view!(request, version);
  let latest = -1n;
  return markSelfTimed({
    fetchEvents: async (descriptor) => {
      const head = await timed.getHeadVersion!(descriptor);
      // Set only once every read succeeded, so a failed head is read again.
      if (head <= latest) return { events: [], nextCursor: String(latest), hasMore: false };
      const read = await Promise.all([...tracked()].map((a) => readAavePosition(view, a, head)));
      latest = head;
      return {
        events: read.map((data, eventIndex) => ({ type: `${AAVE}::position`, data, version: head, sequenceNumber: 0n, eventIndex })),
        nextCursor: String(head),
        hasMore: false,
        throughVersion: head,
      };
    },
  });
}

/** `client.reduce()` over the scan's source narrowed to `only`, as `positions()` calls it. */
export type ReducePositions = (
  only: ReadonlySet<string> | undefined,
  descriptor: ModuleEventDescriptor,
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
  view: ViewAt;
  head: () => Promise<bigint>;
  track: (stop: () => void) => () => void;
  onError: ((error: Error) => void) | undefined;
  /** Follows `refreshSource` over `tracked`, delivering each fetch's positions. */
  refresh: (
    tracked: () => Iterable<string>,
    deliver: (positions: LendingPosition[]) => void,
    options: Pick<PositionsOptions, "maxErrors" | "maxBackoff" | "signal" | "onError"> & { pollInterval: number },
  ) => Subscription;
}

/** A position without its version, so two reads of the same figures compare equal. */
const figures = (p: LendingPosition) =>
  JSON.stringify({ ...p, version: undefined }, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v));

function checkInterval(interval: unknown): number {
  if (interval === undefined) return 60_000;
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
  if (protocol !== "aave") throw new StreamdError(`positions reads one of: aave (got ${String(protocol)})`);
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported positions option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  const { account, threshold: given, refreshInterval, fromVersion, onChange, onCross, ...rest } = options;
  const only = accountSet(account);
  const threshold = checkThreshold(given);
  const interval = checkInterval(refreshInterval);
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
  const stop = () => {
    stopped = true;
    untrack();
    walk?.unsubscribe();
    refresh?.unsubscribe();
  };
  // Tracked until the reduction exists and tracks itself, so destroy() also
  // stops a start read in flight.
  const untrack = host.track(stop);
  const halted = () => stopped || rest.signal?.aborted === true;
  const report = (err: Error) => (rest.onError ?? host.onError)?.(err);
  const startRefresh = () => {
    const { maxErrors, maxBackoff, signal, onError } = rest;
    refresh = host.refresh(
      () => positions.keys(),
      (read) => {
        if (walk?.active !== true) return stop();
        for (const p of read) {
          const [changed, crossed] = apply(p);
          if (!changed) continue;
          const position = positions.get(p.account)!;
          try {
            onChange?.(position);
            if (crossed) onCross?.(position);
          } catch (err) {
            report(new StreamdError(`positions handler threw for ${p.account} at ${p.version}`, err));
          }
        }
      },
      { pollInterval: interval, maxErrors, maxBackoff, signal, onError },
    );
  };

  let notify: [LendingPosition, boolean][] = [];
  const ready = (async () => {
    try {
      let from = fromVersion;
      if (only !== undefined) {
        const at = fromVersion === undefined ? await host.head() : fromVersion - 1n;
        for (const p of await Promise.all([...only].map((a) => readAavePosition(host.view, a, at)))) apply(p);
        from = at + 1n;
      }
      if (halted()) throw new StreamdError("positions was stopped before it was ready");
      walk = reduce(
        only,
        AAVE_DESCRIPTOR,
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
          onChange: (_, event: StreamdEvent) => {
            for (const [p, crossed] of notify) {
              onChange?.(p, event);
              if (crossed) onCross?.(p, event);
            }
          },
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
      return !halted() && (walk === undefined || walk.active);
    },
    unsubscribe: stop,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: stop,
  } as Positions;
}
