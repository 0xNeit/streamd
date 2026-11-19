import { StreamdError } from "@streamd/core";
import type { ViewAt } from "./multisig.js";
import { address, amount } from "./records.js";
import { abortReason, isViewAbort } from "./transfers.js";
import type { LendingPosition, PositionAsset } from "./types.js";
import { fraction } from "./values.js";

/** Echelon's lending package on Aptos mainnet. It publishes no source; the figures below were read from its decompiled bytecode. */
export const ECHELON = "0xc6bc659f1649553c1a3fa05d9727433dc03843baac29473c817d06d39e7621ba";
/** `fixed_point64::FixedPoint64` holds `value * 2^64` as `v`. */
const ONE = 1n << 64n;

/**
 * Each Echelon event that changes a vault, and the fields naming the accounts
 * it changes. A repayer pays from its wallet; a liquidator's vault receives
 * the seized shares. Entering or leaving an efficiency mode, and the
 * manager's `clear_bad_debt` and `reconcile_20250218`, change a vault with no
 * event, so only a head read sees them.
 */
export const ECHELON_EVENTS: Record<string, readonly string[]> = {
  "lending::SupplyEvent": ["account_addr"],
  "lending::WithdrawEvent": ["account_addr"],
  "lending::BorrowEvent": ["account_addr"],
  "lending::RepayEvent": ["borrower_addr"],
  "lending::LiquidateEvent": ["borrower_addr", "liquidator_addr"],
};

/** A market's asset never changes, so it is read once per market and kept. */
export interface EchelonMarket {
  asset: string;
  decimals: number;
}
export type EchelonMarkets = Map<string, Promise<EchelonMarket>>;

const bad = (account: string, what: string) => new StreamdError(`echelon position ${account}: ${what} is not what the view returns`);

function int(value: unknown, account: string, what: string): bigint {
  const n = amount(value);
  if (n === undefined) throw bad(account, what);
  return n;
}

const fixed = (value: unknown, account: string, what: string) => int((value as { v?: unknown } | null)?.v, account, what);

function objects(reply: unknown[], account: string, what: string): string[] {
  if (!Array.isArray(reply[0])) throw bad(account, what);
  return (reply[0] as unknown[]).map((o) => {
    const found = address((o as { inner?: unknown } | null)?.inner);
    if (found === undefined) throw bad(account, what);
    return found;
  });
}

/** `10^d` back to `d`; undefined for anything else. */
function decimalsOf(mantissa: bigint): number | undefined {
  let d = 0;
  for (let m = 1n; m <= mantissa; m *= 10n, d++) if (m === mantissa) return d <= 255 ? d : undefined;
  return undefined;
}

type Call = (fn: string, args: string[]) => Promise<unknown[]>;

/** The market's asset: the coin type for a coin market, as Echelon names it, else the fungible asset's metadata address. */
function marketOf(call: Call, market: string, markets: EchelonMarkets, account: string): Promise<EchelonMarket> {
  const held = markets.get(market);
  if (held !== undefined) return held;
  const read = (async () => {
    const [[isCoin], [mantissa]] = await Promise.all([call("market_is_coin", [market]), call("market_asset_mantissa", [market])]);
    if (typeof isCoin !== "boolean") throw bad(account, "market type");
    const [id] = await call(isCoin ? "market_coin" : "market_asset_metadata", [market]);
    const asset = isCoin ? (typeof id === "string" && id.includes("::") ? id : undefined) : address((id as { inner?: unknown } | null)?.inner);
    const decimals = decimalsOf(int(mantissa, account, "market mantissa"));
    if (asset === undefined || decimals === undefined) throw bad(account, "market asset");
    return { asset, decimals };
  })();
  markets.set(market, read);
  read.catch(() => {
    if (markets.get(market) === read) markets.delete(market);
  });
  return read;
}

/**
 * `account`'s Echelon position at `version`. Echelon's liquidation check,
 * `can_liquidate`, is `account_liquidation_threshold < account_liability_value`,
 * evaluated after accruing the vault's interest; with a debt,
 * `preview_health_factor_given_repay(account, market, 0)` returns that pair after
 * that accrual, so `health` is their exact ratio and below 1 exactly when a
 * liquidation would pass. The plain `account_liability_value` omits interest
 * since the vault last accrued, so it is not used. Values are USD from
 * Echelon's own oracles; when a value view aborts (an oracle refusing a price
 * aborts the sum) the balances are kept and `unknown` says why. Any other
 * failure rejects; a reply not shaped as the view returns throws.
 */
export async function readEchelonPosition(view: ViewAt, account: string, version: bigint, markets: EchelonMarkets): Promise<LendingPosition> {
  const call: Call = (fn, args) => view({ function: `${ECHELON}::lending::${fn}`, typeArguments: [], functionArguments: args }, version);
  const position: LendingPosition = { protocol: "echelon", account, version, assets: [] };
  const [exists] = await call("vault_exists", [account]);
  if (typeof exists !== "boolean") throw bad(account, "vault");
  if (!exists) return { ...position, collateralValue: fraction(0n, 1n), debtValue: fraction(0n, 1n) };
  const [supplied, borrowed] = await Promise.all([
    call("account_collateral_markets", [account]).then((r) => objects(r, account, "collateral markets")),
    call("account_liability_markets", [account]).then((r) => objects(r, account, "liability markets")),
  ]);
  if (supplied.length === 0 && borrowed.length === 0) return { ...position, collateralValue: fraction(0n, 1n), debtValue: fraction(0n, 1n) };
  const [values, assets] = await Promise.all([
    Promise.all([
      call("account_lend_value", [account]),
      borrowed.length > 0
        ? call("preview_health_factor_given_repay", [account, borrowed[0]!, "0"])
        : call("account_liquidation_threshold", [account]),
    ]).then(
      (replies) => ({ replies }),
      (err: unknown) => {
        if (isViewAbort(err)) return { unknown: abortReason(err) };
        throw err;
      },
    ),
    Promise.all(
      [...new Set([...supplied, ...borrowed])].map(async (market): Promise<PositionAsset> => {
        const [info, coins, debt] = await Promise.all([
          marketOf(call, market, markets, account),
          supplied.includes(market) ? call("account_coins", [account, market]) : ["0"],
          borrowed.includes(market) ? call("account_liability", [account, market]) : ["0"],
        ]);
        const amounts = { supplied: int(coins[0], account, "coins"), borrowed: int(debt[0], account, "liability") };
        return { ...info, ...amounts, collateral: amounts.supplied > 0n };
      }),
    ),
  ]);
  const held = assets.filter((a) => a.supplied > 0n || a.borrowed > 0n).sort((a, b) => (a.asset < b.asset ? -1 : a.asset > b.asset ? 1 : 0));
  if ("unknown" in values) return { ...position, assets: held, unknown: values.unknown };
  const [lendReply, figures] = values.replies;
  const lend = fixed(lendReply[0], account, "lend value");
  const threshold = fixed(figures[0], account, "liquidation threshold");
  const liability = borrowed.length > 0 ? fixed(figures[1], account, "liability value") : 0n;
  return {
    ...position,
    assets: held,
    collateralValue: fraction(lend, ONE),
    debtValue: fraction(liability, ONE),
    ...(lend > 0n && { liquidationThreshold: fraction(threshold, lend) }),
    ...(liability > 0n && { health: fraction(threshold, liability) }),
  };
}
