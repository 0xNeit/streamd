import { StreamdError } from "@streamd/core";
import type { ViewRequest } from "@streamd/core";
import { address, amount } from "./records.js";
import type { Fields } from "./records.js";
import { isViewAbort } from "./transfers.js";
import type { Fraction, LendingPosition, PositionAsset } from "./types.js";
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
