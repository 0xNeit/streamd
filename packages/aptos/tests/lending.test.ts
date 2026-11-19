import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { ViewRequest } from "@streamd/core";
import { AAVE, isBelow, readAavePosition } from "../src/lending.js";
import type { ViewAt } from "../src/lending.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const USER = long("5229");
const APT = long("a");
const USDC = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";
const MAX = ((1n << 256n) - 1n).toString();

/** Mainnet's replies for 0x5229ed9f...b5d5 at the head on 2026-11-19, trimmed to two reserves. */
const reserve = (asset: string, decimals: string, scaledA: string, scaledDebt: string) => ({
  decimals,
  scaled_a_token_balance: scaledA,
  scaled_variable_debt: scaledDebt,
  underlying_asset: asset,
  usage_as_collateral_enabled_on_user: scaledA !== "0",
});
const HEAD = {
  get_user_account_data: ["35597659058635600000", "17324075874376000000", "3322566379632648000", "6300", "5800", "1294529380358547658"],
  get_user_reserves_data: [[reserve(USDC, "6", "0", "0"), reserve("0xa", "8", "8141523101", "3865540289")], 0],
  get_user_reserve_data: ["8221548122", "4001126120", "3865540289", "27563785451517651155326", true],
} as Record<string, unknown[]>;

/** Answers each view by function name, recording what was asked. */
function views(replies: Record<string, unknown[] | Error>, asked: [string, unknown[], bigint][] = []): ViewAt {
  return async (request: ViewRequest, version: bigint) => {
    expect(request.function.startsWith(`${AAVE}::`)).toBe(true);
    const name = request.function.split("::")[2]!;
    asked.push([name, [...(request.functionArguments ?? [])], version]);
    const reply = replies[name];
    if (reply === undefined) throw new Error(`unexpected view ${name}`);
    if (reply instanceof Error) throw reply;
    return reply;
  };
}

describe("readAavePosition", () => {
  it("takes the protocol's figures as exact fractions and reads only reserves holding a balance", async () => {
    const asked: [string, unknown[], bigint][] = [];
    const position = await readAavePosition(views(HEAD, asked), USER, 8123600000n);
    expect(position).toEqual({
      protocol: "aave",
      account: USER,
      version: 8123600000n,
      assets: [{ asset: APT, decimals: 8, supplied: 8221548122n, borrowed: 4001126120n, collateral: true }],
      collateralValue: { numerator: 88994147646589n, denominator: 2500000000000n },
      debtValue: { numerator: 2165509484297n, denominator: 125000000000n },
      liquidationThreshold: { numerator: 63n, denominator: 100n },
      health: { numerator: 647264690179273829n, denominator: 500000000000000000n },
    });
    expect(asked.filter(([name]) => name === "get_user_reserve_data")).toEqual([["get_user_reserve_data", [APT, USER], 8123600000n]]);
    expect(asked.every(([, , v]) => v === 8123600000n)).toBe(true);
    expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBe(false);
    expect(isBelow(position, { numerator: 13n, denominator: 10n })).toBe(true);
  });

  it("has no health with no debt, and is never below then", async () => {
    const position = await readAavePosition(
      views({ ...HEAD, get_user_account_data: ["0", "0", "0", "0", "0", MAX], get_user_reserves_data: [[reserve(USDC, "6", "0", "0")], 0] }),
      USER,
      1n,
    );
    expect(position.assets).toEqual([]);
    expect(position.health).toBeUndefined();
    expect(position.debtValue).toEqual({ numerator: 0n, denominator: 1n });
    expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBe(false);
  });

  it("keeps the balances and says why when the account data view aborts", async () => {
    const abort = new StreamdError("Fullnode view failed: Move abort in 0x249676f3::oracle: E_ORACLE_STALE");
    const position = await readAavePosition(views({ ...HEAD, get_user_account_data: abort }), USER, 1n);
    expect(position.unknown).toContain("E_ORACLE_STALE");
    expect(position.assets).toHaveLength(1);
    expect(position.health).toBeUndefined();
    expect(position.collateralValue).toBeUndefined();
    expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBeUndefined();
  });

  it("rejects a network failure and a refusal that is not an abort", async () => {
    await expect(readAavePosition(views({ ...HEAD, get_user_account_data: new StreamdNetworkError("503") }), USER, 1n)).rejects.toThrow("503");
    const setup = Object.assign(new StreamdError("adapter does not run views"), { aborted: false });
    await expect(readAavePosition(views({ ...HEAD, get_user_account_data: setup }), USER, 1n)).rejects.toThrow("does not run views");
    await expect(readAavePosition(views({ ...HEAD, get_user_reserve_data: new StreamdNetworkError("429") }), USER, 1n)).rejects.toThrow("429");
  });

  it("refuses replies not shaped as the views return", async () => {
    const read = (replies: Record<string, unknown[]>) => readAavePosition(views({ ...HEAD, ...replies }), USER, 1n);
    await expect(read({ get_user_account_data: ["1", "2", "3"] })).rejects.toThrow("account data is not");
    await expect(read({ get_user_account_data: ["1", "2", "3", "4", "5", 6] })).rejects.toThrow("account data is not");
    await expect(read({ get_user_reserves_data: [{}, 0] })).rejects.toThrow("reserves is not");
    await expect(read({ get_user_reserves_data: [[{ ...reserve("0xa", "8", "1", "0"), underlying_asset: 7 }], 0] })).rejects.toThrow("reserves is not");
    await expect(read({ get_user_reserves_data: [[reserve("0xa", "300", "1", "0")], 0] })).rejects.toThrow("reserves is not");
    await expect(read({ get_user_reserve_data: ["1", "2", "3", "4", "yes"] })).rejects.toThrow("reserve data is not");
    await expect(read({ get_user_reserve_data: ["-1", "2", "3", "4", true] })).rejects.toThrow("reserve data is not");
  });
});

describe("isBelow", () => {
  const at = (numerator: bigint, denominator: bigint) => ({ protocol: "aave" as const, account: USER, version: 1n, assets: [], health: { numerator, denominator } });

  it("compares exactly, a health equal to the threshold not being below", () => {
    const one = { numerator: 1n, denominator: 1n };
    expect(isBelow(at(10n ** 18n, 10n ** 18n), one)).toBe(false);
    expect(isBelow(at(10n ** 18n - 1n, 10n ** 18n), one)).toBe(true);
    expect(isBelow(at(11n, 10n), { numerator: 110n, denominator: 100n })).toBe(false);
  });
});
