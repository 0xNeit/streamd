import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { StreamdEvent, ViewRequest } from "@streamd/core";
import { ECHELON, readEchelonPosition } from "../src/echelon.js";
import type { EchelonMarkets } from "../src/echelon.js";
import { accountsOf, isBelow } from "../src/lending.js";
import type { ViewAt } from "../src/lending.js";
import { ECHELON_DESCRIPTOR } from "../src/index.js";
import { testClient } from "./helpers.js";

const USER = "0x93c82b4669112ac419babac66281b87579a0cc97247394d359c2ab57d33ca9e3";
const USDC_M = "0xa9c51ca3bcd93978d0c4aada7c4cf47c0791caced3cdc4e15f2c8e0797d1f93c";
const APT_M = "0x761a97787fa8b3ae0cef91ebc2d96e56cc539df5bc88dadabee98ae00363a831";
const STAPT_M = "0xc3c9cc885a52004e0490f6a8291b3bc5854f0e42d931fb124736bd189c9f0f45";
const USDT_M = "0x447b3b516546f28e8c4f6825a6287b09161659e7c500c599c29c28a8492844b8";
const TRUAPT_M = "0x7bd5be03df0fb3841fed337b8e7a353818b917cce6db28b807b691002a122d73";
const TRUAPT = "0xaef6a8c3182e076db72d64324617114cacf9a52f28325edc10b483f7f05da0e7";
const obj = (inner: string) => ({ inner });

/** Mainnet's market replies, read 2026-11-19. */
const MARKETS: Record<string, [boolean, string, unknown]> = {
  [USDC_M]: [true, "1000000", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC"],
  [APT_M]: [true, "100000000", "0x1::aptos_coin::AptosCoin"],
  [STAPT_M]: [true, "100000000", "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::stapt_token::StakedApt"],
  [USDT_M]: [true, "1000000", "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT"],
  [TRUAPT_M]: [false, "100000000", obj(TRUAPT)],
};

type Reply = unknown[] | Error | ((args: unknown[]) => unknown[] | Error);

/** Mainnet's replies for 0x93c82b46...a9e3 at 8114875140, the version before its liquidation. */
const BEFORE: Record<string, Reply> = {
  vault_exists: [true],
  account_collateral_markets: [[obj(USDC_M), obj(APT_M), obj(STAPT_M)]],
  account_liability_markets: [[obj(USDT_M)]],
  account_lend_value: [{ v: "33929853835697841219" }],
  preview_health_factor_given_repay: [{ v: "12114423161962663141" }, { v: "12145825618505606018" }],
  account_liquidation_threshold: [{ v: "12114423161962663141" }],
  account_coins: (args) => [{ [USDC_M]: "813416", [APT_M]: "49", [STAPT_M]: "200010264" }[args[1] as string] ?? "0"],
  account_liability: (args) => [args[1] === USDT_M ? "659359" : "0"],
  market_is_coin: (args) => [MARKETS[args[0] as string]![0]],
  market_asset_mantissa: (args) => [MARKETS[args[0] as string]![1]],
  market_coin: (args) => [MARKETS[args[0] as string]![2]],
  market_asset_metadata: (args) => [MARKETS[args[0] as string]![2]],
};

function views(replies: Record<string, Reply>, asked: [string, unknown[], bigint][] = []): ViewAt {
  return async (request: ViewRequest, version: bigint) => {
    expect(request.function.startsWith(`${ECHELON}::lending::`)).toBe(true);
    const name = request.function.split("::")[2]!;
    const args = [...(request.functionArguments ?? [])];
    asked.push([name, args, version]);
    let reply = replies[name];
    if (typeof reply === "function") reply = reply(args);
    if (reply === undefined) throw new Error(`unexpected view ${name}`);
    if (reply instanceof Error) throw reply;
    return reply;
  };
}

const read = (replies: Record<string, Reply>, markets: EchelonMarkets = new Map(), asked?: [string, unknown[], bigint][]) =>
  readEchelonPosition(views(replies, asked), USER, 8114875140n, markets);

const abort = (where: string) =>
  new StreamdError(
    `Fullnode view ${ECHELON}::lending::account_lend_value at version 7 failed: Request to [Fullnode]: POST https://node.example/v1/view failed with: {"message":"Move abort in ${where}: ERR_ORACLE_INVALID_PRICE(0x6b): ","error_code":"invalid_input"}`,
  );

describe("readEchelonPosition", () => {
  it("takes health from the accrued threshold and liability, exactly, and reads each market's asset", async () => {
    const asked: [string, unknown[], bigint][] = [];
    const position = await read(BEFORE, new Map(), asked);
    expect(position.protocol).toBe("echelon");
    expect(position.version).toBe(8114875140n);
    // 12114423161962663141 / 12145825618505606018, reduced: below 1, as Echelon's liquidation found it.
    expect(position.health!.numerator * 12145825618505606018n).toBe(position.health!.denominator * 12114423161962663141n);
    expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBe(true);
    expect(position.collateralValue).toEqual({ numerator: 33929853835697841219n, denominator: 1n << 64n });
    expect(position.debtValue!.numerator * (1n << 64n)).toBe(12145825618505606018n * position.debtValue!.denominator);
    expect(position.liquidationThreshold!.numerator * 33929853835697841219n).toBe(12114423161962663141n * position.liquidationThreshold!.denominator);
    expect(position.assets).toEqual([
      { asset: "0x111ae3e5bc816a5e63c2da97d0aa3886519e0cd5e4b046659fa35796bd11542a::stapt_token::StakedApt", decimals: 8, supplied: 200010264n, borrowed: 0n, collateral: true },
      { asset: "0x1::aptos_coin::AptosCoin", decimals: 8, supplied: 49n, borrowed: 0n, collateral: true },
      { asset: "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC", decimals: 6, supplied: 813416n, borrowed: 0n, collateral: true },
      { asset: "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT", decimals: 6, supplied: 0n, borrowed: 659359n, collateral: false },
    ]);
    expect(asked.find(([n]) => n === "preview_health_factor_given_repay")![1]).toEqual([USER, USDT_M, "0"]);
    expect(asked.some(([n]) => n === "account_liquidation_threshold" || n === "account_liability_value")).toBe(false);
    expect(asked.every(([, , v]) => v === 8114875140n)).toBe(true);
  });

  it("reads a market's asset once per cache, and a fungible asset market by its metadata", async () => {
    const markets: EchelonMarkets = new Map();
    const fa = { ...BEFORE, account_collateral_markets: [[obj(TRUAPT_M)]], account_coins: ["5"] };
    const asked: [string, unknown[], bigint][] = [];
    await read(fa, markets, asked);
    const second = await read(fa, markets, asked);
    expect(second.assets.find((a) => a.asset === TRUAPT)).toEqual({ asset: TRUAPT, decimals: 8, supplied: 5n, borrowed: 0n, collateral: true });
    expect(asked.filter(([n]) => n === "market_asset_mantissa")).toHaveLength(2);
    expect(asked.filter(([n]) => n === "market_coin")).toHaveLength(1);
    expect(asked.filter(([n]) => n === "market_asset_metadata")).toHaveLength(1);
  });

  it("reads a market again after a failed read of it", async () => {
    const markets: EchelonMarkets = new Map();
    await expect(read({ ...BEFORE, market_asset_mantissa: new StreamdNetworkError("429") }, markets)).rejects.toThrow("429");
    expect(markets.size).toBe(0);
    expect((await read(BEFORE, markets)).assets).toHaveLength(4);
  });

  it("has no health with no vault or no debt, and is never below then", async () => {
    const none = await read({ vault_exists: [false] });
    expect(none.assets).toEqual([]);
    expect(none.collateralValue).toEqual({ numerator: 0n, denominator: 1n });
    expect(none.health).toBeUndefined();
    expect(isBelow(none, { numerator: 1n, denominator: 1n })).toBe(false);
    const asked: [string, unknown[], bigint][] = [];
    const lender = await read({ ...BEFORE, account_liability_markets: [[]] }, new Map(), asked);
    expect(lender.health).toBeUndefined();
    expect(lender.debtValue).toEqual({ numerator: 0n, denominator: 1n });
    expect(lender.liquidationThreshold!.numerator * 33929853835697841219n).toBe(12114423161962663141n * lender.liquidationThreshold!.denominator);
    expect(asked.some(([n]) => n === "preview_health_factor_given_repay")).toBe(false);
    expect(isBelow(lender, { numerator: 1n, denominator: 1n })).toBe(false);
    const empty = await read({ ...BEFORE, account_collateral_markets: [[]], account_liability_markets: [[]], account_lend_value: [{ v: "0" }], account_liquidation_threshold: [{ v: "0" }] });
    expect(empty.liquidationThreshold).toBeUndefined();
    expect(empty.health).toBeUndefined();
  });

  it("has no health when the accrued liability is 0, and is not below then", async () => {
    const position = await read({ ...BEFORE, preview_health_factor_given_repay: [{ v: "5" }, { v: "0" }] });
    expect(position.health).toBeUndefined();
    expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBe(false);
  });

  it("reads coins only where supplied and debt only where borrowed, and a borrow beside an empty supply is not collateral", async () => {
    const asked: [string, unknown[], bigint][] = [];
    await read(BEFORE, new Map(), asked);
    expect(asked.filter(([n, args]) => n === "account_coins" && args[1] === USDT_M)).toEqual([]);
    expect(asked.filter(([n, args]) => n === "account_liability" && args[1] !== USDT_M)).toEqual([]);
    const both = await read({ ...BEFORE, account_collateral_markets: [[obj(USDT_M)]], account_coins: ["0"] });
    expect(both.assets).toEqual([{ asset: MARKETS[USDT_M]![2] as string, decimals: 6, supplied: 0n, borrowed: 659359n, collateral: false }]);
  });

  it("skips the value views for a vault holding nothing", async () => {
    const asked: [string, unknown[], bigint][] = [];
    const position = await read({ ...BEFORE, account_collateral_markets: [[]], account_liability_markets: [[]] }, new Map(), asked);
    expect(position).toMatchObject({ assets: [], collateralValue: { numerator: 0n, denominator: 1n }, debtValue: { numerator: 0n, denominator: 1n } });
    expect(asked.map(([n]) => n).sort()).toEqual(["account_collateral_markets", "account_liability_markets", "vault_exists"]);
  });

  it("drops markets holding nothing", async () => {
    const position = await read({ ...BEFORE, account_coins: ["0"] });
    expect(position.assets.map((a) => a.borrowed)).toEqual([659359n]);
  });

  it("keeps the balances and says why when the oracle refuses a price, without the node's URL", async () => {
    const tiered = "0x92e95ed77b5ac815d3fbc2227e76db238339e9ca43ace45031ec2589bea5b8c::tiered_oracle";
    for (const fn of ["account_lend_value", "preview_health_factor_given_repay"]) {
      const position = await read({ ...BEFORE, [fn]: abort(tiered) });
      expect(position.unknown).toBe(`Move abort in ${tiered}: ERR_ORACLE_INVALID_PRICE(0x6b):`);
      expect(position.assets).toHaveLength(4);
      expect(position.health).toBeUndefined();
      expect(position.collateralValue).toBeUndefined();
      expect(isBelow(position, { numerator: 1n, denominator: 1n })).toBeUndefined();
    }
  });

  it("rejects a network failure, and an abort outside the value views", async () => {
    await expect(read({ ...BEFORE, account_lend_value: new StreamdNetworkError("503") })).rejects.toThrow("503");
    await expect(read({ ...BEFORE, account_coins: new StreamdNetworkError("429") })).rejects.toThrow("429");
    await expect(read({ ...BEFORE, account_liability: abort("0x1::x") })).rejects.toThrow("Move abort");
  });

  it("refuses replies not shaped as the views return", async () => {
    await expect(read({ ...BEFORE, vault_exists: ["yes"] })).rejects.toThrow("vault is not");
    await expect(read({ ...BEFORE, account_collateral_markets: [{}] })).rejects.toThrow("collateral markets is not");
    await expect(read({ ...BEFORE, account_liability_markets: [[{ inner: 7 }]] })).rejects.toThrow("liability markets is not");
    await expect(read({ ...BEFORE, account_lend_value: ["33929853835697841219"] })).rejects.toThrow("lend value is not");
    await expect(read({ ...BEFORE, preview_health_factor_given_repay: [{ v: "1" }] })).rejects.toThrow("liability value is not");
    await expect(read({ ...BEFORE, account_coins: ["-1"] })).rejects.toThrow("coins is not");
    await expect(read({ ...BEFORE, market_asset_mantissa: ["1000"] }).then(() => "read")).resolves.toBe("read");
    await expect(read({ ...BEFORE, market_asset_mantissa: ["1200"] })).rejects.toThrow("market asset is not");
    await expect(read({ ...BEFORE, market_asset_mantissa: ["0"] })).rejects.toThrow("market asset is not");
    await expect(read({ ...BEFORE, market_is_coin: [1] })).rejects.toThrow("market type is not");
    await expect(read({ ...BEFORE, market_coin: ["AptosCoin"] })).rejects.toThrow("market asset is not");
    await expect(read({ ...BEFORE, market_asset_mantissa: [(10n ** 256n).toString()] })).rejects.toThrow("market asset is not");
    await expect(read({ ...BEFORE, preview_health_factor_given_repay: [{}, { v: "1" }] })).rejects.toThrow("liquidation threshold is not");
    await expect(read({ ...BEFORE, account_liability_markets: [[]], account_liquidation_threshold: ["x"] })).rejects.toThrow("liquidation threshold is not");
  });
});

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [A, B, C] = [long("a1"), long("b1"), long("c1")];
const ev = (version: number, name: string, data: Record<string, unknown>, eventIndex = 0): StreamdEvent => ({
  type: `${ECHELON}::lending::${name}`,
  data,
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex,
});

describe("ECHELON_DESCRIPTOR", () => {
  it("is exported, one scan over the five vault-changing event types", () => {
    expect(ECHELON_DESCRIPTOR.kind).toBe("module");
    expect([ECHELON_DESCRIPTOR.eventType].flat().sort() as string[]).toEqual(
      ["BorrowEvent", "LiquidateEvent", "RepayEvent", "SupplyEvent", "WithdrawEvent"].map((n) => `${ECHELON}::lending::${n}`),
    );
  });

  it("names the accounts each event type changes: both sides of a liquidation, not a repayer", () => {
    const market = obj(USDT_M);
    expect(accountsOf(ev(1, "SupplyEvent", { account_addr: A, market_obj: market, amount: "1" }))).toEqual([A]);
    expect(accountsOf(ev(1, "WithdrawEvent", { account_addr: A, market_obj: market, amount: "1" }))).toEqual([A]);
    expect(accountsOf(ev(1, "BorrowEvent", { account_addr: A, market_obj: market, amount: "1" }))).toEqual([A]);
    expect(accountsOf(ev(1, "RepayEvent", { borrower_addr: A, repayer_addr: B, market_obj: market }))).toEqual([A]);
    expect(accountsOf(ev(1, "LiquidateEvent", { liquidator_addr: B, borrower_addr: A, repay_amount: "1" }))).toEqual([A, B]);
    expect(accountsOf(ev(1, "AccrueVaultInterestEvent", { account_addr: A }))).toEqual([]);
    expect(() => accountsOf(ev(1, "RepayEvent", { repayer_addr: B }))).toThrow("does not have its struct's fields");
  });
});

/**
 * A memory adapter whose Echelon views give every account one USDt debt and
 * one USDC supply, with the threshold `threshold(account, version)` against a
 * liability of 1, recording each health read as `account@version` and each
 * market read.
 */
function echelonAdapter(events: StreamdEvent[], threshold: (account: string, version: bigint) => bigint | Error) {
  const memory = new MemoryAdapter(events);
  const reads: string[] = [];
  const marketReads: string[] = [];
  const one = (1n << 64n).toString();
  const replies: Record<string, Reply> = {
    ...BEFORE,
    account_collateral_markets: [[obj(USDC_M)]],
    account_coins: ["5"],
    account_lend_value: [{ v: one }],
  };
  const view = async (request: ViewRequest, version: bigint) => {
    const name = request.function.split("::")[2]!;
    const args = [...(request.functionArguments ?? [])];
    if (name.startsWith("market_")) marketReads.push(name);
    if (name === "preview_health_factor_given_repay") {
      reads.push(`${String(args[0]).slice(-2)}@${version}`);
      const t = threshold(String(args[0]), version);
      if (t instanceof Error) throw t;
      return [{ v: t.toString() }, { v: one }];
    }
    const reply = replies[name];
    return typeof reply === "function" ? (reply(args) as unknown[]) : (reply as unknown[]);
  };
  return { adapter: Object.assign(memory, { view }), memory, reads, marketReads };
}

const waitFor = async (check: () => boolean) => {
  for (let i = 0; i < 400 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  if (!check()) throw new Error("timed out waiting");
};

describe("positions on echelon", () => {
  const HALF = 1n << 63n;

  it("reads at the start, at each event naming a tracked account, and at the head, reporting a crossing", async () => {
    const t: Record<string, bigint | Error> = { [A]: 2n << 64n, [B]: 3n << 64n };
    const { adapter, memory, reads, marketReads } = echelonAdapter(
      [ev(10, "BorrowEvent", { account_addr: A }), ev(11, "SupplyEvent", { account_addr: C })],
      (account) => t[account] ?? 1n,
    );
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const crossed: [string, bigint, boolean | undefined, boolean][] = [];
    const positions = client.positions("echelon", {
      account: [A, B],
      fromVersion: 5n,
      refreshInterval: 10,
      onCross: (p, e) => crossed.push([p.account, p.version, p.below, e === undefined]),
    });
    expect(client.listenerCount()).toBe(0);
    await positions.ready;
    expect(reads.sort()).toEqual(["a1@10", "a1@4", "b1@4"]);
    expect(positions.get(A)).toMatchObject({ protocol: "echelon", version: 10n, health: { numerator: 2n, denominator: 1n }, below: false });
    t[B] = HALF;
    memory.push(ev(20, "LiquidateEvent", { borrower_addr: long("d1"), liquidator_addr: B }));
    await waitFor(() => crossed.length === 1);
    expect(crossed[0]).toEqual([B, 20n, true, false]);
    t[A] = HALF;
    memory.push(ev(30, "SupplyEvent", { account_addr: long("e1") }));
    await waitFor(() => crossed.length === 2);
    expect(crossed[1]).toEqual([A, 30n, true, true]);
    expect(marketReads.filter((n) => n === "market_is_coin")).toHaveLength(2);
    client.destroy();
    expect(positions.active).toBe(false);
  });

  it("holds an unknown position while the oracle refuses a price", async () => {
    const { adapter } = echelonAdapter([], () => new StreamdError("Move abort in 0x92e9::tiered_oracle: ERR_ORACLE_INVALID_PRICE(0x6b): "));
    const client = testClient({ adapter });
    const positions = client.positions("echelon", { account: A, refreshInterval: 0 });
    await positions.ready;
    expect(positions.get(A)).toMatchObject({ unknown: "Move abort in 0x92e9::tiered_oracle: ERR_ORACLE_INVALID_PRICE(0x6b):", assets: [{ supplied: 5n }, { borrowed: 659359n }] });
    expect(positions.get(A)!.below).toBeUndefined();
    client.destroy();
  });
});
