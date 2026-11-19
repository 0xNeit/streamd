import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { StreamdEvent, ViewRequest } from "@streamd/core";
import { AAVE, accountsOf, isBelow, readAavePosition } from "../src/lending.js";
import type { ViewAt } from "../src/lending.js";
import type { LendingPosition } from "../src/types.js";
import { AAVE_DESCRIPTOR } from "../src/index.js";
import { testClient } from "./helpers.js";

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

const ev = (version: number, name: string, data: Record<string, unknown>, eventIndex = 0): StreamdEvent => ({
  type: `${AAVE}::${name}`,
  data,
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex,
});
const [A, B, C] = [long("a1"), long("b1"), long("c1")];
const WAD = 10n ** 18n;

/**
 * A memory adapter whose Aave views give every account one unit of debt and
 * the health factor `health(account, version)` returns, recording each
 * account data read as `account@version`.
 */
function aaveAdapter(events: StreamdEvent[], health: (account: string, version: bigint) => bigint | Error = () => 2n * WAD) {
  const memory = new MemoryAdapter(events);
  const reads: string[] = [];
  const view = async (request: ViewRequest, version: bigint) => {
    const name = request.function.split("::")[2]!;
    const account = String(request.functionArguments![0]);
    if (name === "get_user_reserves_data") return [[], 0];
    reads.push(`${account.slice(-2)}@${version}`);
    const hf = health(account, version);
    if (hf instanceof Error) throw hf;
    return ["2", "1", "0", "8000", "7000", hf.toString()];
  };
  return { adapter: Object.assign(memory, { view }), memory, reads };
}

const waitFor = async (check: () => boolean) => {
  for (let i = 0; i < 100 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  if (!check()) throw new Error("timed out waiting");
};

describe("AAVE_DESCRIPTOR", () => {
  it("is exported, one scan over the ten position-changing event types", () => {
    expect(AAVE_DESCRIPTOR.kind).toBe("module");
    expect(AAVE_DESCRIPTOR.eventType).toHaveLength(10);
    expect([AAVE_DESCRIPTOR.eventType].flat().every((t) => t.startsWith(`${AAVE}::`))).toBe(true);
  });
});

describe("accountsOf", () => {
  it("names each account an event changes, both sides of a transfer, and refuses one without its fields", () => {
    expect(accountsOf(ev(1, "supply_logic::Supply", { user: A, on_behalf_of: B }))).toEqual([B]);
    expect(accountsOf(ev(1, "events::BalanceTransfer", { from: A, to: "0xc1" }))).toEqual([A, C]);
    expect(accountsOf(ev(1, "events::BalanceTransfer", { from: A, to: A }))).toEqual([A]);
    expect(accountsOf({ ...ev(1, "x", {}), type: "0x1::coin::Deposit" })).toEqual([]);
    expect(() => accountsOf(ev(1, "borrow_logic::Repay", { repayer: A }))).toThrow("does not have its struct's fields");
  });
});

describe("positions", () => {
  const history = [
    ev(10, "supply_logic::Supply", { user: C, on_behalf_of: C }),
    ev(11, "borrow_logic::Borrow", { user: A, on_behalf_of: A }),
    ev(11, "events::ReserveUsedAsCollateralEnabled", { user: A }, 1),
    ev(20, "events::BalanceTransfer", { from: A, to: B }),
  ];

  it("reads the accounts before fromVersion, then once per account and version of events naming them", async () => {
    const { adapter, reads } = aaveAdapter(history);
    const client = testClient({ adapter });
    const changed: string[] = [];
    const positions = client.positions("aave", { account: ["0xa1", B], fromVersion: 5n, refreshInterval: 0, onChange: (p) => changed.push(p.account) });
    expect(client.listenerCount()).toBe(0);
    const value = await positions.ready;
    expect(value).toBe(positions.value);
    expect(reads.sort()).toEqual(["a1@11", "a1@20", "a1@4", "b1@20", "b1@4"]);
    expect([...value.keys()].sort()).toEqual([A, B]);
    expect(positions.get("0xa1")).toMatchObject({ protocol: "aave", version: 20n, health: { numerator: 2n, denominator: 1n }, below: false });
    expect(changed).toEqual([]);
    positions.unsubscribe();
    expect(positions.active).toBe(false);
    client.destroy();
  });

  it("tracks every account an event names without account, and calls onChange for live events only", async () => {
    const { adapter, memory } = aaveAdapter(history, (_, version) => 2n * WAD + version);
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const changed: [string, bigint][] = [];
    const positions = client.positions("aave", { fromVersion: 1n, refreshInterval: 0, onChange: (p, e) => changed.push([p.account, e!.version]) });
    await positions.ready;
    expect([...positions.value.keys()].sort()).toEqual([A, B, C]);
    memory.push(ev(30, "borrow_logic::Repay", { user: C, repayer: A }), ev(31, "borrow_logic::Repay", { user: C, repayer: A }));
    await waitFor(() => changed.length === 2);
    expect(changed).toEqual([[C, 30n], [C, 31n]]);
    expect(positions.get(C)!.version).toBe(31n);
    client.destroy();
    expect(positions.active).toBe(false);
  });

  it("reports a crossing once each way, and not on an account's first read or while unknown", async () => {
    const hf: Record<string, bigint | Error> = { [A]: 2n * WAD, [B]: WAD / 2n };
    const { adapter, memory } = aaveAdapter([ev(10, "supply_logic::Supply", { user: A, on_behalf_of: A })], (account) => hf[account] ?? WAD);
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const crossed: [string, boolean | undefined][] = [];
    const changed: bigint[] = [];
    const positions = client.positions("aave", {
      account: [A, B],
      threshold: { numerator: 11n, denominator: 10n },
      refreshInterval: 0,
      onCross: (p) => crossed.push([p.account, p.below]),
      onChange: (p) => changed.push(p.version),
    });
    await positions.ready;
    expect(positions.get(B)!.below).toBe(true);
    hf[A] = WAD;
    memory.push(ev(20, "supply_logic::Withdraw", { user: A }));
    hf[B] = new StreamdError("Move abort in oracle: E_STALE");
    memory.push(ev(30, "supply_logic::Withdraw", { user: B }));
    await waitFor(() => changed.length === 2);
    expect(positions.get(B)).toMatchObject({ version: 30n, unknown: "Move abort in oracle: E_STALE" });
    expect(positions.get(B)!.below).toBeUndefined();
    hf[B] = WAD;
    memory.push(ev(40, "supply_logic::Withdraw", { user: B }));
    hf[A] = 3n * WAD;
    memory.push(ev(50, "supply_logic::Withdraw", { user: A }));
    await waitFor(() => changed.length === 4);
    expect(crossed).toEqual([[A, true], [A, false]]);
    client.destroy();
  });

  it("refuses bad options and adapters at the call, and rejects ready when a start read fails", async () => {
    const { adapter } = aaveAdapter([], () => new StreamdNetworkError("503"));
    const client = testClient({ adapter });
    expect(() => client.positions("echelon" as never)).toThrow("positions reads one of: aave");
    expect(() => client.positions("aave", { where: () => true } as never)).toThrow("Unsupported positions option: where");
    expect(() => client.positions("aave", { account: [] })).toThrow("account must be an address");
    expect(() => client.positions("aave", { account: "alice.apt" })).toThrow("account must be an address");
    expect(() => client.positions("aave", { threshold: { numerator: 1n, denominator: 0n } })).toThrow("threshold must be");
    expect(() => client.positions("aave", { threshold: { numerator: -1n, denominator: 1n } })).toThrow("threshold must be");
    expect(() => client.positions("aave", { threshold: 1 as never })).toThrow("threshold must be");
    expect(() => client.positions("aave", { fromVersion: 0n })).toThrow("fromVersion must be a bigint");
    expect(() => testClient({ adapter: new MemoryAdapter() }).positions("aave")).toThrow("implements view and getHeadVersion");
    const failed = client.positions("aave", { account: A });
    await expect(failed.ready).rejects.toThrow("503");
    expect(failed.active).toBe(false);
    client.destroy();
  });
});

describe("positions refresh", () => {
  const other = (version: number) => ev(version, "supply_logic::Supply", { user: long("ff"), on_behalf_of: long("ff") });

  it("rereads tracked positions at the head, so a price move alone is seen and crosses", async () => {
    let hf = 2n * WAD;
    const { adapter, memory, reads } = aaveAdapter([other(10)], () => hf);
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const seen: [bigint, boolean | undefined, boolean][] = [];
    const positions = client.positions("aave", {
      account: A,
      refreshInterval: 10,
      onChange: (p, e) => seen.push([p.version, p.below, e === undefined]),
      onCross: (p, e) => seen.push([p.version, p.below, e === undefined]),
    });
    await positions.ready;
    hf = WAD / 2n;
    memory.push(other(20));
    await waitFor(() => seen.length === 2);
    expect(seen).toEqual([[20n, true, true], [20n, true, true]]);
    expect(reads).toContain("a1@20");
    positions.unsubscribe();
    const after = reads.length;
    memory.push(other(30));
    await new Promise((r) => setTimeout(r, 50));
    expect(reads.length).toBe(after);
    client.destroy();
  });

  it("keeps a newer head read over an older event read that lands after it", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { adapter, memory } = aaveAdapter([other(10)], (_, version) => 2n * WAD + version);
    const view = adapter.view;
    const slow = Object.assign(adapter, {
      view: async (request: ViewRequest, version: bigint) => (version === 40n && (await gate), view(request, version)),
    });
    const client = testClient({ adapter: slow, defaults: { pollInterval: 5 } });
    const changed: bigint[] = [];
    const positions = client.positions("aave", { account: A, refreshInterval: 10, onChange: (p) => changed.push(p.version) });
    await positions.ready;
    memory.push(ev(40, "supply_logic::Withdraw", { user: A }), other(50));
    await waitFor(() => changed.includes(50n));
    release();
    await new Promise((r) => setTimeout(r, 50));
    expect(positions.get(A)!.version).toBe(50n);
    expect(changed).toEqual([50n]);
    client.destroy();
  });

  it("reads nothing at the head with refreshInterval 0, and reports a failed refresh without stopping", async () => {
    let fail = false;
    const { adapter, memory, reads } = aaveAdapter([other(10)], () => (fail ? new StreamdNetworkError("503") : 2n * WAD));
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const none = client.positions("aave", { account: A, refreshInterval: 0 });
    await none.ready;
    memory.push(other(20));
    await new Promise((r) => setTimeout(r, 50));
    expect(reads).toEqual(["a1@10"]);
    none.unsubscribe();
    const errors: string[] = [];
    const failing = client.positions("aave", { account: B, refreshInterval: 10, maxErrors: 100, onError: (e) => errors.push(`${e.message} ${String((e as { cause?: unknown }).cause)}`) });
    await failing.ready;
    fail = true;
    memory.push(other(30));
    await waitFor(() => errors.length > 0);
    expect(errors[0]).toContain("503");
    expect(failing.active).toBe(true);
    expect(() => client.positions("aave", { refreshInterval: -1 })).toThrow("refreshInterval must be");
    expect(() => client.positions("aave", { refreshInterval: "1" as never })).toThrow("refreshInterval must be");
    client.destroy();
  });
});
