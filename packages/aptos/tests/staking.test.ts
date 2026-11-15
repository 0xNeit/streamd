import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, parseEvent } from "@streamd/core";
import type { EventAdapter, FetchOptions, StreamdEvent } from "@streamd/core";
import { STAKING_DESCRIPTOR, applyStakingEvent, startStakePools } from "../src/staking.js";
import type { ReduceStaking, StakingState } from "../src/staking.js";
import type { AptosClient } from "../src/index.js";
import { testClient } from "./helpers.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const POOL_A = long("a");
const POOL_B = long("b");
const ALICE = long("a11ce");

function ev(version: number, eventIndex: number, type: string, data: unknown): StreamdEvent {
  return { type, data, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}
const rewards = (v: number, i: number, pool: string, amount: string) =>
  ev(v, i, "0x1::stake::DistributeRewards", { pool_address: pool, rewards_amount: amount });
const fee = (v: number, i: number, pool: string, amount: string) =>
  ev(v, i, "0x1::stake::DistributeTransactionFee", { pool_address: pool, fee_amount: amount });
const newEpoch = (v: number, i: number, epoch: string) => ev(v, i, "0x1::reconfiguration::NewEpoch", { epoch });
const commission = (v: number, pool: string, pct: string) =>
  ev(v, 0, "0x1::delegation_pool::CommissionPercentageChange", { pool_address: pool, owner: "0xc", commission_percentage_next_lockup_cycle: pct });
const add = (v: number, pool: string, who: string, amount: string, fee: string) =>
  ev(v, 0, "0x1::delegation_pool::AddStake", { pool_address: pool, delegator_address: who, amount_added: amount, add_stake_fee: fee });
const move = (v: number, kind: "UnlockStake" | "ReactivateStake" | "WithdrawStake", pool: string, who: string, amount: string) => {
  const field = { UnlockStake: "amount_unlocked", ReactivateStake: "amount_reactivated", WithdrawStake: "amount_withdrawn" }[kind];
  return ev(v, 0, `0x1::delegation_pool::${kind}`, { pool_address: pool, delegator_address: who, [field]: amount });
};

function fold(events: StreamdEvent[], only?: ReadonlySet<string>): StakingState {
  const state: StakingState = { pools: new Map() };
  for (const event of events) applyStakingEvent(state, event, only);
  return state;
}

describe("applyStakingEvent", () => {
  it("commits each pool's rewards and fees at NewEpoch, as the epoch that ended", () => {
    const state: StakingState = { pools: new Map() };
    expect(applyStakingEvent(state, fee(100, 0, POOL_A, "7"))).toEqual([]);
    expect(applyStakingEvent(state, rewards(100, 1, POOL_A, "50"))).toEqual([]);
    applyStakingEvent(state, rewards(100, 2, "0xb", "0"));
    expect(state.pools.size).toBe(0);
    const changed = applyStakingEvent(state, newEpoch(100, 3, "18007"));
    expect(changed.map((p) => p.address)).toEqual([POOL_A, POOL_B]);
    expect(state.pools.get(POOL_A)!.rewards).toEqual([{ epoch: 18006n, rewards: 50n, fees: 7n, version: 100n }]);
    expect(state.pools.get(POOL_B)!.rewards).toEqual([{ epoch: 18006n, rewards: 0n, fees: 0n, version: 100n }]);
    expect(state.held).toBeUndefined();
  });

  it("keeps a pool paid only fees, and appends one entry per epoch change", () => {
    const state = fold([
      fee(100, 0, POOL_A, "3"),
      newEpoch(100, 1, "5"),
      rewards(200, 0, POOL_A, "9"),
      newEpoch(200, 1, "6"),
    ]);
    expect(state.pools.get(POOL_A)!.rewards).toEqual([
      { epoch: 4n, rewards: 0n, fees: 3n, version: 100n },
      { epoch: 5n, rewards: 9n, fees: 0n, version: 200n },
    ]);
  });

  it("records commission changes, the latest as the pool's percentage", () => {
    const state = fold([commission(10, POOL_A, "1000"), commission(20, POOL_A, "500")]);
    const pool = state.pools.get(POOL_A)!;
    expect(pool.commissionPercentage).toBe(500n);
    expect(pool.commissionChanges).toEqual([
      { percentage: 1000n, owner: long("c"), version: 10n },
      { percentage: 500n, owner: long("c"), version: 20n },
    ]);
    expect(pool.rewards).toEqual([]);
  });

  it("sums a delegator's moves, its first add marking the join", () => {
    const state = fold([
      move(5, "UnlockStake", POOL_A, "0xa11ce", "1"),
      add(10, POOL_A, ALICE, "1000", "4"),
      add(20, POOL_A, ALICE, "500", "2"),
      move(30, "UnlockStake", POOL_A, ALICE, "300"),
      move(40, "ReactivateStake", POOL_A, ALICE, "100"),
      move(50, "WithdrawStake", POOL_A, ALICE, "150"),
    ]);
    const pool = state.pools.get(POOL_A)!;
    expect([...pool.delegators.keys()]).toEqual([ALICE]);
    expect(pool.delegators.get(ALICE)).toEqual({
      address: ALICE,
      added: 1500n,
      addStakeFees: 6n,
      unlocked: 301n,
      reactivated: 100n,
      withdrawn: 150n,
      firstAddedAt: 10n,
      lastVersion: 50n,
    });
  });

  it("keeps only the pools asked for, while NewEpoch is always read", () => {
    const only = new Set([POOL_B]);
    const state = fold(
      [
        rewards(100, 0, POOL_A, "5"),
        rewards(100, 1, POOL_B, "6"),
        newEpoch(100, 2, "3"),
        rewards(200, 0, POOL_A, "5"),
        newEpoch(200, 1, "4"),
        commission(300, POOL_A, "1"),
        add(400, POOL_A, ALICE, "1", "0"),
        add(500, POOL_B, ALICE, "1", "0"),
      ],
      only,
    );
    expect([...state.pools.keys()]).toEqual([POOL_B]);
    expect(state.pools.get(POOL_B)!.rewards).toEqual([{ epoch: 2n, rewards: 6n, fees: 0n, version: 100n }]);
    expect(state.pools.get(POOL_B)!.delegators.size).toBe(1);
  });

  it("refuses held payments whose transaction has no NewEpoch", () => {
    const later = { pools: new Map() } as StakingState;
    applyStakingEvent(later, rewards(100, 0, POOL_A, "5"));
    expect(() => applyStakingEvent(later, add(101, POOL_A, ALICE, "1", "0"))).toThrow(
      "stake payments at version 100 have no NewEpoch in their transaction",
    );
    const elsewhere = { pools: new Map() } as StakingState;
    applyStakingEvent(elsewhere, fee(100, 0, POOL_A, "5"));
    expect(() => applyStakingEvent(elsewhere, newEpoch(101, 0, "2"))).toThrow(StreamdError);
    expect(elsewhere.pools.size).toBe(0);
  });

  it("throws for a known event without its fields, changing nothing", () => {
    const state = fold([add(10, POOL_A, ALICE, "1", "0"), rewards(20, 0, POOL_A, "4")]);
    const before = structuredClone(state);
    const bad = [
      ev(20, 1, "0x1::stake::DistributeRewards", { pool_address: POOL_A, rewards_amount: "-1" }),
      ev(20, 1, "0x1::stake::DistributeTransactionFee", { pool_address: 7, fee_amount: "1" }),
      ev(20, 1, "0x1::reconfiguration::NewEpoch", { epoch: "0" }),
      ev(20, 1, "0x1::reconfiguration::NewEpoch", null),
      ev(20, 1, "0x1::delegation_pool::CommissionPercentageChange", { pool_address: POOL_B, owner: "0xc" }),
      ev(20, 1, "0x1::delegation_pool::AddStake", { pool_address: POOL_B, delegator_address: ALICE, amount_added: "1" }),
      ev(20, 1, "0x1::delegation_pool::UnlockStake", { pool_address: POOL_B, delegator_address: 1, amount_unlocked: "1" }),
      ev(20, 1, "0x1::delegation_pool::WithdrawStake", { pool_address: POOL_B, delegator_address: ALICE }),
    ];
    for (const event of bad) {
      expect(() => applyStakingEvent(state, event)).toThrow(`${event.type} at 20:1 does not have its struct's fields`);
    }
    expect(state).toEqual(before);
  });

  it("ignores other event types, holding nothing", () => {
    const state: StakingState = { pools: new Map() };
    expect(applyStakingEvent(state, ev(10, 0, "0x1::stake::AddStake", { pool_address: POOL_A, amount_added: "1" }))).toEqual([]);
    expect(applyStakingEvent(state, ev(10, 1, "constructor", {}))).toEqual([]);
    expect(applyStakingEvent(state, ev(10, 2, "0x1::delegation_pool::DistributeCommission", {}))).toEqual([]);
    expect(state).toEqual({ pools: new Map() });
  });
});

const HISTORY = [
  fee(100, 0, POOL_A, "2"),
  rewards(100, 1, POOL_A, "40"),
  rewards(100, 2, POOL_B, "30"),
  newEpoch(100, 3, "10"),
  add(150, POOL_A, ALICE, "1000", "5"),
  commission(160, POOL_A, "800"),
];
const until = async (done: () => boolean) => {
  for (let i = 0; i < 500; i++) {
    if (done()) return;
    await new Promise((r) => setTimeout(r, 2));
  }
  throw new Error("timed out waiting");
};
const settle = () => new Promise((r) => setTimeout(r, 50));

/** A `MemoryAdapter` over `events`, recording each fetch. */
function source(events: StreamdEvent[] = HISTORY) {
  const memory = new MemoryAdapter(events);
  const asked: FetchOptions[] = [];
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => {
      expect(d).toEqual(STAKING_DESCRIPTOR);
      asked.push(o);
      return memory.fetchEvents(d, o);
    },
  };
  return { adapter, memory, asked };
}

describe("stakePools", () => {
  it("folds the history into one map, as no listener", async () => {
    const { adapter, asked } = source();
    const client = testClient({ adapter });
    const monitor = client.stakePools({ fromVersion: 0n, batchSize: 7 });
    const value = await monitor.ready;
    expect(value).toBe(monitor.value);
    expect([...value.keys()].sort()).toEqual([POOL_A, POOL_B]);
    expect(monitor.get("0xa")).toMatchObject({
      rewards: [{ epoch: 9n, rewards: 40n, fees: 2n, version: 100n }],
      commissionPercentage: 800n,
    });
    expect(monitor.get("0xa")!.delegators.get(ALICE)).toMatchObject({ added: 1000n, firstAddedAt: 150n });
    expect(monitor.get("not an address")).toBeUndefined();
    expect(monitor.active).toBe(true);
    expect(client.listenerCount()).toBe(0);
    expect(asked.every((o) => o.limit === 7)).toBe(true);
    client.destroy();
  });

  it("follows live one event a page, reporting each pool an epoch change paid once, with NewEpoch", async () => {
    const { adapter, memory } = source();
    const client = testClient({ adapter });
    const changes: [string, bigint, string][] = [];
    const errors: Error[] = [];
    const monitor = client.stakePools({
      fromVersion: 0n,
      pollInterval: 1,
      batchSize: 1,
      onError: (e) => errors.push(e),
      onChange: (p, e) => changes.push([p.address, e.version, e.type]),
    });
    await monitor.ready;
    expect(changes).toEqual([]);
    memory.push(rewards(300, 0, POOL_A, "41"), rewards(300, 1, POOL_B, "31"), newEpoch(300, 2, "11"));
    memory.push(move(310, "UnlockStake", POOL_A, ALICE, "200"));
    await until(() => changes.length === 3);
    await settle();
    expect(changes).toEqual([
      [POOL_A, 300n, "0x1::reconfiguration::NewEpoch"],
      [POOL_B, 300n, "0x1::reconfiguration::NewEpoch"],
      [POOL_A, 310n, "0x1::delegation_pool::UnlockStake"],
    ]);
    expect(monitor.get(POOL_B)!.rewards.map((r) => r.epoch)).toEqual([9n, 10n]);
    expect(monitor.get(POOL_A)!.rewards.at(-1)).toEqual({ epoch: 10n, rewards: 41n, fees: 0n, version: 300n });
    expect(monitor.get(POOL_A)!.delegators.get(ALICE)!.unlocked).toBe(200n);
    expect(errors).toEqual([]);
    client.destroy();
  });

  it("folds nothing below fromVersion, and only the pools asked for", async () => {
    const client = testClient({ adapter: source().adapter });
    const later = await client.stakePools({ fromVersion: 101n }).ready;
    expect([...later.keys()]).toEqual([POOL_A]);
    expect(later.get(POOL_A)!.rewards).toEqual([]);
    const narrowed = await client.stakePools({ fromVersion: 0n, pool: ["0xb"] }).ready;
    expect([...narrowed.keys()]).toEqual([POOL_B]);
    const one = await client.stakePools({ fromVersion: 0n, pool: "0xa", batchSize: 1 }).ready;
    expect([...one.keys()]).toEqual([POOL_A]);
    expect(one.get(POOL_A)!.rewards).toEqual([{ epoch: 9n, rewards: 40n, fees: 2n, version: 100n }]);
    client.destroy();
  });

  it("resolves an empty map when there are no staking events", async () => {
    const client = testClient({ adapter: source([]).adapter });
    expect((await client.stakePools().ready).size).toBe(0);
    client.destroy();
  });

  it("refuses an unknown option and a malformed pool at the call", () => {
    const client = testClient({ adapter: source().adapter });
    expect(() => client.stakePools({ where: () => true } as never)).toThrow("Unsupported stakePools option: where");
    for (const pool of ["nope", [], ["0xa", 3], 7]) {
      expect(() => client.stakePools({ pool } as never)).toThrow("pool must be an address or a non-empty list of addresses");
    }
    client.destroy();
  });

  it("stops on a malformed event in the backfill or live, reporting once to the call's onError", async () => {
    const message = "0x1::delegation_pool::AddStake at 170:0 does not have its struct's fields";
    const atClient: Error[] = [];
    const atCall: Error[] = [];
    const { adapter, memory } = source();
    const client = testClient({ adapter, onError: (e) => atClient.push(e) });
    const bad = add(170, POOL_A, ALICE, "x", "0");
    const backfill = testClient({ adapter: source([...HISTORY, bad]).adapter, onError: (e) => atClient.push(e) });
    const failing = backfill.stakePools({ fromVersion: 0n, onError: (e) => atCall.push(e) });
    await expect(failing.ready).rejects.toThrow("Reducer threw for 170:0");
    expect(failing.active).toBe(false);

    const live = client.stakePools({ fromVersion: 0n, pollInterval: 1, onError: (e) => atCall.push(e) });
    await live.ready;
    memory.push(bad);
    await until(() => atCall.length === 2);
    await settle();
    expect(atCall.map((e) => (e.cause as Error).message)).toEqual([message, message]);
    expect(atClient).toEqual([]);
    expect(live.active).toBe(false);
    client.destroy();
    backfill.destroy();
  });

  it("passes every allowed option to the reduction, wrapping onChange", () => {
    let seen: Record<string, unknown> = {};
    const reduce: ReduceStaking = (_, __, initial, options) => {
      seen = options as Record<string, unknown>;
      return { value: initial, cursor: undefined, ready: new Promise(() => {}), active: true, unsubscribe() {}, [Symbol.dispose]() {} };
    };
    const signal = new AbortController().signal;
    const onError = () => {};
    const onChange = () => {};
    const passed = { fromVersion: 1n, fromTime: 2n, pollInterval: 3, batchSize: 4, maxErrors: 5, maxBackoff: 6, maxPollInterval: 7, retries: 8, signal, onError };
    startStakePools(reduce, { ...passed, pool: "0xa", onChange });
    const { onChange: wrapped, ...rest } = seen;
    expect(rest).toEqual(passed);
    expect(typeof wrapped).toBe("function");
    expect(wrapped).not.toBe(onChange);
  });

  it("stops on unsubscribe, dispose, abort and destroy", async () => {
    const stops: [string, (m: ReturnType<AptosClient["stakePools"]>, c: AptosClient, a: AbortController) => void][] = [
      ["unsubscribe", (m) => m.unsubscribe()],
      ["dispose", (m) => m[Symbol.dispose]()],
      ["abort", (_, __, a) => a.abort()],
      ["destroy", (_, c) => c.destroy()],
    ];
    for (const [name, stop] of stops) {
      const { adapter, memory } = source();
      const client = testClient({ adapter });
      const controller = new AbortController();
      const monitor = client.stakePools({ fromVersion: 0n, pollInterval: 1, signal: controller.signal });
      await monitor.ready;
      stop(monitor, client, controller);
      expect([name, monitor.active]).toEqual([name, false]);
      memory.push(commission(900, POOL_B, "1"));
      await settle();
      expect([name, monitor.get(POOL_B)!.commissionPercentage]).toEqual([name, undefined]);
      client.destroy();
    }
  });
});

describe("stakePools on recorded mainnet events", () => {
  // One epoch change (8061753711, into epoch 18007) for three pools, one with
  // no fee and zero rewards, then a delegation add_stake and an unlock, with
  // the views read at each version and the one before.
  const fixtures = join(import.meta.dir, "fixtures");
  const events = readFileSync(join(fixtures, "staking.jsonl"), "utf8").trim().split("\n").map((line) => parseEvent(line));
  const views = JSON.parse(readFileSync(join(fixtures, "staking-views.json"), "utf8")) as Record<string, string[]>;
  const read = (fn: string, args: string, version: bigint) => views[`${fn}:${args}:${version}`]!.map(BigInt);
  const near = (a: bigint, b: bigint) => (a > b ? a - b : b - a) <= 2n;

  it("matches the stake and delegation views", async () => {
    const client = testClient({ adapter: new MemoryAdapter(events) });
    const monitor = await client.stakePools({ fromVersion: 0n }).ready;
    const epochChecks = Object.keys(views).filter((k) => k.startsWith("0x1::stake::get_stake:") && k.endsWith(":8061753711"));
    expect(epochChecks).toHaveLength(3);
    for (const key of epochChecks) {
      const pool = key.split(":").at(-2)!;
      const total = (v: bigint) => read("0x1::stake::get_stake", pool, v).reduce((a, b) => a + b);
      const [paid] = monitor.get(pool)!.rewards;
      expect(paid).toMatchObject({ epoch: 18006n, version: 8061753711n });
      expect([pool, paid!.rewards + paid!.fees]).toEqual([pool, total(8061753711n) - total(8061753710n)]);
    }
    expect(monitor.get("0x3e946c97ee31e856978aeecf359433298bf8801cf6540924bf51d9b86b6bfa69")!.rewards[0]).toMatchObject({ rewards: 6303140298n, fees: 167131671n });

    const addPool = "0xdb5247f859ce63dbe8940cf8773be722a60dcc594a8be9aca4b76abceb251b8e";
    const adder = "0xd89e4b43b4fe79ec683e50e74b49fb80e10d17a678312ae0afc6ec16a283b5f2";
    const added = monitor.get(addPool)!.delegators.get(adder)!;
    const [activeBefore] = read("0x1::delegation_pool::get_stake", `${addPool},${adder}`, 8062421089n);
    const [activeAfter] = read("0x1::delegation_pool::get_stake", `${addPool},${adder}`, 8062421090n);
    expect(added).toMatchObject({ added: 10000000000n, addStakeFees: 53424n, firstAddedAt: 8062421090n });
    expect(near(activeAfter! - activeBefore!, added.added - added.addStakeFees)).toBe(true);

    const unlockPool = "0x06099edbe54f242bad50020dfd67646b1e46282999483e7064e70f02f7ea3c15";
    const unlocker = "0xd248aee66c517e674a28e50d8a6051f13214693e1881d258f724d574d2fb4b69";
    const unlocked = monitor.get(unlockPool)!.delegators.get(unlocker)!;
    const before = read("0x1::delegation_pool::get_stake", `${unlockPool},${unlocker}`, 8061738668n);
    const after = read("0x1::delegation_pool::get_stake", `${unlockPool},${unlocker}`, 8061738669n);
    expect(unlocked.unlocked).toBe(13441617720n);
    expect(near(before[0]! - after[0]!, unlocked.unlocked)).toBe(true);
    expect(near(after[2]! - before[2]!, unlocked.unlocked)).toBe(true);
    client.destroy();
  });
});
