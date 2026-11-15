import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { applyStakingEvent } from "../src/staking.js";
import type { StakingState } from "../src/staking.js";

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
      ev(20, 1, "0x1::delegation_pool::CommissionPercentageChange", { pool_address: POOL_A, owner: "0xc" }),
      ev(20, 1, "0x1::delegation_pool::AddStake", { pool_address: POOL_A, delegator_address: ALICE, amount_added: "1" }),
      ev(20, 1, "0x1::delegation_pool::UnlockStake", { pool_address: POOL_A, delegator_address: 1, amount_unlocked: "1" }),
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
