import { StreamdError, eventId } from "@streamd/core";
import type { ModuleEventDescriptor, StreamdEvent } from "@streamd/core";
import { address, amount } from "./records.js";
import type { Fields } from "./records.js";
import type { Delegator, StakePool } from "./types.js";

const REWARDS = "0x1::stake::DistributeRewards";
const FEES = "0x1::stake::DistributeTransactionFee";
const NEW_EPOCH = "0x1::reconfiguration::NewEpoch";
const COMMISSION = "0x1::delegation_pool::CommissionPercentageChange";
const ADD = "0x1::delegation_pool::AddStake";
/** Each delegator move: the `Delegator` sum it adds to, and the field it reads. */
const MOVES = {
  [ADD]: ["added", "amount_added"],
  "0x1::delegation_pool::UnlockStake": ["unlocked", "amount_unlocked"],
  "0x1::delegation_pool::ReactivateStake": ["reactivated", "amount_reactivated"],
  "0x1::delegation_pool::WithdrawStake": ["withdrawn", "amount_withdrawn"],
} as const;
type Move = keyof typeof MOVES;

/**
 * The one scan a monitor reads. Only the module defining an event type can
 * emit it, so every event here is the framework's own.
 */
export const STAKING_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [REWARDS, FEES, NEW_EPOCH, COMMISSION, ...(Object.keys(MOVES) as Move[])],
};

/**
 * What the fold keeps: the pools, and what an epoch change has paid so far.
 * `stake::on_new_epoch` emits each pool's fee and reward before
 * `reconfiguration` emits `NewEpoch`, so they are held until it names the epoch.
 */
export interface StakingState {
  pools: Map<string, StakePool>;
  held?: { version: bigint; paid: Map<string, { rewards: bigint; fees: bigint }> };
}

const malformed = (event: StreamdEvent) =>
  new StreamdError(`${event.type} at ${eventId(event)} does not have its struct's fields`);

function u(field: unknown, event: StreamdEvent): bigint {
  const value = amount(field);
  if (value === undefined) throw malformed(event);
  return value;
}

function addr(field: unknown, event: StreamdEvent): string {
  const value = address(field);
  if (value === undefined) throw malformed(event);
  return value;
}

function pool(state: StakingState, at: string): StakePool {
  let found = state.pools.get(at);
  if (found === undefined) {
    found = { address: at, rewards: [], commissionChanges: [], delegators: new Map() };
    state.pools.set(at, found);
  }
  return found;
}

function delegator(p: StakePool, at: string, version: bigint): Delegator {
  let found = p.delegators.get(at);
  if (found === undefined) {
    found = { address: at, added: 0n, addStakeFees: 0n, unlocked: 0n, reactivated: 0n, withdrawn: 0n, lastVersion: version };
    p.delegators.set(at, found);
  }
  return found;
}

/** Commits what the held epoch change paid, as the epoch `epoch` ended. */
function commit(state: StakingState, epoch: bigint, version: bigint): StakePool[] {
  const changed: StakePool[] = [];
  for (const [at, { rewards, fees }] of state.held?.paid ?? []) {
    const p = pool(state, at);
    p.rewards.push({ epoch, rewards, fees, version });
    changed.push(p);
  }
  state.held = undefined;
  return changed;
}

/**
 * Folds one staking event into `state`, returning the pools it changed (none
 * for a held payment or an event it does not read). `only` keeps those pools
 * alone; `NewEpoch` is read whatever it holds. Throws a `StreamdError`, changing
 * nothing, for an event of a known type without its fields, and for held
 * payments their transaction's `NewEpoch` did not follow, since a source
 * dropping it would misnumber every epoch.
 */
export function applyStakingEvent(state: StakingState, event: StreamdEvent, only?: ReadonlySet<string>): StakePool[] {
  const { type, version } = event;
  const known = type === REWARDS || type === FEES || type === NEW_EPOCH || type === COMMISSION || Object.hasOwn(MOVES, type);
  if (!known) return [];
  if (state.held !== undefined && state.held.version !== version) {
    throw new StreamdError(`stake payments at version ${state.held.version} have no NewEpoch in their transaction`);
  }
  if (typeof event.data !== "object" || event.data === null) throw malformed(event);
  const d = event.data as Fields;
  if (type === NEW_EPOCH) {
    const epoch = u(d.epoch, event);
    if (epoch === 0n) throw malformed(event);
    return commit(state, epoch - 1n, version);
  }
  const at = addr(d.pool_address, event);
  if (type === REWARDS || type === FEES) {
    const paid = u(type === REWARDS ? d.rewards_amount : d.fee_amount, event);
    if (only !== undefined && !only.has(at)) return [];
    state.held ??= { version, paid: new Map() };
    const entry = state.held.paid.get(at) ?? { rewards: 0n, fees: 0n };
    if (type === REWARDS) entry.rewards += paid;
    else entry.fees += paid;
    state.held.paid.set(at, entry);
    return [];
  }
  if (type === COMMISSION) {
    const change = { percentage: u(d.commission_percentage_next_lockup_cycle, event), owner: addr(d.owner, event), version };
    if (only !== undefined && !only.has(at)) return [];
    const p = pool(state, at);
    p.commissionPercentage = change.percentage;
    p.commissionChanges.push(change);
    return [p];
  }
  const [sum, field] = MOVES[type as Move];
  const [who, moved] = [addr(d.delegator_address, event), u(d[field], event)];
  const fee = type === ADD ? u(d.add_stake_fee, event) : undefined;
  if (only !== undefined && !only.has(at)) return [];
  const p = pool(state, at);
  const del = delegator(p, who, version);
  del[sum] += moved;
  if (fee !== undefined) {
    del.addStakeFees += fee;
    del.firstAddedAt ??= version;
  }
  del.lastVersion = version;
  return [p];
}
