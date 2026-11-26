import type { StreamdClient } from "@streamd/core";
import type { StakePool, StakePools, StakePoolsOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { useMonitor, type MonitorStatus } from "./monitor.js";

/** A client that tracks stake pools: an `AptosClient`, or one shaped like it. */
export interface StakePoolsClient extends StreamdClient {
  stakePools(options?: StakePoolsOptions): StakePools;
}

/** `stakePools` options, minus `signal`: the hook stops it on unmount and on input change. */
export interface UseStakePoolsOptions extends Omit<StakePoolsOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StakePoolsClient;
}

export interface UseStakePoolsResult {
  /** Every pool seen, by long-form address; a copy, so a changed pool is a new object and the rest keep theirs. */
  pools: ReadonlyMap<string, StakePool>;
  /** The last error, cleared when a pool next changes. */
  error: Error | undefined;
  status: MonitorStatus;
}

const EMPTY: ReadonlyMap<string, StakePool> = new Map();

/** `client.stakePools(options)` for a component, restarting only when the options change by value. */
export function useStakePools(options: UseStakePoolsOptions = {}): UseStakePoolsResult {
  const { client: explicit, ...listener } = options;
  const client = useClientWith<StakePoolsClient>("stakePools", explicit);
  const { value, error, status } = useMonitor({
    client,
    enabled: true,
    inputs: null,
    options: listener,
    start: (opts: StakePoolsOptions) => client.stakePools(opts),
    snapshot: (handle: StakePools) => structuredClone(handle.value),
    apply: (previous: ReadonlyMap<string, StakePool>, pool: StakePool) => new Map(previous).set(pool.address, structuredClone(pool)),
    empty: EMPTY,
  });
  return { pools: value, error, status };
}
