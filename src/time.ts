import type { EventAdapter, EventDescriptor } from "./types.js";
import { StreamdError } from "./errors.js";
import { requireBlock, requireBlockHeights } from "./adapters/validate.js";

/** Rejects a `fromTime` that is malformed or clashes with another start option. */
export function validateFromTime(options: {
  fromTime?: bigint;
  fromVersion?: unknown;
  fromBlock?: unknown;
}): void {
  const { fromTime } = options;
  if (fromTime === undefined) return;
  if (typeof fromTime !== "bigint" || fromTime < 0n) {
    throw new StreamdError(
      `Invalid fromTime: ${String(fromTime)} (expected a non-negative bigint of microseconds)`,
    );
  }
  if (options.fromVersion !== undefined) {
    throw new StreamdError("Set fromTime or fromVersion, not both");
  }
  if (options.fromBlock !== undefined) {
    throw new StreamdError("Set fromTime or fromBlock, not both");
  }
}

/**
 * The first version of the first block stamped at or after `fromTime`
 * (microseconds), searched between the source's oldest and newest blocks.
 * Earlier than the oldest block clamps to it; later than the newest starts
 * just past the head.
 *
 * Probes alternate between interpolating on block timestamps and bisecting:
 * interpolation lands close on Aptos's steady block times, and the bisection
 * half keeps uneven stretches from degrading it. Plain bisection measured
 * about twice the lookups on mainnet. `stopped` ends the search between
 * lookups once its caller has given up.
 */
export async function resolveFromTime(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  fromTime: bigint,
  stopped: () => boolean = () => false,
): Promise<bigint> {
  if (adapter.getBlockHeights === undefined || adapter.getBlock === undefined) {
    throw new StreamdError("fromTime needs an adapter that implements getBlockHeights and getBlock");
  }
  const getBlock = adapter.getBlock.bind(adapter);
  const blockAt = async (height: bigint) => requireBlock(await getBlock(descriptor, height), height);
  const { oldest, newest } = requireBlockHeights(await adapter.getBlockHeights(descriptor));
  let lo = await blockAt(oldest);
  if (lo.timestamp >= fromTime) return lo.firstVersion;
  let hi = await blockAt(newest);
  if (hi.timestamp < fromTime) return hi.lastVersion + 1n;

  // Invariant: lo.timestamp < fromTime <= hi.timestamp, so the span is positive.
  for (let probe = 0; hi.height - lo.height > 1n; probe++) {
    if (stopped()) throw new StreamdError("fromTime search stopped");
    const span = hi.height - lo.height;
    const guess =
      probe % 2 === 0
        ? lo.height + ((fromTime - lo.timestamp) * span) / (hi.timestamp - lo.timestamp)
        : lo.height + span / 2n;
    const height =
      guess <= lo.height ? lo.height + 1n : guess >= hi.height ? hi.height - 1n : guess;
    const block = await blockAt(height);
    if (block.timestamp < fromTime) lo = block;
    else hi = block;
  }
  return hi.firstVersion;
}
