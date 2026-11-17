import type { BlockHeights, BlockInfo, EventAdapter, EventDescriptor } from "./types.js";
import { StreamdError, StreamdNetworkError } from "./errors.js";
import { requireBlock, requireBlockHeights } from "./adapters/validate.js";

const primaryOnly = new WeakSet<EventDescriptor>();
const archiveBacked = new WeakSet<BlockHeights>();

/** Whether `resolveFromTime` marked this probe descriptor as primary-only. */
export function isPrimaryOnly(descriptor: EventDescriptor): boolean {
  return primaryOnly.has(descriptor);
}

/** Tags a `BlockHeights` copy so `resolveFromTime` knows an archive can extend its oldest bound. */
export function markHasArchive(heights: BlockHeights): BlockHeights {
  if (typeof heights !== "object" || heights === null) return heights;
  const copy = { oldest: heights.oldest, newest: heights.newest };
  archiveBacked.add(copy);
  return copy;
}

/** Whether `heights` came from an archive-backed adapter or pool. */
export function hasArchiveHeights(heights: BlockHeights): boolean {
  return typeof heights === "object" && heights !== null && archiveBacked.has(heights);
}

async function initialBounds(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  fromTime: bigint,
  blockAt: (d: EventDescriptor, height: bigint) => Promise<BlockInfo>,
): Promise<{ d: EventDescriptor; newest: bigint; lo: BlockInfo; hi?: BlockInfo }> {
  const probe = { ...descriptor };
  primaryOnly.add(probe);
  const raw = await adapter.getBlockHeights!(probe);
  const { oldest, newest } = requireBlockHeights(raw);
  const lo = await firstServed((h) => blockAt(probe, h), oldest, newest);
  if (lo.timestamp <= fromTime || !hasArchiveHeights(raw)) return { d: probe, newest, lo };
  const fullRaw = await adapter.getBlockHeights!(descriptor);
  const { oldest: archiveOldest } = requireBlockHeights(fullRaw);
  if (!hasArchiveHeights(fullRaw) || archiveOldest >= lo.height) return { d: probe, newest, lo };
  return {
    d: descriptor,
    newest,
    lo: await firstServed((h) => blockAt(descriptor, h), archiveOldest, lo.height),
    hi: lo,
  };
}

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
 *
 * Aptos mainnet reports an `oldest_block_height` tens of blocks behind its
 * pruner, and the edge moves while the search runs, so a 410 for a block
 * means everything at or below it is gone and the search moves past it.
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
  const readBlock = async (d: EventDescriptor, height: bigint) => {
    if (stopped()) throw new StreamdError("fromTime search stopped");
    return requireBlock(await getBlock(d, height), height);
  };
  const { d, newest, lo: initLo, hi: initHi } = await initialBounds(adapter, descriptor, fromTime, readBlock);
  let lo = initLo;
  if (lo.timestamp >= fromTime) return lo.firstVersion;
  const blockAt = (height: bigint) => readBlock(d, height);
  let hi = initHi ?? (await blockAt(newest));
  if (hi.timestamp < fromTime) return hi.lastVersion + 1n;

  // Invariant: lo.timestamp < fromTime <= hi.timestamp, so the span is positive.
  for (let probe = 0; hi.height - lo.height > 1n; probe++) {
    const span = hi.height - lo.height;
    const guess =
      probe % 2 === 0
        ? lo.height + ((fromTime - lo.timestamp) * span) / (hi.timestamp - lo.timestamp)
        : lo.height + span / 2n;
    const height = guess <= lo.height ? lo.height + 1n : guess >= hi.height ? hi.height - 1n : guess;
    let block: BlockInfo;
    try {
      block = await blockAt(height);
    } catch (err) {
      if (!isPruned(err)) throw err;
      try {
        lo = await firstServed(blockAt, height + 1n, hi.height);
      } catch (pruneErr) {
        // The edge passed `hi` as well, so the answer is pruned: clamp past it.
        if (!isPruned(pruneErr) || hi.height >= newest) throw pruneErr;
        return (await firstServed(blockAt, hi.height + 1n, newest)).firstVersion;
      }
      if (lo.timestamp >= fromTime) return lo.firstVersion;
      continue;
    }
    if (block.timestamp < fromTime) lo = block;
    else hi = block;
  }
  return hi.firstVersion;
}

const isPruned = (err: unknown) => err instanceof StreamdNetworkError && err.statusCode === 410;

/**
 * The first served block at or above `from`, up to `limit`: a doubling stride
 * forward past pruned blocks, then a bisection back to the edge so no served
 * block is skipped.
 */
async function firstServed(
  blockAt: (height: bigint) => Promise<BlockInfo>,
  from: bigint,
  limit: bigint,
): Promise<BlockInfo> {
  let pruned = from - 1n;
  let found: BlockInfo;
  for (let height = from, stride = 1n; ; stride *= 2n) {
    try {
      found = await blockAt(height);
      break;
    } catch (err) {
      if (!isPruned(err) || height >= limit) throw err;
    }
    pruned = height;
    height = height + stride < limit ? height + stride : limit;
  }
  while (found.height - pruned > 1n) {
    const mid = pruned + (found.height - pruned) / 2n;
    try {
      found = await blockAt(mid);
    } catch (err) {
      if (!isPruned(err)) throw err;
      pruned = mid;
    }
  }
  return found;
}
