import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdClient, withHooks } from "@streamd/core";
import type { FetchOptions, MoveEventType } from "@streamd/core";
import { SuiAdapter, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const STAKES = { kind: "module", eventType: `0x${"3".padStart(64, "0")}::validator::StakingRequestEvent` as MoveEventType } as const;
// Mainnet seals a few checkpoints a second, so this is roughly an hour back.
const AN_HOUR = 14_400n;

describe.skipIf(!enabled)("SuiAdapter checkpoints as blocks (mainnet)", () => {
  const adapter = new SuiAdapter(new SuiGrpcClient(networks.mainnet));
  const requests: FetchOptions[] = [];
  const hooked = withHooks(adapter, { onRequest: ({ options }) => void requests.push(options) });

  it("maps checkpoints to blocks and bounds queries and fromTime by them", async () => {
    const { oldest, newest } = await adapter.getBlockHeights();
    expect(oldest).toBeLessThan(newest);
    expect(newest).toBeGreaterThan(300_000_000n);

    // A checkpoint whose time differs from the one before, so fromTime can only land on it.
    let height = newest - AN_HOUR;
    let block = await adapter.getBlock(STAKES, height);
    let prev = await adapter.getBlock(STAKES, height - 1n);
    while (prev.timestamp === block.timestamp) {
      block = prev;
      prev = await adapter.getBlock(STAKES, --height - 1n);
    }
    expect(block.firstVersion).toBe(prev.lastVersion + 1n);
    expect(block.lastVersion).toBeGreaterThanOrEqual(block.firstVersion);
    expect(block.timestamp).toBeGreaterThan(prev.timestamp);
    expect(block.timestamp).toBeGreaterThan(BigInt(Date.now() - 3 * 3_600_000) * 1000n);

    using streamd = new StreamdClient({ adapter: hooked });
    requests.length = 0;
    await streamd.getEvents(STAKES, { fromTime: prev.timestamp + 1n, maxPages: 1 });
    expect(requests[0]?.fromVersion).toBe(block.firstVersion);

    requests.length = 0;
    const inside = await streamd.getEvents(STAKES, { fromBlock: height, toBlock: height, maxPages: 1 });
    expect(requests[0]).toMatchObject({ fromVersion: block.firstVersion, toVersion: block.lastVersion });
    for (const e of inside) {
      expect(e.version).toBeGreaterThanOrEqual(block.firstVersion);
      expect(e.version).toBeLessThanOrEqual(block.lastVersion);
    }
  }, 120_000);
});
