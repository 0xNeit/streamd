import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";
import type { MoveEventType } from "@streamd/core";

/**
 * Hits Sui mainnet. Run with `bun run test:integration`; skipped otherwise so
 * the unit suite stays offline and deterministic.
 */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const STAKE_TYPE = `0x${"3".padStart(64, "0")}::validator::StakingRequestEvent` as MoveEventType;
const STAKES = { kind: "module", eventType: STAKE_TYPE } as const;

describe.skipIf(!enabled)("SuiClient (mainnet)", () => {
  it("reads recent events through the mainnet preset", async () => {
    const head = await new SuiAdapter(new SuiGrpcClient(networks.mainnet)).getHeadVersion();
    expect(head).toBeGreaterThan(1_000_000_000n);
    using sui = new SuiClient({ sui: networks.mainnet });
    const events = await sui.getEvents(STAKES, { fromVersion: -5_000_000n, limit: 3, maxPages: 20 });
    expect(events.length).toBeGreaterThan(0);
    for (const e of events) {
      expect(e.type).toBe(STAKE_TYPE);
      expect(e.version).toBeGreaterThan(head - 6_000_000n);
    }
  }, 60_000);
});
