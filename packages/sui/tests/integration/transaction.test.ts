import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdTimeoutError, StreamdTransactionError } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import { SuiClient, networks } from "../../src/index.js";
import { digest } from "../helpers.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const STAKE_TYPE = `0x${"3".padStart(64, "0")}::validator::StakingRequestEvent` as MoveEventType;

/** A recent failed transaction's digest, found by walking back through checkpoints. */
async function recentFailure(grpc: SuiGrpcClient): Promise<string> {
  const { response: info } = await grpc.ledgerService.getServiceInfo({});
  for (let cp = info.checkpointHeight! - 5n; ; cp--) {
    const { response } = await grpc.ledgerService.getCheckpoint({
      checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: cp },
      readMask: { paths: ["transactions.digest", "transactions.effects.status"] },
    });
    const failed = response.checkpoint?.transactions.find((t) => t.effects?.status?.success === false);
    if (failed?.digest !== undefined) return failed.digest;
  }
}

describe.skipIf(!enabled)("SuiClient transaction events (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("returns a transaction's events where getEvents placed them, and asserts on them", async () => {
    using sui = new SuiClient({ sui: grpc });
    const [seen] = await sui.getEvents({ kind: "module", eventType: STAKE_TYPE }, { fromVersion: -5_000_000n, limit: 1, maxPages: 20 });
    expect(seen?.transactionHash).toBeDefined();
    const events = await sui.eventsOf(seen!.transactionHash!);
    const same = events.find((e) => e.eventIndex === seen!.eventIndex);
    expect(same).toMatchObject({ type: STAKE_TYPE, version: seen!.version, transactionHash: seen!.transactionHash });
    expect(same?.data).toEqual(seen!.data);
    const matched = await sui.expectEvents(seen!.transactionHash!).toEmit({ kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType });
    expect(matched.map((e) => e.eventIndex)).toContain(seen!.eventIndex);
  }, 60_000);

  it("rejects a failed transaction with its abort", async () => {
    using sui = new SuiClient({ sui: grpc });
    const err = await sui.eventsOf(await recentFailure(grpc)).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect((err as StreamdTransactionError).vmStatus).toContain("MoveAbort");
  }, 60_000);

  it("times out on a digest mainnet never saw", async () => {
    using sui = new SuiClient({ sui: grpc });
    const err = await sui.eventsOf(digest(7), undefined, { timeout: 3000 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
  }, 30_000);
});
