import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { eventId } from "@streamd/core";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const CLOCK = `0x${"6".padStart(64, "0")}`;
const DOES_NOT_EXIST = 1;
const EXISTS = 2;
const DELETED = 3;

describe.skipIf(!enabled)("SuiAdapter object descriptors (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);
  const clock = { kind: "object" as const, objectId: "0x6" };

  it("follows the Clock in version order, each change matching the transaction's effects", async () => {
    using sui = new SuiClient({ sui: grpc });
    const changes = await sui.getEvents(clock, { fromVersion: -3000n, limit: 6 });
    expect(changes.length).toBeGreaterThan(1);
    let last = -1n;
    for (const change of changes) {
      expect(change.type).toBe(`0x${"2".padStart(64, "0")}::clock::Clock`);
      const stamp = BigInt((change.data as { timestamp_ms: string }).timestamp_ms);
      expect(stamp >= last).toBe(true);
      last = stamp;
    }
    expect(changes.map((c) => c.version)).toEqual([...changes.map((c) => c.version)].sort((a, b) => (a < b ? -1 : 1)));
    for (const change of changes.slice(0, 3)) {
      const { response } = await grpc.ledgerService.getTransaction({
        digest: change.transactionHash!,
        readMask: { paths: ["effects.changed_objects", "events.events.event_type"] },
      });
      const txn = response.transaction!;
      const position = txn.effects!.changedObjects.findIndex((c) => c.objectId === CLOCK);
      expect(position).toBeGreaterThanOrEqual(0);
      expect(change.eventIndex).toBe((txn.events?.events.length ?? 0) + position);
    }
  }, 120_000);

  it("pages by cursor with no gap and no repeat", async () => {
    const adapter = new SuiAdapter(grpc);
    const start = (await adapter.getHeadVersion()) - 3000n;
    const whole = await adapter.fetchEvents(clock, { fromVersion: start, limit: 6 });
    const first = await adapter.fetchEvents(clock, { fromVersion: start, limit: 3 });
    const rest = await adapter.fetchEvents(clock, { cursor: first.nextCursor, limit: 3 });
    expect(first.hasMore).toBe(true);
    expect([...first.events, ...rest.events].map(eventId)).toEqual(whole.events.map(eventId));
  }, 120_000);

  it("ends a deleted object's history with null", async () => {
    const adapter = new SuiAdapter(grpc);
    const { checkpointHeight } = (await grpc.ledgerService.getServiceInfo({})).response;
    const startCheckpoint = checkpointHeight! - 30n;
    const call = grpc.ledgerService.listTransactions({
      startCheckpoint,
      readMask: { paths: ["digest", "checkpoint", "effects.changed_objects"] },
      options: { limit: 500 },
    } as never);
    let found: { objectId: string; digest: string } | undefined;
    for await (const frame of call.responses) {
      const txn = frame.transaction;
      const gone = txn?.effects?.changedObjects.find(
        (c) => c.inputState === EXISTS && c.outputState === DOES_NOT_EXIST && c.idOperation === DELETED,
      );
      if (found === undefined && gone !== undefined) found = { objectId: gone.objectId!, digest: txn!.digest! };
    }
    expect(found).toBeDefined();
    const { firstVersion } = await adapter.getBlock(clock, startCheckpoint);
    using sui = new SuiClient({ sui: grpc });
    const history = await sui.getEvents({ kind: "object", objectId: found!.objectId }, { fromVersion: firstVersion, toVersion: "head" });
    const deletion = history.at(-1)!;
    expect(deletion).toMatchObject({ transactionHash: found!.digest, data: null });
    expect(deletion.type).toContain("::");
  }, 120_000);
});
