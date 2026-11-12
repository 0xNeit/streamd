import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdError, StreamdNetworkError, type StreamdEvent } from "@streamd/core";
import { SuiClient, networks, ownedBy, primaryName, resolveName } from "../../src/index.js";

/** Hits Sui mainnet (and devnet once); skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const OBJECT = 2;
const ADDRESS = 1;
const ev = (id: string) => ({ type: "0x2::kiosk::ItemListed", data: { id }, version: 1n, sequenceNumber: 0n, eventIndex: 0 }) as StreamdEvent;

describe.skipIf(!enabled)("SuiNS names and ownership (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("resolves both name forms to one address, and the primary name points back", async () => {
    const address = await resolveName(grpc, "scallop.sui");
    expect(address).toMatch(/^0x[0-9a-f]{64}$/);
    expect(await resolveName(grpc, "@scallop")).toBe(address);
    const primary = await primaryName(grpc, address);
    if (primary !== undefined) expect(await resolveName(grpc, primary)).toBe(address);
  }, 30_000);

  it("rejects an unregistered name, and any name on devnet, as a plain StreamdError", async () => {
    for (const pending of [resolveName(grpc, "zz9q8x7-unregistered.sui"), resolveName(new SuiGrpcClient(networks.devnet), "scallop.sui")]) {
      const err = await pending.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
  }, 30_000);

  it("matches objects their owners hold, directly and through objects, at the head", async () => {
    const { checkpointHeight } = (await grpc.ledgerService.getServiceInfo({})).response;
    const changed: { objectId?: string; outputOwner?: { kind?: number } }[] = [];
    const { responses } = grpc.ledgerService.listTransactions({
      readMask: { paths: ["effects.changed_objects"] },
      startCheckpoint: checkpointHeight! - 10n,
      options: { limit: 100, ordering: 0 },
    });
    for await (const frame of responses) changed.push(...(frame.transaction?.effects?.changedObjects ?? []));
    const owned = async (kind: number) => {
      for (const c of changed) {
        if (c.outputOwner?.kind !== kind) continue;
        let owner;
        try {
          ({ owner } = (await grpc.ledgerService.getObject({ objectId: c.objectId!, readMask: { paths: ["owner"] } })).response.object!);
        } catch {
          continue; // Deleted or moved since; take the next.
        }
        if (owner?.kind === kind) return { id: c.objectId!, owner: owner.address! };
      }
      throw new Error(`no live object with owner kind ${kind} in the sample`);
    };
    const coin = await owned(ADDRESS);
    expect(await ownedBy(grpc, coin.owner)(ev(coin.id))).toBe(true);
    expect(await ownedBy(grpc, `0x${"b0b".padStart(64, "0")}`)(ev(coin.id))).toBe(false);
    const child = await owned(OBJECT);
    expect(await ownedBy(grpc, child.owner)(ev(child.id))).toBe(true);
    expect(await ownedBy(grpc, coin.owner)(ev(`0x${"ab".repeat(32)}`))).toBe(false);
  }, 120_000);

  it("follows a name's balances", async () => {
    using sui = new SuiClient({ sui: grpc });
    using wallet = sui.balances("scallop.sui");
    const value = await wallet.ready;
    expect(typeof value.get(`0x${"2".padStart(64, "0")}::sui::SUI`)).toBe("bigint");
  }, 30_000);
});
