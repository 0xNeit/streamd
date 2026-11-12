import { describe, expect, it } from "bun:test";
import { bcs } from "@mysten/sui/bcs";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { Transaction, TransactionDataBuilder } from "@mysten/sui/transactions";
import { toBase64 } from "@mysten/sui/utils";
import { StreamdTransactionError } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const STAKE_TYPE = `0x${"3".padStart(64, "0")}::validator::StakingRequestEvent` as MoveEventType;
const ONE_SUI = 1_000_000_000n;
const u64 = (n: bigint) => toBase64(bcs.u64().serialize(n).toBytes());

describe.skipIf(!enabled)("SuiClient simulations (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("previews a stake as a recent staker, unsigned, and rejects one below the minimum", async () => {
    using sui = new SuiClient({ sui: grpc });
    const recent = await sui.getEvents({ kind: "module", eventType: STAKE_TYPE }, { fromVersion: -5_000_000n, limit: 20, maxPages: 40 });
    // A staker whose coins still cover the stake and gas, since many stake everything they hold.
    let staker: string | undefined;
    let validator: string | undefined;
    for (const { data } of recent) {
      const { staker_address, validator_address } = data as Record<string, string>;
      const { balance } = await grpc.core.getBalance({ owner: staker_address! });
      if (BigInt(balance.coinBalance) >= 2n * ONE_SUI) [staker, validator] = [staker_address, validator_address];
      if (staker !== undefined) break;
    }
    expect(staker).toBeDefined();
    const tx = new Transaction();
    tx.setSender(staker!);
    const [stake] = tx.splitCoins(tx.gas, [ONE_SUI]);
    tx.moveCall({ target: "0x3::sui_system::request_add_stake", arguments: [tx.object("0x5"), stake!, tx.pure.address(validator!)] });
    // Building resolves gas from the staker's own coins; nothing is signed.
    const bytes = await tx.build({ client: grpc });

    const head = await new SuiAdapter(grpc).getHeadVersion();
    const events = await sui.previewEvents(bytes, { kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ type: STAKE_TYPE, sender: staker, data: { amount: String(ONE_SUI), validator_address: validator } });
    expect(events[0]!.transactionHash).toBeUndefined();
    expect(events[0]!.version).toBeGreaterThan(head);
    await sui.expectPreview(bytes).toEmit({ kind: "account", accountAddress: staker! });

    // The same resolved transaction staking 1 MIST, under the validator set's minimum.
    const below = TransactionDataBuilder.fromBytes(bytes);
    const amount = below.inputs.find((i) => i.Pure?.bytes === u64(ONE_SUI))!;
    amount.Pure!.bytes = u64(1n);
    const err = await sui.previewEvents(below.build()).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect((err as StreamdTransactionError).vmStatus).toContain("MoveAbort");
    expect((err as StreamdTransactionError).transactionHash).toBeUndefined();
  }, 60_000);
});
