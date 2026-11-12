import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { eventId } from "@streamd/core";
import { SuiAdapter, SuiClient, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const SUI = `0x${"2".padStart(64, "0")}::sui::SUI`;

describe.skipIf(!enabled)("SuiClient transfers and balances (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("builds records that sum, per address, to each transaction's balance changes less gas", async () => {
    using sui = new SuiClient({ sui: grpc });
    const records = await sui.getTransfers({ fromVersion: -600n, toVersion: "head" });
    expect(records.length).toBeGreaterThan(0);
    const digests = [...new Set(records.map((r) => r.transactionHash!))];
    for (const digest of digests.slice(0, 10)) {
      const { response } = await grpc.ledgerService.getTransaction({
        digest,
        readMask: { paths: ["balance_changes", "effects.gas_used", "transaction.gas_payment.owner"] },
      });
      const txn = response.transaction!;
      const expected = new Map<string, bigint>();
      const add = (key: string, amount: bigint) => expected.set(key, (expected.get(key) ?? 0n) + amount);
      for (const c of txn.balanceChanges) add(`${c.address}|${c.coinType}`, BigInt(c.amount!));
      const gas = txn.effects!.gasUsed!;
      add(`${txn.transaction!.gasPayment!.owner}|${SUI}`, gas.computationCost! + gas.storageCost! - gas.storageRebate!);
      const summed = new Map<string, bigint>();
      for (const { data } of records.filter((r) => r.transactionHash === digest)) {
        if (data.from !== undefined) summed.set(`${data.from}|${data.asset}`, (summed.get(`${data.from}|${data.asset}`) ?? 0n) - data.amount);
        if (data.to !== undefined) summed.set(`${data.to}|${data.asset}`, (summed.get(`${data.to}|${data.asset}`) ?? 0n) + data.amount);
      }
      for (const [key, amount] of expected) if (amount === 0n) expected.delete(key);
      expect(summed).toEqual(expected);
    }
  }, 120_000);

  it("narrows to an account, adds asset info, and reads its balances", async () => {
    using sui = new SuiClient({ sui: grpc });
    const [record] = await sui.getTransfers({ fromVersion: -600n, toVersion: "head", limit: 1 });
    const account = record!.data.from ?? record!.data.to!;
    const mine = await sui.getTransfers({ account, fromVersion: record!.version, toVersion: record!.version, withAssetInfo: true });
    expect(mine.map(eventId)).toContain(eventId(record!));
    expect(mine.every((r) => r.data.from === account || r.data.to === account)).toBe(true);
    const adapter = new SuiAdapter(grpc);
    const info = await adapter.coinInfo(record!.data.asset);
    expect(mine.find((r) => eventId(r) === eventId(record!))!.data.decimals).toBe(info?.decimals);
    expect(await adapter.coinInfo("0x2::sui::SUI")).toEqual({ symbol: "SUI", decimals: 9 });

    const before = await adapter.getBalance(account, SUI);
    using wallet = sui.balances(account);
    const ready = await wallet.ready;
    const after = await adapter.getBalance(account, SUI);
    expect([before, after]).toContain(ready.get(SUI)!);
  }, 120_000);
});
