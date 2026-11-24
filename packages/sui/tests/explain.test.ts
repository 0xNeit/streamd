import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdTimeoutError } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiClient } from "../src/index.js";
import { LONG, digest, fakeLedger, fakeNames, ptb, withTransfers, type FakeTransaction } from "./helpers.js";

const SUI = `${LONG("2")}::sui::SUI`;
const USDC = `${LONG("dba3")}::usdc::USDC`;
const [ALICE, BOB, SPONSOR] = [LONG("a11ce"), LONG("b0b"), LONG("5905")];
const SWAP = `${LONG("abc")}::pool::Swap`;
const GAS = { computationCost: 1000n, storageCost: 2000n, storageRebate: 500n, nonRefundableStorageFee: 5n };
const CALL = { package: "0x2", module: "pay", function: "split_and_transfer", typeArguments: [], arguments: [] };
const [OK, FAILED] = [digest(1), digest(2)];
const TRANSACTIONS: FakeTransaction[] = [
  {
    digest: OK, cp: 1n, ti: 2n, events: [{ type: SWAP, sender: "0xa11ce" }], ptb: ptb([CALL]),
    sender: "0xa11ce", payer: "0x5905", price: 750n, gas: GAS,
    changes: [["0xa11ce", "0x2::sui::SUI", -40n], ["0xb0b", "0x2::sui::SUI", 40n], ["0x5905", "0x2::sui::SUI", -2500n], ["0xa11ce", USDC, -9n], ["0xb0b", USDC, 9n]],
  },
  { digest: FAILED, cp: 3n, ti: 1n, error: "MoveAbort(m::z, 101) in command 1", events: [], sender: "0xa11ce", payer: "0xa11ce", gas: GAS, changes: [["0xa11ce", "0x2::sui::SUI", -2500n]] },
];
const COINS = new Map([[SUI, { symbol: "SUI", decimals: 9 }], [USDC, { symbol: "USDC", decimals: 6 }]]);

function client() {
  const ledger = fakeLedger([1, 3, 2, 4], [], undefined, 0n, TRANSACTIONS);
  const wired = withTransfers(ledger.client, [1, 3, 2, 4], [], new Map(), COINS);
  const names = fakeNames({ "alice.sui": ALICE }, { [ALICE]: "alice.sui" });
  const sui = new SuiClient({ sui: { ...wired.client, nameService: names.nameService } as unknown as SuiGrpcClient });
  return { sui, coins: wired.calls.coins, lookups: ledger.calls.transactions, names: names.calls };
}

describe("SuiClient.explain", () => {
  it("explains a sponsored transaction: status, call, gas, transfers, balance changes and events", async () => {
    const { sui, coins } = client();
    const e = await sui.explain(OK);
    expect(e).toMatchObject({
      simulated: false, version: 3n, transactionHash: OK, success: true, vmStatus: "success",
      sender: ALICE, feePayer: SPONSOR, timestamp: 1_700_000_001_000_007n, swaps: [], sales: [],
    });
    expect(e.entryFunction?.function).toBe(`${LONG("2")}::pay::split_and_transfer`);
    expect(e.gas).toEqual({ ...GAS, fee: 2500n, unitPrice: 750n });
    expect(e.transfers).toEqual([
      { asset: SUI, amount: 40n, from: ALICE, to: BOB, symbol: "SUI", decimals: 9 },
      { asset: USDC, amount: 9n, from: ALICE, to: BOB, symbol: "USDC", decimals: 6 },
    ]);
    expect(e.balanceChanges).toEqual([
      { account: ALICE, asset: SUI, delta: -40n, symbol: "SUI", decimals: 9 },
      { account: ALICE, asset: USDC, delta: -9n, symbol: "USDC", decimals: 6 },
      { account: BOB, asset: SUI, delta: 40n, symbol: "SUI", decimals: 9 },
      { account: BOB, asset: USDC, delta: 9n, symbol: "USDC", decimals: 6 },
    ]);
    expect(e.events.map((x) => x.type)).toEqual([SWAP]);
    expect(coins.sort()).toEqual([SUI, USDC].sort());
    expect(e.names).toBeUndefined();
  });

  it("explains a failed transaction rather than rejecting, with its gas and no transfers", async () => {
    const e = await client().sui.explain(FAILED);
    expect(e).toMatchObject({ version: 7n, success: false, vmStatus: "MoveAbort(m::z, 101) in command 1", sender: ALICE, transfers: [], balanceChanges: [] });
    expect(e.feePayer).toBeUndefined();
    expect(e.gas?.fee).toBe(2500n);
  });

  it("reads no coin details under withAssetInfo: false", async () => {
    const { sui, coins } = client();
    const e = await sui.explain(OK, { withAssetInfo: false });
    expect(e.transfers[0]).toEqual({ asset: SUI, amount: 40n, from: ALICE, to: BOB });
    expect(coins).toEqual([]);
  });

  it("names the sender, sponsor and every counterparty under withNames", async () => {
    const { sui, names } = client();
    const e = await sui.explain(OK, { withNames: true, withAssetInfo: false });
    expect(e.names).toEqual({ [ALICE]: "alice.sui", [SPONSOR]: null, [BOB]: null });
    expect(names.reverse.sort()).toEqual([ALICE, BOB, SPONSOR].sort());
  });

  it("times out on a digest never checkpointed", async () => {
    const err = await client().sui.explain(digest(9), { timeout: 250 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
  });

  it("refuses a hex hash, a bad timeout and a bad withNames before any lookup", async () => {
    const { sui, lookups } = client();
    for (const call of [
      () => sui.explain(`0x${"ab".repeat(32)}`),
      () => sui.explain(OK, { timeout: -1 }),
      () => sui.explain(OK, { withNames: "yes" as unknown as boolean }),
    ]) {
      const err = await call().catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    expect(lookups).toEqual([]);
  });
});
