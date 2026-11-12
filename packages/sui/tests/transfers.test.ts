import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { transfersOf, type BalanceChangeTransaction } from "../src/transfers.js";
import { LONG } from "./helpers.js";

const SUI = `${LONG("2")}::sui::SUI`;
const USDC = `${LONG("dba")}::usdc::USDC`;
const [ALICE, BOB, CAROL, SPONSOR] = [LONG("a11ce"), LONG("b0b"), LONG("ca401"), LONG("5905")];

/** Gas of 3 net: computation 2, storage 5, rebate 4. */
function txn(changes: [string, string, bigint][], extra: Partial<BalanceChangeTransaction> = {}): BalanceChangeTransaction {
  return {
    digest: "D1",
    timestamp: { seconds: 1_700_000_000n, nanos: 5_000 },
    transaction: { sender: "0xa11ce", gasPayment: { owner: ALICE } },
    effects: { gasUsed: { computationCost: 2n, storageCost: 5n, storageRebate: 4n } },
    balanceChanges: changes.map(([address, coinType, amount]) => ({ address, coinType, amount: String(amount) })),
    events: { events: [{}, {}] },
    ...extra,
  };
}

const data = (records: ReturnType<typeof transfersOf>) => records.map((r) => r.data);

describe("transfersOf", () => {
  it("pairs a SUI payment as the amount sent, with gas taken out", () => {
    const records = transfersOf(txn([[ALICE, SUI, -103n], [BOB, SUI, 100n]]), 7n);
    expect(data(records)).toEqual([{ asset: SUI, amount: 100n, from: ALICE, to: BOB }]);
    expect(records[0]).toMatchObject({ type: SUI, version: 7n, sequenceNumber: 0n, eventIndex: 2, transactionHash: "D1", timestamp: 1_700_000_000_000_005n, sender: ALICE });
  });

  it("gives nothing for a gas-only or failed transaction", () => {
    expect(transfersOf(txn([[ALICE, SUI, -3n]]), 1n)).toEqual([]);
    expect(transfersOf(txn([], { effects: { gasUsed: { computationCost: 0n, storageCost: 0n, storageRebate: 0n } } }), 1n)).toEqual([]);
  });

  it("adds gas back to a sponsor, not the sender", () => {
    const sponsored = txn([[SPONSOR, SUI, -3n], [ALICE, SUI, -10n], [BOB, SUI, 10n]], {
      transaction: { sender: ALICE, gasPayment: { owner: SPONSOR } },
    });
    expect(data(transfersOf(sponsored, 1n))).toEqual([{ asset: SUI, amount: 10n, from: ALICE, to: BOB }]);
  });

  it("counts a payer with no listed change as having received the gas", () => {
    expect(data(transfersOf(txn([[BOB, SUI, -3n]]), 1n))).toEqual([{ asset: SUI, amount: 3n, from: BOB, to: ALICE }]);
  });

  it("splits one sender across receivers, and leaves lone sides open", () => {
    const records = transfersOf(txn([[ALICE, USDC, -10n], [BOB, USDC, 4n], [CAROL, USDC, 9n], [ALICE, SUI, -3n]]), 1n);
    expect(data(records)).toEqual([
      { asset: USDC, amount: 4n, from: ALICE, to: BOB },
      { asset: USDC, amount: 6n, from: ALICE, to: CAROL },
      { asset: USDC, amount: 3n, from: undefined, to: CAROL },
    ]);
    expect(data(transfersOf(txn([[ALICE, USDC, -5n], [ALICE, SUI, -3n]]), 1n))).toEqual([{ asset: USDC, amount: 5n, from: ALICE, to: undefined }]);
  });

  it("pairs coin types separately, in listed order, numbering past the events", () => {
    const records = transfersOf(txn([[ALICE, USDC, -5n], [BOB, SUI, -2n], [ALICE, SUI, -1n], [CAROL, SUI, 0n]]), 1n);
    expect(data(records)).toEqual([
      { asset: USDC, amount: 5n, from: ALICE, to: undefined },
      { asset: SUI, amount: 2n, from: BOB, to: ALICE },
    ]);
    expect(records.map((r) => r.eventIndex)).toEqual([2, 3]);
  });

  it("narrows to one coin type before pairing", () => {
    expect(data(transfersOf(txn([[ALICE, USDC, -5n], [BOB, USDC, 5n], [ALICE, SUI, -3n]]), 1n, SUI))).toEqual([]);
    expect(data(transfersOf(txn([[ALICE, USDC, -5n], [BOB, USDC, 5n]]), 1n, USDC))).toHaveLength(1);
  });

  it("normalises short addresses and types", () => {
    expect(data(transfersOf(txn([["0xa11ce", "0x2::sui::SUI", -13n], ["0xb0b", "0x2::sui::SUI", 10n]]), 1n))).toEqual([
      { asset: SUI, amount: 10n, from: ALICE, to: BOB },
    ]);
  });

  it.each(["1.5", "", "abc"])("refuses a malformed amount %p", (amount) => {
    const bad = txn([]);
    bad.balanceChanges = [{ address: ALICE, coinType: SUI, amount }];
    expect(() => transfersOf(bad, 1n)).toThrow(StreamdError);
  });
});
