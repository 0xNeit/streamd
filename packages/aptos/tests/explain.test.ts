import { describe, expect, it } from "bun:test";
import { AccountAddress, Aptos, AptosConfig, Ed25519PrivateKey, Network, parseTypeTag } from "@aptos-labs/ts-sdk";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { StreamdError, parseEvent } from "@streamd/core";
import type { EventAdapter, StreamdEvent, TransactionEvents, ViewRequest } from "@streamd/core";
import { toSale } from "../src/sales.js";
import { toSwap } from "../src/swaps.js";
import { formatExplanation } from "../src/index.js";
import type { TransactionExplanation } from "../src/index.js";
import { testClient } from "./helpers.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT = long("a");
const USDC = long("c0c");
const ALICE = long("a11ce");
const BOB = long("b0b");
const SPONSOR = long("5e");
const HASH = `0x${"ab".repeat(32)}`;
const STORES: Record<string, { asset: string; owner: string }> = {
  [long("a1")]: { asset: APT, owner: ALICE },
  [long("b1")]: { asset: APT, owner: BOB },
  [long("a2")]: { asset: USDC, owner: ALICE },
  [long("b2")]: { asset: USDC, owner: BOB },
};
const INFO: Record<string, [string, number]> = { [APT]: ["APT", 8], [USDC]: ["USDC", 6] };
const FEE = { total_charge_gas_units: "5414", execution_gas_units: "35", io_gas_units: "55", storage_fee_octas: "532400", storage_fee_refund_octas: "1000" };

const ev = (version: bigint, eventIndex: number, type: string, data: unknown, extra: Partial<StreamdEvent> = {}): StreamdEvent => ({
  type,
  data,
  version,
  sequenceNumber: 0n,
  eventIndex,
  sender: "0xA11CE",
  timestamp: 1_700_000_000_000_000n,
  entryFunction: { function: "0x1::aptos_account::transfer", typeArguments: [], arguments: [] },
  ...extra,
});
const fa = (v: bigint, i: number, side: string, store: string, amount: string) =>
  ev(v, i, `0x1::fungible_asset::${side}`, { store: long(store), amount }, { transactionHash: HASH });
const fee = (v: bigint, i: number) => ev(v, i, "0x1::transaction_fee::FeeStatement", FEE, { transactionHash: HASH });

/** Serves one committed transaction and one simulation, with views over `STORES` and `INFO` that record the version read. */
function adapter(found: TransactionEvents, simulated?: TransactionEvents, head = 99n) {
  const views: [string, bigint][] = [];
  const base: EventAdapter = {
    fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }),
    getHeadVersion: async () => head,
    getTransactionEvents: async () => found,
    ...(simulated !== undefined && { simulateTransactionEvents: async () => simulated }),
    view: async (request: ViewRequest, version: bigint) => {
      views.push([request.function, version]);
      const name = request.function.split("::").pop()!;
      if (name === "symbol" || name === "decimals") {
        const info = INFO[String(request.functionArguments![0])]!;
        return [name === "symbol" ? info[0] : String(info[1])];
      }
      const store = STORES[String(request.functionArguments![0])];
      if (store === undefined) throw new StreamdError("aborted");
      return request.function === "0x1::object::owner" ? [store.owner] : [{ inner: store.asset }];
    },
  };
  return { base, views };
}

const transfer: TransactionEvents = {
  version: 10n,
  success: true,
  vmStatus: "Executed successfully",
  gasUnitPrice: 100n,
  events: [fa(10n, 0, "Withdraw", "a1", "250000000"), fa(10n, 1, "Deposit", "b1", "250000000"), fee(10n, 2)],
};

/** A localnet SDK whose transport fails every request, so a test that reaches the network fails. */
function offline(): Aptos {
  const client = { provider: async () => Promise.reject(new Error("no network in tests")) };
  return new Aptos(new AptosConfig({ network: Network.LOCAL, fullnode: "http://fake/v1", client: client as never, clientConfig: { http2: false } }));
}

/** A transfer built offline: explicit ABI and options mean the build makes no request. */
const simulation = (aptos: Aptos) =>
  aptos.transaction.build.simple({
    sender: ALICE,
    data: {
      function: "0x1::aptos_account::transfer",
      functionArguments: [BOB, 1],
      abi: { typeParameters: [], parameters: [parseTypeTag("address"), parseTypeTag("u64")] },
    },
    options: { accountSequenceNumber: 0, gasUnitPrice: 100, maxGasAmount: 1000, expireTimestamp: 9_999_999_999 },
  });

describe("explain a committed transaction", () => {
  it("reports status, envelope, gas and fee, transfers with asset details and balance changes", async () => {
    const { base } = adapter(transfer);
    const explanation = await testClient({ adapter: base }).explain(HASH);
    expect(explanation).toEqual({
      simulated: false,
      version: 10n,
      transactionHash: HASH,
      success: true,
      vmStatus: "Executed successfully",
      sender: ALICE,
      timestamp: 1_700_000_000_000_000n,
      entryFunction: { function: "0x1::aptos_account::transfer", typeArguments: [], arguments: [] },
      gas: { units: 5414n, executionUnits: 35n, ioUnits: 55n, storageFee: 532400n, storageRefund: 1000n, unitPrice: 100n, fee: 541400n },
      transfers: [{ asset: APT, amount: 250000000n, from: ALICE, to: BOB, symbol: "APT", decimals: 8 }],
      balanceChanges: [
        { account: ALICE, asset: APT, delta: -250000000n, symbol: "APT", decimals: 8 },
        { account: BOB, asset: APT, delta: 250000000n, symbol: "APT", decimals: 8 },
      ],
      swaps: [],
      sales: [],
      events: transfer.events,
    });
  });

  it("nets each account over several transfers, skipping zero nets, and reads no asset details when asked not to", async () => {
    const found: TransactionEvents = {
      ...transfer,
      events: [
        fa(10n, 0, "Withdraw", "a1", "5"),
        fa(10n, 1, "Deposit", "b1", "5"),
        fa(10n, 2, "Withdraw", "b1", "5"),
        fa(10n, 3, "Deposit", "a1", "5"),
        fa(10n, 4, "Withdraw", "b2", "7"),
        fa(10n, 5, "Deposit", "a2", "7"),
      ],
    };
    const { base, views } = adapter(found);
    const explanation = await testClient({ adapter: base }).explain(HASH, { withAssetInfo: false });
    expect(explanation.transfers).toHaveLength(3);
    expect(explanation.balanceChanges).toEqual([
      { account: ALICE, asset: USDC, delta: 7n },
      { account: BOB, asset: USDC, delta: -7n },
    ]);
    expect(explanation.gas).toBeUndefined();
    expect(views.some(([fn]) => fn.endsWith("::symbol"))).toBe(false);
  });

  it("leaves the fee out when the source reports no gas price, and names a sponsored transaction's fee payer", async () => {
    const { gasUnitPrice: _, ...unpriced } = transfer;
    const { base } = adapter({ ...unpriced, feePayer: SPONSOR });
    const explanation = await testClient({ adapter: base }).explain(HASH, { withAssetInfo: false });
    expect(explanation.gas).toEqual({ units: 5414n, executionUnits: 35n, ioUnits: 55n, storageFee: 532400n, storageRefund: 1000n });
    expect(explanation.feePayer).toBe(SPONSOR);
  });

  it("explains a failed transaction with its gas instead of rejecting; the chain keeps only its fee statement", async () => {
    const { base } = adapter({ version: 11n, success: false, vmStatus: "Move abort in 0x1::coin: EINSUFFICIENT_BALANCE(0x10006)", gasUnitPrice: 100n, events: [fee(11n, 0)] });
    const explanation = await testClient({ adapter: base }).explain(HASH);
    expect(explanation).toMatchObject({ success: false, vmStatus: "Move abort in 0x1::coin: EINSUFFICIENT_BALANCE(0x10006)", transfers: [], balanceChanges: [] });
    expect(explanation.gas!.fee).toBe(541400n);
  });

  it("takes the hash given when the lookup has no events, and rejects a malformed fee statement", async () => {
    const empty = adapter({ version: 12n, success: true, vmStatus: "Executed successfully", events: [] });
    const explanation = await testClient({ adapter: empty.base }).explain(HASH);
    expect(explanation).toMatchObject({ transactionHash: HASH, transfers: [], events: [] });
    expect("sender" in explanation || "gas" in explanation).toBe(false);
    const bad = adapter({ ...transfer, events: [ev(10n, 0, "0x1::transaction_fee::FeeStatement", { ...FEE, total_charge_gas_units: "x" })] });
    await expect(testClient({ adapter: bad.base }).explain(HASH)).rejects.toThrow("does not have its struct's fields");
  });

  it("rejects with the signal's reason on abort and drops the reads still queued", async () => {
    const { base, views } = adapter(transfer);
    const view = base.view!;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    base.view = async (...args) => (await gate, view(...args));
    const controller = new AbortController();
    const client = testClient({ adapter: base, maxConcurrentRequests: 1 });
    let started = 0;
    const inner = base.view;
    base.view = (...args) => (started++, inner(...args));
    const pending = client.explain(HASH, { signal: controller.signal });
    for (let i = 0; i < 50 && started === 0; i++) await new Promise((r) => setTimeout(r, 1));
    controller.abort(new Error("stop"));
    await expect(pending).rejects.toThrow("stop");
    release();
    await new Promise((r) => setTimeout(r, 10));
    expect(started).toBe(1);
    expect(views.length).toBeLessThanOrEqual(1);
  });

  it("decodes the swaps and sales among its events", async () => {
    const swap = parseEvent(readFileSync(join(import.meta.dir, "fixtures/swaps.jsonl"), "utf8").split("\n")[0]!);
    const sale = parseEvent(readFileSync(join(import.meta.dir, "fixtures/sales.jsonl"), "utf8").split("\n")[0]!);
    const { base } = adapter({ ...transfer, events: [{ ...swap, eventIndex: 0 }, { ...sale, version: swap.version, eventIndex: 1 }] });
    const explanation = await testClient({ adapter: base }).explain(HASH);
    expect(explanation.swaps).toEqual([toSwap(swap)!.data]);
    expect(explanation.swaps[0]!.protocol).toBe("cellana");
    expect(explanation.sales).toEqual([toSale(sale)!.data]);
    expect(explanation.sales[0]!.marketplace).toBe("wapal");
  });

  it("names the sender, fee payer, counterparties, buyers and sellers under withNames", async () => {
    const sale = parseEvent(readFileSync(join(import.meta.dir, "fixtures/sales.jsonl"), "utf8").split("\n")[0]!);
    const { seller, buyer } = toSale(sale)!.data;
    const { base } = adapter({ ...transfer, feePayer: SPONSOR, events: [...transfer.events, { ...sale, version: 10n, eventIndex: 3 }] });
    const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    const reads: string[] = [];
    aptos.getPrimaryName = async ({ address }) => {
      reads.push(address.toString());
      return address.toString() === BOB ? "bob" : undefined;
    };
    aptos.getTargetAddress = async ({ name }) => (name === "bob.apt" ? AccountAddress.from(BOB) : undefined);
    const explanation = await testClient({ adapter: base, aptos }).explain(HASH, { withNames: true, withAssetInfo: false });
    expect(explanation.names).toEqual({ [ALICE]: null, [SPONSOR]: null, [BOB]: "bob.apt", [seller]: null, [buyer]: null });
    expect(reads.sort()).toEqual([ALICE, BOB, SPONSOR, seller, buyer].sort());
  });

  it("waits for the transaction to commit, as eventsOf does", async () => {
    let lookups = 0;
    const { base } = adapter(transfer);
    base.getTransactionEvents = async () => (++lookups < 3 ? undefined : transfer);
    const explanation = await testClient({ adapter: base, defaults: { pollInterval: 1 } }).explain(HASH, { withAssetInfo: false });
    expect(lookups).toBe(3);
    expect(explanation.version).toBe(10n);
    base.getTransactionEvents = async () => undefined;
    await expect(testClient({ adapter: base }).explain(HASH, { timeout: 50 })).rejects.toThrow("not found after 50ms");
  });
});

describe("explain a simulation", () => {

  it("reads its views at the newest state below the simulated version, with no hash", async () => {
    const sim = (v: bigint) => ({ ...transfer, version: v, events: transfer.events.map((e) => ({ ...e, version: v, transactionHash: undefined })) });
    const aptos = offline();
    const ahead = adapter(transfer, sim(120n), 99n);
    const explanation = await testClient({ adapter: ahead.base, aptos }).explain(await simulation(aptos));
    expect(explanation).toMatchObject({ simulated: true, version: 120n, success: true });
    expect("transactionHash" in explanation).toBe(false);
    expect(explanation.transfers).toEqual([{ asset: APT, amount: 250000000n, from: ALICE, to: BOB, symbol: "APT", decimals: 8 }]);
    expect(new Set(ahead.views.map(([, v]) => v))).toEqual(new Set([99n]));

    const behind = adapter(transfer, sim(50n), 99n);
    await testClient({ adapter: behind.base, aptos }).explain(await simulation(aptos), { withAssetInfo: false });
    expect(new Set(behind.views.map(([, v]) => v))).toEqual(new Set([49n]));
  });

  it("leaves out a leg whose store the simulation would create, and refuses a timeout", async () => {
    const aptos = offline();
    const events = [fa(120n, 0, "Withdraw", "a1", "5"), fa(120n, 1, "Deposit", "f9", "5")].map((e) => ({ ...e, transactionHash: undefined }));
    const { base } = adapter(transfer, { version: 120n, success: true, vmStatus: "Executed successfully", events });
    const explanation = await testClient({ adapter: base, aptos }).explain(await simulation(aptos), { withAssetInfo: false });
    expect(explanation.transfers).toEqual([{ asset: APT, amount: 5n, from: ALICE, to: undefined }]);
    expect(explanation.balanceChanges).toEqual([{ account: ALICE, asset: APT, delta: -5n }]);
    await expect(testClient({ adapter: base, aptos }).explain(await simulation(aptos), { timeout: 5 })).rejects.toThrow("a simulation does not wait");
  });

  it("explains a failed simulation and passes the signer key on", async () => {
    const aptos = offline();
    const { base } = adapter(transfer, { version: 5n, success: false, vmStatus: "Out of gas", events: [] });
    let asked: unknown;
    const simulate = base.simulateTransactionEvents!;
    base.simulateTransactionEvents = (s) => ((asked = s), simulate(s));
    const key = new Ed25519PrivateKey(`ed25519-priv-0x${"11".repeat(32)}`).publicKey();
    const explanation = await testClient({ adapter: base, aptos }).explain(await simulation(aptos), { signerPublicKey: key });
    expect(explanation).toMatchObject({ simulated: true, success: false, vmStatus: "Out of gas", transfers: [] });
    expect(explanation.gas).toBeUndefined();
    expect((asked as { signerPublicKey: unknown }).signerPublicKey).toBe(key);
  });
});

describe("explain refusals", () => {
  it("refuses bad input before any read", async () => {
    const { base } = adapter(transfer);
    let lookups = 0;
    base.getTransactionEvents = async () => (lookups++, transfer);
    const client = testClient({ adapter: base });
    await expect(client.explain("0x1234")).rejects.toThrow("Not a valid transaction hash");
    const key = new Ed25519PrivateKey(`ed25519-priv-0x${"11".repeat(32)}`).publicKey();
    await expect(client.explain(HASH, { signerPublicKey: key })).rejects.toThrow("Signer keys are for simulating");
    await expect(client.explain(HASH, { timeout: -1 })).rejects.toThrow("Invalid timeout");
    await expect(client.explain(HASH, { withNames: true })).rejects.toThrow("withNames");
    await expect(client.explain({} as never)).rejects.toThrow("Not a built transaction");
    const { view: _, ...viewless } = base;
    await expect(testClient({ adapter: viewless }).explain(HASH)).rejects.toThrow("explain needs an adapter that implements view");
    const aptos = offline();
    await expect(testClient({ adapter: base, aptos }).explain(await simulation(aptos))).rejects.toThrow("simulateTransactionEvents");
    expect(lookups).toBe(0);
  });

  it("names explain when the adapter cannot look a hash up", async () => {
    const { base } = adapter(transfer);
    const { getTransactionEvents: _, ...unlookable } = base;
    await expect(testClient({ adapter: unlookable }).explain(HASH)).rejects.toThrow("explain needs an adapter that implements getTransactionEvents");
  });

  it("rejects when a store read fails for another reason than a missing store", async () => {
    const { base } = adapter(transfer);
    base.view = async () => {
      throw new Error("socket hang up");
    };
    await expect(testClient({ adapter: base }).explain(HASH)).rejects.toThrow("socket hang up");
  });
});

describe("formatExplanation", () => {
  const base: TransactionExplanation = {
    simulated: false,
    version: 10n,
    transactionHash: HASH,
    success: true,
    vmStatus: "Executed successfully",
    sender: ALICE,
    timestamp: 1_700_000_000_000_000n,
    entryFunction: { function: "0x1::aptos_account::transfer", typeArguments: [], arguments: [] },
    gas: { units: 5414n, executionUnits: 35n, ioUnits: 55n, storageFee: 532400n, storageRefund: 1000n, unitPrice: 100n, fee: 541400n },
    transfers: [{ asset: APT, amount: 250000000n, from: ALICE, to: BOB, symbol: "APT", decimals: 8 }],
    balanceChanges: [
      { account: ALICE, asset: APT, delta: -250000000n, symbol: "APT", decimals: 8 },
      { account: BOB, asset: APT, delta: 250000000n, symbol: "APT", decimals: 8 },
    ],
    swaps: [],
    sales: [],
    events: [],
  };

  it("renders a committed transfer with names, gas and asset details", () => {
    expect(formatExplanation({ ...base, feePayer: SPONSOR, names: { [ALICE]: null, [BOB]: "bob.apt", [SPONSOR]: null } })).toBe(
      [
        `Transaction ${HASH} at version 10, 2023-11-14T22:13:20.000Z`,
        "Status: success (Executed successfully)",
        `Sender: ${ALICE}`,
        `Fee payer: ${SPONSOR}`,
        "Function: 0x1::aptos_account::transfer",
        "Gas: 5414 units at 100 octas, fee 0.005414 APT (storage 0.005324 APT, refund 0.00001 APT)",
        "Transfers:",
        `  2.5 APT from ${ALICE} to ${BOB} (bob.apt)`,
        "Balance changes:",
        `  ${ALICE}: -2.5 APT`,
        `  ${BOB} (bob.apt): +2.5 APT`,
        "Events: 0",
        "",
      ].join("\n"),
    );
  });

  it("renders a failed simulation without a price, a hash or a time", () => {
    const { unitPrice: _p, fee: _f, ...gas } = base.gas!;
    const { transactionHash: _h, timestamp: _t, entryFunction: _e, ...rest } = base;
    const text = formatExplanation({ ...rest, simulated: true, success: false, vmStatus: "Out of gas", gas, transfers: [], balanceChanges: [] });
    expect(text.split("\n")).toEqual([
      "Simulated transaction at version 10",
      "Status: failed (Out of gas)",
      `Sender: ${ALICE}`,
      "Gas: 5414 units (storage 0.005324 APT, refund 0.00001 APT)",
      "Events: 0",
      "",
    ]);
  });

  it("renders raw units without details, lone sides, swaps with their transaction's details, and sales", () => {
    const text = formatExplanation({
      ...base,
      gas: undefined,
      transfers: [
        { asset: USDC, amount: 7n, to: ALICE },
        { asset: APT, amount: 5n, from: BOB, symbol: "APT", decimals: 8 },
      ],
      balanceChanges: [{ account: ALICE, asset: USDC, delta: 7n }],
      swaps: [{ protocol: "cellana", poolId: long("9"), assetIn: APT, amountIn: 5n, assetOut: USDC, amountOut: 7n }],
      sales: [{ marketplace: "wapal", kind: "listing", token: long("70"), seller: BOB, buyer: ALICE, price: 450000000n }],
    });
    expect(text.split("\n").slice(4)).toEqual([
      "Transfers:",
      `  7 ${USDC} to ${ALICE}`,
      `  0.00000005 APT from ${BOB}`,
      "Balance changes:",
      `  ${ALICE}: +7 ${USDC}`,
      "Swaps:",
      `  cellana: 0.00000005 APT in, 7 ${USDC} out, pool ${long("9")}`,
      "Sales:",
      `  wapal listing: ${long("70")} from ${BOB} to ${ALICE} for 450000000 base units`,
      "Events: 0",
      "",
    ]);
  });

  it("replaces control and reordering characters in on-chain text", () => {
    const text = formatExplanation({
      ...base,
      vmStatus: "abort\u001b[2J",
      transfers: [{ asset: APT, amount: 1n, from: ALICE, to: BOB, symbol: "AP\u202eT", decimals: 0 }],
      balanceChanges: [],
      names: { [BOB]: "bob\u009b.apt" },
    });
    expect(text).toContain("Status: success (abort?[2J)");
    expect(text).toContain(`  1 AP?T from ${ALICE} to ${BOB} (bob?.apt)`);
    expect(/[\u0000-\u0009\u000b-\u001f\u007f-\u009f\u202e]/.test(text)).toBe(false);
    expect(formatExplanation({ ...base, feePayer: "0x1\u001b]0;x\u0007" })).toContain("Fee payer: 0x1?]0;x?");
  });

  it("falls back to base units for decimals a u8 cannot hold, and drops a time it cannot render", () => {
    const text = formatExplanation({
      ...base,
      timestamp: 10n ** 30n,
      transfers: [{ asset: APT, amount: 5n, from: ALICE, to: BOB, symbol: "APT", decimals: 300 }],
      balanceChanges: [],
    });
    expect(text.split("\n")[0]).toBe(`Transaction ${HASH} at version 10`);
    expect(text).toContain(`  5 ${APT} from ${ALICE} to ${BOB}`);
  });
});
