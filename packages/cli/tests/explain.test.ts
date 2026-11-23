import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdTimeoutError } from "@streamd/core";
import { formatExplanation } from "@streamd/aptos";
import type { TransactionExplanation } from "@streamd/aptos";
import type { ExplainCommandOptions } from "../src/args.js";
import { runExplain } from "../src/explain.js";

const HASH = `0x${"ab".repeat(32)}`;
const ALICE = `0x${"a11ce".padStart(64, "0")}`;
const BOB = `0x${"b0b".padStart(64, "0")}`;
const APT = `0x${"a".padStart(64, "0")}`;
const options: ExplainCommandOptions = { hash: HASH, network: "mainnet" };

const explanation: TransactionExplanation = {
  simulated: false,
  version: 10n,
  transactionHash: HASH,
  success: true,
  vmStatus: "Executed successfully",
  sender: ALICE,
  timestamp: 1_700_000_000_000_000n,
  gas: { units: 5414n, executionUnits: 35n, ioUnits: 55n, storageFee: 532400n, storageRefund: 1000n, unitPrice: 100n, fee: 541400n },
  transfers: [{ asset: APT, amount: 250000000n, from: ALICE, to: BOB, symbol: "APT", decimals: 8 }],
  balanceChanges: [
    { account: ALICE, asset: APT, delta: -250000000n, symbol: "APT", decimals: 8 },
    { account: BOB, asset: APT, delta: 250000000n, symbol: "APT", decimals: 8 },
  ],
  swaps: [],
  sales: [],
  events: [{ type: "0x1::m::E", data: { n: "1" }, version: 10n, sequenceNumber: 0n, eventIndex: 0 }],
};

async function run(explain: () => Promise<TransactionExplanation>, opts = options, signal = new AbortController().signal) {
  const out: string[] = [];
  const errors: string[] = [];
  const client = { explain: explain as never };
  const code = await runExplain(opts, { client, write: (t) => void out.push(t), error: (m) => errors.push(m), signal });
  return { code, out, errors };
}

describe("runExplain", () => {
  it("prints formatExplanation's text and exits 0", async () => {
    const { code, out, errors } = await run(async () => explanation);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(out).toEqual([formatExplanation(explanation)]);
    expect(out[0]).toContain("Balance changes:");
  });

  it("prints one JSON line with bigints as decimal strings under --json", async () => {
    const { code, out } = await run(async () => explanation, { ...options, json: true });
    expect(code).toBe(0);
    expect(out).toHaveLength(1);
    expect(out[0]!.endsWith("\n")).toBe(true);
    expect(out[0]!.trimEnd()).not.toContain("\n");
    const parsed = JSON.parse(out[0]!) as Record<string, any>;
    expect(parsed.version).toBe("10");
    expect(parsed.transactionHash).toBe(HASH);
    expect(parsed.gas.fee).toBe("541400");
    expect(parsed.balanceChanges[0].delta).toBe("-250000000");
    expect(parsed.events[0].sequenceNumber).toBe("0");
  });

  it("passes the hash and timeout, and leaves an unset timeout out", async () => {
    const calls: unknown[][] = [];
    const client = { explain: (async (...a: unknown[]) => (calls.push(a), explanation)) as never };
    const io = { client, write: () => {}, error: () => {}, signal: new AbortController().signal };
    await runExplain({ ...options, timeout: 500 }, io);
    await runExplain(options, io);
    expect(calls[0]![0]).toBe(HASH);
    expect(calls[0]![1]).toMatchObject({ timeout: 500 });
    expect(calls[1]![1]).not.toHaveProperty("timeout");
  });

  it("prints a failed transaction's explanation, then exits 1 with its status", async () => {
    const failed = { ...explanation, success: false, vmStatus: "Move abort in 0x1::coin: EINSUFFICIENT_BALANCE", transfers: [], balanceChanges: [] };
    const { code, out, errors } = await run(async () => failed, { ...options, json: true });
    expect(code).toBe(1);
    expect((JSON.parse(out[0]!) as { success: boolean }).success).toBe(false);
    expect(errors).toEqual(["streamd: transaction failed at version 10: Move abort in 0x1::coin: EINSUFFICIENT_BALANCE"]);
  });

  it("exits 1 with the message when the lookup fails, printing nothing", async () => {
    const { code, out, errors } = await run(async () => {
      throw new StreamdTimeoutError(`Transaction ${HASH} not found after 500ms`, 500);
    });
    expect(code).toBe(1);
    expect(out).toEqual([]);
    expect(errors).toEqual([`streamd: Transaction ${HASH} not found after 500ms`]);
  });

  it("exits 130, quietly, when interrupted while waiting", async () => {
    const controller = new AbortController();
    const { code, errors } = await run(
      async () => {
        controller.abort();
        throw new StreamdError("stopped");
      },
      options,
      controller.signal,
    );
    expect(code).toBe(130);
    expect(errors).toEqual([]);
  });

  it("exits 1 on a failed write, and 0 if the output simply closed", async () => {
    const client = { explain: (async () => explanation) as never };
    const errors: string[] = [];
    const failing = { client, error: (m: string) => errors.push(m), signal: new AbortController().signal };
    expect(await runExplain(options, { ...failing, write: () => Promise.reject(new Error("disk full")) })).toBe(1);
    expect(errors).toEqual(["streamd: disk full"]);
    const controller = new AbortController();
    const closed = await runExplain(options, {
      client,
      error: () => {},
      signal: controller.signal,
      write: () => {
        controller.abort();
        throw new Error("EPIPE");
      },
    });
    expect(closed).toBe(0);
  });
});
