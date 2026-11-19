import { describe, expect, it } from "bun:test";
import { AccountAddress, EntryFunction, MultiSigTransactionPayload, U64 } from "@aptos-labs/ts-sdk";
import type { ViewRequest } from "@streamd/core";
import { decodeCall, readMultisig } from "../src/multisig.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const M = long("m0");
const [A, B, C] = [long("a"), long("b"), long("c")];

const transfer = (amount: number) =>
  new MultiSigTransactionPayload(
    EntryFunction.build("0x1::aptos_account", "transfer", [], [AccountAddress.from(long("cafe")), new U64(amount)]),
  ).bcsToHex().toString();

const item = (fields: Partial<Record<string, unknown>> = {}) => ({
  creation_time_secs: "1000",
  creator: "0xa",
  payload: { vec: [transfer(1_000_000)] },
  payload_hash: { vec: [] },
  votes: { data: [{ key: "0xa", value: true }] },
  ...fields,
});

/** Answers each view by name, recording what was asked. */
function views(replies: Record<string, unknown>, asked: [string, bigint][] = []) {
  return async (request: ViewRequest, version: bigint) => {
    const name = request.function.split("::")[2]!;
    asked.push([name, version]);
    if (!(name in replies)) throw new Error(`unexpected view ${name}`);
    return [replies[name]];
  };
}

const base = {
  owners: ["0xa", "0xb", "0xc"],
  num_signatures_required: "2",
  last_resolved_sequence_number: "4",
  next_sequence_number: "7",
  get_pending_transactions: [item(), item({ payload: { vec: [] }, payload_hash: { vec: ["0x1df0"] }, votes: { data: [{ key: "0xb", value: false }, { key: "0xc", value: true }] } })],
  timelock_period: "0",
  timelock_override_threshold: { vec: [] },
};

describe("decodeCall", () => {
  it("reads the entry function and each argument's BCS bytes", () => {
    expect(decodeCall(transfer(1_000_000))).toEqual({
      function: "0x1::aptos_account::transfer",
      typeArguments: [],
      arguments: [long("cafe"), "0x40420f0000000000"],
    });
  });

  it("is undefined for bytes that do not decode, or decode with bytes left over", () => {
    expect(decodeCall("0x05")).toBeUndefined();
    expect(decodeCall(`${transfer(1)}00`)).toBeUndefined();
  });
});

describe("readMultisig", () => {
  it("numbers pending transactions from the last resolved one and splits the votes", async () => {
    const asked: [string, bigint][] = [];
    const state = await readMultisig(views(base, asked), M, 50n);
    expect(state).toMatchObject({ address: M, version: 50n, owners: [A, B, C], threshold: 2n, lastResolved: 4n, nextSequenceNumber: 7n });
    expect(state.timelock).toBeUndefined();
    expect([...state.pending.keys()]).toEqual([5n, 6n]);
    expect(state.pending.get(5n)).toMatchObject({ creator: A, createdAtSecs: 1000n, approvals: [A], rejections: [], call: { function: "0x1::aptos_account::transfer" } });
    const hashed = state.pending.get(6n)!;
    expect(hashed).toMatchObject({ payloadHash: "0x1df0", approvals: [C], rejections: [B] });
    expect(hashed.payload).toBeUndefined();
    expect(hashed.call).toBeUndefined();
    expect(asked).toHaveLength(7);
    expect(asked.every(([, v]) => v === 50n)).toBe(true);
  });

  it("gives each transaction its timelock release unless the override is met", async () => {
    const state = await readMultisig(
      views({ ...base, timelock_period: "3600", timelock_override_threshold: { vec: ["1"] } }),
      M,
      1n,
    );
    expect(state.timelock).toEqual({ periodSecs: 3600n, overrideThreshold: 1n });
    expect(state.pending.get(5n)!.executableAtSecs).toBeUndefined();
    const strict = await readMultisig(views({ ...base, timelock_period: "3600" }), M, 1n);
    expect(strict.timelock).toEqual({ periodSecs: 3600n });
    expect(strict.pending.get(5n)!.executableAtSecs).toBe(4600n);
  });

  it("refuses a reply not shaped as the view returns, and passes a failed view on", async () => {
    await expect(readMultisig(views({ ...base, num_signatures_required: 2 }), M, 1n)).rejects.toThrow("signatures required");
    await expect(readMultisig(views({ ...base, get_pending_transactions: [item({ votes: { data: [{ key: "0xa", value: "yes" }] } })] }), M, 1n)).rejects.toThrow("pending transaction 5");
    await expect(readMultisig(views({ ...base, get_pending_transactions: [item({ payload: { vec: ["zz"] } })] }), M, 1n)).rejects.toThrow("pending transaction 5");
    const { owners: _, ...none } = base;
    await expect(readMultisig(views(none), M, 1n)).rejects.toThrow("unexpected view owners");
  });
});
