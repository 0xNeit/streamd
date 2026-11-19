import { describe, expect, it } from "bun:test";
import {
  AccountAddress,
  EntryFunction,
  Hex,
  MultiSigTransactionPayload,
  TransactionPayloadEntryFunction,
  TransactionPayloadMultiSig,
  U64,
  hashValues,
} from "@aptos-labs/ts-sdk";
import type { SimpleTransaction } from "@aptos-labs/ts-sdk";
import { MemoryAdapter } from "@streamd/core";
import type { StreamdEvent, ViewRequest } from "@streamd/core";
import { decodeCall, previewMultisig, readMultisig } from "../src/multisig.js";
import type { PreviewHost } from "../src/multisig.js";
import type { MultisigState } from "../src/types.js";
import { testClient } from "./helpers.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const M = long("e0");
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

const ev = (version: number, eventIndex: number, name: string, account: string): StreamdEvent => ({
  type: `0x1::multisig_account::${name}`,
  data: { multisig_account: account, sequence_number: "5" },
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex,
});

/** A memory adapter whose views answer `base`, with the version read as `next_sequence_number`. */
function multisigAdapter(events: StreamdEvent[], asked: [string, bigint][] = []) {
  const memory = new MemoryAdapter(events);
  const view = async (request: ViewRequest, version: bigint) => {
    const name = request.function.split("::")[2]!;
    asked.push([name, version]);
    if (request.functionArguments?.[0] !== M) throw new Error("not a multisig");
    return [name === "next_sequence_number" ? String(version) : base[name as keyof typeof base]];
  };
  return { adapter: Object.assign(memory, { view }), memory, asked };
}

const waitFor = async (check: () => boolean) => {
  for (let i = 0; i < 100 && !check(); i++) await new Promise((r) => setTimeout(r, 10));
  if (!check()) throw new Error("timed out waiting");
};

describe("multisig", () => {
  const history = [ev(10, 0, "CreateTransaction", long("other")), ev(11, 0, "CreateTransaction", "0xe0"), ev(11, 1, "Vote", M), ev(20, 0, "TransactionExecutionSucceeded", M)];

  it("reads the state before fromVersion, then rereads it once per version of the account's own events", async () => {
    const { adapter, asked } = multisigAdapter(history);
    const client = testClient({ adapter });
    const changes: bigint[] = [];
    const multisig = client.multisig(M, { fromVersion: 5n, onChange: (s) => changes.push(s.version) });
    expect(client.listenerCount()).toBe(0);
    const state = await multisig.ready;
    expect(state).toBe(multisig.value);
    expect(state).toMatchObject({ address: M, version: 20n, nextSequenceNumber: 20n, owners: [A, B, C] });
    expect(multisig.get(5n)?.approvals).toEqual([A]);
    expect([...new Set(asked.map(([, v]) => v))]).toEqual([4n, 11n, 20n]);
    expect(asked).toHaveLength(21);
    expect(changes).toEqual([]);
    expect(multisig.active).toBe(true);
    multisig.unsubscribe();
    expect(multisig.active).toBe(false);
    client.destroy();
  });

  it("starts at the head and calls onChange with the reread state after each live event", async () => {
    const { adapter, memory } = multisigAdapter(history);
    const client = testClient({ adapter, defaults: { pollInterval: 5 } });
    const changes: [bigint, string][] = [];
    const multisig = client.multisig("0xe0", { onChange: (s: MultisigState, e) => changes.push([s.version, e.type]) });
    expect((await multisig.ready).version).toBe(20n);
    memory.push(ev(30, 0, "Vote", long("other")), ev(31, 0, "Vote", M), ev(31, 1, "TransactionExecutionSucceeded", M));
    await waitFor(() => changes.length === 2);
    expect(changes).toEqual([
      [31n, "0x1::multisig_account::Vote"],
      [31n, "0x1::multisig_account::TransactionExecutionSucceeded"],
    ]);
    expect(multisig.value.version).toBe(31n);
    client.destroy();
  });

  it("refuses bad options and adapters at the call, and rejects ready when the start read fails", async () => {
    const { adapter } = multisigAdapter([]);
    const client = testClient({ adapter });
    expect(() => client.multisig(M, { where: () => true } as never)).toThrow("Unsupported multisig option: where");
    expect(() => client.multisig(M, { fromVersion: -1n })).toThrow("fromVersion must be a bigint");
    expect(() => client.multisig(M, { fromVersion: 3 as never })).toThrow("fromVersion must be a bigint");
    expect(() => testClient({ adapter: new MemoryAdapter() }).multisig(M)).toThrow("implements view and getHeadVersion");
    const other = client.multisig(long("beef"), { fromVersion: 1n });
    await expect(other.ready).rejects.toThrow("not a multisig");
    expect(other.active).toBe(false);
    client.destroy();
  });
});

describe("previewMultisig", () => {
  const sha3 = (hex: string) => Hex.fromHexInput(hashValues([Hex.fromHexInput(hex).toUint8Array()])).toString();
  const event = (type: string, account = M): StreamdEvent => ({ type, data: { multisig_account: account }, version: 99n, sequenceNumber: 0n, eventIndex: 0 });
  const simulated = [
    event("0x1::fungible_asset::Withdraw"),
    event("0x1::multisig_account::Vote"),
    event("0x1::multisig_account::Vote", long("other")),
    event("0x1::multisig_account::TransactionExecutionSucceeded"),
    event("0x1::transaction_fee::FeeStatement"),
  ];

  async function setup(canExecute: string[]) {
    const state = await readMultisig(
      views({ ...base, get_pending_transactions: [item(), item({ payload: { vec: [] }, payload_hash: { vec: [sha3(transfer(5))] } })] }),
      M,
      50n,
    );
    const built: [string, unknown][] = [];
    const asked: string[] = [];
    const host: PreviewHost = {
      view: async (request) => {
        asked.push(String(request.functionArguments![0]));
        return [canExecute.includes(String(request.functionArguments![0]))];
      },
      head: async () => 60n,
      build: async (sender, payload) => (built.push([sender, payload]), { sender } as unknown as SimpleTransaction),
      simulate: async () => simulated,
    };
    return { state, host, built, asked };
  }

  it("sends the next transaction through the multisig as the first owner can_execute names", async () => {
    const { state, host, built, asked } = await setup([B, C]);
    const preview = await previewMultisig(host, state, 5n);
    expect(asked).toEqual([A, B]);
    expect(built[0]![0]).toBe(B);
    expect(built[0]![1]).toBeInstanceOf(TransactionPayloadMultiSig);
    expect(preview).toMatchObject({ sequenceNumber: 5n, sender: B, partial: false });
    expect(preview.events.map((e) => e.type)).toEqual(["0x1::fungible_asset::Withdraw", "0x1::multisig_account::Vote", "0x1::transaction_fee::FeeStatement"]);
    expect(preview.executorEvents.map((e) => e.type)).toEqual(["0x1::multisig_account::Vote", "0x1::multisig_account::TransactionExecutionSucceeded"]);
  });

  it("falls back to the entry function as the multisig when no owner can execute, or it is not next", async () => {
    const none = await setup([]);
    expect(await previewMultisig(none.host, none.state, 5n)).toMatchObject({ sender: M, partial: true });
    expect(none.built[0]![1]).toBeInstanceOf(TransactionPayloadEntryFunction);
    const later = await setup([A]);
    expect(await previewMultisig(later.host, later.state, 6n, { payload: transfer(5) })).toMatchObject({ sender: M, partial: true });
    expect(later.asked).toEqual([]);
  });

  it("checks a hash-only payload before building anything, and refuses what it cannot preview", async () => {
    const { state, host, built } = await setup([]);
    await expect(previewMultisig(host, state, 6n)).rejects.toThrow("stores only its payload's hash");
    await expect(previewMultisig(host, state, 6n, { payload: transfer(6) })).rejects.toThrow("does not match");
    await expect(previewMultisig(host, state, 6n, { payload: "nothex" })).rejects.toThrow("bytes or 0x hex");
    await expect(previewMultisig(host, state, 5n, { payload: transfer(1_000_000) })).rejects.toThrow("stores its payload");
    await expect(previewMultisig(host, state, 7n)).rejects.toThrow("transaction 7 is not pending");
    await expect(previewMultisig(host, state, 5 as never)).rejects.toThrow("bigint sequence number");
    expect(built).toEqual([]);
    await previewMultisig(host, state, 6n, { payload: Hex.fromHexInput(transfer(5)).toUint8Array() });
    expect(built).toHaveLength(1);
  });

  it("waits for ready on the client", async () => {
    const { adapter } = multisigAdapter([]);
    const client = testClient({ adapter });
    const multisig = client.multisig(M);
    await expect(multisig.preview(6n)).rejects.toThrow("stores only its payload's hash");
    client.destroy();
  });
});
