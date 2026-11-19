import { Deserializer, Hex, MultiSigTransactionPayload } from "@aptos-labs/ts-sdk";
import { StreamdError } from "@streamd/core";
import type { EntryFunctionCall, ViewRequest } from "@streamd/core";
import { address, amount } from "./records.js";
import type { Fields } from "./records.js";
import type { MultisigState, MultisigTimelock, PendingMultisigTransaction } from "./types.js";

const MODULE = "0x1::multisig_account";

/** A view read at a version, as `EventAdapter.view` reads one. */
export type ViewAt = (request: ViewRequest, version: bigint) => Promise<unknown[]>;

const bad = (account: string, what: string) => new StreamdError(`multisig ${account}: ${what} is not what the view returns`);

function int(value: unknown, account: string, what: string): bigint {
  const n = amount(value);
  if (n === undefined) throw bad(account, what);
  return n;
}

function addressOf(value: unknown, account: string, what: string): string {
  const a = address(value);
  if (a === undefined) throw bad(account, what);
  return a;
}

/** A Move `Option<T>`, written `{ vec: [] }` or `{ vec: [value] }`. */
function option(value: unknown, account: string, what: string): unknown {
  const vec = typeof value === "object" && value !== null ? (value as Fields).vec : undefined;
  if (!Array.isArray(vec) || vec.length > 1) throw bad(account, what);
  return vec[0];
}

function hex(value: unknown, account: string, what: string): string | undefined {
  if (value !== undefined && (typeof value !== "string" || !/^0x(?:[0-9a-fA-F]{2})*$/.test(value))) throw bad(account, what);
  return value;
}

/** The entry function a stored `MultisigTransactionPayload` calls, or `undefined` for bytes that do not decode whole. */
export function decodeCall(payload: string): EntryFunctionCall | undefined {
  try {
    const bytes = new Deserializer(Hex.fromHexInput(payload).toUint8Array());
    const call = MultiSigTransactionPayload.deserialize(bytes).transaction_payload;
    if (bytes.remaining() !== 0) return undefined;
    const { address: at, name } = call.module_name;
    return {
      function: `${at.toString()}::${name.identifier}::${call.function_name.identifier}`,
      typeArguments: call.type_args.map((t) => t.toString()),
      arguments: call.args.map((arg) => Hex.fromHexInput(arg.bcsToBytes()).toString()),
    };
  } catch {
    return undefined;
  }
}

function pendingAt(item: unknown, sequenceNumber: bigint, timelock: MultisigTimelock | undefined, account: string): PendingMultisigTransaction {
  const what = `pending transaction ${sequenceNumber}`;
  if (typeof item !== "object" || item === null) throw bad(account, what);
  const d = item as Fields;
  const votes = typeof d.votes === "object" && d.votes !== null ? (d.votes as Fields).data : undefined;
  if (!Array.isArray(votes)) throw bad(account, what);
  const [approvals, rejections]: [string[], string[]] = [[], []];
  for (const vote of votes as unknown[]) {
    const { key, value } = (vote ?? {}) as Fields;
    if (typeof value !== "boolean") throw bad(account, what);
    (value ? approvals : rejections).push(addressOf(key, account, what));
  }
  const payload = hex(option(d.payload, account, what), account, what);
  const payloadHash = hex(option(d.payload_hash, account, what), account, what);
  const createdAtSecs = int(d.creation_time_secs, account, what);
  const call = payload === undefined ? undefined : decodeCall(payload);
  const overridden = timelock?.overrideThreshold !== undefined && BigInt(approvals.length) >= timelock.overrideThreshold;
  return {
    sequenceNumber,
    creator: addressOf(d.creator, account, what),
    createdAtSecs,
    ...(payload !== undefined && { payload }),
    ...(payloadHash !== undefined && { payloadHash }),
    ...(call !== undefined && { call }),
    approvals,
    rejections,
    ...(timelock !== undefined && !overridden && { executableAtSecs: createdAtSecs + timelock.periodSecs }),
  };
}

/**
 * `account`'s state from seven `0x1::multisig_account` views at `version`,
 * read together. Throws a `StreamdError` for a reply not shaped as the view
 * returns, and passes a failed view on as it is (an account that is not a
 * multisig aborts them).
 */
export async function readMultisig(view: ViewAt, account: string, version: bigint): Promise<MultisigState> {
  const read = async (fn: string) => {
    const [value] = await view({ function: `${MODULE}::${fn}`, typeArguments: [], functionArguments: [account] }, version);
    return value;
  };
  const [owners, threshold, lastResolved, next, pending, period, override] = await Promise.all(
    ["owners", "num_signatures_required", "last_resolved_sequence_number", "next_sequence_number", "get_pending_transactions", "timelock_period", "timelock_override_threshold"].map(read),
  );
  if (!Array.isArray(owners)) throw bad(account, "owners");
  if (!Array.isArray(pending)) throw bad(account, "pending transactions");
  const periodSecs = int(period, account, "timelock period");
  const overrideThreshold = option(override, account, "timelock override threshold");
  const timelock: MultisigTimelock | undefined =
    periodSecs === 0n
      ? undefined
      : { periodSecs, ...(overrideThreshold !== undefined && { overrideThreshold: int(overrideThreshold, account, "timelock override threshold") }) };
  const resolved = int(lastResolved, account, "last resolved sequence number");
  return {
    address: account,
    version,
    owners: owners.map((o) => addressOf(o, account, "owners")),
    threshold: int(threshold, account, "signatures required"),
    ...(timelock !== undefined && { timelock }),
    lastResolved: resolved,
    nextSequenceNumber: int(next, account, "next sequence number"),
    pending: new Map(pending.map((item, i) => {
      const seq = resolved + 1n + BigInt(i);
      return [seq, pendingAt(item, seq, timelock, account)];
    })),
  };
}
