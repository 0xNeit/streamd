import { Deserializer, Hex, MultiSigTransactionPayload } from "@aptos-labs/ts-sdk";
import { RequestLimiter, StreamdError } from "@streamd/core";
import type { EntryFunctionCall, EventAdapter, ModuleEventDescriptor, ReduceOptions, Reducer, Reduction, StreamdEvent, ViewRequest } from "@streamd/core";
import { address, amount, pageSource } from "./records.js";
import type { Fields } from "./records.js";
import type { Multisig, MultisigOptions, MultisigState, MultisigTimelock, PendingMultisigTransaction } from "./types.js";

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

/**
 * Every event `0x1::multisig_account` emits. Each names its account in a
 * `multisig_account` field, and the account never signs (owners create and
 * vote, an owner executes), so this one scan, narrowed by that field, is how
 * an account's changes are seen.
 */
export const MULTISIG_DESCRIPTOR: ModuleEventDescriptor = {
  kind: "module",
  eventType: [
    "AddOwners",
    "RemoveOwners",
    "UpdateSignaturesRequired",
    "MetadataUpdated",
    "TimelockUpdated",
    "TimelockRemoved",
    "CreateTransaction",
    "Vote",
    "ExecuteRejectedTransaction",
    "TransactionExecutionSucceeded",
    "TransactionExecutionFailed",
  ].map((name) => `${MODULE}::${name}` as const),
};

/**
 * The scan's source for one account: other accounts' events are dropped, and
 * each kept event carries the state reread at its version as `state[0]`, the
 * views read once per version and at most four at a time.
 */
export function multisigSource(adapter: EventAdapter, account: string, requestTimeout?: number): EventAdapter {
  if (typeof adapter.view !== "function") throw new StreamdError("multisig needs an adapter that implements view");
  // Outside the client's limiter, as governance's views are: the page asking
  // holds a slot. Timed, so a view that never settles fails the page.
  const views = new RequestLimiter(4, Infinity, requestTimeout).wrap(adapter);
  const view: ViewAt = (request, version) => views.view!(request, version);
  return pageSource(adapter, (events) => {
    const kept = events.filter((e) => address((e.data as Fields | null)?.multisig_account) === account);
    const read = new Map<bigint, Promise<MultisigState>>();
    for (const { version } of kept) if (!read.has(version)) read.set(version, readMultisig(view, account, version));
    return Promise.all(kept.map(async (e) => ({ ...e, state: [await read.get(e.version)!] })));
  });
}

/** `client.reduce()` over the account's source, as the companion calls it. */
export type ReduceMultisig = (
  account: string,
  descriptor: ModuleEventDescriptor,
  reducer: Reducer<null>,
  initial: null,
  options: ReduceOptions<null>,
) => Reduction<null>;

const ALLOWED: readonly string[] = [
  "fromVersion",
  "pollInterval",
  "batchSize",
  "maxErrors",
  "maxBackoff",
  "maxPollInterval",
  "retries",
  "signal",
  "onError",
  "onChange",
] satisfies readonly (keyof MultisigOptions)[];

function replace(state: MultisigState, next: MultisigState): void {
  delete (state as { timelock?: MultisigTimelock }).timelock;
  Object.assign(state, next);
}

/**
 * `AptosClient.multisig()` without `preview`: the state read at the start
 * (the head, or the version before `fromVersion`) once `account` resolves,
 * then one reduction from
 * the version after, each event replacing the state with the one its source
 * read at its version.
 */
export function startMultisig(
  reduce: ReduceMultisig,
  host: { view: ViewAt; head: () => Promise<bigint> },
  account: () => Promise<string>,
  options: MultisigOptions = {},
): Omit<Multisig, "preview"> {
  const refused = Object.keys(options).find((k) => !ALLOWED.includes(k));
  if (refused !== undefined) {
    throw new StreamdError(`Unsupported multisig option: ${refused} (allowed: ${ALLOWED.join(", ")})`);
  }
  const { fromVersion, onChange, ...rest } = options;
  if (fromVersion !== undefined && (typeof fromVersion !== "bigint" || fromVersion < 1n)) {
    throw new StreamdError("multisig fromVersion must be a bigint version of at least 1");
  }
  const state: MultisigState = { address: "", version: 0n, owners: [], threshold: 0n, lastResolved: 0n, nextSequenceNumber: 0n, pending: new Map() };
  let walk: Reduction<null> | undefined;
  let stopped = false;
  const stop = () => {
    stopped = true;
    walk?.unsubscribe();
  };
  const ready = (async () => {
    try {
      const at = fromVersion === undefined ? await host.head() : fromVersion - 1n;
      const resolved = await account();
      replace(state, await readMultisig(host.view, resolved, at));
      if (stopped) throw new StreamdError("multisig was stopped before it was ready");
      walk = reduce(
        resolved,
        MULTISIG_DESCRIPTOR,
        (s, event) => {
          replace(state, (event.state as MultisigState[])[0]!);
          return s;
        },
        null,
        { ...rest, fromVersion: at + 1n, onChange: (_, event: StreamdEvent) => onChange?.(state, event) },
      );
      await walk.ready;
      return state;
    } catch (err) {
      stopped = true;
      throw err;
    }
  })();
  ready.catch(() => {});
  return {
    value: state,
    get: (sequenceNumber) => state.pending.get(sequenceNumber),
    ready,
    get active() {
      return !stopped && (walk === undefined || walk.active);
    },
    unsubscribe: stop,
    [Symbol.dispose ?? Symbol.for("Symbol.dispose")]: stop,
  } as Omit<Multisig, "preview">;
}
