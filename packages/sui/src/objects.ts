import { normalizeStructTag, normalizeSuiAddress } from "@mysten/sui/utils";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { toJson, type ProtoValue } from "./json.js";
import { micros } from "./transfers.js";

/** One entry of a transaction's `effects.changed_objects`. */
export interface ChangedObject {
  objectId?: string;
  inputVersion?: bigint;
  outputState?: number;
  outputVersion?: bigint;
  objectType?: string;
}

/** The part of a `ListTransactions` transaction that object changes are built from. */
export interface ObjectChangeTransaction {
  digest?: string;
  timestamp?: { seconds: bigint; nanos: number };
  transaction?: { sender?: string };
  events?: { events: unknown[] };
  effects?: { changedObjects?: ChangedObject[] };
  objects?: { objects: { objectId?: string; version?: bigint; objectType?: string; json?: ProtoValue }[] };
}

const DOES_NOT_EXIST = 1;

// A package's type is not a struct tag; it is kept as the node reports it.
function typeOf(type: string): string {
  try {
    return normalizeStructTag(type);
  } catch {
    return type;
  }
}

/**
 * The changes one transaction made to `objectId` (long form), at `version`:
 * `data` the object's JSON at its output version, or `null` when the
 * transaction deleted or wrapped it. `eventIndex` is the transaction's event
 * count plus the change's position in its effects, so a change never shares
 * an id with an event.
 */
export function objectChangesOf(txn: ObjectChangeTransaction, version: bigint, objectId: string): StreamdEvent[] {
  const first = txn.events?.events.length ?? 0;
  const held = (at: bigint | undefined) =>
    txn.objects?.objects.find((o) => o.objectId !== undefined && normalizeSuiAddress(o.objectId) === objectId && o.version === at);
  const out: StreamdEvent[] = [];
  (txn.effects?.changedObjects ?? []).forEach((change, position) => {
    if (change.objectId === undefined || normalizeSuiAddress(change.objectId) !== objectId) return;
    if (change.outputState === undefined) throw new StreamdError(`Sui node returned a change to ${objectId} without its output state in transaction ${txn.digest}`);
    const gone = change.outputState === DOES_NOT_EXIST;
    const object = held(gone ? change.inputVersion : change.outputVersion);
    if (!gone && object === undefined) {
      throw new StreamdError(`Sui node returned transaction ${txn.digest} without ${objectId} at version ${change.outputVersion}`);
    }
    const type = change.objectType ?? object?.objectType;
    if (type === undefined) throw new StreamdError(`Sui node returned a change to ${objectId} without its type in transaction ${txn.digest}`);
    const event: StreamdEvent = { type: typeOf(type), data: gone ? null : toJson(object!.json), version, sequenceNumber: 0n, eventIndex: first + position };
    if (txn.digest !== undefined) event.transactionHash = txn.digest;
    if (txn.timestamp !== undefined) event.timestamp = micros(txn.timestamp);
    const sender = txn.transaction?.sender;
    if (sender !== undefined && sender !== "") event.sender = normalizeSuiAddress(sender);
    out.push(event);
  });
  return out;
}
