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

/** The part of a `GetTransaction` answer that object states are read from. */
export interface StateTransaction {
  effects?: {
    changedObjects?: ChangedObject[];
    unchangedConsensusObjects?: { kind?: number; objectId?: string; version?: bigint }[];
  };
  objects?: ObjectChangeTransaction["objects"];
}

interface GraphqlObject {
  address?: string;
  version?: number;
  asMoveObject?: { contents?: { json?: unknown } | null } | null;
}
interface Connection<T> {
  pageInfo?: { hasNextPage?: boolean };
  nodes?: T[];
}
/** The effects of a GraphQL `transaction(digest)` answer, as the adapter's query asks for them. */
export interface GraphqlEffects {
  objectChanges?: Connection<{ address?: string; outputState?: GraphqlObject | null }>;
  unchangedConsensusObjects?: Connection<{ __typename?: string; object?: GraphqlObject | null }>;
}

const OBJECT_WRITE = 2;
const READ_ONLY_ROOT = 1;

const same = (raw: string | undefined, id: string) => raw !== undefined && normalizeSuiAddress(raw) === id;

function untouched(id: string, digest: string): StreamdError {
  return new StreamdError(
    `Transaction ${digest} did not write or read ${id}, so it has no state as that transaction left it: only objects in its effects can be read`,
  );
}
const notObject = (id: string, digest: string) =>
  new StreamdError(`Transaction ${digest} left ${id} as no Move object (a package or an accumulator)`);

/**
 * Each of `ids` (long form) as transaction `digest` left it, from its gRPC
 * effects and object set: the JSON at the output version of an object it
 * wrote, at the read version of a consensus object it only read, `null` for
 * one it deleted or wrapped. An object outside its effects is refused.
 */
export function objectStatesOf(txn: StateTransaction, ids: readonly string[], digest: string): unknown[] {
  return ids.map((id) => {
    let version: bigint | undefined;
    const change = txn.effects?.changedObjects?.find((c) => same(c.objectId, id));
    if (change !== undefined) {
      if (change.outputState === DOES_NOT_EXIST) return null;
      if (change.outputState !== OBJECT_WRITE) throw notObject(id, digest);
      version = change.outputVersion;
    } else {
      const read = txn.effects?.unchangedConsensusObjects?.find((u) => u.kind === READ_ONLY_ROOT && same(u.objectId, id));
      if (read === undefined) throw untouched(id, digest);
      version = read.version;
    }
    const object = txn.objects?.objects.find((o) => same(o.objectId, id) && o.version === version);
    if (object?.json === undefined) throw new StreamdError(`Sui node returned transaction ${digest} without ${id} at version ${version}`);
    return toJson(object.json);
  });
}

function nodesOf<T>(connection: Connection<T> | undefined, digest: string): T[] {
  if (!Array.isArray(connection?.nodes)) throw new StreamdError(`Sui GraphQL returned the effects of ${digest} in an unexpected shape`);
  return connection.nodes;
}

/** `objectStatesOf` over the effects Sui GraphQL returns for a transaction the fullnode has pruned. */
export function graphqlStatesOf(effects: GraphqlEffects, ids: readonly string[], digest: string): unknown[] {
  const changes = nodesOf(effects?.objectChanges, digest);
  const reads = nodesOf(effects?.unchangedConsensusObjects, digest);
  const contents = (object: GraphqlObject, id: string) => {
    const json = object.asMoveObject?.contents?.json;
    if (json === undefined || json === null) throw notObject(id, digest);
    return json;
  };
  return ids.map((id) => {
    const change = changes.find((c) => same(c?.address, id));
    if (change !== undefined) return change.outputState == null ? null : contents(change.outputState, id);
    const read = reads.find((r) => r?.__typename === "ConsensusObjectRead" && same(r.object?.address, id));
    if (read?.object != null) return contents(read.object, id);
    if (effects.objectChanges?.pageInfo?.hasNextPage || effects.unchangedConsensusObjects?.pageInfo?.hasNextPage) {
      throw new StreamdError(`Transaction ${digest} touched more objects than one GraphQL page of 1024 holds, and ${id} is not in it`);
    }
    throw untouched(id, digest);
  });
}
