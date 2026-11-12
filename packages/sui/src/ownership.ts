import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { isValidSuiAddress, normalizeSuiAddress } from "@mysten/sui/utils";
import { RequestLimiter, StreamdError } from "@streamd/core";
import type { EventPredicate, StreamdEvent } from "@streamd/core";
import { networkError } from "./adapter.js";
import { addressOrName, nameService, resolveName } from "./names.js";

const DEFAULT_FIELDS = ["id", "object_id", "nft_id"];
const DEFAULT_MAX_CONCURRENT = 4;
// Entries are per (object, version), so overflowing only costs a repeat read.
const CACHE_SIZE = 1000;
const MAX_HOPS = 8;
// `sui.rpc.v2.Owner.OwnerKind`.
const ADDRESS = 1;
const OBJECT = 2;
const CONSENSUS_ADDRESS = 5;

type GetObject = SuiGrpcClient["ledgerService"]["getObject"];

export interface OwnedByOptions {
  /** Top-level `data` fields that may name an object, checked in order (default: `id`, `object_id`, `nft_id`). */
  fields?: string[];
  /** Object reads this predicate keeps in flight at once (default: 4). */
  maxConcurrentRequests?: number;
}

/**
 * Matches events whose `data` names an object `wallet` owns, directly or
 * through objects it owns, as ownership stands at the node's head when the
 * predicate runs. Sui reads an object by its own version, not a ledger
 * version, so there is no read of ownership as it stood at `event.version`:
 * a live listener sees current owners, and a history query today's owners.
 *
 * `wallet` is an address or an object id (objects own objects on Sui). A
 * shared, immutable, deleted or wrapped object is not owned, nor is a
 * dynamic field of a table or bag, whose parent id is no object of its own.
 * Answers are cached per object and event version.
 *
 * `wallet` may be a SuiNS name (`alice.sui`), resolved once on the first
 * evaluation through `resolveName`; a failed resolution is retried on the next.
 */
export function ownedBy(sui: SuiGrpcClient, wallet: string, options: OwnedByOptions = {}): EventPredicate {
  const address = addressOrName(wallet);
  const getObject = (sui as { ledgerService?: { getObject?: unknown } } | null)?.ledgerService?.getObject;
  if (typeof getObject !== "function") throw new StreamdError("ownedBy needs a SuiGrpcClient");
  if (address === undefined) nameService(sui);
  const fields = options.fields ?? DEFAULT_FIELDS;
  if (!Array.isArray(fields) || fields.length === 0 || fields.some((f) => typeof f !== "string" || f === "")) {
    throw new StreamdError("ownedBy fields must be a non-empty array of field names");
  }
  const max = options.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT;
  if (!Number.isInteger(max) || max < 1) {
    throw new StreamdError(`Invalid maxConcurrentRequests: ${String(max)} (expected a positive integer)`);
  }
  let owner: Promise<string> | undefined;
  const resolveOwner = (): Promise<string> => {
    if (owner !== undefined) return owner;
    const pending = address !== undefined ? Promise.resolve(address) : resolveName(sui, wallet);
    pending.catch(() => owner === pending && (owner = undefined));
    return (owner = pending);
  };
  const limiter = new RequestLimiter(max);
  const read = (id: string) => limiter.run(() => ownerOf(sui.ledgerService.getObject.bind(sui.ledgerService), id));
  const cache = new Map<string, Promise<boolean>>();

  const owns = (object: string, version: bigint): Promise<boolean> => {
    const key = `${object}:${version}`;
    let answer = cache.get(key);
    if (answer !== undefined) {
      cache.delete(key);
    } else {
      answer = resolveOwner().then((wallet) => walk(read, object, wallet));
      answer.catch(() => cache.get(key) === answer && cache.delete(key));
      if (cache.size >= CACHE_SIZE) cache.delete(cache.keys().next().value!);
    }
    cache.set(key, answer);
    return answer;
  };

  return async (event: StreamdEvent) => {
    for (const object of namedObjects(event.data, fields)) {
      if (await owns(object, event.version)) return true;
    }
    return false;
  };
}

function namedObjects(data: unknown, fields: readonly string[]): string[] {
  if (typeof data !== "object" || data === null) return [];
  const out: string[] = [];
  for (const field of fields) {
    const value = (data as Record<string, unknown>)[field];
    if (typeof value !== "string" || !value.startsWith("0x")) continue;
    const id = normalizeSuiAddress(value);
    if (isValidSuiAddress(id)) out.push(id);
  }
  return out;
}

/** Up the owner chain from `object` until it reaches `wallet`, or an owner that is not an object. */
async function walk(read: (id: string) => Promise<Owner | undefined>, object: string, wallet: string): Promise<boolean> {
  let id = object;
  for (let hop = 0; hop < MAX_HOPS; hop++) {
    const owner = await read(id);
    if (owner === undefined) return false;
    if (owner.address === wallet && (owner.kind === ADDRESS || owner.kind === OBJECT || owner.kind === CONSENSUS_ADDRESS)) return true;
    if (owner.kind !== OBJECT) return false;
    id = owner.address;
  }
  return false;
}

interface Owner {
  kind: number | undefined;
  address: string;
}

/** `id`'s owner at the head, or `undefined` when no object has that id. */
async function ownerOf(getObject: GetObject, id: string): Promise<Owner | undefined> {
  let owner;
  try {
    owner = (await getObject({ objectId: id, readMask: { paths: ["owner"] } })).response.object?.owner;
  } catch (err) {
    const code = (err as { code?: unknown } | null)?.code;
    if (code === "NOT_FOUND") return undefined;
    if (code === "INVALID_ARGUMENT") {
      throw new StreamdError(`ownedBy read of object ${id} was refused: ${err instanceof Error ? err.message : "unknown error"}`);
    }
    networkError(err);
  }
  if (owner === undefined) throw new StreamdError(`ownedBy read of object ${id} returned no owner`);
  return { kind: owner.kind, address: owner.address === undefined ? "" : normalizeSuiAddress(owner.address) };
}
