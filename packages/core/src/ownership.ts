import type { Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
import { RequestLimiter } from "./limiter.js";
import { addressOrName, requireNameResolver, resolveName } from "./names.js";
import type { EventPredicate, StreamdEvent } from "./types.js";

const DEFAULT_FIELDS = ["store", "object", "token", "token_address"];
const DEFAULT_MAX_CONCURRENT = 4;
// Entries are per (object, version), so overflowing only costs a repeat read.
const CACHE_SIZE = 1000;
const VM_ABORTED = 4016;

export interface OwnedByOptions {
  /**
   * Top-level `data` fields that may name a store or object, checked in order
   * (default: `store`, `object`, `token`, `token_address`).
   */
  fields?: string[];
  /** View calls this predicate keeps in flight at once (default: 4). */
  maxConcurrentRequests?: number;
}

/**
 * Matches events whose `data` names a store or object `wallet` owns, directly
 * or through objects it owns, as ownership stood once the event's transaction
 * had run (`0x1::object::owns` at `event.version`, as `withState` reads).
 *
 * Built for `0x1::fungible_asset::Deposit`/`Withdraw` (`store`) and
 * `0x1::object::Transfer` (`object`). An object that does not exist at that
 * version, such as a store the transaction deleted, is not owned. Answers are
 * cached per object and version; events from `previewEvents` carry a version
 * no node has committed, so the read fails there.
 *
 * `wallet` may be an Aptos name (`alice.apt`), resolved once on the first
 * evaluation through `resolveName`; a failed resolution is retried on the next.
 */
export function ownedBy(
  aptos: Aptos,
  wallet: string,
  options: OwnedByOptions = {},
): EventPredicate {
  const address = addressOrName(wallet);
  if (address === undefined) requireNameResolver(aptos);
  if (typeof (aptos as { viewJson?: unknown } | null)?.viewJson !== "function") {
    throw new StreamdError("ownedBy needs an Aptos instance");
  }
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
    const pending = address !== undefined ? Promise.resolve(address) : resolveName(aptos, wallet);
    pending.catch(() => owner === pending && (owner = undefined));
    return (owner = pending);
  };
  const limiter = new RequestLimiter(max);
  const cache = new Map<string, Promise<boolean>>();

  const owns = (object: string, version: bigint): Promise<boolean> => {
    const key = `${object}:${version}`;
    let answer = cache.get(key);
    if (answer !== undefined) {
      cache.delete(key);
    } else {
      answer = resolveOwner().then((wallet) => limiter.run(() => readOwns(aptos, object, wallet, version)));
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
    let value = (data as Record<string, unknown>)[field];
    if (typeof value === "object" && value !== null) value = (value as { inner?: unknown }).inner;
    const address = typeof value === "string" ? normalizeAddress(value) : undefined;
    if (address !== undefined) out.push(address);
  }
  return out;
}

async function readOwns(aptos: Aptos, object: string, owner: string, version: bigint): Promise<boolean> {
  let result: unknown;
  try {
    result = await aptos.viewJson({
      payload: {
        function: "0x1::object::owns",
        typeArguments: ["0x1::object::ObjectCore"],
        functionArguments: [object, owner],
      },
      options: { ledgerVersion: version },
    });
  } catch (err) {
    const { status, data } = (err ?? {}) as { status?: unknown; data?: { vm_error_code?: unknown } };
    // `owns` aborts when the object is missing at this version, or nests too deep to reach the wallet.
    if (status === 400 && data?.vm_error_code === VM_ABORTED) return false;
    const message = `ownedBy view of ${object} at version ${version} failed: ${
      err instanceof Error ? err.message : "unknown error"
    }`;
    if (status === 400) throw new StreamdError(message);
    throw new StreamdNetworkError(message, typeof status === "number" ? status : undefined);
  }
  if (!Array.isArray(result) || typeof result[0] !== "boolean") {
    throw new StreamdError(`ownedBy view of ${object} at version ${version} returned no boolean`);
  }
  return result[0];
}
