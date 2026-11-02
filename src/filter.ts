import { AccountAddress } from "@aptos-labs/ts-sdk";
import type { EventPredicate, StreamdEvent } from "./types.js";

const HEX_ADDRESS = /^0x[0-9a-fA-F]{1,64}$/;
const DEFAULT_MAX_DEPTH = 8;

/**
 * Canonical long form of an address, or undefined if the value isn't one.
 *
 * Aptos renders the same account as `0x1` or `0x0000…0001` interchangeably, so
 * comparing raw strings silently misses matches. Everything here compares
 * normalized forms.
 */
export function normalizeAddress(value: string): string | undefined {
  if (!HEX_ADDRESS.test(value)) return undefined;
  try {
    return AccountAddress.from(value).toStringLong();
  } catch {
    return undefined;
  }
}

export interface InvolvesAddressOptions {
  /**
   * Restrict the search to these top-level keys of `data` (e.g. `["owner"]`).
   * Omit to search the whole payload.
   */
  fields?: string[];
  /** How deep to recurse into nested objects/arrays (default: 8). */
  maxDepth?: number;
}

/**
 * Matches events whose payload mentions `address` anywhere.
 *
 * Module events carry the account inside `data` rather than in the event
 * identity, so this is how you narrow "all Deposit events" down to "Deposit
 * events for this wallet":
 *
 * ```ts
 * streamd.on(
 *   { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
 *   (e) => console.log(e),
 *   { where: involvesAddress(myWallet) },
 * );
 * ```
 */
export function involvesAddress(
  address: string,
  options: InvolvesAddressOptions = {},
): EventPredicate {
  const target = normalizeAddress(address);
  if (target === undefined) {
    throw new TypeError(`Not a valid Aptos address: ${address}`);
  }

  const maxDepth = options.maxDepth ?? DEFAULT_MAX_DEPTH;
  const fields = options.fields;

  return (event: StreamdEvent): boolean => {
    const data = event.data;
    if (fields === undefined) return contains(data, target, maxDepth);
    if (!isRecord(data)) return false;
    return fields.some((f) => contains(data[f], target, maxDepth));
  };
}

/** Matches events whose type is exactly one of `types`. */
export function ofType(...types: string[]): EventPredicate {
  const set = new Set(types);
  return (event) => set.has(event.type);
}

/** Combines predicates; an event must satisfy all of them. */
export function every(...predicates: EventPredicate[]): EventPredicate {
  return (event) => predicates.every((p) => p(event));
}

/** Combines predicates; an event must satisfy at least one. */
export function some(...predicates: EventPredicate[]): EventPredicate {
  return (event) => predicates.some((p) => p(event));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function contains(value: unknown, target: string, depth: number): boolean {
  if (depth < 0) return false;

  if (typeof value === "string") {
    return normalizeAddress(value) === target;
  }
  if (Array.isArray(value)) {
    return value.some((v) => contains(v, target, depth - 1));
  }
  if (isRecord(value)) {
    return Object.values(value).some((v) => contains(v, target, depth - 1));
  }
  return false;
}
