import { AccountAddress } from "@aptos-labs/ts-sdk";
import { StreamdError } from "./errors.js";
import type { EventPredicate, StreamdEvent } from "./types.js";

const HEX_ADDRESS = /^0x[0-9a-fA-F]{1,64}$/;
const ARRAY_INDEX = /^(0|[1-9]\d*)$/;
const MOVE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const DEFAULT_MAX_DEPTH = 8;
const MISSING: unique symbol = Symbol("missing");

export type FieldPath = string | readonly (string | number)[];
export type FieldValue = string | number | bigint | boolean | null;

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

/**
 * Matches events whose type is declared in one of `modules`, each written
 * `address::module` (e.g. `"0x1::coin"`), whatever the struct or its generics.
 * Addresses compare in canonical long form.
 */
export function ofModule(...modules: string[]): EventPredicate {
  const keys = new Set(modules.map(parseModuleId));
  return (event) => {
    const [address, module, struct] = event.type.split("::");
    if (module === undefined || struct === undefined) return false;
    const normalized = normalizeAddress(address);
    return normalized !== undefined && keys.has(`${normalized}::${module}`);
  };
}

/**
 * Matches events whose payload has `value` at `path` inside `event.data`.
 *
 * `path` is either a dot-separated string (`"store"`, `"inner.owner"`,
 * `"holders.0.owner"`) or an array of keys and indices (`["inner", "owner"]`,
 * `["holders", 0, "owner"]`, or `["key.with.dots"]`). When `value` is a valid
 * Aptos address, both sides are compared in canonical long form.
 */
export function whereField(
  path: FieldPath,
  value: FieldValue,
): EventPredicate {
  const segments = parseFieldPath(path);
  validateFieldValue(value);
  const normalizedTarget =
    typeof value === "string" ? normalizeAddress(value) : undefined;

  return (event: StreamdEvent): boolean => {
    const actual = readField(event.data, segments);
    if (actual === MISSING) return false;
    if (typeof value === "string") {
      if (typeof actual !== "string") return false;
      if (actual === value) return true;
      return (
        normalizedTarget !== undefined &&
        normalizeAddress(actual) === normalizedTarget
      );
    }
    return actual === value;
  };
}

/** Combines predicates; an event must satisfy all of them. */
export function every(...predicates: EventPredicate[]): EventPredicate {
  return (event) => predicates.every((p) => p(event));
}

/** Combines predicates; an event must satisfy at least one. */
export function some(...predicates: EventPredicate[]): EventPredicate {
  return (event) => predicates.some((p) => p(event));
}

/** Negates `predicate`; an event matches when `predicate(event)` is false. */
export function not(predicate: EventPredicate): EventPredicate {
  if (typeof predicate !== "function") {
    throw new StreamdError("Predicate must be a function");
  }
  return (event) => !predicate(event);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseModuleId(id: string): string {
  const parts = typeof id === "string" ? id.split("::") : [];
  const address = parts.length === 2 ? normalizeAddress(parts[0]) : undefined;
  if (address === undefined || !MOVE_IDENTIFIER.test(parts[1])) {
    throw new StreamdError(
      `Module must be written address::module: ${JSON.stringify(id)}`,
    );
  }
  return `${address}::${parts[1]}`;
}

function parseFieldPath(path: FieldPath): (string | number)[] {
  if (typeof path === "string") {
    if (path.length === 0) {
      throw new StreamdError("Field path must not be empty");
    }
    const segments = path.split(".");
    if (segments.some((s) => s.length === 0)) {
      throw new StreamdError(`Invalid field path: ${JSON.stringify(path)}`);
    }
    return segments;
  }
  if (Array.isArray(path)) {
    if (path.length === 0) {
      throw new StreamdError("Field path must not be empty");
    }
    for (const seg of path) {
      if (typeof seg === "string") {
        if (seg.length === 0) {
          throw new StreamdError("Field path segment must not be empty");
        }
      } else if (typeof seg === "number") {
        if (!Number.isSafeInteger(seg) || seg < 0) {
          throw new StreamdError(`Invalid array index in field path: ${seg}`);
        }
      } else {
        throw new StreamdError(`Invalid field path segment: ${String(seg)}`);
      }
    }
    return [...path];
  }
  throw new StreamdError(
    "Field path must be a dot-separated string or an array of keys",
  );
}

function validateFieldValue(value: FieldValue): void {
  if (value === undefined) {
    throw new StreamdError("Field filter value must not be undefined");
  }
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new StreamdError(
      `Field filter value must be a finite number: ${value}`,
    );
  }
}

function readField(
  data: unknown,
  segments: readonly (string | number)[],
): unknown {
  let current: unknown = data;
  for (const seg of segments) {
    if (Array.isArray(current)) {
      const index =
        typeof seg === "number"
          ? seg
          : ARRAY_INDEX.test(seg)
            ? Number(seg)
            : -1;
      if (
        !Number.isSafeInteger(index) ||
        index < 0 ||
        index >= current.length
      ) {
        return MISSING;
      }
      current = current[index];
    } else if (isRecord(current)) {
      const key = String(seg);
      if (!Object.hasOwn(current, key)) return MISSING;
      current = current[key];
    } else {
      return MISSING;
    }
  }
  return current;
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
