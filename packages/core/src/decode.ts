import { StreamdError } from "./errors.js";
import type { StreamdEvent } from "./types.js";

export const UNSIGNED_INTEGER = /^\d+$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Thrown where a present value does not fit its path; the caller names the event. */
class Mismatch extends Error {}

/**
 * `value` with the path's leaves from index `i` on turned into `bigint`. A key
 * absent at any depth leaves the value as it was; only what changes is copied.
 */
function convert(value: unknown, path: readonly string[], i: number): unknown {
  if (i === path.length) {
    if (typeof value === "bigint") return value;
    if (typeof value === "string" && UNSIGNED_INTEGER.test(value)) return BigInt(value);
    throw new Mismatch();
  }
  const segment = path[i]!;
  if (segment === "*") {
    if (!Array.isArray(value)) throw new Mismatch();
    return value.map((item) => convert(item, path, i + 1));
  }
  if (!isRecord(value)) throw new Mismatch();
  if (!Object.hasOwn(value, segment)) return value;
  const next = convert(value[segment], path, i + 1);
  return next === value[segment] ? value : { ...value, [segment]: next };
}

/**
 * Turns the named fields of `event.data` from the fullnode's decimal strings
 * (how u64, u128 and u256 arrive in JSON) into `bigint`. A name is a path:
 * segments joined by `.`, with `*` for every element of an array, as in
 * `"legs.*.amount"`. Returns `undefined` when no fields are named, so callers
 * skip the copy entirely. The result is a copy: adapters may hand out the same
 * objects on every fetch.
 */
export function fieldDecoder(
  fields: readonly string[] | undefined,
): ((event: StreamdEvent) => StreamdEvent) | undefined {
  if (fields === undefined) return undefined;
  if (!Array.isArray(fields) || fields.some((f) => typeof f !== "string" || f === "")) {
    throw new StreamdError("decode must be an array of non-empty field names");
  }
  const paths = fields.map((field) => {
    const path = field.split(".");
    if (path.includes("") || path[0] === "*") {
      throw new StreamdError(`decode path "${field}" needs a field first and no empty segments`);
    }
    return path;
  });
  if (paths.length === 0) return undefined;

  return (event) => {
    // A descriptor serving several event types may name fields only some carry.
    if (!isRecord(event.data)) return event;
    let data: unknown = event.data;
    paths.forEach((path, n) => {
      try {
        data = convert(data, path, 0);
      } catch (err) {
        if (!(err instanceof Mismatch)) throw err;
        throw new StreamdError(
          `Cannot decode field "${fields[n]}" of ${event.version}:${event.eventIndex} as an unsigned integer`,
        );
      }
    });
    return data === event.data ? event : { ...event, data };
  };
}
