import { StreamdError } from "./errors.js";
import type { StreamdEvent } from "./types.js";

export const UNSIGNED_INTEGER = /^\d+$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Turns the named top-level fields of `event.data` from the fullnode's decimal
 * strings (how u64, u128 and u256 arrive in JSON) into `bigint`. Returns
 * `undefined` when no fields are named, so callers skip the copy entirely.
 * The result is a copy: adapters may hand out the same objects on every fetch.
 */
export function fieldDecoder(
  fields: readonly string[] | undefined,
): ((event: StreamdEvent) => StreamdEvent) | undefined {
  if (fields === undefined) return undefined;
  if (!Array.isArray(fields) || fields.some((f) => typeof f !== "string" || f === "")) {
    throw new StreamdError("decode must be an array of non-empty field names");
  }
  if (fields.length === 0) return undefined;

  return (event) => {
    const data = event.data;
    // A descriptor serving several event types may name fields only some carry.
    if (!isRecord(data) || !fields.some((f) => Object.hasOwn(data, f))) return event;
    const decoded = { ...data };
    for (const field of fields) {
      if (!Object.hasOwn(data, field)) continue;
      const value = data[field];
      if (typeof value === "bigint") continue;
      if (typeof value !== "string" || !UNSIGNED_INTEGER.test(value)) {
        throw new StreamdError(
          `Cannot decode field "${field}" of ${event.version}:${event.eventIndex} as an unsigned integer`,
        );
      }
      decoded[field] = BigInt(value);
    }
    return { ...event, data: decoded };
  };
}
