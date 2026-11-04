import { StreamdError } from "../errors.js";
import { normalizeAddress } from "../filter.js";
import type { ModuleEventDescriptor } from "../types.js";

export const DEFAULT_LIMIT = 25;
const MOVE_EVENT_TYPE = /^[^:\s]+::[^:\s]+::[^:\s<>]+(?:<.+>)?$/;

/**
 * Caller input is refused as a plain `StreamdError` before any request, so it
 * is never mistaken for a network failure worth retrying.
 */
export function requireAddress(value: string): string {
  const address = normalizeAddress(value);
  if (address === undefined) {
    throw new StreamdError(`Not a valid Aptos address: ${value}`);
  }
  return address;
}

export function parseCursor(cursor: string): bigint {
  if (!/^\d+$/.test(cursor)) {
    throw new StreamdError(`Not a cursor this adapter issued: ${cursor}`);
  }
  return BigInt(cursor);
}

export function validateOffset(offset: number | undefined): void {
  if (
    offset !== undefined &&
    !(Number.isSafeInteger(offset) && offset >= 0)
  ) {
    throw new StreamdError(`Not a valid offset: ${offset}`);
  }
}

export function requireEventType(value: unknown): string {
  if (typeof value !== "string" || !MOVE_EVENT_TYPE.test(value)) {
    throw new StreamdError(`Not a valid Move event type: ${String(value)}`);
  }
  return value;
}

export function moduleTypeFilter(
  eventType: ModuleEventDescriptor["eventType"],
): (type: string) => boolean {
  if (Array.isArray(eventType)) {
    if (eventType.length === 0) {
      throw new StreamdError(
        "Module descriptor requires at least one eventType",
      );
    }
    const types = new Set(eventType.map(requireEventType));
    return (type) => types.has(type);
  }
  const single = requireEventType(eventType);
  return (type) => type === single;
}
