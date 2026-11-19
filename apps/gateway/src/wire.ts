import {
  StreamdError,
  StreamdNetworkError,
  handlePath,
  isObject,
  requireAddress,
  requireEventType,
  requireResourceType,
} from "@streamd/core";
import type { EventDescriptor, FetchOptions } from "@streamd/core";

type MoveType = `${string}::${string}::${string}`;

const U64_MAX = 2n ** 64n - 1n;
const MAX_TYPES = 100;
const MAX_LIMIT = 1000;

/** JSON with every bigint as a decimal string, the gateway's wire form for descriptors and options. */
export function toJson(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v));
}

function invalid(message: string): never {
  throw new StreamdError(message);
}

function text(value: unknown, field: string): string {
  if (typeof value !== "string" || value === "") invalid(`"${field}" must be a non-empty string`);
  return value;
}

function decimal(value: unknown, field: string): bigint {
  if (typeof value !== "string" || !/^\d{1,20}$/.test(value)) invalid(`"${field}" must be a decimal string`);
  const n = BigInt(value);
  if (n > U64_MAX) invalid(`"${field}" must fit in a u64`);
  return n;
}

function types(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_TYPES) {
    invalid(`"${field}" must be a list of 1 to ${MAX_TYPES} event types`);
  }
  return value.map((type) => requireEventType(type));
}

/**
 * A descriptor from untrusted JSON, rebuilt from its known fields only, so
 * nothing a client adds reaches the adapter. `creationNumber` is a decimal string.
 */
export function parseDescriptor(value: unknown): EventDescriptor {
  if (!isObject(value) || Array.isArray(value)) invalid("descriptor must be an object");
  switch (value.kind) {
    case "module":
      return {
        kind: "module",
        eventType: typeof value.eventType === "string"
          ? (requireEventType(value.eventType) as MoveType)
          : (types(value.eventType, "eventType") as MoveType[]),
      };
    case "handle": {
      const accountAddress = text(value.accountAddress, "accountAddress");
      const descriptor: EventDescriptor = value.creationNumber !== undefined
        ? { kind: "handle", accountAddress, creationNumber: decimal(value.creationNumber, "creationNumber") }
        : {
            kind: "handle",
            accountAddress,
            eventHandleStruct: text(value.eventHandleStruct, "eventHandleStruct"),
            fieldName: text(value.fieldName, "fieldName"),
          };
      if (value.creationNumber !== undefined && (value.eventHandleStruct !== undefined || value.fieldName !== undefined)) {
        invalid("A handle descriptor takes creationNumber or eventHandleStruct and fieldName, not both");
      }
      handlePath(descriptor);
      return descriptor;
    }
    case "account": {
      const accountAddress = text(value.accountAddress, "accountAddress");
      requireAddress(accountAddress);
      const descriptor: EventDescriptor = { kind: "account", accountAddress };
      if (value.eventTypes !== undefined) descriptor.eventTypes = types(value.eventTypes, "eventTypes");
      if (value.received !== undefined) {
        if (typeof value.received !== "boolean") invalid('"received" must be a boolean');
        if (value.received) descriptor.received = true;
      }
      return descriptor;
    }
    case "resource": {
      const accountAddress = text(value.accountAddress, "accountAddress");
      requireAddress(accountAddress);
      return { kind: "resource", accountAddress, resourceType: requireResourceType(value.resourceType) as MoveType };
    }
    case "object": {
      const objectId = text(value.objectId, "objectId");
      requireAddress(objectId);
      return { kind: "object", objectId };
    }
    default:
      return invalid('descriptor "kind" must be module, handle, account, resource or object');
  }
}

/** `fetchEvents` options from untrusted JSON: versions are decimal strings, unknown fields dropped. */
export function parseFetchOptions(value: unknown): FetchOptions {
  if (value === undefined) return {};
  if (!isObject(value) || Array.isArray(value)) invalid("options must be an object");
  const options: FetchOptions = {};
  if (value.cursor !== undefined) options.cursor = text(value.cursor, "cursor");
  if (value.fromVersion !== undefined) options.fromVersion = decimal(value.fromVersion, "fromVersion");
  if (value.toVersion !== undefined) options.toVersion = decimal(value.toVersion, "toVersion");
  if (value.limit !== undefined) {
    if (!Number.isInteger(value.limit) || (value.limit as number) < 1 || (value.limit as number) > MAX_LIMIT) {
      invalid(`"limit" must be an integer from 1 to ${MAX_LIMIT}`);
    }
    options.limit = value.limit as number;
  }
  if (value.offset !== undefined) {
    if (!Number.isSafeInteger(value.offset) || (value.offset as number) < 0) invalid('"offset" must be a non-negative integer');
    options.offset = value.offset as number;
  }
  if (value.orderBy !== undefined) {
    if (value.orderBy !== "asc" && value.orderBy !== "desc") invalid('"orderBy" must be "asc" or "desc"');
    options.orderBy = value.orderBy;
  }
  return options;
}

export interface WireError {
  name: string;
  message: string;
  statusCode?: number;
  retryAfter?: number;
}

/** What a client is told about `err`: its kind, its redacted message and any status and retry hint. */
export function toWireError(err: unknown, redact: (text: string) => string): WireError {
  const message = redact(err instanceof Error ? err.message : String(err));
  if (err instanceof StreamdNetworkError) {
    const wire: WireError = { name: "StreamdNetworkError", message };
    if (err.statusCode !== undefined) wire.statusCode = err.statusCode;
    if (err.retryAfter !== undefined) wire.retryAfter = err.retryAfter;
    return wire;
  }
  return { name: err instanceof StreamdError ? "StreamdError" : "Error", message };
}

// A plain StreamdError stops a poller as bad input, so only one the gateway
// called invalid input comes back as one; anything else is retried.
export function fromWireError(body: unknown, status: number, retryAfter?: number): StreamdError {
  const error = isObject(body) && isObject(body.error) ? body.error : undefined;
  const message = typeof error?.message === "string" ? error.message : `gateway answered ${status}`;
  if (error?.name === "StreamdError") return new StreamdError(message);
  const statusCode = typeof error?.statusCode === "number" ? error.statusCode : status;
  const hint = typeof error?.retryAfter === "number" ? error.retryAfter : retryAfter;
  return new StreamdNetworkError(message, statusCode, undefined, hint);
}
