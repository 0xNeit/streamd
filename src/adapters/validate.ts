import { StreamdError } from "../errors.js";
import { normalizeAddress } from "../filter.js";
import type {
  CreationNumberEventDescriptor,
  EventAdapter,
  HandleEventDescriptor,
  ModuleEventDescriptor,
  TransactionSimulation,
  ViewRequest,
} from "../types.js";

export const DEFAULT_LIMIT = 25;
const U64_MAX = 2n ** 64n - 1n;
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

/** A transaction hash as `0x` and 64 hex digits; it lands in a URL path. */
export function requireTransactionHash(value: unknown): string {
  if (typeof value !== "string" || !/^0x[0-9a-fA-F]{64}$/.test(value)) {
    const shown = typeof value === "string" ? JSON.stringify(value.slice(0, 80)) : typeof value;
    throw new StreamdError(`Not a valid transaction hash: ${shown}`);
  }
  return value;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/**
 * A single-signer transaction and optional public key to simulate. Refusals
 * name only what is wrong, never the transaction's contents.
 */
export function requireSimulation(value: unknown): TransactionSimulation {
  const simulation = value as Partial<Record<keyof TransactionSimulation, unknown>> | null;
  const transaction = simulation?.transaction;
  if (!isObject(transaction) || !isObject(transaction.rawTransaction)) {
    throw new StreamdError("Not a built transaction: pass the result of aptos.transaction.build.simple");
  }
  if (transaction.secondarySignerAddresses !== undefined) {
    throw new StreamdError("Only single-signer transactions can be simulated, not multi-agent ones");
  }
  if (simulation!.signerPublicKey !== undefined && !isObject(simulation!.signerPublicKey)) {
    throw new StreamdError("signerPublicKey must be a PublicKey");
  }
  return value as TransactionSimulation;
}

const VIEW_FUNCTION = /^0x[0-9a-fA-F]{1,64}::[A-Za-z_][A-Za-z0-9_]*::[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A view request with a well-formed function id and argument lists. Refusals
 * show at most the start of the function id, never the arguments.
 */
export function requireViewRequest(value: unknown): ViewRequest {
  if (!isObject(value)) throw new StreamdError("A view request must be an object");
  const { function: fn, typeArguments, functionArguments } = value;
  if (typeof fn !== "string" || !VIEW_FUNCTION.test(fn)) {
    const shown = typeof fn === "string" ? JSON.stringify(fn.slice(0, 80)) : typeof fn;
    throw new StreamdError(`Not a valid view function id (address::module::function): ${shown}`);
  }
  if (
    typeArguments !== undefined &&
    (!Array.isArray(typeArguments) || typeArguments.some((t) => typeof t !== "string" || t === ""))
  ) {
    throw new StreamdError(`typeArguments for ${fn} must be an array of non-empty strings`);
  }
  if (functionArguments !== undefined && !Array.isArray(functionArguments)) {
    throw new StreamdError(`functionArguments for ${fn} must be an array`);
  }
  // A fresh object, so a getter cannot answer the check and the request differently.
  const checked: ViewRequest = { function: fn as ViewRequest["function"] };
  if (typeArguments !== undefined) checked.typeArguments = [...(typeArguments as string[])];
  if (functionArguments !== undefined) checked.functionArguments = [...functionArguments];
  return checked;
}

export function requireLedgerVersion(value: unknown): bigint {
  if (typeof value !== "bigint" || value < 0n || value > U64_MAX) {
    throw new StreamdError(`Not a valid ledger version: ${String(value)} (expected a u64 bigint)`);
  }
  return value;
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

/**
 * The fullnode path for a `handle` descriptor's events, in either form, after
 * checking that it names exactly one. Every part lands in the URL path.
 */
export function handlePath(
  descriptor: HandleEventDescriptor | CreationNumberEventDescriptor,
): string {
  const address = requireAddress(descriptor.accountAddress);
  const { creationNumber, eventHandleStruct, fieldName } = descriptor as Partial<
    HandleEventDescriptor & { creationNumber: unknown }
  >;
  if (creationNumber !== undefined) {
    if (eventHandleStruct !== undefined || fieldName !== undefined) {
      throw new StreamdError(
        "A handle descriptor takes creationNumber or eventHandleStruct and fieldName, not both",
      );
    }
    if (typeof creationNumber !== "bigint" || creationNumber < 0n || creationNumber > U64_MAX) {
      throw new StreamdError(
        `Not a valid creationNumber: ${String(creationNumber)} (expected a u64 bigint)`,
      );
    }
    return `accounts/${address}/events/${creationNumber}`;
  }
  if (
    typeof eventHandleStruct !== "string" ||
    eventHandleStruct === "" ||
    typeof fieldName !== "string" ||
    fieldName === ""
  ) {
    throw new StreamdError(
      "A handle descriptor needs eventHandleStruct and fieldName, or creationNumber",
    );
  }
  return (
    `accounts/${address}/events/` +
    `${encodeURIComponent(eventHandleStruct)}/${encodeURIComponent(fieldName)}`
  );
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

export function requireAdapter(value: unknown, label: string): EventAdapter {
  if (
    typeof value !== "object" ||
    value === null ||
    typeof (value as EventAdapter).fetchEvents !== "function"
  ) {
    throw new StreamdError(`${label} requires a valid EventAdapter`);
  }
  return value as EventAdapter;
}
