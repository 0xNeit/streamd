import { fieldDecoder, UNSIGNED_INTEGER } from "./decode.js";
import { StreamdError } from "./errors.js";
import type { EventGuid, StreamdEvent } from "./types.js";

function bigintToString(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

/**
 * One event as a single line of JSON. JSON has no bigint, so every `bigint`,
 * including any inside `data`, becomes a decimal string. The field order is
 * fixed, which keeps the `streamd tail` output stable.
 */
export function serializeEvent(event: StreamdEvent): string {
  return JSON.stringify(
    {
      type: event.type,
      data: event.data,
      version: event.version,
      sequenceNumber: event.sequenceNumber,
      eventIndex: event.eventIndex,
      transactionHash: event.transactionHash,
      timestamp: event.timestamp,
      sender: event.sender,
      guid: event.guid,
      state: event.state,
    },
    bigintToString,
  );
}

export interface ParseEventOptions {
  /**
   * Top-level `data` fields to turn back into `bigint`, as the `decode`
   * listener and query option does. Bigints nested deeper stay decimal strings.
   */
  decode?: readonly string[];
}

function unsigned(record: Record<string, unknown>, field: string): bigint {
  const value = record[field];
  if (typeof value !== "string" || !UNSIGNED_INTEGER.test(value)) {
    throw new StreamdError(`Cannot parse event: "${field}" must be an unsigned decimal string`);
  }
  return BigInt(value);
}

function guidOf(value: unknown): EventGuid {
  const guid = value as Record<string, unknown> | null;
  if (typeof guid !== "object" || guid === null || typeof guid.accountAddress !== "string") {
    throw new StreamdError('Cannot parse event: "guid" must be { creationNumber, accountAddress }');
  }
  return { creationNumber: unsigned(guid, "creationNumber"), accountAddress: guid.accountAddress };
}

/**
 * Reads an event written by `serializeEvent` (or a `streamd tail` line),
 * restoring `version`, `sequenceNumber`, `timestamp` and `guid.creationNumber` as `bigint`. The
 * `TData` type is the caller's claim about `data`, not a runtime check.
 */
export function parseEvent<TData = unknown>(
  text: string,
  options: ParseEventOptions = {},
): StreamdEvent<TData> {
  const decode = fieldDecoder(options.decode);
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new StreamdError("Cannot parse event: not valid JSON", err);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new StreamdError("Cannot parse event: expected a JSON object");
  }
  const record = parsed as Record<string, unknown>;
  if (typeof record.type !== "string" || record.type === "") {
    throw new StreamdError('Cannot parse event: "type" must be a non-empty string');
  }
  const eventIndex = record.eventIndex;
  if (typeof eventIndex !== "number" || !Number.isSafeInteger(eventIndex) || eventIndex < 0) {
    throw new StreamdError('Cannot parse event: "eventIndex" must be a non-negative integer');
  }
  const event: StreamdEvent = {
    type: record.type,
    data: record.data,
    version: unsigned(record, "version"),
    sequenceNumber: unsigned(record, "sequenceNumber"),
    eventIndex,
  };
  if (record.transactionHash !== undefined) {
    if (typeof record.transactionHash !== "string") {
      throw new StreamdError('Cannot parse event: "transactionHash" must be a string');
    }
    event.transactionHash = record.transactionHash;
  }
  if (record.timestamp !== undefined) event.timestamp = unsigned(record, "timestamp");
  if (record.sender !== undefined) {
    if (typeof record.sender !== "string") {
      throw new StreamdError('Cannot parse event: "sender" must be a string');
    }
    event.sender = record.sender;
  }
  if (record.guid !== undefined) event.guid = guidOf(record.guid);
  if (record.state !== undefined) {
    if (!Array.isArray(record.state)) {
      throw new StreamdError('Cannot parse event: "state" must be an array');
    }
    event.state = record.state;
  }
  return (decode ? decode(event) : event) as StreamdEvent<TData>;
}
