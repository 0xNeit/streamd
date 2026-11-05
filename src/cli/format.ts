import type { StreamdEvent } from "../types.js";

function bigintToString(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

/**
 * One event as a single JSON line (without the newline). JSON has no bigint,
 * so every `bigint`, including any inside `data`, becomes a decimal string.
 */
export function formatEvent(event: StreamdEvent): string {
  return JSON.stringify(
    {
      type: event.type,
      data: event.data,
      version: event.version,
      sequenceNumber: event.sequenceNumber,
      eventIndex: event.eventIndex,
      transactionHash: event.transactionHash,
      timestamp: event.timestamp,
    },
    bigintToString,
  );
}
