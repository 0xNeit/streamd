import type { EventAdapter, EventDescriptor, StreamdEvent } from "@streamd/core";
import type { AptosTransactionSimulation, EventsOfOptions, PreviewEventsOptions } from "./types.js";
import { requireSimulation } from "./simulation.js";
import type { RequestLimiter } from "@streamd/core";
import {
  eventsOf as committedEventsOf,
  previewEvents as corePreviewEvents,
  requireTransactionHash,
  simulatedEvents as coreSimulatedEvents,
} from "@streamd/core";

/**
 * Polls `adapter` every `pollInterval` ms until the transaction is committed,
 * then returns its events narrowed by `descriptor`, `decode` and `where`.
 * Input is refused before the first lookup.
 */
export async function eventsOf(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  pollInterval: number,
  transactionHash: string,
  descriptor: EventDescriptor | undefined,
  options: EventsOfOptions,
): Promise<StreamdEvent[]> {
  requireTransactionHash(transactionHash);
  return committedEventsOf(adapter, limiter, pollInterval, transactionHash, descriptor, options);
}

/** Every event the simulation emits, or a `StreamdTransactionError` when it fails. */
export async function simulatedEvents(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  transaction: unknown,
  descriptor: EventDescriptor | undefined,
  keys: Pick<PreviewEventsOptions, "signerPublicKey" | "secondarySignersPublicKeys" | "feePayerPublicKey">,
): Promise<StreamdEvent[]> {
  return coreSimulatedEvents(adapter, limiter, simulation(transaction, keys), descriptor);
}

/**
 * Simulates `transaction` through `adapter` and returns the events it would
 * emit, narrowed as `eventsOf` narrows them. Input is refused before the call.
 */
export async function previewEvents(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  transaction: unknown,
  descriptor: EventDescriptor | undefined,
  options: PreviewEventsOptions,
): Promise<StreamdEvent[]> {
  return corePreviewEvents(adapter, limiter, simulation(transaction, options), descriptor, options);
}

function simulation(
  transaction: unknown,
  keys: Pick<PreviewEventsOptions, "signerPublicKey" | "secondarySignersPublicKeys" | "feePayerPublicKey">,
): AptosTransactionSimulation {
  const { signerPublicKey, secondarySignersPublicKeys, feePayerPublicKey } = keys;
  return requireSimulation({
    transaction,
    signerPublicKey,
    ...(secondarySignersPublicKeys !== undefined && { secondarySignersPublicKeys }),
    ...(feePayerPublicKey !== undefined && { feePayerPublicKey }),
  });
}
