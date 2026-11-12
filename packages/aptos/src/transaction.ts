import type { EventAdapter, EventDescriptor, StreamdEvent } from "@streamd/core";
import type { EventsOfOptions, PreviewEventsOptions } from "./types.js";
import { requireSimulation } from "./simulation.js";
import { StreamdError, StreamdTransactionError } from "@streamd/core";
import type { RequestLimiter } from "@streamd/core";
import { eventsOf as committedEventsOf, narrower, requireTransactionHash } from "@streamd/core";

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
  const { signerPublicKey, secondarySignersPublicKeys, feePayerPublicKey } = keys;
  const simulation = requireSimulation({
    transaction,
    signerPublicKey,
    ...(secondarySignersPublicKeys !== undefined && { secondarySignersPublicKeys }),
    ...(feePayerPublicKey !== undefined && { feePayerPublicKey }),
  });
  if (adapter.simulateTransactionEvents === undefined) {
    throw new StreamdError("previewEvents needs an adapter that implements simulateTransactionEvents");
  }
  const found = await limiter.run(() => adapter.simulateTransactionEvents!(simulation, descriptor));
  if (!found.success) {
    throw new StreamdTransactionError(
      `Simulated transaction failed at version ${found.version}: ${found.vmStatus}`,
      undefined,
      found.version,
      found.vmStatus,
    );
  }
  return found.events;
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
  const narrow = narrower(descriptor, options);
  return narrow(await simulatedEvents(adapter, limiter, transaction, descriptor, options));
}
