import type {
  EventAdapter,
  EventDescriptor,
  EventsOfOptions,
  PreviewEventsOptions,
  StreamdEvent,
  TransactionEvents,
} from "./types.js";
import { StreamdError, StreamdTimeoutError, StreamdTransactionError } from "./errors.js";
import { filterEvents, normalizeAddress } from "./filter.js";
import { fieldDecoder } from "./decode.js";
import type { RequestLimiter } from "./limiter.js";
import { onAbort, raceAbort, sleep } from "./utils.js";
import {
  handlePath,
  moduleTypeFilter,
  requireAddress,
  requireSimulation,
  requireTransactionHash,
} from "./adapters/validate.js";

/** Matches the SDK's own `waitForTransaction` default. */
const DEFAULT_TRANSACTION_TIMEOUT = 20_000;
const DEFAULT_LOOKUP_INTERVAL = 1000;
/**
 * A `pollInterval` of 0 suits `on()` catching up, where a page with `hasMore`
 * is real work; here it would be a request per round trip for the whole wait.
 */
const MIN_LOOKUP_INTERVAL = 100;
/** setTimeout fires at once past this. */
const MAX_LOOKUP_INTERVAL = 2_147_483_647;

/** Which of a transaction's events `descriptor` selects. */
function eventMatcher(descriptor: EventDescriptor): (event: StreamdEvent) => boolean {
  switch (descriptor?.kind) {
    case "module": {
      const keep = moduleTypeFilter(descriptor.eventType);
      return (event) => keep(event.type);
    }
    case "account": {
      const account = requireAddress(descriptor.accountAddress);
      const types = descriptor.eventTypes === undefined ? undefined : new Set(descriptor.eventTypes);
      return (event) =>
        event.sender !== undefined &&
        normalizeAddress(event.sender) === account &&
        (types === undefined || types.has(event.type));
    }
    case "handle": {
      handlePath(descriptor);
      const { creationNumber } = descriptor;
      // An event carries its handle's guid, not the struct and field holding it.
      if (creationNumber === undefined) {
        throw new StreamdError(
          "Transaction events match handle events by guid: pass the handle's creationNumber " +
            "(listEventHandles reports it) instead of eventHandleStruct and fieldName",
        );
      }
      const owner = requireAddress(descriptor.accountAddress);
      return (event) =>
        event.guid !== undefined &&
        event.guid.creationNumber === creationNumber &&
        normalizeAddress(event.guid.accountAddress) === owner;
    }
    case "resource":
      throw new StreamdError(
        "Transaction lookups carry events, not write sets, so a resource descriptor cannot narrow them",
      );
    default:
      throw new StreamdError(
        `Unknown descriptor kind: ${String((descriptor as { kind?: unknown } | null)?.kind)}`,
      );
  }
}

/**
 * Checks `descriptor` and `decode` now, so bad input is refused before any
 * adapter call; the returned function narrows a transaction's events by
 * them, then `where`, keeping transaction order.
 */
export function narrower(
  descriptor: EventDescriptor | undefined,
  options: Pick<EventsOfOptions, "decode" | "where">,
): (events: StreamdEvent[]) => StreamdEvent[] | Promise<StreamdEvent[]> {
  const keep = descriptor === undefined ? undefined : eventMatcher(descriptor);
  const decode = fieldDecoder(options.decode);
  const where = options.where;
  return (events) => {
    if (keep !== undefined) events = events.filter(keep);
    if (decode !== undefined) events = events.map(decode);
    return where === undefined ? events : filterEvents(events, where);
  };
}

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
  const narrow = narrower(descriptor, options);
  const events = await committedEvents(adapter, limiter, pollInterval, transactionHash, descriptor, options);
  return raceAbort(Promise.resolve(narrow(events)), options.signal);
}

/**
 * Polls `adapter` every `pollInterval` ms until the transaction is committed,
 * then returns all its events; `descriptor` only routes the lookup. Rejects
 * with `StreamdTransactionError` for a failed transaction. `timeout` bounds the
 * caller's wait, an in-flight lookup included, but the adapter gets no signal:
 * a hung lookup keeps its `maxConcurrentRequests` slot until it settles or
 * the client's `requestTimeout` passes.
 */
export async function committedEvents(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  pollInterval: number,
  transactionHash: string,
  descriptor: EventDescriptor | undefined,
  options: Pick<EventsOfOptions, "timeout" | "signal">,
): Promise<StreamdEvent[]> {
  const hash = requireTransactionHash(transactionHash);
  if (adapter.getTransactionEvents === undefined) {
    throw new StreamdError("eventsOf needs an adapter that implements getTransactionEvents");
  }
  const outer = options.signal;
  if (outer?.aborted) throw outer.reason;

  const timeout = options.timeout ?? DEFAULT_TRANSACTION_TIMEOUT;
  const interval = Number.isFinite(pollInterval)
    ? Math.min(Math.max(pollInterval, MIN_LOOKUP_INTERVAL), MAX_LOOKUP_INTERVAL)
    : DEFAULT_LOOKUP_INTERVAL;
  const wait = new AbortController();
  const timer = setTimeout(
    () => wait.abort(new StreamdTimeoutError(`Transaction ${hash} not found after ${timeout}ms`, timeout)),
    timeout,
  );
  const detach = onAbort(outer, () => wait.abort(outer!.reason));
  let found: TransactionEvents | undefined;
  try {
    for (;;) {
      found = await raceAbort(
        limiter.run(() => adapter.getTransactionEvents!(hash, descriptor), wait.signal),
        wait.signal,
      );
      if (found !== undefined) break;
      await sleep(interval, wait.signal);
    }
  } finally {
    clearTimeout(timer);
    detach();
  }

  if (!found.success) {
    throw new StreamdTransactionError(
      `Transaction ${hash} failed at version ${found.version}: ${found.vmStatus}`,
      hash,
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
