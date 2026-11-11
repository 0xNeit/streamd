import type {
  EventAdapter,
  EventDescriptor,
  EventExpectation,
  EventPredicate,
  ExpectEventsOptions,
  ExpectPreviewOptions,
  EventOf,
  StreamdEvent,
} from "./types.js";
import { StreamdAssertionError } from "./errors.js";
import type { RequestLimiter } from "./limiter.js";
import { committedEvents, narrower, simulatedEvents } from "./transaction.js";
import { descriptorKey, raceAbort } from "./utils.js";

function bigintToString(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

function listing(events: readonly StreamdEvent[]): string {
  if (events.length === 0) return "it emitted no events";
  const lines = events.map(
    (e) => `  [${e.eventIndex}] ${e.type} ${JSON.stringify(e.data, bigintToString)}`,
  );
  return `it emitted ${events.length} event(s):\n${lines.join("\n")}`;
}

/**
 * `toEmit` and `notToEmit` over the events `load` finds for `subject`; each
 * assertion loads them afresh, and nothing is loaded until one runs.
 */
function expectation(
  subject: string,
  transactionHash: string | undefined,
  load: (descriptor: EventDescriptor) => Promise<StreamdEvent[]>,
  options: Pick<ExpectEventsOptions, "decode" | "signal">,
): EventExpectation {
  const match = async (descriptor: EventDescriptor, where: EventPredicate | undefined) => {
    const narrow = narrower(descriptor, { decode: options.decode, where });
    const all = await load(descriptor);
    const what = `${descriptorKey(descriptor)}${where === undefined ? "" : " matching where"}`;
    return { all, matched: await raceAbort(Promise.resolve(narrow(all)), options.signal), what };
  };
  return {
    async toEmit<TData>(descriptor: EventDescriptor<TData>, where?: EventPredicate) {
      const { all, matched, what } = await match(descriptor, where);
      if (matched.length > 0) return matched as unknown as EventOf<TData>[];
      throw new StreamdAssertionError(`Expected ${subject} to emit ${what}, but ${listing(all)}`, transactionHash, all);
    },
    async notToEmit(descriptor: EventDescriptor, where?: EventPredicate) {
      const { all, matched, what } = await match(descriptor, where);
      if (matched.length === 0) return;
      throw new StreamdAssertionError(
        `Expected ${subject} not to emit ${what}, but ${matched.length} ` +
          `matched (event index ${matched.map((e) => e.eventIndex).join(", ")}); ${listing(all)}`,
        transactionHash,
        all,
      );
    },
  };
}

/** The assertions behind `StreamdClient.expectEvents`; each one looks the transaction up. */
export function expectEvents(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  pollInterval: number,
  transactionHash: string,
  options: ExpectEventsOptions,
): EventExpectation {
  return expectation(
    `transaction ${transactionHash}`,
    transactionHash,
    (descriptor) => committedEvents(adapter, limiter, pollInterval, transactionHash, descriptor, options),
    options,
  );
}

/** The assertions behind `StreamdClient.expectPreview`; each one simulates the transaction. */
export function expectPreview(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  transaction: unknown,
  options: ExpectPreviewOptions,
): EventExpectation {
  return expectation(
    "the simulated transaction",
    undefined,
    (descriptor) => simulatedEvents(adapter, limiter, transaction, descriptor, options.signerPublicKey),
    options,
  );
}
