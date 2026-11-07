import type {
  EventAdapter,
  EventDescriptor,
  EventExpectation,
  EventPredicate,
  ExpectEventsOptions,
  StreamdEvent,
} from "./types.js";
import { StreamdAssertionError } from "./errors.js";
import type { RequestLimiter } from "./limiter.js";
import { committedEvents, narrower } from "./transaction.js";
import { descriptorKey } from "./utils.js";

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

/** The assertions behind `StreamdClient.expectEvents`; each one looks the transaction up. */
export function expectEvents(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  pollInterval: number,
  transactionHash: string,
  options: ExpectEventsOptions,
): EventExpectation {
  const match = async (descriptor: EventDescriptor, where: EventPredicate | undefined) => {
    const narrow = narrower(descriptor, { decode: options.decode, where });
    const all = await committedEvents(
      adapter,
      limiter,
      pollInterval,
      transactionHash,
      descriptor,
      options,
    );
    const what = `${descriptorKey(descriptor)}${where === undefined ? "" : " matching where"}`;
    return { all, matched: narrow(all), what };
  };
  return {
    async toEmit<TData>(descriptor: EventDescriptor<TData>, where?: EventPredicate) {
      const { all, matched, what } = await match(descriptor, where);
      if (matched.length > 0) return matched as StreamdEvent<TData>[];
      throw new StreamdAssertionError(
        `Expected transaction ${transactionHash} to emit ${what}, but ${listing(all)}`,
        transactionHash,
        all,
      );
    },
    async notToEmit(descriptor: EventDescriptor, where?: EventPredicate) {
      const { all, matched, what } = await match(descriptor, where);
      if (matched.length === 0) return;
      throw new StreamdAssertionError(
        `Expected transaction ${transactionHash} not to emit ${what}, but ${matched.length} ` +
          `matched (event index ${matched.map((e) => e.eventIndex).join(", ")}); ${listing(all)}`,
        transactionHash,
        all,
      );
    },
  };
}
