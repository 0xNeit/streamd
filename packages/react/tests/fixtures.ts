import {
  MemoryAdapter,
  StreamdNetworkError,
  type EventAdapter,
  type EventDescriptor,
  type FetchOptions,
  type MoveEventType,
  type StreamdEvent,
} from "@streamd/core";

export const TYPE: MoveEventType = "0x1::coin::Deposit";
export const OTHER: MoveEventType = "0x1::coin::Withdraw";

export function ev(version: bigint, type = TYPE): StreamdEvent {
  return { type, data: { v: String(version) }, version, sequenceNumber: 0n, eventIndex: 0 };
}

/**
 * A `MemoryAdapter` over a list tests can append to, that counts fetches and
 * fails with a `StreamdNetworkError` while `failing` is set.
 */
export class TestAdapter implements EventAdapter {
  fetches = 0;
  failing = false;

  constructor(public events: StreamdEvent[] = []) {}

  getHeadVersion() {
    return new MemoryAdapter(this.events).getHeadVersion();
  }

  async fetchEvents(descriptor: EventDescriptor, options: FetchOptions) {
    this.fetches++;
    if (this.failing) throw new StreamdNetworkError("rate limited");
    return new MemoryAdapter(this.events).fetchEvents(descriptor, options);
  }
}
