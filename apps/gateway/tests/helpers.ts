import { MemoryAdapter } from "@streamd/core";
import type { EventDescriptor, FetchOptions, StreamdEvent } from "@streamd/core";

export const PING = "0x1::ping::Ping";
export const PONG = "0x1::ping::Pong";
export const PINGS: EventDescriptor = { kind: "module", eventType: PING };

export function ev(version: number, type = PING): StreamdEvent {
  return { type, data: { n: String(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

/** A `MemoryAdapter` that counts its `fetchEvents` calls. */
export class CountingAdapter extends MemoryAdapter {
  readonly calls: FetchOptions[] = [];

  override fetchEvents(descriptor: EventDescriptor, options: FetchOptions) {
    this.calls.push(options);
    return super.fetchEvents(descriptor, options);
  }
}

export async function waitFor(condition: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeout) throw new Error("timed out waiting");
    await Bun.sleep(5);
  }
}
