import type { EventAdapter, EventDescriptor, StreamdEvent, ViewRequest } from "./types.js";
import { StreamdError } from "./errors.js";
import { requireViewRequest } from "./adapters/validate.js";

export function validateWithState(withState: unknown): void {
  if (withState !== undefined && typeof withState !== "function") {
    throw new StreamdError("withState must be a function returning a view request or undefined");
  }
}

/** The checked request `withState` builds for `event`, or `undefined` for none. */
export function buildStateRequest(
  withState: (event: StreamdEvent) => ViewRequest | undefined,
  event: StreamdEvent,
): ViewRequest | undefined {
  const request = withState(event);
  return request === undefined ? undefined : requireViewRequest(request);
}

/**
 * A copy of `event` carrying the view's result at the event's own version,
 * as state stood once the emitting transaction had run.
 */
export async function readState(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  event: StreamdEvent,
  request: ViewRequest,
): Promise<StreamdEvent> {
  if (adapter.view === undefined) {
    throw new StreamdError("withState needs an adapter that implements view");
  }
  const state: unknown = await adapter.view(request, event.version, descriptor);
  if (!Array.isArray(state)) {
    throw new StreamdError(
      `view ${request.function} returned no array for ${event.version}:${event.eventIndex}`,
    );
  }
  return { ...event, state };
}
