import type { EventAdapter, EventDescriptor, StateRequest, StreamdEvent } from "./types.js";
import { StreamdError } from "./errors.js";
import { isObject, requireObjectRequest, requireViewRequest } from "./adapters/validate.js";

export function validateWithState(withState: unknown): void {
  if (withState !== undefined && typeof withState !== "function") {
    throw new StreamdError("withState must be a function returning a view request, an object request or undefined");
  }
}

/** The checked request `withState` builds for `event`, or `undefined` for none. */
export function buildStateRequest(
  withState: (event: StreamdEvent) => StateRequest | undefined,
  event: StreamdEvent,
): StateRequest | undefined {
  const request: unknown = withState(event);
  if (request === undefined) return undefined;
  return isObject(request) && "objects" in request ? requireObjectRequest(request) : requireViewRequest(request);
}

/**
 * A copy of `event` carrying what the request read at the event's own
 * version, as state stood once the emitting transaction had run.
 */
export async function readState(
  adapter: EventAdapter,
  descriptor: EventDescriptor,
  event: StreamdEvent,
  request: StateRequest,
): Promise<StreamdEvent> {
  const at = `${event.version}:${event.eventIndex}`;
  if ("objects" in request) {
    if (adapter.readObjects === undefined) {
      throw new StreamdError("withState object reads need an adapter that implements readObjects");
    }
    const state: unknown = await adapter.readObjects(request, event, descriptor);
    if (!Array.isArray(state) || state.length !== request.objects.length) {
      throw new StreamdError(`readObjects returned no array of ${request.objects.length} for ${at}`);
    }
    return { ...event, state };
  }
  if (adapter.view === undefined) {
    throw new StreamdError("withState needs an adapter that implements view");
  }
  const state: unknown = await adapter.view(request, event.version, descriptor);
  if (!Array.isArray(state)) {
    throw new StreamdError(`view ${request.function} returned no array for ${at}`);
  }
  return { ...event, state };
}
