import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
} from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { requireAdapter } from "./validate.js";

/**
 * Tries a list of `EventAdapter` sources in order on each `fetchEvents` call,
 * falling back to the next adapter when one fails with a network or source
 * error. Invalid caller input (a `StreamdError` that is not a
 * `StreamdNetworkError`) is rethrown at once without trying later adapters.
 */
export class FallbackAdapter implements EventAdapter {
  private readonly adapters: readonly EventAdapter[];

  constructor(adapters: readonly EventAdapter[]) {
    if (!Array.isArray(adapters) || adapters.length === 0) {
      throw new StreamdError("FallbackAdapter requires at least one adapter");
    }
    this.adapters = adapters.map((adapter, index) =>
      requireAdapter(adapter, `FallbackAdapter[${index}]`),
    );
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    let lastError: unknown;
    for (const adapter of this.adapters) {
      try {
        return await adapter.fetchEvents(descriptor, { ...options });
      } catch (err) {
        if (
          err instanceof StreamdError &&
          !(err instanceof StreamdNetworkError)
        ) {
          throw err;
        }
        lastError = err;
      }
    }
    throw lastError;
  }
}
