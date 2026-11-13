import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
} from "../types.js";
import { StreamdError } from "../errors.js";
import { requireAdapter } from "./validate.js";

/** What `onRequest` sees before each `fetchEvents` call. */
export interface AdapterRequest {
  descriptor: EventDescriptor;
  options: FetchOptions;
}

/**
 * What `onResponse` sees once a `fetchEvents` call settles: the page on
 * success, the error on failure. `durationMs` times the source call alone.
 */
export type AdapterResponse = AdapterRequest & { durationMs: number } & (
    | { page: EventPage; error?: undefined }
    | { page?: undefined; error: unknown }
  );

/**
 * Synchronous observers for `withHooks`. A hook that throws fails the fetch;
 * a returned value, including a Promise, is ignored.
 */
export interface AdapterHooks {
  onRequest?(request: AdapterRequest): void;
  onResponse?(response: AdapterResponse): void;
}

function requireHook<K extends keyof AdapterHooks>(
  hooks: AdapterHooks,
  name: K,
): AdapterHooks[K] {
  const hook = hooks[name];
  if (hook !== undefined && typeof hook !== "function") {
    throw new StreamdError(`withHooks ${name} must be a function`);
  }
  return hook;
}

/**
 * Wraps `adapter` so every `fetchEvents` call is reported to `hooks`, to log
 * or time requests. `getHeadVersion`, `getEarliestVersion`, `getBlock`, `getBlockHeights`, `getTransactionEvents`, `simulateTransactionEvents`, `view` and `readObjects` pass through,
 * unhooked, only when the wrapped adapter has them.
 */
export function withHooks(
  adapter: EventAdapter,
  hooks: AdapterHooks,
): EventAdapter {
  const source = requireAdapter(adapter, "withHooks");
  if (typeof hooks !== "object" || hooks === null) {
    throw new StreamdError("withHooks requires a hooks object");
  }
  const onRequest = requireHook(hooks, "onRequest");
  const onResponse = requireHook(hooks, "onResponse");

  const wrapped: EventAdapter = {
    async fetchEvents(descriptor, options) {
      onRequest?.({ descriptor, options });
      const started = performance.now();
      let page: EventPage;
      try {
        page = await source.fetchEvents(descriptor, options);
      } catch (error) {
        const durationMs = performance.now() - started;
        onResponse?.({ descriptor, options, durationMs, error });
        throw error;
      }
      const durationMs = performance.now() - started;
      onResponse?.({ descriptor, options, durationMs, page });
      return page;
    },
  };
  if (source.getHeadVersion !== undefined) {
    wrapped.getHeadVersion = (descriptor) => source.getHeadVersion!(descriptor);
  }
  if (source.getEarliestVersion !== undefined) {
    wrapped.getEarliestVersion = (descriptor) =>
      source.getEarliestVersion!(descriptor);
  }
  if (source.getBlock !== undefined) {
    wrapped.getBlock = (descriptor, height) => source.getBlock!(descriptor, height);
  }
  if (source.getBlockHeights !== undefined) {
    wrapped.getBlockHeights = (descriptor) => source.getBlockHeights!(descriptor);
  }
  if (source.getTransactionEvents !== undefined) {
    wrapped.getTransactionEvents = (hash, descriptor) =>
      source.getTransactionEvents!(hash, descriptor);
  }
  if (source.simulateTransactionEvents !== undefined) {
    wrapped.simulateTransactionEvents = (simulation, descriptor) =>
      source.simulateTransactionEvents!(simulation, descriptor);
  }
  if (source.view !== undefined) {
    wrapped.view = (request, ledgerVersion, descriptor) =>
      source.view!(request, ledgerVersion, descriptor);
  }
  if (source.readObjects !== undefined) {
    wrapped.readObjects = (request, at, descriptor) => source.readObjects!(request, at, descriptor);
  }
  return wrapped;
}
