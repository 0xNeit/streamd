import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
} from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { requireAdapter } from "./validate.js";

async function tryInOrder<T>(
  adapters: readonly EventAdapter[],
  run: (adapter: EventAdapter) => Promise<T>,
): Promise<T> {
  let lastError: unknown;
  for (const adapter of adapters) {
    try {
      return await run(adapter);
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

/**
 * Tries a list of `EventAdapter` sources in order on each `fetchEvents` call,
 * falling back to the next adapter when one fails with a network or source
 * error. Invalid caller input (a `StreamdError` that is not a
 * `StreamdNetworkError`) is rethrown at once without trying later adapters.
 * A page from any source but the first comes back with `final: false`.
 * `throughVersion` is kept from whichever source answered, since it describes
 * how far that source scanned for the events it returned. Each optional
 * method is defined only when some source has it, and tries those sources in
 * order under the same rules.
 */
export class FallbackAdapter implements EventAdapter {
  private readonly adapters: readonly EventAdapter[];

  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];

  constructor(adapters: readonly EventAdapter[]) {
    if (!Array.isArray(adapters) || adapters.length === 0) {
      throw new StreamdError("FallbackAdapter requires at least one adapter");
    }
    this.adapters = adapters.map((adapter, index) =>
      requireAdapter(adapter, `FallbackAdapter[${index}]`),
    );

    const head = this.sources("getHeadVersion");
    if (head.length > 0) {
      this.getHeadVersion = (descriptor) =>
        tryInOrder(head, (adapter) => adapter.getHeadVersion!(descriptor));
    }
    const earliest = this.sources("getEarliestVersion");
    if (earliest.length > 0) {
      this.getEarliestVersion = (descriptor) =>
        tryInOrder(earliest, (adapter) => adapter.getEarliestVersion!(descriptor));
    }
    const blocks = this.sources("getBlock");
    if (blocks.length > 0) {
      this.getBlock = (descriptor, height) =>
        tryInOrder(blocks, (adapter) => adapter.getBlock!(descriptor, height));
    }
    const heights = this.sources("getBlockHeights");
    if (heights.length > 0) {
      this.getBlockHeights = (descriptor) =>
        tryInOrder(heights, (adapter) => adapter.getBlockHeights!(descriptor));
    }
    const lookups = this.sources("getTransactionEvents");
    if (lookups.length > 0) {
      this.getTransactionEvents = (hash, descriptor) =>
        tryInOrder(lookups, (adapter) => adapter.getTransactionEvents!(hash, descriptor));
    }
    const simulators = this.sources("simulateTransactionEvents");
    if (simulators.length > 0) {
      this.simulateTransactionEvents = (simulation, descriptor) =>
        tryInOrder(simulators, (adapter) =>
          adapter.simulateTransactionEvents!(simulation, descriptor),
        );
    }
    const views = this.sources("view");
    if (views.length > 0) {
      this.view = (request, ledgerVersion, descriptor) =>
        tryInOrder(views, (adapter) => adapter.view!(request, ledgerVersion, descriptor));
    }
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    return tryInOrder(this.adapters, async (adapter) => {
      const page = await adapter.fetchEvents(descriptor, { ...options });
      // Which source answers depends on availability, so only the first
      // source's page is the request's settled answer.
      return page.final === true && adapter !== this.adapters[0]
        ? { ...page, final: false }
        : page;
    });
  }

  private sources(name: keyof EventAdapter): EventAdapter[] {
    return this.adapters.filter((adapter) => adapter[name] !== undefined);
  }
}
