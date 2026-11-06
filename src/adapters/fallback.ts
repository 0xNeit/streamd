import type {
  BlockHeights,
  BlockInfo,
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
  TransactionEvents,
  TransactionSimulation,
  ViewRequest,
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
 * how far that source scanned for the events it returned.
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
    return tryInOrder(this.adapters, async (adapter) => {
      const page = await adapter.fetchEvents(descriptor, { ...options });
      // Which source answers depends on availability, so only the first
      // source's page is the request's settled answer.
      return page.final === true && adapter !== this.adapters[0]
        ? { ...page, final: false }
        : page;
    });
  }

  /** From the first adapter that reports one, with the same fallback rules. */
  async getHeadVersion(descriptor: EventDescriptor): Promise<bigint> {
    const reporting = this.adapters.filter((a) => a.getHeadVersion !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source reports a head version");
    }
    return tryInOrder(reporting, (adapter) =>
      adapter.getHeadVersion!(descriptor),
    );
  }

  /** From the first adapter that reports one, with the same fallback rules. */
  async getEarliestVersion(descriptor: EventDescriptor): Promise<bigint> {
    const reporting = this.adapters.filter((a) => a.getEarliestVersion !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source reports an earliest version");
    }
    return tryInOrder(reporting, (adapter) =>
      adapter.getEarliestVersion!(descriptor),
    );
  }

  /** From the first adapter that reports blocks, with the same fallback rules. */
  async getBlock(descriptor: EventDescriptor, height: bigint): Promise<BlockInfo> {
    const reporting = this.adapters.filter((a) => a.getBlock !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source reports blocks");
    }
    return tryInOrder(reporting, (adapter) => adapter.getBlock!(descriptor, height));
  }

  /** From the first adapter that reports block heights, with the same fallback rules. */
  async getBlockHeights(descriptor: EventDescriptor): Promise<BlockHeights> {
    const reporting = this.adapters.filter((a) => a.getBlockHeights !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source reports block heights");
    }
    return tryInOrder(reporting, (adapter) => adapter.getBlockHeights!(descriptor));
  }

  /** From the first adapter that looks transactions up, with the same fallback rules. */
  async getTransactionEvents(
    transactionHash: string,
    descriptor?: EventDescriptor,
  ): Promise<TransactionEvents | undefined> {
    const reporting = this.adapters.filter((a) => a.getTransactionEvents !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source looks up transactions");
    }
    return tryInOrder(reporting, (adapter) =>
      adapter.getTransactionEvents!(transactionHash, descriptor),
    );
  }

  /** From the first adapter that simulates transactions, with the same fallback rules. */
  async simulateTransactionEvents(
    simulation: TransactionSimulation,
    descriptor?: EventDescriptor,
  ): Promise<TransactionEvents> {
    const reporting = this.adapters.filter((a) => a.simulateTransactionEvents !== undefined);
    if (reporting.length === 0) {
      throw new StreamdError("No FallbackAdapter source simulates transactions");
    }
    return tryInOrder(reporting, (adapter) =>
      adapter.simulateTransactionEvents!(simulation, descriptor),
    );
  }

  /** From the first adapter that runs views, with the same fallback rules. */
  async view(
    request: ViewRequest,
    ledgerVersion: bigint,
    descriptor?: EventDescriptor,
  ): Promise<unknown[]> {
    const reporting = this.adapters.filter((a) => a.view !== undefined);
    if (reporting.length === 0) throw new StreamdError("No FallbackAdapter source runs views");
    return tryInOrder(reporting, (adapter) => adapter.view!(request, ledgerVersion, descriptor));
  }
}
