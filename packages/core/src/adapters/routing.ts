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
import { StreamdError } from "../errors.js";
import { requireAdapter } from "./validate.js";

export interface RoutingAdapterRoutes {
  /** Adapter used for `{ kind: "module" }` descriptors. */
  module?: EventAdapter;
  /** Adapter used for `{ kind: "handle" }` descriptors. */
  handle?: EventAdapter;
  /** Adapter used for `{ kind: "account" }` descriptors. */
  account?: EventAdapter;
  /** Adapter used for `{ kind: "resource" }` descriptors. */
  resource?: EventAdapter;
  /** Fallback adapter used for any descriptor kind omitted above. */
  default?: EventAdapter;
}

/**
 * Composes `EventAdapter` instances by descriptor `kind` (`"module"`,
 * `"handle"`, `"account"`, `"resource"`), with an optional `default` adapter for kinds not
 * listed explicitly. Each optional method is defined only when some route or
 * the default has it, and still rejects for a kind whose adapter lacks it.
 */
export class RoutingAdapter implements EventAdapter {
  private readonly moduleAdapter?: EventAdapter;
  private readonly handleAdapter?: EventAdapter;
  private readonly accountAdapter?: EventAdapter;
  private readonly resourceAdapter?: EventAdapter;
  private readonly defaultAdapter?: EventAdapter;

  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];
  declare readObjects?: EventAdapter["readObjects"];

  constructor(routes: RoutingAdapterRoutes) {
    if (typeof routes !== "object" || routes === null) {
      throw new StreamdError("RoutingAdapter requires a routes object");
    }
    if (routes.module !== undefined) {
      this.moduleAdapter = requireAdapter(
        routes.module,
        "RoutingAdapter.module",
      );
    }
    if (routes.handle !== undefined) {
      this.handleAdapter = requireAdapter(
        routes.handle,
        "RoutingAdapter.handle",
      );
    }
    if (routes.account !== undefined) {
      this.accountAdapter = requireAdapter(
        routes.account,
        "RoutingAdapter.account",
      );
    }
    if (routes.resource !== undefined) {
      this.resourceAdapter = requireAdapter(routes.resource, "RoutingAdapter.resource");
    }
    if (routes.default !== undefined) {
      this.defaultAdapter = requireAdapter(
        routes.default,
        "RoutingAdapter.default",
      );
    }
    if (
      this.moduleAdapter === undefined &&
      this.handleAdapter === undefined &&
      this.accountAdapter === undefined &&
      this.resourceAdapter === undefined &&
      this.defaultAdapter === undefined
    ) {
      throw new StreamdError(
        "RoutingAdapter requires at least one route or a default adapter",
      );
    }
    this.exposeOptionalMethods();
  }

  /** Defines each optional method only when some route or the default has it. */
  private exposeOptionalMethods(): void {
    const routes = [
      this.moduleAdapter,
      this.handleAdapter,
      this.accountAdapter,
      this.resourceAdapter,
      this.defaultAdapter,
    ];
    const served = (name: keyof EventAdapter) =>
      routes.some((adapter) => adapter?.[name] !== undefined);
    if (served("getHeadVersion")) this.getHeadVersion = (d) => this.headVersion(d);
    if (served("getEarliestVersion")) this.getEarliestVersion = (d) => this.earliestVersion(d);
    if (served("getBlock")) this.getBlock = (d, height) => this.block(d, height);
    if (served("getBlockHeights")) this.getBlockHeights = (d) => this.blockHeights(d);
    if (served("getTransactionEvents")) {
      this.getTransactionEvents = (hash, d) => this.transactionEvents(hash, d);
    }
    if (served("simulateTransactionEvents")) {
      this.simulateTransactionEvents = (simulation, d) => this.simulation(simulation, d);
    }
    if (served("view")) {
      this.view = (request, ledgerVersion, d) => this.runView(request, ledgerVersion, d);
    }
    if (served("readObjects")) {
      this.readObjects = async (request, at, d) => this.reader(d).readObjects!(request, at, d);
    }
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    const adapter = this.selectAdapter(descriptor);
    return adapter.fetchEvents(descriptor, options);
  }

  private async headVersion(descriptor: EventDescriptor): Promise<bigint> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getHeadVersion === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report a head version`,
      );
    }
    return adapter.getHeadVersion(descriptor);
  }

  private async earliestVersion(descriptor: EventDescriptor): Promise<bigint> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getEarliestVersion === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report an earliest version`,
      );
    }
    return adapter.getEarliestVersion(descriptor);
  }

  private async block(descriptor: EventDescriptor, height: bigint): Promise<BlockInfo> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getBlock === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report blocks`,
      );
    }
    return adapter.getBlock(descriptor, height);
  }

  private async blockHeights(descriptor: EventDescriptor): Promise<BlockHeights> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getBlockHeights === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report block heights`,
      );
    }
    return adapter.getBlockHeights(descriptor);
  }

  /** From the adapter for `descriptor`'s kind, or the `default` adapter without one. */
  private async transactionEvents(
    transactionHash: string,
    descriptor?: EventDescriptor,
  ): Promise<TransactionEvents | undefined> {
    const adapter =
      descriptor === undefined ? this.defaultAdapter : this.selectAdapter(descriptor);
    if (adapter === undefined) {
      throw new StreamdError(
        "RoutingAdapter needs a descriptor or a default adapter to look up a transaction",
      );
    }
    if (adapter.getTransactionEvents === undefined) {
      throw new StreamdError(
        descriptor === undefined
          ? "The default adapter does not look up transactions"
          : `The adapter for descriptor kind "${descriptor.kind}" does not look up transactions`,
      );
    }
    return adapter.getTransactionEvents(transactionHash, descriptor);
  }

  /** From the adapter for `descriptor`'s kind, or the `default` adapter without one. */
  private async simulation(
    simulation: TransactionSimulation,
    descriptor?: EventDescriptor,
  ): Promise<TransactionEvents> {
    const adapter =
      descriptor === undefined ? this.defaultAdapter : this.selectAdapter(descriptor);
    if (adapter === undefined) {
      throw new StreamdError(
        "RoutingAdapter needs a descriptor or a default adapter to simulate a transaction",
      );
    }
    if (adapter.simulateTransactionEvents === undefined) {
      throw new StreamdError(
        descriptor === undefined
          ? "The default adapter does not simulate transactions"
          : `The adapter for descriptor kind "${descriptor.kind}" does not simulate transactions`,
      );
    }
    return adapter.simulateTransactionEvents(simulation, descriptor);
  }

  /** From the adapter for `descriptor`'s kind, or the `default` adapter without one. */
  private async runView(
    request: ViewRequest,
    ledgerVersion: bigint,
    descriptor?: EventDescriptor,
  ): Promise<unknown[]> {
    const adapter =
      descriptor === undefined ? this.defaultAdapter : this.selectAdapter(descriptor);
    if (adapter === undefined) {
      throw new StreamdError("RoutingAdapter needs a descriptor or a default adapter to run a view");
    }
    if (adapter.view === undefined) {
      throw new StreamdError(
        descriptor === undefined
          ? "The default adapter does not run views"
          : `The adapter for descriptor kind "${descriptor.kind}" does not run views`,
      );
    }
    return adapter.view(request, ledgerVersion, descriptor);
  }

  /** The adapter for `descriptor`'s kind, or the `default` adapter without one, refusing one that cannot read objects. */
  private reader(descriptor?: EventDescriptor): EventAdapter {
    const adapter = descriptor === undefined ? this.defaultAdapter : this.selectAdapter(descriptor);
    if (adapter === undefined) throw new StreamdError("RoutingAdapter needs a descriptor or a default adapter to read objects");
    if (adapter.readObjects === undefined) {
      throw new StreamdError(
        descriptor === undefined
          ? "The default adapter does not read objects"
          : `The adapter for descriptor kind "${descriptor.kind}" does not read objects`,
      );
    }
    return adapter;
  }

  private selectAdapter(descriptor: EventDescriptor): EventAdapter {
    let target: EventAdapter | undefined;
    switch (descriptor?.kind) {
      case "module":
        target = this.moduleAdapter ?? this.defaultAdapter;
        break;
      case "handle":
        target = this.handleAdapter ?? this.defaultAdapter;
        break;
      case "account":
        target = this.accountAdapter ?? this.defaultAdapter;
        break;
      case "resource":
        target = this.resourceAdapter ?? this.defaultAdapter;
        break;
      case "object":
        target = this.defaultAdapter;
        break;
      default: {
        const unknown: never = descriptor;
        throw new StreamdError(
          `Unknown descriptor kind: ${String((unknown as { kind?: unknown } | null)?.kind)}`,
        );
      }
    }
    if (target === undefined) {
      throw new StreamdError(
        `No adapter configured for descriptor kind "${descriptor.kind}"`,
      );
    }
    return target;
  }
}
