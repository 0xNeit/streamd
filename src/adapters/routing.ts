import type {
  EventAdapter,
  EventDescriptor,
  EventPage,
  FetchOptions,
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
  /** Fallback adapter used for any descriptor kind omitted above. */
  default?: EventAdapter;
}

/**
 * Composes `EventAdapter` instances by descriptor `kind` (`"module"`,
 * `"handle"`, `"account"`), with an optional `default` adapter for kinds not
 * listed explicitly.
 */
export class RoutingAdapter implements EventAdapter {
  private readonly moduleAdapter?: EventAdapter;
  private readonly handleAdapter?: EventAdapter;
  private readonly accountAdapter?: EventAdapter;
  private readonly defaultAdapter?: EventAdapter;

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
      this.defaultAdapter === undefined
    ) {
      throw new StreamdError(
        "RoutingAdapter requires at least one route or a default adapter",
      );
    }
  }

  async fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions = {},
  ): Promise<EventPage> {
    const adapter = this.selectAdapter(descriptor);
    return adapter.fetchEvents(descriptor, options);
  }

  async getHeadVersion(descriptor: EventDescriptor): Promise<bigint> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getHeadVersion === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report a head version`,
      );
    }
    return adapter.getHeadVersion(descriptor);
  }

  async getEarliestVersion(descriptor: EventDescriptor): Promise<bigint> {
    const adapter = this.selectAdapter(descriptor);
    if (adapter.getEarliestVersion === undefined) {
      throw new StreamdError(
        `The adapter for descriptor kind "${descriptor.kind}" does not report an earliest version`,
      );
    }
    return adapter.getEarliestVersion(descriptor);
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
