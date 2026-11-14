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
import { normalizeAddress } from "../filter.js";
import { requireAdapter } from "./validate.js";

const ADDRESS_TOKEN = /\b0x[0-9a-fA-F]{1,64}\b/g;
const IDENTIFIER = "[A-Za-z_][A-Za-z0-9_]*";
const TYPE_KEY = new RegExp(`^(0x[0-9a-f]{64})::(${IDENTIFIER})::(${IDENTIFIER})(<.+>)?$`);
const MODULE_KEY = new RegExp(`^0x[0-9a-f]{64}::${IDENTIFIER}$`);

/** Whitespace removed and every address in long form, so `0x1` and its padded form match. */
function canonical(value: string): string {
  return value.replace(/\s+/g, "").replace(ADDRESS_TOKEN, (address) => normalizeAddress(address)!);
}

interface ParsedType {
  type: string;
  base: string;
  module: string;
}

function parseType(value: unknown): ParsedType | undefined {
  if (typeof value !== "string") return undefined;
  const type = canonical(value);
  const match = TYPE_KEY.exec(type);
  if (match === null) return undefined;
  const [, address, module, struct] = match;
  return { type, base: `${address}::${module}::${struct}`, module: `${address}::${module}` };
}

function parseModule(value: string): string | undefined {
  const id = canonical(value);
  return MODULE_KEY.test(id) ? id : undefined;
}

function routeMap(
  value: unknown,
  label: "types" | "modules",
  parse: (key: string) => string | undefined,
): Map<string, EventAdapter> {
  const map = new Map<string, EventAdapter>();
  if (value === undefined) return map;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new StreamdError(`RoutingAdapter.${label} must be an object of adapters by key`);
  }
  const form = label === "types" ? "address::module::Struct" : "address::module";
  for (const [key, adapter] of Object.entries(value)) {
    const id = parse(key);
    if (id === undefined) {
      throw new StreamdError(`RoutingAdapter.${label} key must be written ${form}: ${JSON.stringify(key)}`);
    }
    if (map.has(id)) {
      throw new StreamdError(`RoutingAdapter.${label} key ${JSON.stringify(key)} names ${id} again`);
    }
    map.set(id, requireAdapter(adapter, `RoutingAdapter.${label}[${JSON.stringify(key)}]`));
  }
  return map;
}

/** An adapter, the route it was found by, and how a refusal names it. */
interface Route {
  adapter: EventAdapter;
  label: string;
  phrase: string;
}

export interface RoutingAdapterRoutes {
  /**
   * Adapters for `module` descriptors by Move type, `address::module::Struct`. A key without
   * type arguments also serves that struct with any; an exact generic key wins.
   */
  types?: Readonly<Record<string, EventAdapter>>;
  /** Adapters for `module` descriptors by the module declaring the type, `address::module`. */
  modules?: Readonly<Record<string, EventAdapter>>;
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
 * listed explicitly. A `module` descriptor first tries `types`, then `modules`, by its event
 * types with addresses compared in long form; a list of types must resolve to one adapter, so
 * a descriptor always reaches the same source and its cursor never crosses to another. Each
 * optional method is defined only when some route or the default has it, and still rejects
 * for a descriptor whose adapter lacks it.
 */
export class RoutingAdapter implements EventAdapter {
  private readonly moduleAdapter?: EventAdapter;
  private readonly handleAdapter?: EventAdapter;
  private readonly accountAdapter?: EventAdapter;
  private readonly resourceAdapter?: EventAdapter;
  private readonly defaultAdapter?: EventAdapter;
  private readonly typeRoutes: Map<string, EventAdapter>;
  private readonly moduleRoutes: Map<string, EventAdapter>;

  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare getTransactionEventsByVersion?: EventAdapter["getTransactionEventsByVersion"];
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
    this.typeRoutes = routeMap(routes.types, "types", (key) => parseType(key)?.type);
    this.moduleRoutes = routeMap(routes.modules, "modules", parseModule);
    if (
      this.typeRoutes.size === 0 &&
      this.moduleRoutes.size === 0 &&
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
      ...this.typeRoutes.values(),
      ...this.moduleRoutes.values(),
    ];
    const served = (name: keyof EventAdapter) =>
      routes.some((adapter) => adapter?.[name] !== undefined);
    if (served("getHeadVersion")) this.getHeadVersion = (d) => this.headVersion(d);
    if (served("getEarliestVersion")) this.getEarliestVersion = (d) => this.earliestVersion(d);
    if (served("getBlock")) this.getBlock = (d, height) => this.block(d, height);
    if (served("getBlockHeights")) this.getBlockHeights = (d) => this.blockHeights(d);
    if (served("getTransactionEvents")) {
      this.getTransactionEvents = async (hash, d) =>
        this.lookup(d, "getTransactionEvents").getTransactionEvents!(hash, d);
    }
    if (served("getTransactionEventsByVersion")) {
      this.getTransactionEventsByVersion = async (version, d) =>
        this.lookup(d, "getTransactionEventsByVersion").getTransactionEventsByVersion!(version, d);
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
    return this.route(descriptor).adapter.fetchEvents(descriptor, options);
  }

  private async headVersion(descriptor: EventDescriptor): Promise<bigint> {
    const { adapter, phrase } = this.route(descriptor);
    if (adapter.getHeadVersion === undefined) {
      throw new StreamdError(`The adapter for ${phrase} does not report a head version`);
    }
    return adapter.getHeadVersion(descriptor);
  }

  private async earliestVersion(descriptor: EventDescriptor): Promise<bigint> {
    const { adapter, phrase } = this.route(descriptor);
    if (adapter.getEarliestVersion === undefined) {
      throw new StreamdError(`The adapter for ${phrase} does not report an earliest version`);
    }
    return adapter.getEarliestVersion(descriptor);
  }

  private async block(descriptor: EventDescriptor, height: bigint): Promise<BlockInfo> {
    const { adapter, phrase } = this.route(descriptor);
    if (adapter.getBlock === undefined) {
      throw new StreamdError(`The adapter for ${phrase} does not report blocks`);
    }
    return adapter.getBlock(descriptor, height);
  }

  private async blockHeights(descriptor: EventDescriptor): Promise<BlockHeights> {
    const { adapter, phrase } = this.route(descriptor);
    if (adapter.getBlockHeights === undefined) {
      throw new StreamdError(`The adapter for ${phrase} does not report block heights`);
    }
    return adapter.getBlockHeights(descriptor);
  }

  /** The route for `descriptor`, or the `default` adapter without one, refusing one without `method`. */
  private optional(
    descriptor: EventDescriptor | undefined,
    method: keyof EventAdapter,
    action: string,
    missing: string,
  ): EventAdapter {
    const route = descriptor === undefined ? undefined : this.route(descriptor);
    const adapter = route?.adapter ?? this.defaultAdapter;
    if (adapter === undefined) {
      throw new StreamdError(`RoutingAdapter needs a descriptor or a default adapter to ${action}`);
    }
    if (adapter[method] === undefined) {
      throw new StreamdError(
        route === undefined
          ? `The default adapter does not ${missing}`
          : `The adapter for ${route.phrase} does not ${missing}`,
      );
    }
    return adapter;
  }

  private lookup(
    descriptor: EventDescriptor | undefined,
    method: "getTransactionEvents" | "getTransactionEventsByVersion",
  ): EventAdapter {
    const what = method === "getTransactionEvents" ? "transactions" : "transactions by version";
    return this.optional(descriptor, method, "look up a transaction", `look up ${what}`);
  }

  private async simulation(
    simulation: TransactionSimulation,
    descriptor?: EventDescriptor,
  ): Promise<TransactionEvents> {
    const adapter = this.optional(
      descriptor,
      "simulateTransactionEvents",
      "simulate a transaction",
      "simulate transactions",
    );
    return adapter.simulateTransactionEvents!(simulation, descriptor);
  }

  private async runView(
    request: ViewRequest,
    ledgerVersion: bigint,
    descriptor?: EventDescriptor,
  ): Promise<unknown[]> {
    return this.optional(descriptor, "view", "run a view", "run views").view!(
      request,
      ledgerVersion,
      descriptor,
    );
  }

  private reader(descriptor?: EventDescriptor): EventAdapter {
    return this.optional(descriptor, "readObjects", "read objects", "read objects");
  }

  /** The route for `descriptor`: its event types' routes for `module`, else its kind's, else `default`. */
  private route(descriptor: EventDescriptor): Route {
    const kind = this.kindRoute(descriptor);
    if (descriptor.kind !== "module" || (this.typeRoutes.size === 0 && this.moduleRoutes.size === 0)) {
      return this.required(kind, descriptor);
    }
    const eventType: unknown = descriptor.eventType;
    const types: readonly unknown[] = Array.isArray(eventType) ? eventType : [eventType];
    const resolved = types.map((type) => ({ type, route: this.typeRoute(type) ?? kind }));
    const first = resolved[0]?.route;
    if (resolved.some(({ route }) => route?.adapter !== first?.adapter)) {
      const each = resolved.map(({ type, route }) => `${String(type)} (${route?.label ?? "no route"})`);
      throw new StreamdError(
        `A module descriptor's event types span routes: ${each.join(", ")}; ` +
          "use one descriptor per route, or a list of descriptors",
      );
    }
    return this.required(first ?? kind, descriptor);
  }

  private typeRoute(type: unknown): Route | undefined {
    const parsed = parseType(type);
    if (parsed === undefined) return undefined;
    const candidates: ["types" | "modules", string][] = [
      ["types", parsed.type],
      ["types", parsed.base],
      ["modules", parsed.module],
    ];
    for (const [map, key] of candidates) {
      const adapter = (map === "types" ? this.typeRoutes : this.moduleRoutes).get(key);
      if (adapter !== undefined) {
        const label = `${map}[${JSON.stringify(key)}]`;
        return { adapter, label, phrase: `route ${label}` };
      }
    }
    return undefined;
  }

  private required(route: Route | undefined, descriptor: EventDescriptor): Route {
    if (route === undefined) {
      throw new StreamdError(`No adapter configured for descriptor kind "${descriptor.kind}"`);
    }
    return route;
  }

  private kindRoute(descriptor: EventDescriptor): Route | undefined {
    let target: EventAdapter | undefined;
    switch (descriptor?.kind) {
      case "module":
        target = this.moduleAdapter;
        break;
      case "handle":
        target = this.handleAdapter;
        break;
      case "account":
        target = this.accountAdapter;
        break;
      case "resource":
        target = this.resourceAdapter;
        break;
      case "object":
        break;
      default: {
        const unknown: never = descriptor;
        throw new StreamdError(
          `Unknown descriptor kind: ${String((unknown as { kind?: unknown } | null)?.kind)}`,
        );
      }
    }
    const phrase = `descriptor kind "${descriptor.kind}"`;
    if (target !== undefined) return { adapter: target, label: descriptor.kind, phrase };
    return this.defaultAdapter === undefined
      ? undefined
      : { adapter: this.defaultAdapter, label: "default", phrase };
  }
}
