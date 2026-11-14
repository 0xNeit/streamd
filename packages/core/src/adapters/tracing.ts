import type { EventAdapter, EventPage, FetchOptions, TransactionEvents } from "../types.js";
import { StreamdError, StreamdNetworkError } from "../errors.js";
import { METHODS, isRateLimited, type AdapterMethod } from "./metrics.js";
import { isObject, requireAdapter } from "./validate.js";

export type SpanAttributes = Record<string, string | number | boolean | string[]>;

/** The part of an OpenTelemetry `Span` that `withTracing` calls. */
export interface SpanLike {
  setAttributes(attributes: SpanAttributes): unknown;
  setStatus(status: { code: number; message?: string }): unknown;
  recordException(exception: Error | string): unknown;
  end(): unknown;
}

/**
 * The part of an OpenTelemetry `Tracer` that `withTracing` calls, declared
 * structurally so `trace.getTracer(name)` satisfies it without streamd
 * depending on `@opentelemetry/api`.
 */
export interface TracerLike {
  startSpan(name: string, options?: { attributes?: SpanAttributes }): SpanLike;
}

const STATUS_ERROR = 2;

const DESCRIPTOR_AT: Record<AdapterMethod, number> = {
  fetchEvents: 0,
  getHeadVersion: 0,
  getEarliestVersion: 0,
  getBlock: 0,
  getBlockHeights: 0,
  getTransactionEvents: 1,
  getTransactionEventsByVersion: 1,
  simulateTransactionEvents: 1,
  view: 2,
  readObjects: 2,
};

const DESCRIPTOR_FIELDS: Record<string, string> = {
  eventType: "streamd.event_type",
  eventTypes: "streamd.event_type",
  accountAddress: "streamd.account",
  eventHandleStruct: "streamd.handle.struct",
  fieldName: "streamd.handle.field",
  creationNumber: "streamd.handle.creation_number",
  resourceType: "streamd.resource_type",
  objectId: "streamd.object_id",
};

function attributeValue(value: unknown): string | string[] | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "bigint") return String(value);
  if (Array.isArray(value) && value.every((v) => typeof v === "string")) return [...value];
  return undefined;
}

function put(out: SpanAttributes, key: string, value: unknown): void {
  const attribute = attributeValue(value);
  if (attribute !== undefined) out[key] = attribute;
}

function requestAttributes(method: AdapterMethod, args: unknown[]): SpanAttributes {
  const out: SpanAttributes = {};
  const descriptor = args[DESCRIPTOR_AT[method]];
  if (isObject(descriptor)) {
    put(out, "streamd.descriptor.kind", descriptor.kind);
    for (const [field, key] of Object.entries(DESCRIPTOR_FIELDS)) put(out, key, descriptor[field]);
  }
  if (method === "fetchEvents") {
    const options = (args[1] ?? {}) as FetchOptions;
    if (typeof options.limit === "number") out["streamd.limit"] = options.limit;
    out["streamd.cursor_present"] = options.cursor !== undefined;
  } else if (method === "getBlock") {
    put(out, "streamd.block_height", args[1]);
  } else if (method === "getTransactionEvents") {
    put(out, "streamd.transaction_hash", args[0]);
  } else if (method === "getTransactionEventsByVersion") {
    put(out, "streamd.version", args[0]);
  } else if (method === "view") {
    put(out, "streamd.view.function", (args[0] as { function?: unknown }).function);
    put(out, "streamd.version", args[1]);
  } else if (method === "readObjects") {
    put(out, "streamd.version", (args[1] as { version?: unknown }).version);
  }
  return out;
}

function resultAttributes(method: AdapterMethod, result: unknown): SpanAttributes {
  if (method === "fetchEvents") {
    const page = result as EventPage;
    return {
      "streamd.event_count": page.events.length,
      "streamd.has_more": page.hasMore,
      "streamd.final": page.final === true,
    };
  }
  if (method === "getTransactionEvents" || method === "getTransactionEventsByVersion") {
    if (result === undefined) return { "streamd.found": false };
    const tx = result as TransactionEvents;
    return { "streamd.found": true, "streamd.event_count": tx.events.length, "streamd.success": tx.success };
  }
  if (method === "simulateTransactionEvents") {
    const tx = result as TransactionEvents;
    return { "streamd.event_count": tx.events.length, "streamd.success": tx.success };
  }
  return {};
}

function errorAttributes(err: unknown): SpanAttributes {
  const out: SpanAttributes = {
    "error.type": err instanceof Error ? err.name : "_OTHER",
    "streamd.rate_limited": isRateLimited(err),
  };
  if (err instanceof StreamdNetworkError && err.statusCode !== undefined) {
    out["http.response.status_code"] = err.statusCode;
  }
  return out;
}

// A tracer bug must never fail the call it observes.
function safely(action: () => unknown): void {
  try {
    action();
  } catch {}
}

function startSpan(tracer: TracerLike, method: AdapterMethod, args: unknown[]): SpanLike | undefined {
  let attributes: SpanAttributes = {};
  safely(() => (attributes = requestAttributes(method, args)));
  try {
    const span = tracer.startSpan(`streamd.${method}`, { attributes });
    return isObject(span) ? span : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Wraps `adapter` so every call opens one span on `tracer`, named
 * `streamd.<method>`, ended once the call settles. Calls, results and errors
 * pass through unchanged, and a method the adapter lacks stays absent.
 */
export function withTracing(adapter: EventAdapter, tracer: TracerLike): EventAdapter {
  const source = requireAdapter(adapter, "withTracing");
  if (!isObject(tracer) || typeof tracer.startSpan !== "function") {
    throw new StreamdError("withTracing requires a tracer with a startSpan method");
  }

  const wrapped = {} as EventAdapter;
  for (const method of METHODS) {
    if (typeof source[method] !== "function") continue;
    const call = (source[method] as (...args: unknown[]) => Promise<unknown>).bind(source);
    Object.assign(wrapped, {
      [method]: async (...args: unknown[]) => {
        const span = startSpan(tracer, method, args);
        if (span === undefined) return call(...args);
        try {
          const result = await call(...args);
          safely(() => span.setAttributes(resultAttributes(method, result)));
          return result;
        } catch (err) {
          safely(() => span.setAttributes(errorAttributes(err)));
          safely(() => span.recordException(err instanceof Error ? err : String(err)));
          safely(() => span.setStatus({ code: STATUS_ERROR, message: err instanceof Error ? err.message : String(err) }));
          throw err;
        } finally {
          safely(() => span.end());
        }
      },
    });
  }
  return wrapped;
}
