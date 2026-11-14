import { describe, expect, it } from "bun:test";
import { withTracing, type SpanAttributes, type SpanLike, type TracerLike } from "../src/adapters/tracing.js";
import { CachingAdapter } from "../src/adapters/caching.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { EventAdapter, EventDescriptor, EventPage, StreamdEvent, ViewRequest } from "../src/types.js";

const PING = "0x1::test::Ping";
const descriptor: EventDescriptor = { kind: "module", eventType: PING };

function ev(version: number, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: PING, data: {}, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0, ...extra };
}

interface Recorded {
  name: string;
  attributes: SpanAttributes;
  statuses: { code: number; message?: string }[];
  exceptions: (Error | string)[];
  ends: number;
}

function recordingTracer() {
  const spans: Recorded[] = [];
  const tracer: TracerLike = {
    startSpan(name, options) {
      const span: Recorded = { name, attributes: { ...options?.attributes }, statuses: [], exceptions: [], ends: 0 };
      spans.push(span);
      return {
        setAttributes: (attributes) => Object.assign(span.attributes, attributes),
        setStatus: (status) => span.statuses.push(status),
        recordException: (exception) => span.exceptions.push(exception),
        end: () => span.ends++,
      };
    },
  };
  return { tracer, spans };
}

describe("withTracing", () => {
  it("opens one span per call, named for the method, ended once with status unset", async () => {
    const { tracer, spans } = recordingTracer();
    const adapter = withTracing(new MemoryAdapter([ev(1), ev(2), ev(3)]), tracer);
    const page = await adapter.fetchEvents(descriptor, { fromVersion: 0n, limit: 2 });
    expect(page.events).toHaveLength(2);
    await adapter.getHeadVersion!(descriptor);
    expect(spans.map((s) => s.name)).toEqual(["streamd.fetchEvents", "streamd.getHeadVersion"]);
    expect(spans.every((s) => s.ends === 1 && s.statuses.length === 0 && s.exceptions.length === 0)).toBe(true);
    expect(spans[0]!.attributes).toEqual({
      "streamd.descriptor.kind": "module",
      "streamd.event_type": PING,
      "streamd.limit": 2,
      "streamd.cursor_present": false,
      "streamd.event_count": 2,
      "streamd.has_more": true,
      "streamd.final": false,
    });
    expect(spans[1]!.attributes).toEqual({ "streamd.descriptor.kind": "module", "streamd.event_type": PING });
  });

  it("names each descriptor kind's fields, never the cursor's value", async () => {
    const { tracer, spans } = recordingTracer();
    const adapter = withTracing({ fetchEvents: async () => ({ events: [], hasMore: false, final: true }) }, tracer);
    const descriptors: EventDescriptor[] = [
      { kind: "module", eventType: [PING, "0x1::test::Pong"] },
      { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::m::S", fieldName: "f" },
      { kind: "handle", accountAddress: "0x1", creationNumber: 7n },
      { kind: "account", accountAddress: "0x2", eventTypes: [PING] },
      { kind: "resource", accountAddress: "0x3", resourceType: "0x1::m::R" },
      { kind: "object", objectId: "0x4" },
    ];
    for (const d of descriptors) await adapter.fetchEvents(d, { cursor: "secret-cursor" });
    const requested = spans.map(({ attributes }) => {
      const { "streamd.event_count": _c, "streamd.has_more": _h, "streamd.final": final, ...rest } = attributes;
      expect(final).toBe(true);
      return rest;
    });
    const cursor = { "streamd.cursor_present": true };
    expect(requested).toEqual([
      { "streamd.descriptor.kind": "module", "streamd.event_type": [PING, "0x1::test::Pong"], ...cursor },
      { "streamd.descriptor.kind": "handle", "streamd.account": "0x1", "streamd.handle.struct": "0x1::m::S", "streamd.handle.field": "f", ...cursor },
      { "streamd.descriptor.kind": "handle", "streamd.account": "0x1", "streamd.handle.creation_number": "7", ...cursor },
      { "streamd.descriptor.kind": "account", "streamd.account": "0x2", "streamd.event_type": [PING], ...cursor },
      { "streamd.descriptor.kind": "resource", "streamd.account": "0x3", "streamd.resource_type": "0x1::m::R", ...cursor },
      { "streamd.descriptor.kind": "object", "streamd.object_id": "0x4", ...cursor },
    ]);
    expect(JSON.stringify(spans)).not.toContain("secret-cursor");
  });

  it("names each optional method's arguments and results, and passes them through", async () => {
    const { tracer, spans } = recordingTracer();
    const seen: unknown[][] = [];
    const tx = { version: 9n, success: false, vmStatus: "abort", events: [ev(9)] };
    const source: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      getEarliestVersion: async () => 1n,
      getBlock: async (...args) => (seen.push(args), { height: 5n, firstVersion: 1n, lastVersion: 2n, timestamp: 0n }),
      getBlockHeights: async () => ({ oldest: 0n, newest: 9n }),
      getTransactionEvents: async (hash) => (hash === "0xfound" ? tx : undefined),
      getTransactionEventsByVersion: async () => tx,
      simulateTransactionEvents: async () => ({ ...tx, success: true }),
      view: async (...args) => (seen.push(args), ["state"]),
      readObjects: async () => [null],
    };
    const adapter = withTracing(source, tracer);
    expect(await adapter.getEarliestVersion!(descriptor)).toBe(1n);
    await adapter.getBlock!(descriptor, 5n);
    await adapter.getBlockHeights!(descriptor);
    expect(await adapter.getTransactionEvents!("0xfound")).toBe(tx);
    expect(await adapter.getTransactionEvents!("0xmissing", descriptor)).toBeUndefined();
    await adapter.getTransactionEventsByVersion!(9n);
    await adapter.simulateTransactionEvents!({ transaction: { secret: "payload" } });
    const request: ViewRequest = { function: "0x1::m::f", functionArguments: ["secret-arg"] };
    expect(await adapter.view!(request, 8n, descriptor)).toEqual(["state"]);
    await adapter.readObjects!({ objects: ["0x5"] }, { version: 3n });
    expect(seen).toEqual([[descriptor, 5n], [request, 8n, descriptor]]);
    const kind = { "streamd.descriptor.kind": "module", "streamd.event_type": PING };
    expect(spans.map((s) => [s.name, s.attributes])).toEqual([
      ["streamd.getEarliestVersion", kind],
      ["streamd.getBlock", { ...kind, "streamd.block_height": "5" }],
      ["streamd.getBlockHeights", kind],
      ["streamd.getTransactionEvents", { "streamd.transaction_hash": "0xfound", "streamd.found": true, "streamd.event_count": 1, "streamd.success": false }],
      ["streamd.getTransactionEvents", { ...kind, "streamd.transaction_hash": "0xmissing", "streamd.found": false }],
      ["streamd.getTransactionEventsByVersion", { "streamd.version": "9", "streamd.found": true, "streamd.event_count": 1, "streamd.success": false }],
      ["streamd.simulateTransactionEvents", { "streamd.event_count": 1, "streamd.success": true }],
      ["streamd.view", { ...kind, "streamd.view.function": "0x1::m::f", "streamd.version": "8" }],
      ["streamd.readObjects", { "streamd.version": "3" }],
    ]);
    expect(JSON.stringify(spans)).not.toContain("secret");
  });

  it("records a failure on the span and rethrows it unchanged", async () => {
    const failures: unknown[] = [
      new StreamdNetworkError("limited", 429),
      new StreamdNetworkError("hinted", 503, undefined, 100),
      new StreamdNetworkError("down"),
      new TypeError("bug"),
      "a string",
    ];
    const { tracer, spans } = recordingTracer();
    const adapter = withTracing({ fetchEvents: async () => Promise.reject(failures[spans.length - 1]) }, tracer);
    for (const failure of failures) {
      await expect(adapter.fetchEvents(descriptor, {})).rejects.toBe(failure);
    }
    const base = { "streamd.descriptor.kind": "module", "streamd.event_type": PING, "streamd.cursor_present": false };
    expect(spans.map((s) => s.attributes)).toEqual([
      { ...base, "error.type": "StreamdNetworkError", "streamd.rate_limited": true, "http.response.status_code": 429 },
      { ...base, "error.type": "StreamdNetworkError", "streamd.rate_limited": true, "http.response.status_code": 503 },
      { ...base, "error.type": "StreamdNetworkError", "streamd.rate_limited": false },
      { ...base, "error.type": "TypeError", "streamd.rate_limited": false },
      { ...base, "error.type": "_OTHER", "streamd.rate_limited": false },
    ]);
    expect(spans.map((s) => s.statuses)).toEqual([
      [{ code: 2, message: "limited" }],
      [{ code: 2, message: "hinted" }],
      [{ code: 2, message: "down" }],
      [{ code: 2, message: "bug" }],
      [{ code: 2, message: "a string" }],
    ]);
    expect(spans.map((s) => s.exceptions)).toEqual([...failures.slice(0, 4).map((f) => [f as Error]), ["a string"]]);
    expect(spans.every((s) => s.ends === 1)).toBe(true);
  });

  it("traces a method that throws synchronously", async () => {
    const { tracer, spans } = recordingTracer();
    const boom = new Error("sync");
    const adapter = withTracing({ fetchEvents: () => { throw boom; } }, tracer);
    await expect(adapter.fetchEvents(descriptor, {})).rejects.toBe(boom);
    expect(spans[0]).toMatchObject({ statuses: [{ code: 2, message: "sync" }], exceptions: [boom], ends: 1 });
  });

  it("ends a span only once the call settles", async () => {
    const { tracer, spans } = recordingTracer();
    let finish = () => {};
    const source: EventAdapter = {
      fetchEvents: () => new Promise((resolve) => (finish = () => resolve({ events: [], hasMore: false }))),
    };
    const pending = withTracing(source, tracer).fetchEvents(descriptor, {});
    expect(spans[0]!.ends).toBe(0);
    finish();
    await pending;
    expect(spans[0]!.ends).toBe(1);
  });

  it("wraps optional methods only when the adapter has them, bound to it", async () => {
    const { tracer } = recordingTracer();
    const bare = withTracing({ fetchEvents: async () => ({ events: [], hasMore: false }) }, tracer);
    expect(Object.keys(bare)).toEqual(["fetchEvents"]);
    expect(typeof bare.view).toBe("undefined");
    class Stateful extends MemoryAdapter {}
    const memory = withTracing(new Stateful([ev(1)]), tracer);
    expect(typeof memory.getHeadVersion).toBe("function");
    expect(memory.view).toBeUndefined();
    expect(await memory.getHeadVersion!(descriptor)).toBe(1n);
  });

  it("never fails a call over a tracer fault", async () => {
    const memory = new MemoryAdapter([ev(1)]);
    const throwingStart: TracerLike = {
      startSpan: () => {
        throw new Error("tracer down");
      },
    };
    const noSpan = { startSpan: () => undefined } as unknown as TracerLike;
    const brokenSpan: SpanLike = {
      setAttributes: () => {
        throw new Error("x");
      },
      setStatus: () => {
        throw new Error("x");
      },
      recordException: () => {
        throw new Error("x");
      },
      end: () => {
        throw new Error("x");
      },
    };
    const throwingSpan: TracerLike = { startSpan: () => brokenSpan };
    const reject = async () => Promise.reject(new Error("async tracer bug"));
    const rejectingSpan: TracerLike = {
      startSpan: () => ({ setAttributes: reject, setStatus: reject, recordException: reject, end: reject }),
    };
    const asyncStart = { startSpan: reject } as unknown as TracerLike;
    const oddSpans = [42, {}].map((span) => ({ startSpan: () => span }) as unknown as TracerLike);
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    for (const tracer of [throwingStart, noSpan, throwingSpan, rejectingSpan, asyncStart, ...oddSpans]) {
      const adapter = withTracing(memory, tracer);
      expect((await adapter.fetchEvents(descriptor, { fromVersion: 0n })).events).toHaveLength(1);
      const boom = new StreamdError("boom");
      const failing = withTracing({ fetchEvents: async () => Promise.reject(boom) }, tracer);
      await expect(failing.fetchEvents(descriptor, {})).rejects.toBe(boom);
    }
    await new Promise((r) => setTimeout(r, 0));
    process.off("unhandledRejection", onUnhandled);
    expect(unhandled).toEqual([]);
  });

  it("keeps the descriptor's attributes when another argument is malformed", async () => {
    const { tracer, spans } = recordingTracer();
    const adapter = withTracing({ ...new MemoryAdapter(), fetchEvents: async () => ({ events: [], hasMore: false }), view: async () => [], readObjects: async () => [] }, tracer);
    await adapter.view!(null as never, 3n, descriptor);
    await adapter.readObjects!({ objects: [] }, null as never, descriptor);
    const kind = { "streamd.descriptor.kind": "module", "streamd.event_type": PING };
    expect(spans.map((s) => s.attributes)).toEqual([{ ...kind, "streamd.version": "3" }, kind]);
  });

  it("still traces a malformed descriptor or page, which pass through", async () => {
    const { tracer, spans } = recordingTracer();
    const page = { events: undefined, hasMore: false } as unknown as EventPage;
    const adapter = withTracing({ fetchEvents: async () => page }, tracer);
    expect(await adapter.fetchEvents(null as never, undefined as never)).toBe(page);
    expect(spans[0]).toMatchObject({ name: "streamd.fetchEvents", attributes: { "streamd.cursor_present": false }, ends: 1 });
    const strange = { kind: 1, eventType: { not: "a string" } } as unknown as EventDescriptor;
    await adapter.fetchEvents(strange, {});
    expect(spans[1]!.attributes).toEqual({ "streamd.cursor_present": false });
  });

  it("refuses a non-adapter or a tracer without startSpan", () => {
    const { tracer } = recordingTracer();
    expect(() => withTracing({} as never, tracer)).toThrow(StreamdError);
    for (const bad of [undefined, null, {}, { startSpan: "no" }]) {
      expect(() => withTracing(new MemoryAdapter(), bad as never)).toThrow(StreamdError);
    }
  });

  it("opens a span per adapter call under a client", async () => {
    const { tracer, spans } = recordingTracer();
    const client = new StreamdClient({ adapter: withTracing(new MemoryAdapter([ev(1), ev(2)]), tracer) });
    const events = await client.getEvents(descriptor, { fromVersion: 0n });
    expect(events).toHaveLength(2);
    expect(spans.length).toBeGreaterThan(0);
    expect(spans.every((s) => s.name === "streamd.fetchEvents" && s.ends === 1)).toBe(true);
    client.destroy();
  });

  it("makes a span for a cache hit outside CachingAdapter, and none inside it", async () => {
    const final: EventAdapter = { fetchEvents: async () => ({ events: [ev(1)], hasMore: false, final: true }) };
    const outside = recordingTracer();
    const cachedThenTraced = withTracing(new CachingAdapter(final), outside.tracer);
    await cachedThenTraced.fetchEvents(descriptor, { fromVersion: 1n });
    await cachedThenTraced.fetchEvents(descriptor, { fromVersion: 1n });
    expect(outside.spans).toHaveLength(2);

    const inside = recordingTracer();
    const tracedThenCached = new CachingAdapter(withTracing(final, inside.tracer));
    await tracedThenCached.fetchEvents(descriptor, { fromVersion: 1n });
    await tracedThenCached.fetchEvents(descriptor, { fromVersion: 1n });
    expect(inside.spans).toHaveLength(1);
  });
});

// The shape of `@opentelemetry/api`'s Tracer and Span, which is not installed.
enum SpanStatusCode {
  UNSET = 0,
  OK = 1,
  ERROR = 2,
}
type AttributeValue = string | number | boolean | (string | null | undefined)[] | (number | null | undefined)[] | (boolean | null | undefined)[];
interface Attributes {
  [key: string]: AttributeValue | undefined;
}
interface OtelSpan {
  spanContext(): { traceId: string; spanId: string; traceFlags: number };
  setAttribute(key: string, value: AttributeValue): this;
  setAttributes(attributes: Attributes): this;
  addEvent(name: string, attributes?: Attributes): this;
  setStatus(status: { code: SpanStatusCode; message?: string }): this;
  updateName(name: string): this;
  end(endTime?: number | Date): void;
  isRecording(): boolean;
  recordException(exception: Error | string | { message: string }, time?: number | Date): void;
}
interface OtelTracer {
  startSpan(name: string, options?: { kind?: number; attributes?: Attributes; root?: boolean }, context?: object): OtelSpan;
  startActiveSpan<F extends (span: OtelSpan) => unknown>(name: string, fn: F): ReturnType<F>;
}

describe("TracerLike", () => {
  it("is satisfied by an OpenTelemetry-shaped tracer", () => {
    const otel = {} as OtelTracer;
    const tracer: TracerLike = otel;
    expect(tracer).toBe(otel);
  });
});
