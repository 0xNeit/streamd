import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import {
  MemoryAdapter,
  StreamdClient,
  StreamdNetworkError,
  StreamdTimeoutError,
  StreamdTransactionError,
  type EventDescriptor,
  type EventsOfOptions,
  type StreamdEvent,
  type TransactionEvents,
} from "@streamd/core";
import {
  StreamdProvider,
  useTransactionEvents,
  type UseTransactionEventsOptions,
  type UseTransactionEventsResult,
} from "../src/index.js";
import { OTHER, TYPE, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const A = `0x${"a".repeat(64)}`;
const B = `0x${"b".repeat(64)}`;
const FAILED = `0x${"c".repeat(64)}`;
const THROWS = `0x${"d".repeat(64)}`;

function tagged(version: bigint, hash: string, type = TYPE): StreamdEvent {
  return { ...ev(version, type), transactionHash: hash };
}

function setup() {
  const memory = new MemoryAdapter([tagged(1n, A), { ...tagged(1n, A, OTHER), eventIndex: 1 }, tagged(2n, B)]);
  const lookups: string[] = [];
  let gate: Promise<void> | undefined;
  const adapter = {
    fetchEvents: memory.fetchEvents.bind(memory),
    async getTransactionEvents(hash: string): Promise<TransactionEvents | undefined> {
      lookups.push(hash);
      if (hash === THROWS) throw new StreamdNetworkError("rate limited");
      if (hash === FAILED) return { version: 3n, success: false, vmStatus: "Move abort: 0x1", events: [] };
      await gate;
      return memory.getTransactionEvents(hash);
    },
  };
  const client = new StreamdClient({ adapter });
  const signals: AbortSignal[] = [];
  const eventsOf = client.eventsOf.bind(client);
  client.eventsOf = ((h: string, d: EventDescriptor<never> | undefined, o: EventsOfOptions) => {
    signals.push(o.signal!);
    return eventsOf(h, d, o);
  }) as typeof client.eventsOf;
  const results: UseTransactionEventsResult<unknown>[] = [];
  function Tx({ hash, options }: { hash: string | null; options?: UseTransactionEventsOptions }) {
    results.push(useTransactionEvents(hash, { ...options }));
    return null;
  }
  const el = (hash: string | null, options?: UseTransactionEventsOptions) =>
    createElement(StreamdProvider, { client }, createElement(Tx, { hash, options }));
  return {
    client,
    lookups,
    signals,
    results,
    el,
    last: () => results.at(-1)!,
    hold: () => {
      let release!: () => void;
      gate = new Promise<void>((r) => (release = r));
      return () => {
        gate = undefined;
        release();
      };
    },
  };
}

const LOADING = { data: undefined, error: undefined, loading: true, vmStatus: undefined };

describe("useTransactionEvents", () => {
  it("waits, then returns the transaction's events", async () => {
    const t = setup();
    const root = await render(t.el(A));
    expect(t.results[0]).toEqual(LOADING);
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data!.map((e) => e.type)).toEqual([TYPE, OTHER]);
    expect(t.last().error).toBeUndefined();
    await unmount(root);
  });

  it("narrows by descriptor", async () => {
    const t = setup();
    const root = await render(t.el(A, { descriptor: { kind: "module", eventType: OTHER } }));
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data!.map((e) => e.type)).toEqual([OTHER]);
    await unmount(root);
  });

  it("reports a failed transaction with its status", async () => {
    const t = setup();
    const root = await render(t.el(FAILED));
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().error).toBeInstanceOf(StreamdTransactionError);
    expect(t.last().vmStatus).toBe("Move abort: 0x1");
    expect(t.last().data).toBeUndefined();
    await unmount(root);
  });

  it("reports a timeout and a lookup failure without a status", async () => {
    const t = setup();
    const root = await render(t.el(`0x${"e".repeat(64)}`, { timeout: 1 }));
    await waitFor(() => expect(t.last().error).toBeInstanceOf(StreamdTimeoutError));
    expect(t.last().vmStatus).toBeUndefined();
    await rerender(root, t.el(THROWS));
    await waitFor(() => expect(t.last().error).toBeInstanceOf(StreamdNetworkError));
    expect(t.last().vmStatus).toBeUndefined();
    expect(t.last().loading).toBe(false);
    await unmount(root);
  });

  it("never shows the previous result under a new hash", async () => {
    const t = setup();
    const root = await render(t.el(A));
    await waitFor(() => expect(t.last().data).toBeDefined());
    const release = t.hold();
    const before = t.results.length;
    await rerender(root, t.el(B));
    expect(t.results[before]).toEqual(LOADING);
    release();
    await waitFor(() => expect(t.last().data?.map((e) => e.version)).toEqual([2n]));
    await unmount(root);
  });

  it("aborts the wait it replaces and ignores its result", async () => {
    const t = setup();
    const release = t.hold();
    const root = await render(t.el(A));
    await rerender(root, t.el(B));
    expect(t.signals[0]!.aborted).toBe(true);
    release();
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data!.map((e) => e.version)).toEqual([2n]);
    await unmount(root);
  });

  it("does not wait again for equal inline options, and does for different ones", async () => {
    const t = setup();
    const root = await render(t.el(A, { timeout: 500 }));
    await waitFor(() => expect(t.last().loading).toBe(false));
    await rerender(root, t.el(A, { timeout: 500 }));
    expect(t.signals.length).toBe(1);
    await rerender(root, t.el(A, { timeout: 600 }));
    expect(t.signals.length).toBe(2);
    await waitFor(() => expect(t.last().loading).toBe(false));
    await unmount(root);
  });

  it("calls the latest where without waiting again", async () => {
    const t = setup();
    const release = t.hold();
    const root = await render(t.el(A, { where: () => true }));
    await rerender(root, t.el(A, { where: () => false }));
    expect(t.signals.length).toBe(1);
    release();
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data).toEqual([]);
    await unmount(root);
  });

  it("uses a different client's own request, not the previous client's result", async () => {
    const t = setup();
    const other = new StreamdClient({ adapter: new MemoryAdapter([tagged(9n, A)]) });
    function Tx({ client }: { client: StreamdClient }) {
      t.results.push(useTransactionEvents(A, { client }));
      return null;
    }
    const root = await render(createElement(Tx, { client: t.client }));
    await waitFor(() => expect(t.last().data?.length).toBe(2));
    const before = t.results.length;
    await rerender(root, createElement(Tx, { client: other }));
    expect(t.results[before]).toEqual(LOADING);
    await waitFor(() => expect(t.last().data?.map((e) => e.version)).toEqual([9n]));
    await unmount(root);
  });

  it("never reports the abort of StrictMode's first effect", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el(A)));
    await waitFor(() => expect(t.last().data?.length).toBe(2));
    expect(t.results.every((r) => r.error === undefined)).toBe(true);
    await unmount(root);
  });

  it("aborts on unmount", async () => {
    const t = setup();
    t.hold();
    const root = await render(t.el(A));
    await unmount(root);
    expect(t.signals[0]!.aborted).toBe(true);
  });

  it("waits for nothing with a null hash", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toEqual({ ...LOADING, loading: false });
    expect(t.signals.length).toBe(0);
    await unmount(root);
  });

  it("looks up nothing during server rendering", () => {
    const t = setup();
    renderToString(t.el(A));
    expect(t.lookups.length).toBe(0);
    expect(t.last().loading).toBe(true);
  });
});
