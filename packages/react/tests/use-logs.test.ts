import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { StreamdClient, StreamdNetworkError, type EventFilter, type MoveEventType } from "@streamd/core";
import { StreamdProvider, useLogs, type UseLogsOptions, type UseLogsResult } from "../src/index.js";
import { OTHER, TYPE, TestAdapter, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

function setup(events = [ev(1n), ev(2n), ev(3n, OTHER)]) {
  const adapter = new TestAdapter(events);
  const client = new StreamdClient({ adapter });
  const signals: AbortSignal[] = [];
  const getEvents = client.getEvents.bind(client);
  client.getEvents = ((d: never, f?: EventFilter) => {
    signals.push(f!.signal!);
    return getEvents(d, f);
  }) as typeof client.getEvents;
  const results: UseLogsResult<unknown>[] = [];
  // Inline descriptor and options on purpose: new objects every render.
  function Logs({ type, options }: { type: MoveEventType | null; options?: UseLogsOptions }) {
    results.push(useLogs(type === null ? null : { kind: "module", eventType: type }, { ...options }));
    return null;
  }
  const el = (type: MoveEventType | null, options?: UseLogsOptions) =>
    createElement(StreamdProvider, { client }, createElement(Logs, { type, options }));
  return { adapter, client, signals, results, el, last: () => results.at(-1)! };
}

describe("useLogs", () => {
  it("loads the matching events", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    expect(t.results[0]).toEqual({ data: undefined, error: undefined, loading: true });
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data!.map((e) => e.version)).toEqual([1n, 2n]);
    expect(t.last().error).toBeUndefined();
    await unmount(root);
  });

  it("does not refetch for an equal inline descriptor, and does for a different one", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { limit: 5 }));
    await waitFor(() => expect(t.last().loading).toBe(false));
    await rerender(root, t.el(TYPE, { limit: 5 }));
    expect(t.signals.length).toBe(1);

    await rerender(root, t.el(OTHER, { limit: 5 }));
    expect(t.signals.length).toBe(2);
    expect(t.signals[0]!.aborted).toBe(true);
    await waitFor(() => expect(t.last().data?.map((e) => e.version)).toEqual([3n]));
    await unmount(root);
  });

  it("aborts the replaced request and never shows its result", async () => {
    const t = setup();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const fetchEvents = t.adapter.fetchEvents.bind(t.adapter);
    let calls = 0;
    t.adapter.fetchEvents = async (d, o) => {
      if (calls++ === 0) await gate;
      return fetchEvents(d, o);
    };
    const root = await render(t.el(TYPE));
    await rerender(root, t.el(OTHER));
    expect(t.signals[0]!.aborted).toBe(true);
    await waitFor(() => expect(t.last().data?.map((e) => e.version)).toEqual([3n]));
    release();
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data!.map((e) => e.version)).toEqual([3n]);
    await unmount(root);
  });

  it("reports an error and keeps the previous data", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await waitFor(() => expect(t.last().data).toBeDefined());
    t.adapter.failing = true;
    await rerender(root, t.el(TYPE, { limit: 1 }));
    await waitFor(() => expect(t.last().error).toBeInstanceOf(StreamdNetworkError));
    expect(t.last().loading).toBe(false);
    expect(t.last().data!.map((e) => e.version)).toEqual([1n, 2n]);

    t.adapter.failing = false;
    await rerender(root, t.el(TYPE, { limit: 2 }));
    await waitFor(() => expect(t.last().error).toBeUndefined());
    expect(t.last().data!.map((e) => e.version)).toEqual([1n, 2n]);
    await unmount(root);
  });

  it("calls the latest where without refetching", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { where: () => true }));
    await waitFor(() => expect(t.last().loading).toBe(false));
    await rerender(root, t.el(TYPE, { where: () => false }));
    expect(t.signals.length).toBe(1);
    await unmount(root);
  });

  it("aborts on unmount", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await unmount(root);
    expect(t.signals[0]!.aborted).toBe(true);
  });

  it("fetches nothing for a null descriptor", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toEqual({ data: undefined, error: undefined, loading: false });
    expect(t.signals.length).toBe(0);
    await unmount(root);
  });

  it("fetches nothing during server rendering", () => {
    const t = setup();
    renderToString(t.el(TYPE));
    expect(t.adapter.fetches).toBe(0);
    expect(t.signals.length).toBe(0);
    expect(t.last().loading).toBe(true);
  });
});
