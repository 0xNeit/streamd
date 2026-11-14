import "./dom.js";
import { describe, expect, it } from "bun:test";
import { act, createElement } from "react";
import { renderToString } from "react-dom/server";
import { StreamdClient, StreamdNetworkError, type EventFilter, type MoveEventType } from "@streamd/core";
import { StreamdProvider, useLogs, type UseLogsOptions, type UseLogsResult } from "../src/index.js";
import { OTHER, TYPE, TestAdapter, ev } from "./fixtures.js";
import type { StreamdEvent } from "@streamd/core";
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
    expect(t.results[0]).toMatchObject({ data: undefined, error: undefined, loading: true });
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
    expect(t.last()).toMatchObject({ data: undefined, error: undefined, loading: false });
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

  describe("refetch", () => {
    const versions = (r: UseLogsResult<unknown>) => r.data?.map((e) => e.version);

    /** Holds the adapter's next fetch on a gate, answering from the events held when it was made. */
    function holdNext(adapter: TestAdapter) {
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const fetchEvents = adapter.fetchEvents.bind(adapter);
      let held = true;
      adapter.fetchEvents = async (d, o) => {
        if (!held) return fetchEvents(d, o);
        held = false;
        const snapshot: StreamdEvent[] = [...adapter.events];
        await gate;
        const saved = adapter.events;
        adapter.events = snapshot;
        try {
          return await fetchEvents(d, o);
        } finally {
          adapter.events = saved;
        }
      };
      return release;
    }

    it("picks up events added since, and moves a relative window to the new head", async () => {
      const t = setup([ev(1n), ev(2n)]);
      const root = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      t.adapter.events.push(ev(4n));
      await act(() => t.last().refetch());
      expect(versions(t.last())).toEqual([1n, 2n, 4n]);
      await unmount(root);

      const w = setup([ev(1n), ev(2n)]);
      const wroot = await render(w.el(TYPE, { fromVersion: -1n }));
      await waitFor(() => expect(w.last().loading).toBe(false));
      expect(versions(w.last())).toEqual([1n, 2n]);
      w.adapter.events.push(ev(5n), ev(6n));
      await act(() => w.last().refetch());
      expect(versions(w.last())).toEqual([5n, 6n]);
      await unmount(wroot);
    });

    it("keeps the previous data while loading", async () => {
      const t = setup([ev(1n), ev(2n)]);
      const root = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      t.adapter.events.push(ev(4n));
      const release = holdNext(t.adapter);
      let done!: Promise<void>;
      await act(async () => {
        done = t.last().refetch();
      });
      expect(t.last()).toMatchObject({ loading: true, error: undefined });
      expect(versions(t.last())).toEqual([1n, 2n]);
      release();
      await act(() => done);
      expect(t.last().loading).toBe(false);
      expect(versions(t.last())).toEqual([1n, 2n, 4n]);
      await unmount(root);
    });

    it("is the same function across renders and input changes", async () => {
      const t = setup();
      const root = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      const first = t.results[0]!.refetch;
      await rerender(root, t.el(TYPE, { limit: 1 }));
      await rerender(root, t.el(OTHER));
      await waitFor(() => expect(versions(t.last())).toEqual([3n]));
      expect(new Set(t.results.map((r) => r.refetch))).toEqual(new Set([first]));
      await act(() => first());
      expect(t.signals.length).toBe(4);
      expect(versions(t.last())).toEqual([3n]);
      await unmount(root);
    });

    it("aborts an earlier refetch, and only the latest sets state", async () => {
      const t = setup([ev(1n), ev(2n)]);
      const root = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      const release = holdNext(t.adapter);
      let older!: Promise<void>;
      await act(async () => {
        older = t.last().refetch();
      });
      t.adapter.events.push(ev(4n));
      await act(() => t.last().refetch());
      expect(t.signals[1]!.aborted).toBe(true);
      expect(t.signals[2]!.aborted).toBe(false);
      expect(versions(t.last())).toEqual([1n, 2n, 4n]);
      const renders = t.results.length;
      release();
      await act(() => older);
      expect(t.results.length).toBe(renders);
      expect(versions(t.last())).toEqual([1n, 2n, 4n]);
      expect(t.last().loading).toBe(false);
      await unmount(root);
    });

    it("keeps data on a failed refetch, then clears the error on a successful one", async () => {
      const t = setup([ev(1n), ev(2n)]);
      const root = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      t.adapter.failing = true;
      await act(() => t.last().refetch());
      expect(t.last().error).toBeInstanceOf(StreamdNetworkError);
      expect(t.last().loading).toBe(false);
      expect(versions(t.last())).toEqual([1n, 2n]);

      t.adapter.failing = false;
      t.adapter.events.push(ev(4n));
      await act(() => t.last().refetch());
      expect(t.last().error).toBeUndefined();
      expect(versions(t.last())).toEqual([1n, 2n, 4n]);
      await unmount(root);
    });

    it("does nothing with no descriptor, after unmount, or during server rendering", async () => {
      const idle = setup();
      const root = await render(idle.el(null));
      await act(() => idle.last().refetch());
      expect(idle.signals.length).toBe(0);
      expect(idle.last()).toMatchObject({ data: undefined, loading: false });
      await unmount(root);

      const t = setup();
      const mounted = await render(t.el(TYPE));
      await waitFor(() => expect(t.last().loading).toBe(false));
      const { refetch } = t.last();
      const renders = t.results.length;
      await unmount(mounted);
      await refetch();
      expect(t.signals.length).toBe(1);
      expect(t.results.length).toBe(renders);

      const s = setup();
      renderToString(s.el(TYPE));
      await s.last().refetch();
      expect(s.adapter.fetches).toBe(0);
      expect(s.signals.length).toBe(0);
    });
  });
});
