import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, act, createElement } from "react";
import { renderToString } from "react-dom/server";
import { StreamdClient, StreamdNetworkError, type EventFilter, type MoveEventType, type StreamdEvent } from "@streamd/core";
import { StreamdProvider, useEventPages, type UseEventPagesOptions, type UseEventPagesResult } from "../src/index.js";
import { OTHER, TYPE, TestAdapter, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const FIVE = [ev(1n), ev(2n), ev(3n), ev(4n), ev(5n), ev(6n, OTHER)];

function setup(events = FIVE) {
  const adapter = new TestAdapter(events);
  const client = new StreamdClient({ adapter });
  const signals: AbortSignal[] = [];
  const filters: EventFilter[] = [];
  const getEventPages = client.getEventPages.bind(client);
  client.getEventPages = ((d: never, f?: EventFilter, size?: number) => {
    signals.push(f!.signal!);
    filters.push(f!);
    return getEventPages(d, f, size);
  }) as typeof client.getEventPages;
  const results: UseEventPagesResult<unknown>[] = [];
  // Inline descriptor and options on purpose: new objects every render.
  function Pages({ type, options }: { type: MoveEventType | null; options?: UseEventPagesOptions }) {
    results.push(useEventPages(type === null ? null : { kind: "module", eventType: type }, { pageSize: 2, ...options }));
    return null;
  }
  const el = (type: MoveEventType | null, options?: UseEventPagesOptions) =>
    createElement(StreamdProvider, { client }, createElement(Pages, { type, options }));
  return { adapter, client, signals, filters, results, el, last: () => results.at(-1)! };
}

const versions = (r: UseEventPagesResult<unknown>) => r.pages.map((p) => p.map((e) => e.version));
const settled = (t: ReturnType<typeof setup>) => waitFor(() => expect(t.last().loading).toBe(false));

/** Holds the adapter's next fetch until the returned function is called. */
function holdNext(adapter: TestAdapter) {
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const fetchEvents = adapter.fetchEvents.bind(adapter);
  adapter.fetchEvents = async (d, o) => {
    adapter.fetchEvents = fetchEvents;
    await gate;
    return fetchEvents(d, o);
  };
  return release;
}

describe("useEventPages", () => {
  it("loads the first page on mount and appends one page per loadMore", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    expect(t.results[0]).toMatchObject({ pages: [], hasMore: true, loading: true, error: undefined });
    await settled(t);
    expect(versions(t.last())).toEqual([[1n, 2n]]);
    await act(() => t.last().loadMore());
    expect(versions(t.last())).toEqual([[1n, 2n], [3n, 4n]]);
    await act(() => t.last().loadMore());
    expect(versions(t.last())).toEqual([[1n, 2n], [3n, 4n], [5n]]);
    expect(t.last()).toMatchObject({ hasMore: true, loading: false, error: undefined });
    await act(() => t.last().loadMore());
    expect(versions(t.last()).length).toBe(3);
    expect(t.last().hasMore).toBe(false);
    const fetches = t.adapter.fetches;
    await act(() => t.last().loadMore());
    expect(t.adapter.fetches).toBe(fetches);
    expect(t.signals.length).toBe(1);
    await unmount(root);
  });

  it("ends at once on an empty range", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { fromVersion: 100n }));
    await settled(t);
    expect(t.last()).toMatchObject({ pages: [], hasMore: false, error: undefined });
    await unmount(root);
  });

  it("pages newest first under desc, and counts limit across pages", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { orderBy: "desc" }));
    await settled(t);
    await act(() => t.last().loadMore());
    await act(() => t.last().loadMore());
    expect(versions(t.last())).toEqual([[5n, 4n], [3n, 2n], [1n]]);
    await unmount(root);

    const l = setup();
    const lroot = await render(l.el(TYPE, { limit: 3 }));
    await settled(l);
    await act(() => l.last().loadMore());
    await act(() => l.last().loadMore());
    expect(versions(l.last())).toEqual([[1n, 2n], [3n]]);
    expect(l.last().hasMore).toBe(false);
    await unmount(lroot);
  });

  it("starts nothing while a page is in flight", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await settled(t);
    const release = holdNext(t.adapter);
    let first!: Promise<void>;
    await act(async () => {
      first = t.last().loadMore();
    });
    expect(t.last().loading).toBe(true);
    const fetches = t.adapter.fetches;
    await act(() => t.last().loadMore());
    expect(t.adapter.fetches).toBe(fetches);
    release();
    await act(() => first);
    expect(versions(t.last())).toEqual([[1n, 2n], [3n, 4n]]);
    await unmount(root);
  });

  it("keeps one loadMore across renders and a changed descriptor", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await settled(t);
    const first = t.results[0]!.loadMore;
    await rerender(root, t.el(OTHER));
    await waitFor(() => expect(versions(t.last())).toEqual([[6n]]));
    expect(new Set(t.results.map((r) => r.loadMore))).toEqual(new Set([first]));
    await act(() => first());
    expect(t.last().hasMore).toBe(false);
    await unmount(root);
  });

  it("resets in the render that changes the descriptor, aborting the page in flight", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await settled(t);
    await act(() => t.last().loadMore());
    const release = holdNext(t.adapter);
    let pending!: Promise<void>;
    await act(async () => {
      pending = t.last().loadMore();
    });
    const before = t.results.length;
    await rerender(root, t.el(TYPE, { fromVersion: 3n }));
    expect(t.results[before]).toMatchObject({ pages: [], hasMore: true, loading: true, error: undefined });
    expect(t.signals[0]!.aborted).toBe(true);
    release();
    await act(() => pending);
    await settled(t);
    expect(versions(t.last())).toEqual([[3n, 4n]]);
    await unmount(root);
  });

  it("changes nothing on an equal inline re-render", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, { fromVersion: 1n, where: () => true }));
    await settled(t);
    const fetches = t.adapter.fetches;
    const result = t.last();
    await rerender(root, t.el(TYPE, { fromVersion: 1n, where: () => true }));
    expect(t.signals.length).toBe(1);
    expect(t.adapter.fetches).toBe(fetches);
    expect(t.last()).toBe(result);
    await unmount(root);
  });

  it("aborts on unmount, and loadMore then does nothing", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    const { loadMore } = t.last();
    const renders = t.results.length;
    await unmount(root);
    expect(t.signals[0]!.aborted).toBe(true);
    await loadMore();
    expect(t.results.length).toBe(renders);
    expect(t.signals.length).toBe(1);
  });

  it("walks once under StrictMode", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el(TYPE)));
    await settled(t);
    await act(() => t.last().loadMore());
    expect(versions(t.last())).toEqual([[1n, 2n], [3n, 4n]]);
    expect(t.signals.filter((s) => !s.aborted).length).toBe(1);
    await unmount(root);
    expect(t.signals.every((s) => s.aborted)).toBe(true);
  });

  it("fetches nothing for a null descriptor or during server rendering", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toMatchObject({ pages: [], hasMore: false, loading: false, error: undefined });
    await act(() => t.last().loadMore());
    expect(t.signals.length).toBe(0);
    await unmount(root);

    const s = setup();
    renderToString(s.el(TYPE));
    await s.last().loadMore();
    expect(s.adapter.fetches).toBe(0);
    expect(s.last()).toMatchObject({ pages: [], hasMore: true, loading: true });
  });

  describe("retry after a failure", () => {
    const at = (version: bigint, eventIndex: number): StreamdEvent => ({ ...ev(version), eventIndex });
    // Version 3 holds three events, so a page boundary can fall inside it.
    const SPLIT = [ev(1n), ev(2n), at(3n, 0), at(3n, 1), at(3n, 2), ev(4n), ev(5n)];
    const ids = (r: UseEventPagesResult<unknown>) => r.pages.flat().map((e) => `${e.version}:${e.eventIndex}`);

    async function walkAll(t: ReturnType<typeof setup>) {
      while (t.last().hasMore) await act(() => t.last().loadMore());
    }

    async function expected(options: EventFilter) {
      const client = new StreamdClient({ adapter: new TestAdapter(SPLIT) });
      const events = await client.getEvents({ kind: "module", eventType: TYPE }, options);
      return events.map((e) => `${e.version}:${e.eventIndex}`);
    }

    it("keeps the pages, then carries on from the next event, inside a split transaction", async () => {
      for (const limit of [undefined, 5]) {
        const t = setup(SPLIT);
        const root = await render(t.el(TYPE, { limit }));
        await settled(t);
        await act(() => t.last().loadMore());
        expect(ids(t.last())).toEqual(["1:0", "2:0", "3:0", "3:1"]);
        t.adapter.failing = true;
        await act(() => t.last().loadMore());
        expect(t.last().error).toBeInstanceOf(StreamdNetworkError);
        expect(t.last()).toMatchObject({ hasMore: true, loading: false });
        expect(ids(t.last())).toEqual(["1:0", "2:0", "3:0", "3:1"]);
        t.adapter.failing = false;
        await act(() => t.last().loadMore());
        expect(t.last().error).toBeUndefined();
        expect(ids(t.last()).slice(4, 5)).toEqual(["3:2"]);
        expect(t.filters.map((f) => [f.fromVersion, f.limit])).toEqual([
          [undefined, limit],
          [3n, limit === undefined ? undefined : 3],
        ]);
        await walkAll(t);
        expect(ids(t.last())).toEqual(await expected({ limit }));
        await unmount(root);
      }
    });

    it("carries on newest first under desc", async () => {
      const t = setup(SPLIT);
      let failing = false;
      Object.assign(t.adapter, {
        view: async (_request: unknown, version: bigint) => {
          if (failing) throw new StreamdNetworkError("view failed");
          return [String(version)];
        },
      });
      const withState = () => ({ function: "0x1::m::f" }) as never;
      const root = await render(t.el(TYPE, { orderBy: "desc", limit: 6, withState }));
      await settled(t);
      expect(ids(t.last())).toEqual(["5:0", "4:0"]);
      await act(() => t.last().loadMore());
      expect(ids(t.last())).toEqual(["5:0", "4:0", "3:2", "3:1"]);
      failing = true;
      await act(() => t.last().loadMore());
      expect(t.last().error).toBeInstanceOf(StreamdNetworkError);
      failing = false;
      await act(() => t.last().loadMore());
      expect(t.last().error).toBeUndefined();
      expect(ids(t.last())[4]).toBe("3:0");
      expect(t.filters.map((f) => [f.toVersion, f.limit])).toEqual([
        [undefined, 6],
        [3n, 4],
      ]);
      await walkAll(t);
      expect(ids(t.last())).toEqual(await expected({ orderBy: "desc", limit: 6 }));
      expect(t.last().pages.flat().every((e) => e.state !== undefined)).toBe(true);
      await unmount(root);
    });

    it("retries the same walk when the first page failed", async () => {
      const t = setup();
      t.adapter.failing = true;
      const root = await render(t.el(TYPE, { fromVersion: 2n }));
      await settled(t);
      expect(t.last()).toMatchObject({ pages: [], hasMore: true });
      expect(t.last().error).toBeInstanceOf(StreamdNetworkError);
      t.adapter.failing = false;
      await act(() => t.last().loadMore());
      expect(versions(t.last())).toEqual([[2n, 3n]]);
      expect(t.last().error).toBeUndefined();
      await unmount(root);
    });
  });
});
