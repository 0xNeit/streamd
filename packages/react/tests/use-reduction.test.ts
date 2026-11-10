import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import { StreamdClient, StreamdNetworkError, type MoveEventType, type Reducer, type StreamdEvent } from "@streamd/core";
import { StreamdProvider, useReduction, type UseReductionOptions, type UseReductionResult } from "../src/index.js";
import { TYPE, TestAdapter, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const FAST = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 } satisfies UseReductionOptions<bigint, unknown>;
const add: Reducer<bigint> = (sum, e: StreamdEvent) => sum + e.version;

function setup(events = [ev(1n), ev(2n), ev(3n)]) {
  const adapter = new TestAdapter(events);
  const client = new StreamdClient({ adapter });
  let starts = 0;
  const reduce = client.reduce.bind(client);
  client.reduce = ((...args: Parameters<typeof reduce>) => (starts++, reduce(...args))) as typeof client.reduce;
  const results: UseReductionResult<bigint>[] = [];
  interface Props {
    type: MoveEventType | null;
    reducer: Reducer<bigint>;
    initial: bigint;
    options: UseReductionOptions<bigint, unknown>;
  }
  function Sum({ type, reducer, initial, options }: Props) {
    results.push(useReduction(type === null ? null : { kind: "module", eventType: type }, reducer, initial, { ...options }));
    return null;
  }
  const el = (type: MoveEventType | null, reducer: Reducer<bigint> = add, initial = 0n, options = FAST as UseReductionOptions<bigint, unknown>) =>
    createElement(StreamdProvider, { client }, createElement(Sum, { type, reducer, initial, options }));
  return { adapter, client, el, starts: () => starts, last: () => results.at(-1)!, results };
}

describe("useReduction", () => {
  it("shows initial while loading, then the backfill, then live updates", async () => {
    const t = setup();
    const root = await render(t.el(TYPE, add, 100n));
    expect(t.results[0]).toEqual({ value: 100n, error: undefined, status: "idle" });
    await waitFor(() => expect(t.last()).toEqual({ value: 106n, error: undefined, status: "live" }));
    t.adapter.events.push(ev(4n));
    await waitFor(() => expect(t.last().value).toBe(110n));
    await unmount(root);
  });

  it("calls the latest reducer without restarting, and restarts on a changed initial", async () => {
    const t = setup();
    const root = await render(t.el(TYPE));
    await waitFor(() => expect(t.last().value).toBe(6n));
    const double: Reducer<bigint> = (sum, e) => sum + e.version * 2n;
    await rerender(root, t.el(TYPE, double));
    t.adapter.events.push(ev(4n));
    await waitFor(() => expect(t.last().value).toBe(14n));
    expect(t.starts()).toBe(1);
    await rerender(root, t.el(TYPE, add, 1000n));
    await waitFor(() => expect(t.last().value).toBe(1010n));
    expect(t.starts()).toBe(2);
    await unmount(root);
  });

  it("waits on a null descriptor", async () => {
    const t = setup();
    const root = await render(t.el(null, add, 5n));
    expect(t.last()).toEqual({ value: 5n, error: undefined, status: "idle" });
    expect(t.starts()).toBe(0);
    await rerender(root, t.el(TYPE, add, 5n));
    await waitFor(() => expect(t.last().value).toBe(11n));
    await unmount(root);
  });

  it("surfaces a failed backfill and a throwing reducer as stopped", async () => {
    const down = setup();
    down.adapter.failing = true;
    const seen: Error[] = [];
    const failed = await render(down.el(TYPE, add, 7n, { ...FAST, onError: (e) => seen.push(e) }));
    await waitFor(() => expect(down.last().status).toBe("stopped"));
    expect(down.last().error).toBeInstanceOf(StreamdNetworkError);
    expect(down.last().value).toBe(7n);
    expect(seen).toHaveLength(1);
    await unmount(failed);

    const t = setup();
    const boom: Reducer<bigint> = () => {
      throw new Error("boom");
    };
    const root = await render(t.el(TYPE, boom));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error!.message).toContain("Reducer threw");
    await unmount(root);
  });

  it("does nothing during server rendering", () => {
    const t = setup();
    expect(renderToString(t.el(TYPE))).toBe("");
    expect(t.starts()).toBe(0);
    expect(t.last().status).toBe("idle");
  });

  it("folds once under StrictMode and stops on unmount", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el(TYPE)));
    await waitFor(() => expect(t.last().value).toBe(6n));
    await unmount(root);
    const fetches = t.adapter.fetches;
    await new Promise((r) => setTimeout(r, 30));
    expect(t.adapter.fetches).toBe(fetches);
  });
});
