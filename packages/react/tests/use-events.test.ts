import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import {
  StreamdClient,
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  type MoveEventType,
} from "@streamd/core";
import {
  StreamdProvider,
  useEvents,
  type UseEventsOptions,
  type UseEventsResult,
} from "../src/index.js";
import { OTHER, TYPE, TestAdapter, ev } from "./fixtures.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const FAST = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 } satisfies UseEventsOptions;

function setup(events = [ev(1n), ev(2n), ev(3n)]) {
  const adapter = new TestAdapter(events);
  const client = new StreamdClient({ adapter });
  let subscribes = 0;
  const onBatch = client.onBatch.bind(client);
  client.onBatch = ((...args: Parameters<typeof onBatch>) => {
    subscribes++;
    return onBatch(...args);
  }) as typeof client.onBatch;
  const results: UseEventsResult<unknown>[] = [];
  // Inline descriptor and options on purpose: new objects every render.
  function Events({ type, options }: { type: MoveEventType | null; options?: UseEventsOptions }) {
    results.push(useEvents(type === null ? null : { kind: "module", eventType: type }, { ...options }));
    return null;
  }
  const el = (type: MoveEventType | null, options: UseEventsOptions = FAST) =>
    createElement(StreamdProvider, { client }, createElement(Events, { type, options }));
  const versions = () => results.at(-1)!.events.map((e) => e.version);
  return {
    adapter,
    client,
    el,
    versions,
    subscribes: () => subscribes,
    last: () => results.at(-1)!,
  };
}

describe("useEvents", () => {
  it("delivers events and keeps only the most recent maxEvents", async () => {
    const t = setup([ev(1n), ev(2n), ev(3n), ev(4n), ev(5n)]);
    const root = await render(t.el(TYPE, { ...FAST, maxEvents: 3 }));
    await waitFor(() => expect(t.versions()).toEqual([3n, 4n, 5n]));
    expect(t.last().status).toBe("live");
    t.adapter.events.push(ev(6n));
    await waitFor(() => expect(t.versions()).toEqual([4n, 5n, 6n]));
    await unmount(root);
  });

  it("filters with an async where", async () => {
    const t = setup();
    const where = async (e: { version: bigint }) => e.version !== 2n;
    const root = await render(t.el(TYPE, { ...FAST, where }));
    await waitFor(() => expect(t.versions()).toEqual([1n, 3n]));
    await unmount(root);
  });

  it("keeps one subscription for an equal inline descriptor and swaps it for a different one", async () => {
    const t = setup([ev(1n), ev(2n, OTHER)]);
    const root = await render(t.el(TYPE));
    await waitFor(() => expect(t.versions()).toEqual([1n]));
    await rerender(root, t.el(TYPE));
    await rerender(root, t.el(TYPE, { ...FAST }));
    expect(t.subscribes()).toBe(1);
    expect(t.client.listenerCount()).toBe(1);

    await rerender(root, t.el(OTHER));
    expect(t.subscribes()).toBe(2);
    expect(t.client.listenerCount()).toBe(1);
    await waitFor(() => expect(t.versions()).toEqual([2n]));
    await unmount(root);
    expect(t.client.listenerCount()).toBe(0);
  });

  it("delivers each event once with one listener under StrictMode", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el(TYPE)));
    await waitFor(() => expect(t.versions()).toEqual([1n, 2n, 3n]));
    // StrictMode mounted, unmounted and remounted the effect.
    expect(t.subscribes()).toBe(2);
    expect(t.client.listenerCount()).toBe(1);
    t.adapter.events.push(ev(4n));
    await waitFor(() => expect(t.versions()).toEqual([1n, 2n, 3n, 4n]));
    await unmount(root);
    expect(t.client.listenerCount()).toBe(0);
  });

  it("reports an error, keeps the events, and clears it when events arrive again", async () => {
    const t = setup();
    const errors: Error[] = [];
    const options = { ...FAST, onError: (err: Error) => errors.push(err) };
    const root = await render(t.el(TYPE, options));
    await waitFor(() => expect(t.versions()).toEqual([1n, 2n, 3n]));
    t.adapter.failing = true;
    await waitFor(() => expect(t.last().error).toBeInstanceOf(StreamdPollingError));
    expect(t.last().error!.cause).toBeInstanceOf(StreamdNetworkError);
    expect(t.last().status).toBe("live");
    expect(t.versions()).toEqual([1n, 2n, 3n]);
    expect(errors.length).toBeGreaterThan(0);

    t.adapter.failing = false;
    t.adapter.events.push(ev(4n));
    await waitFor(() => expect(t.last().error).toBeUndefined());
    expect(t.versions()).toEqual([1n, 2n, 3n, 4n]);
    await unmount(root);
  });

  it("reports stopped once maxErrors is reached", async () => {
    const t = setup();
    t.adapter.failing = true;
    const root = await render(t.el(TYPE, { ...FAST, maxErrors: 2 }));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(Error);
    expect(t.client.listenerCount()).toBe(0);
    await unmount(root);
  });

  it("stays idle with a null descriptor", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.last()).toEqual({ events: [], error: undefined, status: "idle" });
    expect(t.subscribes()).toBe(0);
    await unmount(root);
  });

  it("starts no subscription during server rendering", () => {
    const t = setup();
    renderToString(t.el(TYPE));
    expect(t.subscribes()).toBe(0);
    expect(t.adapter.fetches).toBe(0);
    expect(t.last().status).toBe("idle");
  });

  it("refuses a maxEvents that is not a positive integer", () => {
    const t = setup();
    for (const maxEvents of [0, -1, 1.5]) {
      expect(() => renderToString(t.el(TYPE, { maxEvents }))).toThrow(StreamdError);
    }
  });
});
