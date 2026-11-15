import "./dom.js";
import { describe, expect, it } from "bun:test";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError, adapterMetrics, type EventAdapter } from "@streamd/core";
import { StreamdDevtools, StreamdProvider, type StreamdDevtoolsProps } from "../src/index.js";
import { OTHER, TYPE, TestAdapter, ev } from "./fixtures.js";
import { waitFor } from "./render.js";
import pkg from "../package.json";

const LIVE = { fromVersion: 0n, pollInterval: 5, maxBackoff: 10 };

// The DOM lib here has no iterable NodeList.
const all = (el: ParentNode, selector: string): Element[] => Array.prototype.slice.call(el.querySelectorAll(selector));

async function mount(client: StreamdClient, props: StreamdDevtoolsProps = {}) {
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  const el = createElement(StreamdProvider, { client }, createElement(StreamdDevtools, { interval: 10, defaultOpen: true, ...props }));
  await act(async () => root.render(el));
  const rows = () =>
    all(all(container, "table")[0]!, "tbody tr").map((tr) => all(tr, "td").map((td) => td.textContent),
    );
  const done = async () => {
    await act(async () => root.unmount());
    container.remove();
    client.destroy();
  };
  return { container, rows, done };
}

describe("StreamdDevtools", () => {
  it("renders nothing on the server", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    expect(renderToString(createElement(StreamdDevtools, { client, defaultOpen: true }))).toBe("");
  });

  it("lists subscriptions with descriptor, cursor and version, following new events", async () => {
    const adapter = new TestAdapter([ev(1n), ev(2n)]);
    const client = new StreamdClient({ adapter });
    client.on({ kind: "module", eventType: TYPE }, () => {}, { ...LIVE, id: "deposits" });
    client.reduce({ kind: "account", accountAddress: "0xa", eventTypes: [OTHER] }, (n: number) => n, 0, LIVE);
    const t = await mount(client);
    await waitFor(() => expect(t.rows()[0]!.slice(0, 5)).toEqual(["deposits", TYPE, "live", "2", "2"]));
    expect(t.rows()[1]!.slice(1, 3)).toEqual([`account 0xa ${OTHER}`, "live"]);
    adapter.events.push(ev(3n));
    await waitFor(() => expect(t.rows()[0]!.slice(3, 5)).toEqual(["3", "3"]));
    expect(t.container.textContent).toContain("streamd (2 running)");
    await t.done();
  });

  it("shows pause, retrying with the redacted error, and keeps ended ones", async () => {
    const adapter = new TestAdapter([ev(1n)]);
    const client = new StreamdClient({ adapter, onError: () => {} });
    const paused = client.on({ kind: "module", eventType: TYPE }, () => {}, { ...LIVE, id: "p" });
    const t = await mount(client);
    await waitFor(() => expect(t.rows()[0]![4]).toBe("1"));
    paused.pause();
    await waitFor(() => expect(t.rows()[0]![2]).toBe("paused"));

    const failing: EventAdapter = {
      async fetchEvents() {
        throw new StreamdNetworkError("GET https://node.example/v1/KEY/x failed", 429);
      },
    };
    const other = new StreamdClient({ adapter: failing, onError: () => {} });
    other.on({ kind: "module", eventType: TYPE }, () => {}, { ...LIVE, id: "f", maxErrors: 1000, maxBackoff: 20 });
    const u = await mount(other);
    await waitFor(() => expect(u.rows()[0]![2]).toBe("retrying"));
    expect(u.rows()[0]![5]).toMatch(/^\d+ StreamdPollingError: Polling failed \(\d+\/1000\) \(cause: GET <url> failed\)$/);
    expect(u.container.textContent).not.toContain("KEY");

    paused.unsubscribe();
    await waitFor(() => expect(t.rows()[0]!.slice(0, 3)).toEqual(["p", TYPE, "ended"]));
    expect(t.container.textContent).toContain("streamd (0 running)");
    await t.done();
    await u.done();
  });

  it("keeps at most 20 ended subscriptions, newest first", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    const t = await mount(client);
    for (let i = 0; i < 25; i++) {
      const sub = client.on({ kind: "module", eventType: TYPE }, () => {}, { ...LIVE, id: `s${i}` });
      await waitFor(() => expect(t.rows().some((r) => r[0] === `s${i}`)).toBe(true));
      sub.unsubscribe();
    }
    await waitFor(() => expect(t.rows()[0]![0]).toBe("s24"));
    await waitFor(() => expect(t.rows().map((r) => r[0])).toEqual(Array.from({ length: 20 }, (_, i) => `s${24 - i}`)));
    await t.done();
  });

  it("shows metrics from one wrapper or a labelled record", async () => {
    const metered = adapterMetrics(new MemoryAdapter([ev(1n)]));
    const client = new StreamdClient({ adapter: metered.adapter });
    client.on({ kind: "module", eventType: TYPE }, () => {}, LIVE);
    const metricRows = (c: HTMLElement) =>
      all(all(c, "table")[1]!, "tbody tr").map((tr) => all(tr, "td").slice(0, 4).map((td) => td.textContent),
      );
    const t = await mount(client, { metrics: metered });
    await waitFor(() => expect(metricRows(t.container)[0]!.slice(0, 2)).toEqual(["adapter", "fetchEvents"]));
    expect(Number(metricRows(t.container)[0]![2])).toBeGreaterThan(0);
    expect(metricRows(t.container)[0]![3]).toBe("0");
    const other = adapterMetrics(new MemoryAdapter([]));
    const u = await mount(new StreamdClient({ adapter: other.adapter }), { metrics: { primary: metered, archive: other } });
    await waitFor(() => expect(metricRows(u.container).map((r) => r[0])).toContain("primary"));
    await t.done();
    await u.done();
  });

  it("starts closed with only the toggle, and offers no other control", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    client.on({ kind: "module", eventType: TYPE }, () => {}, LIVE);
    const t = await mount(client, { defaultOpen: false });
    expect(t.container.querySelector("table")).toBeNull();
    const buttons = t.container.querySelectorAll("button");
    expect(buttons.length).toBe(1);
    expect(buttons[0]!.getAttribute("aria-expanded")).toBe("false");
    await act(async () => (buttons[0] as HTMLButtonElement).click());
    expect(t.container.querySelectorAll("table").length).toBe(1);
    expect(t.container.querySelectorAll("button, input, select, textarea").length).toBe(1);
    await t.done();
  });

  it("refuses a bad interval", () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    for (const interval of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => renderToString(createElement(StreamdDevtools, { client, interval }))).toThrow(StreamdError);
    }
  });

  it("adds no runtime dependency", () => {
    expect(Object.keys(pkg.dependencies).sort()).toEqual(["@streamd/aptos", "@streamd/core"]);
  });
});
