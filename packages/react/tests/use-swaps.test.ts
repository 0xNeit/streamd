import "./dom.js";
import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement } from "react";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdClient, StreamdError, parseEvent } from "@streamd/core";
import { AptosClient, type Swap } from "@streamd/aptos";
import { StreamdProvider, useSwaps, type UseEventsResult, type UseSwapsOptions } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

/** The aptos package's mainnet swap events, one per supported DEX. */
const EVENTS = readFileSync(join(import.meta.dir, "../../aptos/tests/fixtures/swaps.jsonl"), "utf8")
  .trim()
  .split("\n")
  .map((line) => parseEvent(line));

function setup() {
  const events = [...EVENTS];
  const client = new AptosClient({
    adapter: { fetchEvents: (d, o) => new MemoryAdapter(events).fetchEvents(d, o) },
    aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }),
  });
  let subscribes = 0;
  const swaps = client.swaps.bind(client);
  client.swaps = ((...args: Parameters<typeof swaps>) => (subscribes++, swaps(...args))) as typeof client.swaps;
  const results: UseEventsResult<Swap>[] = [];
  function Feed({ options }: { options: UseSwapsOptions | null }) {
    results.push(useSwaps(options === null ? null : { ...options }));
    return null;
  }
  const el = (options: UseSwapsOptions | null) => createElement(StreamdProvider, { client }, createElement(Feed, { options }));
  return { el, events, subscribes: () => subscribes, last: () => results.at(-1)! };
}

const FAST = { fromVersion: 0n, pollInterval: 5 } satisfies UseSwapsOptions;
const protocols = (r: UseEventsResult<Swap>) => [...new Set(r.events.map((e) => e.data.protocol))].sort();

describe("useSwaps", () => {
  it("delivers decoded swaps, keeping the most recent maxEvents", async () => {
    const t = setup();
    const all = await render(t.el(FAST));
    await waitFor(() => expect(t.last().events.length).toBeGreaterThan(5));
    const count = t.last().events.length;
    expect(t.last().status).toBe("live");
    expect(typeof t.last().events[0]!.data.amountIn).toBe("bigint");
    await unmount(all);

    const root = await render(t.el({ ...FAST, maxEvents: 2 }));
    await waitFor(() => expect(t.last().events).toHaveLength(2));
    await new Promise((r) => setTimeout(r, 30));
    expect(t.last().events.map((e) => e.version)).toEqual(EVENTS.slice(-2).map((e) => e.version));
    expect(count).toBeGreaterThan(2);
    await unmount(root);
  });

  it("narrows by protocol, and keeps one subscription for equal inline options", async () => {
    const t = setup();
    const root = await render(t.el({ ...FAST, protocol: "hyperion" }));
    await waitFor(() => expect(t.last().events.length).toBeGreaterThan(0));
    expect(protocols(t.last())).toEqual(["hyperion"]);
    await rerender(root, t.el({ ...FAST, protocol: "hyperion" }));
    expect(t.subscribes()).toBe(1);
    await rerender(root, t.el({ ...FAST, protocol: "cellana" }));
    await waitFor(() => expect(protocols(t.last())).toEqual(["cellana"]));
    expect(t.subscribes()).toBe(2);
    await unmount(root);
  });

  it("waits on null, and stops polling on unmount", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.subscribes()).toBe(0);
    expect(t.last().status).toBe("idle");
    await rerender(root, t.el(FAST));
    await waitFor(() => expect(t.last().events.length).toBeGreaterThan(0));
    await unmount(root);
    const seen = t.last().events.length;
    t.events.push({ ...EVENTS[0]!, version: 9_000_000_000n });
    await new Promise((r) => setTimeout(r, 30));
    expect(t.last().events).toHaveLength(seen);
  });

  it("shows a refused protocol as stopped with its error", async () => {
    const t = setup();
    const errors: Error[] = [];
    const root = await render(
      t.el({ ...FAST, protocol: "uniswap" as never, onError: (err) => errors.push(err) }),
    );
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(StreamdError);
    expect(errors).toHaveLength(1);
    await unmount(root);
  });

  it("refuses a client without swaps", async () => {
    const errors: unknown[] = [];
    function Probe() {
      try {
        useSwaps({ client: new StreamdClient({ adapter: new MemoryAdapter() }) as never });
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const root = await render(createElement(Probe));
    expect(String(errors[0])).toContain("This hook needs a client with swaps, such as an AptosClient");
    expect(String(errors[0])).not.toContain("SuiClient");
    await unmount(root);
  });
});
