import "./dom.js";
import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createElement } from "react";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError, parseEvent } from "@streamd/core";
import { AptosClient, type Sale } from "@streamd/aptos";
import { StreamdProvider, useSales, type UseEventsResult, type UseSalesOptions } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

/** The aptos package's mainnet fill events from Wapal and Tradeport. */
const EVENTS = readFileSync(join(import.meta.dir, "../../aptos/tests/fixtures/sales.jsonl"), "utf8")
  .trim()
  .split("\n")
  .map((line) => parseEvent(line));

function setup() {
  const client = new AptosClient({
    adapter: { fetchEvents: (d, o) => new MemoryAdapter(EVENTS).fetchEvents(d, o) },
    aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }),
  });
  let subscribes = 0;
  const sales = client.sales.bind(client);
  client.sales = ((...args: Parameters<typeof sales>) => (subscribes++, sales(...args))) as typeof client.sales;
  const results: UseEventsResult<Sale>[] = [];
  function Feed({ options }: { options: UseSalesOptions | null }) {
    results.push(useSales(options === null ? null : { ...options }));
    return null;
  }
  const el = (options: UseSalesOptions | null) => createElement(StreamdProvider, { client }, createElement(Feed, { options }));
  return { el, subscribes: () => subscribes, last: () => results.at(-1)! };
}

const FAST = { fromVersion: 0n, pollInterval: 5 } satisfies UseSalesOptions;
const markets = (r: UseEventsResult<Sale>) => [...new Set(r.events.map((e) => e.data.marketplace))].sort();

describe("useSales", () => {
  it("delivers decoded sales, narrowed by marketplace, restarting only on a changed one", async () => {
    const t = setup();
    const root = await render(t.el({ ...FAST, marketplace: "wapal" }));
    await waitFor(() => expect(t.last().events.length).toBeGreaterThan(0));
    expect(markets(t.last())).toEqual(["wapal"]);
    expect(typeof t.last().events[0]!.data.price).toBe("bigint");
    await rerender(root, t.el({ ...FAST, marketplace: "wapal" }));
    expect(t.subscribes()).toBe(1);
    await rerender(root, t.el({ ...FAST, marketplace: "tradeport" }));
    await waitFor(() => expect(markets(t.last())).toEqual(["tradeport"]));
    expect(t.subscribes()).toBe(2);
    await unmount(root);
  });

  it("keeps the most recent maxEvents", async () => {
    const t = setup();
    const root = await render(t.el({ ...FAST, maxEvents: 1 }));
    await waitFor(() => expect(t.last().events.at(-1)?.version).toBe(EVENTS.at(-1)!.version));
    expect(t.last().events).toHaveLength(1);
    await unmount(root);
  });

  it("waits on null, and shows a refused marketplace as stopped", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.subscribes()).toBe(0);
    expect(t.last().status).toBe("idle");
    await rerender(root, t.el({ ...FAST, marketplace: "opensea" as never }));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(StreamdError);
    await unmount(root);
  });
});
