import "./dom.js";
import { describe, expect, it } from "bun:test";
import { StrictMode, createElement } from "react";
import { renderToString } from "react-dom/server";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError, StreamdPollingError, type EventAdapter, type ViewRequest } from "@streamd/core";
import { AptosClient } from "@streamd/aptos";
import { StreamdProvider, useBalances, type UseBalancesOptions, type UseBalancesResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [ALICE, APT, USDC] = [long("a11ce"), long("a"), long("bae2")];
const FAST = { pollInterval: 2, maxBackoff: 2 } satisfies UseBalancesOptions;

/** An `AptosClient` over a chain whose head and balances a test moves by hand, counting views. */
function setup() {
  const chain = { head: 50n, amounts: new Map([[APT, "100"], [USDC, "200"]]), views: 0, failing: undefined as Error | undefined };
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => new MemoryAdapter([]).fetchEvents(d, o),
    getHeadVersion: async () => chain.head,
    view: async (request: ViewRequest) => {
      chain.views++;
      if (chain.failing !== undefined) throw chain.failing;
      const asset = request.function === "0x1::coin::balance" ? APT : (request.functionArguments![1] as string);
      return [chain.amounts.get(asset) ?? "0"];
    },
  };
  const client = new AptosClient({ adapter, aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }) });
  let starts = 0;
  const balances = client.balances.bind(client);
  client.balances = ((...args: Parameters<typeof balances>) => (starts++, balances(...args))) as typeof client.balances;
  const results: UseBalancesResult[] = [];
  function Wallet({ address, options }: { address: string | null | undefined; options: UseBalancesOptions }) {
    results.push(useBalances(address, { ...options }));
    return null;
  }
  const el = (address: string | null | undefined, options: UseBalancesOptions = FAST) =>
    createElement(StreamdProvider, { client }, createElement(Wallet, { address, options }));
  return { chain, el, results, starts: () => starts, last: () => results.at(-1)! };
}

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("useBalances", () => {
  it("waits on null, undefined and an empty address", async () => {
    const t = setup();
    const root = await render(t.el(null));
    await rerender(root, t.el(undefined));
    await rerender(root, t.el(""));
    expect(t.last()).toEqual({ balances: new Map(), error: undefined, status: "idle" });
    expect(t.starts()).toBe(0);
    await unmount(root);
  });

  it("goes from loading to live, then re-renders only when a balance changes", async () => {
    const t = setup();
    const seen: bigint[] = [];
    const root = await render(t.el(ALICE, { ...FAST, onChange: (b) => seen.push(b.get(APT)!) }));
    expect(t.results[0]!.status).toBe("idle");
    expect(t.results.some((r) => r.status === "loading")).toBe(true);
    await waitFor(() => expect(t.last()).toEqual({ balances: new Map([[APT, 100n]]), error: undefined, status: "live" }));

    const renders = t.results.length;
    const views = t.chain.views;
    await waitFor(() => expect(t.chain.views).toBeGreaterThan(views + 3));
    expect(t.results.length).toBe(renders);

    const latest: bigint[] = [];
    await rerender(root, t.el(ALICE, { ...FAST, onChange: (b) => latest.push(b.get(APT)!) }));
    t.chain.head = 51n;
    t.chain.amounts.set(APT, "90");
    await waitFor(() => expect(t.last().balances).toEqual(new Map([[APT, 90n]])));
    expect(latest).toEqual([90n]);
    expect(seen).toEqual([]);
    expect(t.starts()).toBe(1);
    await unmount(root);
  });

  it("restarts on a changed asset, and not on an equal one", async () => {
    const t = setup();
    const root = await render(t.el(ALICE, { ...FAST, asset: [USDC] }));
    await waitFor(() => expect(t.last().balances).toEqual(new Map([[USDC, 200n]])));
    await rerender(root, t.el(ALICE, { ...FAST, asset: [USDC] }));
    expect(t.starts()).toBe(1);
    await rerender(root, t.el(ALICE, { ...FAST, asset: [USDC, APT] }));
    await waitFor(() => expect(t.last().balances).toEqual(new Map([[USDC, 200n], [APT, 100n]])));
    expect(t.starts()).toBe(2);
    await unmount(root);
  });

  it("stops with the error when the first read fails or polling gives up", async () => {
    const down = setup();
    down.chain.failing = new StreamdNetworkError("rate limited");
    const errors: Error[] = [];
    const failed = await render(down.el(ALICE, { ...FAST, onError: (e) => errors.push(e) }));
    await waitFor(() => expect(down.last().status).toBe("stopped"));
    expect(down.last().error).toBeInstanceOf(StreamdNetworkError);
    expect(errors).toHaveLength(1);
    await unmount(failed);

    const t = setup();
    const root = await render(t.el(ALICE, { ...FAST, maxErrors: 2 }));
    await waitFor(() => expect(t.last().status).toBe("live"));
    t.chain.failing = new StreamdNetworkError("rate limited");
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(StreamdPollingError);
    expect(t.last().balances).toEqual(new Map([[APT, 100n]]));
    await unmount(root);
  });

  it("shows a malformed asset as stopped instead of throwing", async () => {
    const t = setup();
    const errors: Error[] = [];
    const root = await render(t.el(ALICE, { asset: "nope", onError: (e) => errors.push(e) }));
    expect(t.last().status).toBe("stopped");
    expect(t.last().error).toBeInstanceOf(StreamdError);
    expect(errors).toHaveLength(1);
    expect(t.chain.views).toBe(0);
    await unmount(root);
  });

  it("does nothing during server rendering", () => {
    const t = setup();
    expect(renderToString(t.el(ALICE))).toBe("");
    expect(t.starts()).toBe(0);
    expect(t.last().status).toBe("idle");
  });

  it("stops reading on unmount under StrictMode", async () => {
    const t = setup();
    const root = await render(createElement(StrictMode, null, t.el(ALICE)));
    await waitFor(() => expect(t.last().status).toBe("live"));
    await unmount(root);
    const views = t.chain.views;
    await pause(30);
    expect(t.chain.views).toBe(views);
  });

  it("refuses a client without balances", async () => {
    const errors: unknown[] = [];
    function Probe() {
      try {
        useBalances(ALICE, { client: new StreamdClient({ adapter: new MemoryAdapter() }) as never });
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const root = await render(createElement(Probe));
    expect(String(errors[0])).toContain("This hook needs a client with balances, such as an AptosClient or a SuiClient");
    await unmount(root);
  });
});
