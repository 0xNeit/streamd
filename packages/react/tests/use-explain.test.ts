import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdClient, StreamdNetworkError, type StreamdEvent, type TransactionEvents } from "@streamd/core";
import { AptosClient, type ExplainOptions } from "@streamd/aptos";
import { StreamdProvider, useExplain, type UseExplainOptions, type UseExplainResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const A = `0x${"a".repeat(64)}`;
const B = `0x${"b".repeat(64)}`;
const FAILED = `0x${"c".repeat(64)}`;
const THROWS = `0x${"d".repeat(64)}`;
const SENDER = `0x${"5".repeat(64)}`;

const fee = (version: bigint, hash: string): StreamdEvent => ({
  type: "0x1::transaction_fee::FeeStatement",
  data: { total_charge_gas_units: "7", execution_gas_units: "4", io_gas_units: "3", storage_fee_octas: "0", storage_fee_refund_octas: "0" },
  version,
  sequenceNumber: 0n,
  eventIndex: 0,
  transactionHash: hash,
  sender: SENDER,
});

/** An `AptosClient` whose lookups answer from `MemoryAdapter`, `FAILED` as an abort and `THROWS` as a network error. */
function setup() {
  const memory = new MemoryAdapter([fee(1n, A), fee(2n, B)]);
  const lookups: string[] = [];
  let gate: Promise<void> | undefined;
  const adapter = {
    fetchEvents: memory.fetchEvents.bind(memory),
    view: async () => [],
    async getTransactionEvents(hash: string): Promise<TransactionEvents | undefined> {
      lookups.push(hash);
      if (hash === THROWS) throw new StreamdNetworkError("rate limited");
      if (hash === FAILED) return { version: 3n, success: false, vmStatus: "Move abort: 0x1", events: [fee(3n, FAILED)] };
      await gate;
      return memory.getTransactionEvents(hash);
    },
  };
  const client = new AptosClient({ adapter, aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }) });
  const signals: AbortSignal[] = [];
  const explain = client.explain.bind(client);
  client.explain = ((target: string, options: ExplainOptions) => (signals.push(options.signal!), explain(target, options))) as typeof client.explain;
  const results: UseExplainResult[] = [];
  function Tx({ hash, options }: { hash: string | null | undefined; options?: UseExplainOptions }) {
    results.push(useExplain(hash, { ...options }));
    return null;
  }
  const el = (hash: string | null | undefined, options?: UseExplainOptions) =>
    createElement(StreamdProvider, { client }, createElement(Tx, { hash, options }));
  const hold = () => {
    let release!: () => void;
    gate = new Promise<void>((r) => (release = r));
    return () => {
      gate = undefined;
      release();
    };
  };
  return { el, lookups, signals, results, hold, last: () => results.at(-1)! };
}

const LOADING = { data: undefined, error: undefined, loading: true };

describe("useExplain", () => {
  it("loads, then returns the explanation", async () => {
    const t = setup();
    const root = await render(t.el(A));
    expect(t.results[0]).toEqual(LOADING);
    await waitFor(() => expect(t.last().loading).toBe(false));
    const data = t.last().data!;
    expect([data.success, data.version, data.transactionHash, data.sender]).toEqual([true, 1n, A, SENDER]);
    expect(data.gas?.units).toBe(7n);
    expect(t.last().error).toBeUndefined();
    await unmount(root);
  });

  it("explains a failed transaction rather than erroring", async () => {
    const t = setup();
    const root = await render(t.el(FAILED));
    await waitFor(() => expect(t.last().loading).toBe(false));
    expect(t.last().data?.success).toBe(false);
    expect(t.last().data?.vmStatus).toBe("Move abort: 0x1");
    expect(t.last().error).toBeUndefined();
    await unmount(root);
  });

  it("reports a lookup failure and a timeout as error", async () => {
    const t = setup();
    const root = await render(t.el(THROWS));
    await waitFor(() => expect(t.last().error).toBeInstanceOf(StreamdNetworkError));
    expect(t.last()).toMatchObject({ data: undefined, loading: false });
    await rerender(root, t.el(`0x${"e".repeat(64)}`, { timeout: 1 }));
    await waitFor(() => expect(t.last().error?.name).toBe("StreamdTimeoutError"));
    await unmount(root);
  });

  it("aborts the read it replaces, never showing its result under the new hash", async () => {
    const t = setup();
    const release = t.hold();
    const root = await render(t.el(A));
    await waitFor(() => expect(t.lookups).toContain(A));
    await rerender(root, t.el(B));
    expect(t.signals[0]!.aborted).toBe(true);
    expect(t.last()).toEqual(LOADING);
    release();
    await waitFor(() => expect(t.last().data?.version).toBe(2n));
    await unmount(root);
    expect(t.signals[1]!.aborted).toBe(true);
  });

  it("keeps one read for equal inline options, and reads nothing for null", async () => {
    const t = setup();
    const root = await render(t.el(null));
    await rerender(root, t.el(undefined));
    expect(t.last()).toEqual({ data: undefined, error: undefined, loading: false });
    await rerender(root, t.el(A, { withAssetInfo: false }));
    await waitFor(() => expect(t.last().data).toBeDefined());
    await rerender(root, t.el(A, { withAssetInfo: false }));
    expect(t.signals).toHaveLength(1);
    await unmount(root);
  });

  it("reads nothing during server rendering, and refuses a client without explain", async () => {
    const t = setup();
    renderToString(t.el(A));
    expect(t.signals).toHaveLength(0);
    const errors: unknown[] = [];
    function Probe() {
      try {
        useExplain(A, { client: new StreamdClient({ adapter: new MemoryAdapter() }) as never });
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const root = await render(createElement(Probe));
    expect(String(errors[0])).toContain("This hook needs a client with explain, such as an AptosClient");
    await unmount(root);
  });
});
