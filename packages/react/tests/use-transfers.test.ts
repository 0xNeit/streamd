import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { MemoryAdapter, StreamdClient, type EventAdapter, type StreamdEvent, type Transfer } from "@streamd/core";
import { StreamdProvider, useTransfers, type UseTransfersOptions } from "../src/index.js";
import type { UseEventsResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [ALICE, BOB, APT] = [long("a11ce"), long("b0b"), long("a")];
const owners: Record<string, string> = { [long("1")]: ALICE, [long("2")]: BOB };

const fa = (version: bigint, index: number, side: string, store: string): StreamdEvent => ({
  type: `0x1::fungible_asset::${side}`,
  data: { store: long(store), amount: "5" },
  version,
  sequenceNumber: 0n,
  eventIndex: index,
});

function setup() {
  const events = [fa(1n, 0, "Deposit", "1"), fa(2n, 0, "Deposit", "2"), fa(3n, 0, "Deposit", "1")];
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => new MemoryAdapter(events).fetchEvents(d, o),
    view: async (r) =>
      r.function === "0x1::object::owner" ? [owners[String(r.functionArguments![0])]] : [{ inner: APT }],
  };
  const client = new StreamdClient({ adapter });
  let subscribes = 0;
  const transfers = client.transfers.bind(client);
  client.transfers = ((...args: Parameters<typeof transfers>) => (subscribes++, transfers(...args))) as typeof client.transfers;
  const results: UseEventsResult<Transfer>[] = [];
  function Feed({ options }: { options: UseTransfersOptions | null }) {
    results.push(useTransfers(options === null ? null : { ...options }));
    return null;
  }
  const el = (options: UseTransfersOptions | null) =>
    createElement(StreamdProvider, { client }, createElement(Feed, { options }));
  return { el, events, subscribes: () => subscribes, last: () => results.at(-1)! };
}

const FAST = { fromVersion: 0n, pollInterval: 5 } satisfies UseTransfersOptions;

describe("useTransfers", () => {
  it("delivers records, keeping the most recent maxEvents", async () => {
    const t = setup();
    const root = await render(t.el({ ...FAST, maxEvents: 2 }));
    await waitFor(() => expect(t.last().events.map((e) => e.version)).toEqual([2n, 3n]));
    expect(t.last().events[1]!.data).toEqual({ asset: APT, amount: 5n, from: undefined, to: ALICE });
    expect(t.last().status).toBe("live");
    await unmount(root);
  });

  it("narrows to one account, and keeps one subscription for equal inline options", async () => {
    const t = setup();
    const root = await render(t.el({ ...FAST, account: BOB }));
    await waitFor(() => expect(t.last().events.map((e) => e.version)).toEqual([2n]));
    await rerender(root, t.el({ ...FAST, account: BOB }));
    expect(t.subscribes()).toBe(1);
    await rerender(root, t.el({ ...FAST, account: ALICE }));
    await waitFor(() => expect(t.last().events.map((e) => e.version)).toEqual([1n, 3n]));
    expect(t.subscribes()).toBe(2);
    await unmount(root);
  });

  it("waits on null, then starts", async () => {
    const t = setup();
    const root = await render(t.el(null));
    expect(t.subscribes()).toBe(0);
    expect(t.last().status).toBe("idle");
    await rerender(root, t.el(FAST));
    await waitFor(() => expect(t.last().events).toHaveLength(3));
    await unmount(root);
  });

  it("stops polling on unmount", async () => {
    const t = setup();
    const root = await render(t.el(FAST));
    await waitFor(() => expect(t.last().events).toHaveLength(3));
    await unmount(root);
    const seen = t.last().events.length;
    t.events.push(fa(4n, 0, "Deposit", "1"));
    await new Promise((r) => setTimeout(r, 30));
    expect(t.last().events).toHaveLength(seen);
  });
});
