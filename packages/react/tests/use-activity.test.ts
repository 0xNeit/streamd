import "./dom.js";
import { describe, expect, it } from "bun:test";
import { createElement } from "react";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdClient, type StreamdEvent } from "@streamd/core";
import { AptosClient, type Activity } from "@streamd/aptos";
import { StreamdProvider, useActivity, type UseActivityOptions, type UseEventsResult } from "../src/index.js";
import { render, rerender, unmount, waitFor } from "./render.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [ALICE, BOB] = [long("a11ce"), long("b0b")];
const COIN = "0x1::aptos_coin::AptosCoin";

const coin = (version: bigint, index: number, side: string, account: string, sender: string): StreamdEvent => ({
  type: `0x1::coin::Coin${side}`,
  data: { coin_type: COIN, account, amount: "5" },
  version,
  sequenceNumber: 0n,
  eventIndex: index,
  sender,
});

/** Alice pays Bob at 1, Bob pays Alice at 2 and 3; `MemoryAdapter` serves every event to an `account` walk. */
function setup() {
  const events = [1n, 2n, 3n].flatMap((v) => {
    const [from, to] = v === 1n ? [ALICE, BOB] : [BOB, ALICE];
    return [coin(v, 0, "Withdraw", from, from), coin(v, 1, "Deposit", to, from)];
  });
  const client = new AptosClient({
    adapter: { fetchEvents: (d, o) => new MemoryAdapter(events).fetchEvents(d, o), view: async () => [] },
    aptos: new AptosConfig({ network: Network.DEVNET, clientConfig: { http2: false } }),
  });
  const addresses: string[] = [];
  const activity = client.activity.bind(client);
  client.activity = ((address: string, ...rest: [never, never]) => (addresses.push(address), activity(address, ...rest))) as typeof client.activity;
  const results: UseEventsResult<Activity>[] = [];
  function Feed({ address, options }: { address: string | null | undefined; options: UseActivityOptions }) {
    results.push(useActivity(address, { ...options }));
    return null;
  }
  const el = (address: string | null | undefined, options: UseActivityOptions = FAST) =>
    createElement(StreamdProvider, { client }, createElement(Feed, { address, options }));
  return { el, events, addresses, last: () => results.at(-1)! };
}

const FAST = { fromVersion: 0n, pollInterval: 5 } satisfies UseActivityOptions;

describe("useActivity", () => {
  it("delivers one record per transaction with its direction and transfers", async () => {
    const t = setup();
    const root = await render(t.el(ALICE));
    await waitFor(() => expect(t.last().events.map((e) => e.data.direction)).toEqual(["sent", "received", "received"]));
    expect(t.last().events[0]!.data.transfers).toEqual([{ asset: COIN, amount: 5n, from: ALICE, to: BOB }]);
    expect(t.last().events[1]!.data.balanceChanges).toEqual([{ asset: COIN, delta: 5n }]);
    expect(t.last().status).toBe("live");
    await unmount(root);
  });

  it("keeps maxEvents, and restarts only on a changed address or options", async () => {
    const t = setup();
    const root = await render(t.el(ALICE, { ...FAST, maxEvents: 1 }));
    await waitFor(() => expect(t.last().events.map((e) => e.version)).toEqual([3n]));
    await rerender(root, t.el(ALICE, { ...FAST, maxEvents: 1 }));
    expect(t.addresses).toEqual([ALICE]);
    await rerender(root, t.el(BOB, { ...FAST, maxEvents: 1 }));
    await waitFor(() => expect(t.last().events[0]?.data.direction).toBe("sent"));
    expect(t.addresses).toEqual([ALICE, BOB]);
    await unmount(root);
  });

  it("waits on null, undefined and an empty address, and stops on unmount", async () => {
    const t = setup();
    const root = await render(t.el(null));
    await rerender(root, t.el(undefined));
    await rerender(root, t.el(""));
    expect(t.addresses).toEqual([]);
    expect(t.last().status).toBe("idle");
    await rerender(root, t.el(ALICE));
    await waitFor(() => expect(t.last().events).toHaveLength(3));
    await unmount(root);
    t.events.push(coin(4n, 0, "Deposit", ALICE, BOB));
    await new Promise((r) => setTimeout(r, 30));
    expect(t.last().events).toHaveLength(3);
  });

  it("shows a refused address as stopped with its error", async () => {
    const t = setup();
    const errors: Error[] = [];
    const root = await render(t.el("not an address", { ...FAST, onError: (err) => errors.push(err) }));
    await waitFor(() => expect(t.last().status).toBe("stopped"));
    expect(t.last().error).toBeInstanceOf(Error);
    expect(errors).toHaveLength(1);
    await unmount(root);
  });

  it("refuses a client without activity", async () => {
    const errors: unknown[] = [];
    function Probe() {
      try {
        useActivity(ALICE, { client: new StreamdClient({ adapter: new MemoryAdapter() }) as never });
      } catch (err) {
        errors.push(err);
      }
      return null;
    }
    const root = await render(createElement(Probe));
    expect(String(errors[0])).toContain("This hook needs a client with activity, such as an AptosClient");
    await unmount(root);
  });
});
