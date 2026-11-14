import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdError } from "@streamd/core";
import type { EventAdapter, FetchOptions, StreamdEvent, ViewRequest } from "@streamd/core";
import { activityDescriptor, activitySource } from "../src/activity.js";
import type { Activity } from "../src/types.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT = long("a");
const ALICE = long("a11ce");
const BOB = long("b0b");
const CAROL = long("ca201");
const COIN = "0x1::aptos_coin::AptosCoin";
/** Fungible stores: the asset each holds and who owns it. */
const STORES: Record<string, { asset: string; owner: string }> = {
  [long("a1")]: { asset: APT, owner: ALICE },
  [long("b1")]: { asset: APT, owner: BOB },
};

function ev(version: number, eventIndex: number, type: string, data: unknown, sender?: string): StreamdEvent {
  return {
    type,
    data,
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: `0x${version.toString(16).padStart(64, "0")}`,
    ...(sender !== undefined && { sender }),
  };
}
const fa = (v: number, i: number, side: string, store: string, amount: string, sender?: string) =>
  ev(v, i, `0x1::fungible_asset::${side}`, { store: long(store), amount }, sender);
const coin = (v: number, i: number, side: string, account: string, amount: string, sender?: string) =>
  ev(v, i, `0x1::coin::Coin${side}`, { coin_type: COIN, account, amount }, sender);
const fee = (v: number, i: number, sender?: string) => ev(v, i, "0x1::transaction_fee::FeeStatement", { total_charge_gas_units: "7" }, sender);

/** A `MemoryAdapter`, which serves an `account` descriptor every event it holds, with views over `STORES`. */
function adapter(events: StreamdEvent[]): EventAdapter & { descriptors: unknown[] } {
  const memory = new MemoryAdapter(events);
  const descriptors: unknown[] = [];
  return {
    descriptors,
    fetchEvents: (d, o) => (descriptors.push(d), memory.fetchEvents(d, o)),
    getHeadVersion: () => memory.getHeadVersion(),
    view: async (request: ViewRequest) => {
      const store = STORES[String(request.functionArguments![0])];
      if (store === undefined) throw new StreamdError("aborted");
      return request.function === "0x1::object::owner" ? [store.owner] : [{ inner: "0xa" }];
    },
  };
}

const all: FetchOptions = { fromVersion: 0n, limit: 100 };

async function read(source: EventAdapter, options: FetchOptions = all): Promise<StreamdEvent<Activity>[]> {
  return (await source.fetchEvents(activityDescriptor(ALICE), options)).events as StreamdEvent<Activity>[];
}

describe("activitySource", () => {
  it("gives one record per transaction with its direction, own transfers, deltas and every event", async () => {
    const base = adapter([
      fee(10, 0, ALICE),
      coin(10, 1, "Withdraw", ALICE, "500", ALICE),
      coin(10, 2, "Deposit", BOB, "500", ALICE),
      coin(11, 0, "Withdraw", CAROL, "300", CAROL),
      coin(11, 1, "Deposit", ALICE, "300", CAROL),
      coin(11, 2, "Withdraw", CAROL, "40", CAROL),
      coin(11, 3, "Deposit", BOB, "40", CAROL),
      fee(11, 4, CAROL),
    ]);
    const records = await read(activitySource(base, async () => ALICE));
    expect(records.map((r) => [`${r.version}:${r.eventIndex}`, r.data.direction, r.data.transfers, r.data.balanceChanges])).toEqual([
      ["10:0", "sent", [{ asset: COIN, amount: 500n, from: ALICE, to: BOB }], [{ asset: COIN, delta: -500n }]],
      ["11:0", "received", [{ asset: COIN, amount: 300n, from: CAROL, to: ALICE }], [{ asset: COIN, delta: 300n }]],
    ]);
    expect(records[1]!.data.events.map((e) => e.eventIndex)).toEqual([0, 1, 2, 3, 4]);
    expect(records[1]!.sender).toBe(CAROL);
    expect(records[1]!.transactionHash).toBe(`0x${(11).toString(16).padStart(64, "0")}`);
    expect(base.descriptors).toEqual([{ kind: "account", accountAddress: ALICE, received: true }]);
  });

  it("reads fungible stores by view, nets a transfer to itself out, and keeps assets apart", async () => {
    const base = adapter([
      fa(20, 0, "Withdraw", "a1", "100", ALICE),
      fa(20, 1, "Deposit", "a1", "100", ALICE),
      fa(20, 2, "Withdraw", "a1", "25", ALICE),
      fa(20, 3, "Deposit", "b1", "25", ALICE),
      coin(20, 4, "Deposit", ALICE, "9", ALICE),
    ]);
    const [record] = await read(activitySource(base, async () => ALICE));
    expect(record!.data.transfers).toEqual([
      { asset: APT, amount: 100n, from: ALICE, to: ALICE },
      { asset: APT, amount: 25n, from: ALICE, to: BOB },
      { asset: COIN, amount: 9n, from: undefined, to: ALICE },
    ]);
    expect(record!.data.balanceChanges).toEqual([
      { asset: APT, delta: -25n },
      { asset: COIN, delta: 9n },
    ]);
  });

  it("reads a transaction with no sender, or only a fee, as an entry with no transfers", async () => {
    const records = await read(activitySource(adapter([fee(30, 0), fee(31, 0, ALICE)]), async () => ALICE));
    expect(records.map((r) => [r.data.direction, r.data.transfers, r.data.balanceChanges])).toEqual([
      ["received", [], []],
      ["sent", [], []],
    ]);
  });

  it("names the account, the sender and every counterparty, one lookup each", async () => {
    const asked: string[] = [];
    const name = async (address: string) => (asked.push(address), address === BOB ? "bob.apt" : null);
    const base = adapter([coin(40, 0, "Withdraw", BOB, "5", BOB), coin(40, 1, "Deposit", ALICE, "5", BOB)]);
    const [record] = await read(activitySource(base, async () => ALICE, false, undefined, name));
    expect(record!.data.names).toEqual({ [ALICE]: null, [BOB]: "bob.apt" });
    expect(asked).toEqual([ALICE, BOB]);
  });

  it("orders records newest first under desc", async () => {
    const records = await read(activitySource(adapter([fee(1, 0, ALICE), fee(2, 0, BOB)]), async () => ALICE), {
      ...all,
      orderBy: "desc",
    });
    expect(records.map((r) => r.version)).toEqual([2n, 1n]);
  });

  it("refuses an adapter without view", () => {
    const memory = new MemoryAdapter([]);
    expect(() => activitySource(memory, async () => ALICE)).toThrow("activity needs an adapter that implements view");
  });
});
