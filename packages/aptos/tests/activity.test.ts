import { describe, expect, it } from "bun:test";
import { AccountAddress, Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, replayAdapter } from "@streamd/core";
import type { EventAdapter, FetchOptions, StreamdEvent, ViewRequest } from "@streamd/core";
import { activityDescriptor, activitySource } from "../src/activity.js";
import { FullnodeScanAdapter } from "../src/index.js";
import type { Activity } from "../src/types.js";
import { makeAptos, testClient } from "./helpers.js";

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

describe("activity through the client", () => {
  const HISTORY = [
    coin(1, 0, "Withdraw", ALICE, "5", ALICE),
    coin(1, 1, "Deposit", BOB, "5", ALICE),
    coin(2, 0, "Withdraw", BOB, "7", BOB),
    coin(2, 1, "Deposit", ALICE, "7", BOB),
    fee(3, 0, ALICE),
  ];

  /** A mainnet client whose SDK answers ANS reads from a fixed table, counting them. */
  const named = (base: EventAdapter) => {
    const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    const reads: string[] = [];
    aptos.getPrimaryName = async ({ address }) => {
      reads.push(`primary ${address.toString()}`);
      return address.toString() === BOB ? "bob" : undefined;
    };
    aptos.getTargetAddress = async ({ name }) => {
      reads.push(`target ${name}`);
      const target = { "alice.apt": ALICE, "bob.apt": BOB }[name];
      return target === undefined ? undefined : AccountAddress.from(target);
    };
    return { client: testClient({ adapter: base, aptos }), reads };
  };

  it("returns records over a range, with where and limit on the records", async () => {
    const client = testClient({ adapter: adapter(HISTORY) });
    const all = await client.getActivity(ALICE, { fromVersion: 0n });
    expect(all.map((r) => [r.version, r.data.direction, r.data.balanceChanges])).toEqual([
      [1n, "sent", [{ asset: COIN, delta: -5n }]],
      [2n, "received", [{ asset: COIN, delta: 7n }]],
      [3n, "sent", []],
    ]);
    const sent = await client.getActivity(ALICE, { fromVersion: 0n, where: (r) => (r.data as Activity).direction === "sent", limit: 1 });
    expect(sent.map((r) => r.version)).toEqual([1n]);
  });

  it("resolves a name once, and names the sender and every counterparty under withNames", async () => {
    const base = adapter(HISTORY);
    const { client, reads } = named(base);
    const records = await client.getActivity("alice.apt", { fromVersion: 0n, toVersion: 2n, withNames: true });
    expect(base.descriptors).toEqual([{ kind: "account", accountAddress: ALICE, received: true }]);
    expect(records.map((r) => [r.senderName, r.data.names])).toEqual([
      [null, { [ALICE]: null, [BOB]: "bob.apt" }],
      ["bob.apt", { [ALICE]: null, [BOB]: "bob.apt" }],
    ]);
    expect(reads.filter((r) => r === "target alice.apt")).toHaveLength(1);
    expect(reads.filter((r) => r.startsWith("primary"))).toHaveLength(2);
  });

  it("names records under maxConcurrentRequests: 1, whose only slot the page holds", async () => {
    const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    aptos.getPrimaryName = async () => undefined;
    const client = testClient({ adapter: adapter(HISTORY), aptos, maxConcurrentRequests: 1, requestTimeout: 1000 });
    const records = await client.getActivity(ALICE, { fromVersion: 0n, toVersion: 1n, withNames: true });
    expect(records.map((r) => r.data.names)).toEqual([{ [ALICE]: null, [BOB]: null }]);
  });

  it("follows live, delivering pushed transactions once, as no listener", async () => {
    const memory = new MemoryAdapter(HISTORY.slice(0, 2));
    const base: EventAdapter = { ...adapter([]), fetchEvents: (d, o) => memory.fetchEvents(d, o) };
    const client = testClient({ adapter: base });
    const seen: bigint[] = [];
    const sub = client.activity(ALICE, (r) => void seen.push(r.version), { fromVersion: 0n, pollInterval: 1 });
    expect(client.listenerCount()).toBe(0);
    for (let i = 0; i < 200 && seen.length < 1; i++) await new Promise((r) => setTimeout(r, 2));
    memory.push(coin(9, 0, "Withdraw", BOB, "1", BOB), coin(9, 1, "Deposit", ALICE, "1", BOB));
    for (let i = 0; i < 200 && seen.length < 2; i++) await new Promise((r) => setTimeout(r, 2));
    expect(seen).toEqual([1n, 9n]);
    sub.unsubscribe();
    expect(sub.active).toBe(false);
    client.destroy();
  });

  it("refuses at the call an adapter without view, withNames off mainnet, and a missing address", () => {
    const client = testClient({ adapter: new MemoryAdapter(HISTORY) });
    expect(() => client.activity(ALICE, () => {})).toThrow("activity needs an adapter that implements view");
    const devnet = testClient({ adapter: adapter(HISTORY) });
    expect(() => devnet.activity(ALICE, () => {}, { withNames: true })).toThrow("withNames");
    expect(() => devnet.activity(undefined as unknown as string, () => {})).toThrow("activity needs an address");
  });

  it("passes on the fullnode's refusal of received transactions", async () => {
    const client = testClient({ adapter: new FullnodeScanAdapter(makeAptos()) });
    await expect(client.getActivity(ALICE, { fromVersion: 1n })).rejects.toThrow("serve received ones with IndexerAccountAdapter");
  });
});

describe("activity on a recorded mainnet range", () => {
  // A wallet sending and receiving USDt, recorded through IndexerAccountAdapter routed beside
  // FullnodeScanAdapter. Each entry's sender, hash, event count and deltas were checked against
  // the fullnode, the deltas by balance views just before and at each version (gas aside).
  const WALLET = "0xadad8cfa3131518aee9bec795a96425aa0427c11c4e08cb2200c1f08574f691e";
  const USDT = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b";
  const recording = JSON.parse(readFileSync(join(import.meta.dir, "fixtures/activity.json"), "utf8"));

  it("gives one entry per listed transaction with direction, counterparty and delta", async () => {
    const client = testClient({ adapter: replayAdapter(recording) });
    const records = await client.getActivity(WALLET, { fromVersion: 8053135000n, toVersion: 8053170600n, withAssetInfo: true });
    const rows = records.map(({ version, data }) => {
      const [t] = data.transfers;
      return [version, data.direction, data.transfers.length, t!.from === WALLET ? t!.to : t!.from, data.balanceChanges[0]!.delta];
    });
    expect(rows).toEqual([
      [8053135206n, "received", 1, "0x91aeb97a90cd413764ac909c0cc123ba2e49ebb9d6d78d0522ec6a3a6e8a31b5", 30000000n],
      [8053144673n, "sent", 1, "0xb918cbbb3006dbac666054bca903c51bbcdf6f015ee947357e2fb7b471e6a248", -142861730n],
      [8053144977n, "sent", 1, "0xaa91187da59bf234b9a6c309b2638413ce6ce12ab48931a677a61fad67aa1eda", -70032025n],
      [8053159418n, "sent", 1, "0x0d378fcd22c47f8706ac36ad1ac49ac9f8467cc92309359dabbab1ba14492a7b", -24990000n],
      [8053159535n, "sent", 1, "0x78c5c1c5f19a0f191f11cd0faefa503cdfa97960d8e2c9522940d808beabce09", -4598990000n],
      [8053170054n, "sent", 1, "0xacf4719bea10736ed1e9bb5a29eed4fa6d5a078938978761378f87055b5d6bc3", -5999990000n],
      [8053170149n, "sent", 1, "0x78c5c1c5f19a0f191f11cd0faefa503cdfa97960d8e2c9522940d808beabce09", -361990000n],
      // One octa of APT from a look-alike of the 8053170054 recipient.
      [8053170565n, "received", 1, "0xacf490ac30e7855960d58a8e683c0e9361fa71fad6b5b466d9ddadcae71e6bc3", 1n],
    ]);
    expect(records[0]!.data.transfers[0]).toEqual({
      asset: USDT,
      amount: 30000000n,
      from: "0x91aeb97a90cd413764ac909c0cc123ba2e49ebb9d6d78d0522ec6a3a6e8a31b5",
      to: WALLET,
      symbol: "USDt",
      decimals: 6,
    });
    expect(records[7]!.data.balanceChanges).toEqual([{ asset: long("a"), delta: 1n }]);
    expect(records[1]!.transactionHash).toBe("0xfb5f5179699843afe8cbdea800de01e896b5842bdccd42441d1faf70c8ddee07");
    expect(records[1]!.entryFunction?.function).toBe("0x1::primary_fungible_store::transfer");
    expect(records.every((r) => r.data.events.length === 3 && r.eventIndex === 0)).toBe(true);
  });
});
