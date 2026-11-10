import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { BALANCE_DESCRIPTOR, balanceSource } from "../src/balances.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { BalanceChange, EventAdapter, FetchOptions, StreamdEvent, ViewRequest } from "../src/types.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT = long("a");
const USDC = long("bae2");
const NFT = long("deed");
const ALICE = long("a11ce");
const BOB = long("b0b");
const COIN = "0x1::usdt::USDT";
const COIN_KEY = `${long("1")}::usdt::USDT`;
const BALANCE = "0x1::primary_fungible_store::balance";
const COIN_BALANCE = "0x1::coin::balance";

const STORES: Record<string, { asset: string; owner: string }> = {
  [long("a1")]: { asset: APT, owner: ALICE },
  [long("b1")]: { asset: APT, owner: BOB },
  [long("a2")]: { asset: USDC, owner: ALICE },
  [long("b2")]: { asset: USDC, owner: BOB },
};

const base = (version: number, eventIndex: number) => ({ version: BigInt(version), sequenceNumber: 0n, eventIndex });
const fa = (v: number, i: number, side: string, store: string, amount = "5"): StreamdEvent => ({
  type: `0x1::fungible_asset::${side}`,
  data: { store: long(store), amount },
  ...base(v, i),
});
const coin = (v: number, i: number, side: string, account: string, coin_type = COIN): StreamdEvent => ({
  type: `0x1::coin::Coin${side}`,
  data: { coin_type, account, amount: "5" },
  ...base(v, i),
});
const fee = (v: number, i: number, sender: string | undefined): StreamdEvent => ({
  type: "0x1::transaction_fee::FeeStatement",
  data: {},
  ...base(v, i),
  ...(sender === undefined ? {} : { sender }),
});
const moved = (v: number, i: number, object: string, from: string, to: string): StreamdEvent => ({
  type: "0x1::object::Transfer",
  data: { object, from, to },
  ...base(v, i),
});

interface Read {
  fn: string;
  type?: string;
  asset?: string;
  version: bigint;
}

/** Balances are `100 + version`, so a read's version shows in its answer. */
function chain(events: StreamdEvent[], fail?: (read: Read) => Error | undefined) {
  const reads: Read[] = [];
  const memory = new MemoryAdapter(events);
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => memory.fetchEvents(d, o),
    getHeadVersion: async () => 50n,
    view: async (request: ViewRequest, version: bigint) => {
      const [first, second] = request.functionArguments as string[];
      if (request.function === "0x1::object::owner") return [STORES[first!]!.owner];
      if (request.function === "0x1::fungible_asset::store_metadata") return [{ inner: STORES[first!]!.asset }];
      if (request.function === "0x4::token::creator") return [ALICE];
      const read = { fn: request.function, type: request.typeArguments?.[0], asset: second, version };
      reads.push(read);
      const error = fail?.(read);
      if (error !== undefined) throw error;
      if (second === NFT) throw new StreamdError("aborted");
      return [String(100n + version)];
    },
  };
  return { adapter, reads };
}

const all: FetchOptions = { fromVersion: 0n, limit: 100 };
const of = (who: string) => async () => who;

async function changes(source: EventAdapter) {
  const page = await source.fetchEvents(BALANCE_DESCRIPTOR, all);
  return page.events.map((e) => ({ at: `${e.version}:${e.eventIndex}`, ...(e.data as BalanceChange) }));
}

describe("balanceSource", () => {
  it("reads each changed asset at the version that changed it", async () => {
    const { adapter, reads } = chain([fa(1, 0, "Withdraw", "a1"), fa(1, 1, "Deposit", "b1"), fa(2, 0, "Deposit", "a2")]);
    expect(await changes(balanceSource(adapter, of(ALICE)))).toEqual([
      { at: "1:1", asset: APT, balance: 101n },
      { at: "2:0", asset: USDC, balance: 102n },
    ]);
    expect(reads).toEqual([
      { fn: COIN_BALANCE, type: "0x1::aptos_coin::AptosCoin", asset: undefined, version: 1n },
      { fn: BALANCE, type: "0x1::fungible_asset::Metadata", asset: USDC, version: 2n },
    ]);
  });

  it("treats the account's own gas as a change to APT, and nobody else's", async () => {
    const { adapter, reads } = chain([fee(3, 0, ALICE), fee(4, 0, BOB), fee(5, 0, undefined)]);
    expect(await changes(balanceSource(adapter, of(ALICE)))).toEqual([{ at: "3:0", asset: APT, balance: 103n }]);
    expect(reads).toHaveLength(1);
  });

  it("makes one read per asset per version, and one APT entry however it arrived", async () => {
    const { adapter, reads } = chain([
      fa(1, 0, "Withdraw", "a1"),
      fa(1, 1, "Deposit", "b1"),
      coin(1, 2, "Deposit", ALICE, "0x1::aptos_coin::AptosCoin"),
      fee(1, 3, ALICE),
      coin(2, 0, "Deposit", ALICE),
    ]);
    expect(await changes(balanceSource(adapter, of(ALICE)))).toEqual([
      { at: "1:1", asset: APT, balance: 101n },
      { at: "2:0", asset: COIN_KEY, balance: 102n },
    ]);
    expect(reads.map((r) => r.version)).toEqual([1n, 2n]);
    expect(reads[1]).toEqual({ fn: COIN_BALANCE, type: COIN_KEY, asset: undefined, version: 2n });
  });

  it("skips token objects, remembering them, and legacy tokens without a read", async () => {
    const id = { token_data_id: { creator: ALICE, collection: "Apes", name: "Ape" }, property_version: "0" };
    const { adapter, reads } = chain([
      moved(1, 0, NFT, BOB, ALICE),
      moved(2, 0, NFT, ALICE, BOB),
      { type: "0x3::token::TokenDeposit", data: { account: ALICE, id, amount: "1" }, ...base(3, 0) },
      fa(4, 0, "Deposit", "a2"),
    ]);
    expect(await changes(balanceSource(adapter, of(ALICE)))).toEqual([{ at: "4:0", asset: USDC, balance: 104n }]);
    expect(reads.map((r) => r.asset)).toEqual([NFT, USDC]);
  });

  it("narrows to one asset, and ignores gas unless that asset is APT", async () => {
    const events = [fa(1, 0, "Deposit", "a2"), fa(2, 0, "Deposit", "a1"), fee(3, 0, ALICE)];
    const usdc = balanceSource(chain(events).adapter, of(ALICE), USDC);
    expect((await changes(usdc)).map((c) => c.at)).toEqual(["1:0"]);
    const apt = balanceSource(chain(events).adapter, of(ALICE), APT);
    expect((await changes(apt)).map((c) => c.at)).toEqual(["2:0", "3:0"]);
  });

  it("fails the page when a view fails, and refuses a malformed amount", async () => {
    const down = chain([fa(1, 0, "Deposit", "a2")], () => new StreamdNetworkError("unavailable", 503));
    await expect(changes(balanceSource(down.adapter, of(ALICE)))).rejects.toBeInstanceOf(StreamdNetworkError);
    const bad = chain([fa(1, 0, "Deposit", "a2")]);
    const odd: EventAdapter = {
      ...bad.adapter,
      view: async (r, v, d) => (r.function === BALANCE ? ["-1"] : bad.adapter.view!(r, v, d)),
    };
    await expect(changes(balanceSource(odd, of(ALICE)))).rejects.toThrow("returned no amount");
  });
});

describe("client.balances", () => {
  const until = async (done: () => boolean) => {
    for (let i = 0; i < 300 && !done(); i++) await new Promise((r) => setTimeout(r, 2));
  };

  it("reads the asset at the head, then follows changes", async () => {
    const { adapter, reads } = chain([fa(50, 0, "Deposit", "a2"), fa(60, 0, "Deposit", "a2")]);
    const client = new StreamdClient({ adapter });
    const seen: bigint[] = [];
    const balances = client.balances(ALICE, {
      asset: USDC,
      pollInterval: 1,
      onChange: (all, change) => seen.push(all.get(USDC)! + change.data.balance),
    });
    expect((await balances.ready).get(USDC)).toBe(150n);
    await until(() => seen.length === 1);
    expect(balances.get(USDC)).toBe(160n);
    expect(balances.get(`0x${USDC.slice(2).toUpperCase()}`)).toBe(160n);
    expect(balances.value).toEqual(new Map([[USDC, 160n]]));
    expect(seen).toEqual([320n]);
    expect(reads[0]!.version).toBe(50n);
    expect(client.listenerCount()).toBe(0);
    client.destroy();
    expect(balances.active).toBe(false);
  });

  it("fills the map as assets move, and takes APT in either form", async () => {
    const { adapter } = chain([fee(70, 0, ALICE)]);
    const client = new StreamdClient({ adapter: { ...adapter, getHeadVersion: async () => 60n } });
    const balances = client.balances(ALICE, { pollInterval: 1 });
    await balances.ready;
    await until(() => balances.value.size === 1);
    expect(balances.get("0xa")).toBe(170n);
    expect(balances.get("0x1::aptos_coin::AptosCoin")).toBe(170n);
    balances.unsubscribe();
    expect(balances.active).toBe(false);
  });

  it("rejects ready on a token object or a failed first read, and reports it", async () => {
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter: chain([]).adapter, onError: (e) => errors.push(e) });
    const token = client.balances(ALICE, { asset: NFT });
    await expect(token.ready).rejects.toThrow("Not a fungible");
    expect(token.active).toBe(false);
    expect(errors).toHaveLength(1);
    const down = new StreamdClient({
      adapter: chain([], () => new StreamdNetworkError("unavailable", 503)).adapter,
    });
    await expect(down.balances(ALICE, { asset: USDC }).ready).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("refuses bad input at the call", () => {
    const client = new StreamdClient({ adapter: chain([]).adapter });
    expect(() => client.balances("0xnothex")).toThrow(StreamdError);
    expect(() => client.balances(ALICE, { fromVersion: 1n } as never)).toThrow("Unsupported balances option");
    expect(() => client.balances(ALICE, { asset: "nope" })).toThrow(StreamdError);
    expect(() => client.balances(ALICE, { asset: `${ALICE}::Apes::Ape::0` })).toThrow("legacy token");
    expect(() => new StreamdClient({ adapter: new MemoryAdapter([]) }).balances(ALICE)).toThrow("view");
  });

  it("stops with its signal, rejecting a first read still running", async () => {
    const controller = new AbortController();
    const client = new StreamdClient({ adapter: chain([]).adapter });
    const balances = client.balances(ALICE, { asset: USDC, signal: controller.signal });
    controller.abort(new StreamdError("cancelled"));
    await expect(balances.ready).rejects.toThrow("cancelled");
    expect(balances.active).toBe(false);
  });
});
