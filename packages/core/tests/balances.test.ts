import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { BalanceChange, EventAdapter, EventDescriptor, FetchOptions, StreamdEvent, ViewRequest } from "../src/types.js";

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
  [long("a1")]: { asset: USDC, owner: ALICE },
  [long("b1")]: { asset: USDC, owner: BOB },
};

const fa = (version: number, store: string): StreamdEvent => ({
  type: "0x1::fungible_asset::Deposit",
  data: { store: long(store), amount: "5" },
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex: 0,
});
const moved = (version: number, object: string, from: string, to: string): StreamdEvent => ({
  type: "0x1::object::Transfer",
  data: { object, from, to },
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex: 0,
});

interface Read {
  fn: string;
  type?: string;
  asset?: string;
  version: bigint;
}

/** A chain whose head and balances a test moves by hand. */
function chain(events: StreamdEvent[] = []) {
  const state = {
    head: 50n,
    amounts: new Map<string, string>([[APT, "100"], [USDC, "200"], [COIN_KEY, "300"]]),
    failViews: undefined as Error | undefined,
    scans: 0,
    scanFails: false,
    reads: [] as Read[],
  };
  const adapter: EventAdapter = {
    fetchEvents: async (d: EventDescriptor, o: FetchOptions) => {
      state.scans++;
      if (state.scanFails) throw new StreamdNetworkError("scan down", 503);
      return new MemoryAdapter(events).fetchEvents(d, o);
    },
    getHeadVersion: async () => state.head,
    view: async (request: ViewRequest, version: bigint) => {
      const [first, second] = request.functionArguments as string[];
      if (request.function === "0x1::object::owner") return [STORES[first!]!.owner];
      if (request.function === "0x1::fungible_asset::store_metadata") return [{ inner: STORES[first!]!.asset }];
      if (request.function === "0x4::token::creator") return [ALICE];
      state.reads.push({ fn: request.function, type: request.typeArguments?.[0], asset: second, version });
      if (state.failViews !== undefined) throw state.failViews;
      if (second === NFT) throw new StreamdError("aborted");
      const key = request.function === COIN_BALANCE
        ? request.typeArguments![0] === "0x1::aptos_coin::AptosCoin" ? APT : request.typeArguments![0]!
        : second!;
      return [state.amounts.get(key) ?? "0"];
    },
  };
  return { adapter, state };
}

const until = async (done: () => boolean) => {
  for (let i = 0; i < 400 && !done(); i++) await new Promise((r) => setTimeout(r, 2));
};
const FAST = { pollInterval: 1, maxBackoff: 1 };

describe("client.balances with an asset", () => {
  it("reads it at the head, reports changes, and scans nothing", async () => {
    const { adapter, state } = chain();
    const client = new StreamdClient({ adapter });
    const changes: StreamdEvent<BalanceChange>[] = [];
    const wallet = client.balances(ALICE, { ...FAST, asset: USDC, onChange: (_, c) => changes.push(c) });
    expect((await wallet.ready).get(USDC)).toBe(200n);
    expect(state.reads[0]).toEqual({ fn: BALANCE, type: "0x1::fungible_asset::Metadata", asset: USDC, version: 50n });

    state.head = 60n;
    state.amounts.set(USDC, "150");
    await until(() => changes.length === 1);
    expect(changes[0]!.version).toBe(60n);
    expect(changes[0]!.data).toEqual({ asset: USDC, balance: 150n });
    expect(wallet.get(USDC)).toBe(150n);
    expect(wallet.get(`0x${USDC.slice(2).toUpperCase()}`)).toBe(150n);
    expect(wallet.value).toEqual(new Map([[USDC, 150n]]));

    const reads = state.reads.length;
    await until(() => state.reads.length > reads + 5);
    expect(changes).toHaveLength(1);
    expect(state.scans).toBe(0);
    expect(client.listenerCount()).toBe(0);
    client.destroy();
    expect(wallet.active).toBe(false);
  });

  it("updates a coin type with no events at all, as a paired coin needs", async () => {
    const { adapter, state } = chain();
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, { ...FAST, asset: COIN });
    await wallet.ready;
    state.head = 51n;
    state.amounts.set(COIN_KEY, "305");
    await until(() => wallet.get(COIN) === 305n);
    expect(wallet.get(COIN)).toBe(305n);
    expect(state.reads[0]).toEqual({ fn: COIN_BALANCE, type: COIN_KEY, asset: undefined, version: 50n });
    client.destroy();
  });

  it("skips a head that went backwards, so a balance cannot flip to an older value", async () => {
    const { adapter, state } = chain();
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, { ...FAST, asset: USDC });
    await wallet.ready;
    state.head = 40n;
    state.amounts.set(USDC, "1");
    const before = state.reads.length;
    await new Promise((r) => setTimeout(r, 20));
    expect(wallet.get(USDC)).toBe(200n);
    expect(state.reads.length).toBe(before);
    client.destroy();
  });

  it("retries after a failed view, and stops when polling gives up", async () => {
    const { adapter, state } = chain();
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, { ...FAST, asset: USDC, maxErrors: 3, onError: (e) => errors.push(e) });
    await wallet.ready;
    state.failViews = new StreamdNetworkError("unavailable", 503);
    await until(() => errors.length >= 1);
    state.failViews = undefined;
    state.head = 70n;
    state.amounts.set(USDC, "9");
    await until(() => wallet.get(USDC) === 9n);
    expect(wallet.get(USDC)).toBe(9n);
    expect(wallet.active).toBe(true);
    state.failViews = new StreamdNetworkError("unavailable", 503);
    await until(() => !wallet.active);
    expect(wallet.active).toBe(false);
    client.destroy();
  });

  it("rejects ready on a token object or a failed first read, and reports it", async () => {
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter: chain().adapter, onError: (e) => errors.push(e) });
    const token = client.balances(ALICE, { asset: NFT });
    await expect(token.ready).rejects.toThrow("Not a fungible");
    expect(token.active).toBe(false);
    expect(errors).toHaveLength(1);
    const down = chain();
    down.state.failViews = new StreamdNetworkError("unavailable", 503);
    await expect(new StreamdClient({ adapter: down.adapter }).balances(ALICE, { asset: USDC }).ready).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("refuses a malformed amount as a failed poll", async () => {
    const { adapter } = chain();
    const errors: Error[] = [];
    let odd = false;
    const client = new StreamdClient({
      adapter: { ...adapter, view: async (r, v) => (odd ? ["-1"] : adapter.view!(r, v)) },
    });
    const wallet = client.balances(ALICE, { ...FAST, asset: USDC, onError: (e) => errors.push(e) });
    await wallet.ready;
    odd = true;
    await until(() => errors.length > 0);
    expect(errors[0]!.message).toContain("returned no amount");
    client.destroy();
  });
});

describe("client.balances without an asset", () => {
  it("follows APT from the start, in either form", async () => {
    const { adapter, state } = chain();
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, FAST);
    await wallet.ready;
    expect(wallet.get("0xa")).toBe(100n);
    expect(wallet.get("0x1::aptos_coin::AptosCoin")).toBe(100n);
    expect(state.reads[0]).toEqual({ fn: COIN_BALANCE, type: "0x1::aptos_coin::AptosCoin", asset: undefined, version: 50n });
    state.head = 55n;
    state.amounts.set(APT, "90");
    await until(() => wallet.get("0xa") === 90n);
    expect(wallet.get("0xa")).toBe(90n);
    wallet.unsubscribe();
    expect(wallet.active).toBe(false);
  });

  it("adds an asset once a transfer shows the wallet moving it, ignoring others' and tokens", async () => {
    const { adapter, state } = chain([moved(51, NFT, BOB, ALICE), fa(52, "b1"), fa(53, "a1")]);
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, FAST);
    await wallet.ready;
    state.head = 60n;
    await until(() => wallet.get(USDC) !== undefined);
    expect(wallet.get(USDC)).toBe(200n);
    await until(() => state.reads.filter((r) => r.asset === NFT).length > 0);
    const nft = state.reads.filter((r) => r.asset === NFT).length;
    await new Promise((r) => setTimeout(r, 20));
    expect(state.reads.filter((r) => r.asset === NFT).length).toBe(nft);
    expect(wallet.value.has(NFT)).toBe(false);
    client.destroy();
  });

  it("keeps polling when the discovery scan gives up", async () => {
    const { adapter, state } = chain();
    state.scanFails = true;
    const errors: Error[] = [];
    const client = new StreamdClient({ adapter });
    const wallet = client.balances(ALICE, { ...FAST, maxErrors: 2, onError: (e) => errors.push(e) });
    await wallet.ready;
    await until(() => errors.length >= 2);
    state.head = 70n;
    state.amounts.set(APT, "55");
    await until(() => wallet.get("0xa") === 55n);
    expect(wallet.get("0xa")).toBe(55n);
    expect(wallet.active).toBe(true);
    client.destroy();
  });
});

describe("client.balances input and lifetime", () => {
  it("refuses bad input at the call", () => {
    const client = new StreamdClient({ adapter: chain().adapter });
    expect(() => client.balances("0xnothex")).toThrow(StreamdError);
    expect(() => client.balances(ALICE, { fromVersion: 1n } as never)).toThrow("Unsupported balances option");
    expect(() => client.balances(ALICE, { asset: "nope" })).toThrow(StreamdError);
    expect(() => client.balances(ALICE, { asset: `${ALICE}::Apes::Ape::0` })).toThrow("legacy token");
    expect(() => new StreamdClient({ adapter: new MemoryAdapter([]) }).balances(ALICE)).toThrow("view");
  });

  it("stops with its signal, rejecting a first read still running", async () => {
    const controller = new AbortController();
    const client = new StreamdClient({ adapter: chain().adapter });
    const wallet = client.balances(ALICE, { asset: USDC, signal: controller.signal });
    controller.abort(new StreamdError("cancelled"));
    await expect(wallet.ready).rejects.toThrow("cancelled");
    expect(wallet.active).toBe(false);
  });
});
