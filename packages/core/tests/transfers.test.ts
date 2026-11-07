import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { TRANSFER_DESCRIPTOR, transferSource } from "../src/transfers.js";
import type { EventAdapter, FetchOptions, StreamdEvent, Transfer, ViewRequest } from "../src/types.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT = long("a");
const USDC = long("bae2");
const ALICE = long("a11ce");
const BOB = long("b0b");
const CAROL = long("ca201");

const OWNER = "0x1::object::owner";
const METADATA = "0x1::fungible_asset::store_metadata";

/** Stores by short name: the asset each holds and who owns it. */
const STORES: Record<string, { asset: string; owner: string }> = {
  a1: { asset: APT, owner: ALICE },
  b1: { asset: APT, owner: BOB },
  c1: { asset: APT, owner: CAROL },
  a2: { asset: USDC, owner: ALICE },
  b2: { asset: USDC, owner: BOB },
};
const store = (name: string) => long(name);

function fa(version: number, eventIndex: number, side: "Withdraw" | "Deposit", name: string, amount: string): StreamdEvent {
  return {
    type: `0x1::fungible_asset::${side}`,
    data: { store: store(name), amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  };
}

function coin(version: number, eventIndex: number, side: "Withdraw" | "Deposit", account: string, amount: string): StreamdEvent {
  return {
    type: `0x1::coin::Coin${side}`,
    data: { coin_type: "0x1::aptos_coin::AptosCoin", account, amount },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex,
  };
}

interface Call {
  function: string;
  store: string;
  version: bigint;
}

/** A `MemoryAdapter` with a `view` answering from `STORES`, or from `answer` when given. */
function withViews(events: StreamdEvent[], answer?: (call: Call) => unknown[] | undefined) {
  const calls: Call[] = [];
  const memory = new MemoryAdapter(events);
  const adapter: EventAdapter = {
    fetchEvents: (d, o) => memory.fetchEvents(d, o),
    getHeadVersion: () => memory.getHeadVersion(),
    view: async (request: ViewRequest, version: bigint) => {
      const call = { function: request.function, store: String(request.functionArguments![0]), version };
      calls.push(call);
      const custom = answer?.(call);
      if (custom !== undefined) return custom;
      const entry = Object.entries(STORES).find(([name]) => store(name) === call.store)?.[1];
      if (entry === undefined) throw new StreamdError("aborted");
      // The fullnode returns special addresses such as APT's 0xa in short form.
      const asset = entry.asset === APT ? "0xa" : entry.asset;
      return request.function === OWNER ? [entry.owner] : [{ inner: asset }];
    },
  };
  return { adapter, calls };
}

const all: FetchOptions = { fromVersion: 0n, limit: 100 };

async function transfers(source: EventAdapter, options: FetchOptions = all) {
  const page = await source.fetchEvents(TRANSFER_DESCRIPTOR, options);
  return page.events.map((e) => ({ at: `${e.version}:${e.eventIndex}`, ...(e.data as Transfer) }));
}

describe("transferSource", () => {
  it("pairs a withdraw with its deposit, reading asset and owners at the event version", async () => {
    const { adapter, calls } = withViews([fa(5, 0, "Withdraw", "a1", "100"), fa(5, 1, "Deposit", "b1", "100")]);
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "5:1", asset: APT, amount: 100n, from: ALICE, to: BOB },
    ]);
    expect(calls).toContainEqual({ function: OWNER, store: store("a1"), version: 5n });
    expect(calls).toContainEqual({ function: METADATA, store: store("b1"), version: 5n });
  });

  it("pairs each deposit with the nearest earlier waiting withdraw of the same asset and amount", async () => {
    const { adapter } = withViews([
      fa(7, 0, "Withdraw", "a2", "50"),
      fa(7, 1, "Deposit", "b2", "50"),
      fa(7, 2, "Withdraw", "b2", "50"),
      fa(7, 3, "Withdraw", "a1", "9"),
      fa(7, 4, "Deposit", "a2", "50"),
      fa(7, 5, "Deposit", "c1", "9"),
    ]);
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "7:1", asset: USDC, amount: 50n, from: ALICE, to: BOB },
      { at: "7:4", asset: USDC, amount: 50n, from: BOB, to: ALICE },
      { at: "7:5", asset: APT, amount: 9n, from: ALICE, to: CAROL },
    ]);
  });

  it("leaves a lone deposit without from, a lone withdraw without to, and differing amounts unpaired", async () => {
    const { adapter } = withViews([
      fa(1, 0, "Deposit", "b1", "10"),
      fa(2, 0, "Withdraw", "a1", "10"),
      fa(3, 0, "Withdraw", "a1", "10"),
      fa(3, 1, "Deposit", "b1", "9"),
      fa(4, 0, "Withdraw", "a1", "10"),
      fa(5, 0, "Deposit", "b1", "10"),
    ]);
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "1:0", asset: APT, amount: 10n, from: undefined, to: BOB },
      { at: "2:0", asset: APT, amount: 10n, from: ALICE, to: undefined },
      { at: "3:0", asset: APT, amount: 10n, from: ALICE, to: undefined },
      { at: "3:1", asset: APT, amount: 9n, from: undefined, to: BOB },
      { at: "4:0", asset: APT, amount: 10n, from: ALICE, to: undefined },
      { at: "5:0", asset: APT, amount: 10n, from: undefined, to: BOB },
    ]);
  });

  it("reads coin events from account and coin_type with no view", async () => {
    const { adapter, calls } = withViews([coin(3, 0, "Withdraw", long("A11CE"), "7"), coin(3, 1, "Deposit", BOB, "7")]);
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "3:1", asset: "0x1::aptos_coin::AptosCoin", amount: 7n, from: ALICE, to: BOB },
    ]);
    expect(calls).toEqual([]);
  });

  it("keeps the anchor event's identity and transaction fields", async () => {
    const deposit = { ...fa(5, 1, "Deposit", "b1", "100"), transactionHash: "0xab", timestamp: 9n, sender: ALICE };
    const { adapter } = withViews([fa(5, 0, "Withdraw", "a1", "100"), deposit]);
    const [event] = (await transferSource(adapter).fetchEvents(TRANSFER_DESCRIPTOR, all)).events;
    expect(event).toMatchObject({ type: deposit.type, version: 5n, eventIndex: 1, transactionHash: "0xab", timestamp: 9n, sender: ALICE });
  });

  it("filters by asset before reading owners, by short address or coin type", async () => {
    const events = [
      fa(1, 0, "Withdraw", "a2", "5"),
      fa(1, 1, "Deposit", "b2", "5"),
      fa(2, 0, "Deposit", "b1", "6"),
      coin(3, 0, "Deposit", BOB, "7"),
    ];
    const { adapter, calls } = withViews(events);
    expect((await transfers(transferSource(adapter, "0xa"))).map((t) => t.at)).toEqual(["2:0"]);
    expect(calls.filter((c) => c.function === OWNER).map((c) => c.store)).toEqual([store("b1")]);
    expect((await transfers(transferSource(withViews(events).adapter, "0x01::aptos_coin::AptosCoin"))).map((t) => t.at)).toEqual(["3:0"]);
  });

  it("reads a store's metadata once across pages", async () => {
    const { adapter, calls } = withViews([fa(1, 0, "Deposit", "b1", "1"), fa(2, 0, "Deposit", "b1", "2")]);
    const source = transferSource(adapter);
    await transfers(source, { fromVersion: 0n, toVersion: 1n });
    await transfers(source, { fromVersion: 2n });
    expect(calls.filter((c) => c.function === METADATA)).toHaveLength(1);
    expect(calls.filter((c) => c.function === OWNER)).toHaveLength(2);
  });

  it("reads a store gone after its transaction as it stood before, and skips one gone both times", async () => {
    const { adapter, calls } = withViews(
      [fa(9, 0, "Withdraw", "a1", "3"), fa(9, 1, "Deposit", "dead", "3")],
      (call) => {
        if (call.store === store("a1") && call.version === 9n) throw new StreamdError("deleted");
        return undefined;
      },
    );
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "9:0", asset: APT, amount: 3n, from: ALICE, to: undefined },
    ]);
    expect(calls).toContainEqual({ function: METADATA, store: store("a1"), version: 8n });
    expect(calls).toContainEqual({ function: METADATA, store: store("dead"), version: 8n });
  });

  it("rejects the page on a network error from a view", async () => {
    const { adapter } = withViews([fa(1, 0, "Deposit", "b1", "1")], () => {
      throw new StreamdNetworkError("unavailable", 503);
    });
    const failed = transferSource(adapter).fetchEvents(TRANSFER_DESCRIPTOR, all);
    await expect(failed).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("skips events without the expected fields", async () => {
    const odd = { ...fa(1, 0, "Deposit", "b1", "1"), data: { store: "nope", amount: "1" } };
    const { adapter, calls } = withViews([odd, { ...fa(2, 0, "Deposit", "b1", "x") }, coin(3, 0, "Deposit", "nope", "1")]);
    expect(await transfers(transferSource(adapter))).toEqual([]);
    expect(calls).toEqual([]);
  });

  it("orders records ascending, and descending for orderBy desc", async () => {
    const events = [fa(1, 0, "Withdraw", "a1", "1"), fa(1, 1, "Deposit", "b1", "2"), fa(2, 0, "Deposit", "b1", "3")];
    expect((await transfers(transferSource(withViews(events).adapter))).map((t) => t.at)).toEqual(["1:0", "1:1", "2:0"]);
    const desc = await transfers(transferSource(withViews(events).adapter), { ...all, orderBy: "desc" });
    expect(desc.map((t) => t.at)).toEqual(["2:0", "1:1", "1:0"]);
  });

  it("refuses an adapter without view and a malformed asset", () => {
    expect(() => transferSource(new MemoryAdapter())).toThrow(StreamdError);
    const { adapter } = withViews([]);
    expect(() => transferSource(adapter, "nope")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::coin")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "")).toThrow(StreamdError);
  });
});
