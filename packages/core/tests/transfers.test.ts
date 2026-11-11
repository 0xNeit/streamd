import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { TRANSFER_DESCRIPTOR, formatAmount, transferSource } from "../src/transfers.js";
import type { EventAdapter, FetchOptions, StreamdEvent, Transfer, ViewRequest } from "../src/types.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT = long("a");
const USDC = long("bae2");
const ALICE = long("a11ce");
const BOB = long("b0b");
const CAROL = long("ca201");

const OWNER = "0x1::object::owner";
const METADATA = "0x1::fungible_asset::store_metadata";
const CREATOR = "0x4::token::creator";
const NFT = long("deed");
const PLAIN = long("ab1e");
/** Token objects and their creators; any other object aborts `0x4::token::creator`. */
const TOKENS: Record<string, string> = { [NFT]: CAROL };

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

function moved(version: number, eventIndex: number, object: string, from: string, to: string): StreamdEvent {
  return { type: "0x1::object::Transfer", data: { object, from, to }, version: BigInt(version), sequenceNumber: 0n, eventIndex };
}

const LEGACY_ID = { token_data_id: { creator: long("c0de"), collection: "Apes", name: "Ape #1" }, property_version: "0" };
const LEGACY = `${long("c0de")}::Apes::Ape #1::0`;

function legacy(version: number, eventIndex: number, side: "Withdraw" | "Deposit", account: string, id: unknown = LEGACY_ID): StreamdEvent {
  return { type: `0x3::token::Token${side}`, data: { account, id, amount: "1" }, version: BigInt(version), sequenceNumber: 0n, eventIndex };
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
      if (request.function === CREATOR) {
        if (TOKENS[call.store] === undefined) throw new StreamdError("aborted");
        return [TOKENS[call.store]];
      }
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
    const coinOnly = withViews(events);
    expect((await transfers(transferSource(coinOnly.adapter, "0x01::aptos_coin::AptosCoin"))).map((t) => t.at)).toEqual(["3:0"]);
    expect(coinOnly.calls).toEqual([]);
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

  it("reports a token object's transfer as one record, reading whether it is a token once", async () => {
    const { adapter, calls } = withViews([moved(4, 2, long("DEED"), long("A11CE"), BOB), moved(6, 0, NFT, BOB, CAROL)]);
    const source = transferSource(adapter);
    expect(await transfers(source, { fromVersion: 0n, toVersion: 4n })).toEqual([
      { at: "4:2", asset: NFT, amount: 1n, from: ALICE, to: BOB },
    ]);
    expect(await transfers(source, { fromVersion: 5n })).toEqual([{ at: "6:0", asset: NFT, amount: 1n, from: BOB, to: CAROL }]);
    expect(calls).toEqual([{ function: CREATOR, store: NFT, version: 4n }]);
  });

  it("skips an object that is not a token, without reading it again", async () => {
    const { adapter, calls } = withViews([moved(4, 0, PLAIN, ALICE, BOB), moved(6, 0, PLAIN, BOB, ALICE)]);
    const source = transferSource(adapter);
    expect(await transfers(source, { fromVersion: 0n, toVersion: 4n })).toEqual([]);
    expect(await transfers(source, { fromVersion: 5n })).toEqual([]);
    expect(calls).toEqual([
      { function: CREATOR, store: PLAIN, version: 4n },
      { function: CREATOR, store: PLAIN, version: 3n },
    ]);
  });

  it("reads a token burned after its transfer as it stood before the transaction", async () => {
    const { adapter } = withViews([moved(9, 0, NFT, ALICE, BOB)], (call) => {
      if (call.version === 9n) throw new StreamdError("burned");
      return undefined;
    });
    expect(await transfers(transferSource(adapter))).toEqual([{ at: "9:0", asset: NFT, amount: 1n, from: ALICE, to: BOB }]);
  });

  it("pairs legacy token withdraws and deposits by token id, with no view", async () => {
    const { adapter, calls } = withViews([
      legacy(3, 0, "Withdraw", ALICE),
      legacy(3, 1, "Deposit", BOB),
      legacy(4, 0, "Deposit", CAROL, { ...LEGACY_ID, property_version: "2" }),
    ]);
    expect(await transfers(transferSource(adapter))).toEqual([
      { at: "3:1", asset: LEGACY, amount: 1n, from: ALICE, to: BOB },
      { at: "4:0", asset: `${long("c0de")}::Apes::Ape #1::2`, amount: 1n, from: undefined, to: CAROL },
    ]);
    expect(calls).toEqual([]);
  });

  it("filters tokens by object address or legacy id before any read", async () => {
    const events = [
      moved(1, 0, NFT, ALICE, BOB),
      moved(2, 0, PLAIN, ALICE, BOB),
      legacy(3, 0, "Deposit", BOB),
      coin(4, 0, "Deposit", BOB, "7"),
    ];
    const byObject = withViews(events);
    expect((await transfers(transferSource(byObject.adapter, long("DEED")))).map((t) => t.at)).toEqual(["1:0"]);
    expect(byObject.calls).toEqual([{ function: CREATOR, store: NFT, version: 1n }]);
    const byId = withViews(events);
    expect((await transfers(transferSource(byId.adapter, `${long("C0DE")}::Apes::Ape #1::00`))).map((t) => t.at)).toEqual(["3:0"]);
    expect(byId.calls).toEqual([]);
    const byCoin = withViews(events);
    expect((await transfers(transferSource(byCoin.adapter, "0x1::aptos_coin::AptosCoin"))).map((t) => t.at)).toEqual(["4:0"]);
    expect(byCoin.calls).toEqual([]);
  });

  it("skips malformed object and legacy token events", async () => {
    const { adapter, calls } = withViews([
      { ...moved(1, 0, NFT, ALICE, BOB), data: { object: NFT, from: ALICE } },
      legacy(2, 0, "Deposit", BOB, { token_data_id: { creator: long("c0de"), collection: "Apes" }, property_version: "0" }),
      legacy(3, 0, "Deposit", BOB, { ...LEGACY_ID, property_version: "x" }),
      legacy(4, 0, "Deposit", "nope"),
    ]);
    expect(await transfers(transferSource(adapter))).toEqual([]);
    expect(calls).toEqual([]);
  });

  it("rejects the page on a network error from the token read, and reads again on retry", async () => {
    let down = true;
    const { adapter, calls } = withViews([moved(1, 0, NFT, ALICE, BOB)], () => {
      if (down) throw new StreamdNetworkError("unavailable", 503);
      return undefined;
    });
    const source = transferSource(adapter);
    await expect(source.fetchEvents(TRANSFER_DESCRIPTOR, all)).rejects.toBeInstanceOf(StreamdNetworkError);
    down = false;
    expect(await transfers(source)).toEqual([{ at: "1:0", asset: NFT, amount: 1n, from: ALICE, to: BOB }]);
    expect(calls).toHaveLength(2);
  });

  it("refuses an adapter without view and a malformed asset", () => {
    expect(() => transferSource(new MemoryAdapter())).toThrow(StreamdError);
    const { adapter } = withViews([]);
    expect(() => transferSource(adapter, "nope")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::coin")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::a::b junk")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::lp::LP<0x1::aptos_coin::AptosCoin>")).not.toThrow();
    expect(() => transferSource(adapter, "0x1::a::b::x")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::a::b::")).toThrow(StreamdError);
    expect(() => transferSource(adapter, "0x1::My Coll::Token #1::0")).not.toThrow();
  });
});

describe("transferSource account filter", () => {
  const events = [
    fa(1, 0, "Withdraw", "a1", "5"),
    fa(1, 1, "Deposit", "b1", "5"),
    fa(2, 0, "Withdraw", "b1", "7"),
    fa(2, 1, "Deposit", "c1", "7"),
    fa(3, 0, "Deposit", "a2", "9"),
    fa(4, 0, "Withdraw", "a1", "3"),
    fa(5, 0, "Withdraw", "a1", "2"),
    fa(5, 1, "Deposit", "a1", "2"),
    moved(6, 0, NFT, BOB, ALICE),
    moved(7, 0, NFT, ALICE, CAROL),
  ];
  const of = (who: string) => async () => who;

  it("keeps what the account sent or received, each once", async () => {
    const source = transferSource(withViews(events).adapter, undefined, of(ALICE));
    expect(await transfers(source)).toEqual([
      { at: "1:1", asset: APT, amount: 5n, from: ALICE, to: BOB },
      { at: "3:0", asset: USDC, amount: 9n, from: undefined, to: ALICE },
      { at: "4:0", asset: APT, amount: 3n, from: ALICE, to: undefined },
      { at: "5:1", asset: APT, amount: 2n, from: ALICE, to: ALICE },
      { at: "6:0", asset: NFT, amount: 1n, from: BOB, to: ALICE },
      { at: "7:0", asset: NFT, amount: 1n, from: ALICE, to: CAROL },
    ]);
  });

  it("drops records between other accounts, and combines with asset and desc", async () => {
    const carol = transferSource(withViews(events).adapter, undefined, of(CAROL));
    expect((await transfers(carol)).map((t) => t.at)).toEqual(["2:1", "7:0"]);
    const usdc = transferSource(withViews(events).adapter, USDC, of(ALICE));
    expect((await transfers(usdc)).map((t) => t.at)).toEqual(["3:0"]);
    const desc = transferSource(withViews(events).adapter, undefined, of(CAROL));
    expect((await transfers(desc, { ...all, orderBy: "desc" })).map((t) => t.at)).toEqual(["7:0", "2:1"]);
  });

  it("resolves the account before fetching, and a failure fetches nothing", async () => {
    const { adapter, calls } = withViews(events);
    let fetched = 0;
    const counted: EventAdapter = { ...adapter, fetchEvents: (d, o) => (fetched++, adapter.fetchEvents(d, o)) };
    const source = transferSource(counted, undefined, () => Promise.reject(new StreamdError("no such name")));
    await expect(transfers(source)).rejects.toThrow("no such name");
    expect(fetched).toBe(0);
    expect(calls).toEqual([]);
  });
});

describe("transferSource with asset info", () => {
  const COIN = "0x1::aptos_coin::AptosCoin";
  const pairAt = (version: number, name = "a1", to = "b1"): StreamdEvent[] => [
    fa(version, 0, "Withdraw", name, "100"),
    fa(version, 1, "Deposit", to, "100"),
  ];

  /** `withViews` plus symbol and decimals, recording each info request in full. */
  function infoViews(events: StreamdEvent[], fail?: () => unknown) {
    const requests: { fn: string; args: unknown; types: unknown; version: bigint }[] = [];
    const inner = withViews(events);
    const adapter: EventAdapter = {
      ...inner.adapter,
      view: async (request: ViewRequest, version: bigint, descriptor) => {
        if (!/::(symbol|decimals)$/.test(request.function)) {
          return inner.adapter.view!(request, version, descriptor);
        }
        requests.push({
          fn: request.function,
          args: request.functionArguments,
          types: request.typeArguments,
          version,
        });
        const failure = fail?.();
        if (failure !== undefined) throw failure;
        return request.function.endsWith("symbol") ? ["APT"] : [8];
      },
    };
    return { adapter, requests };
  }

  const fetch = async (source: EventAdapter, options: FetchOptions = all) =>
    (await source.fetchEvents(TRANSFER_DESCRIPTOR, options)).events.map((e) => e.data as Transfer);

  it("reads a fungible asset's details at the record's version", async () => {
    const { adapter, requests } = infoViews(pairAt(7));
    const [record] = await fetch(transferSource(adapter, undefined, undefined, true));
    expect(record).toEqual({ asset: APT, amount: 100n, from: ALICE, to: BOB, symbol: "APT", decimals: 8 });
    expect(requests.sort((a, b) => a.fn.localeCompare(b.fn))).toEqual([
      { fn: "0x1::fungible_asset::decimals", args: [APT], types: ["0x1::object::ObjectCore"], version: 7n },
      { fn: "0x1::fungible_asset::symbol", args: [APT], types: ["0x1::object::ObjectCore"], version: 7n },
    ]);
  });

  it("reads a coin's details with its type argument", async () => {
    const { adapter, requests } = infoViews([coin(3, 0, "Deposit", ALICE, "5")]);
    const [record] = await fetch(transferSource(adapter, undefined, undefined, true));
    expect(record!.symbol).toBe("APT");
    expect(requests.map((r) => [r.fn, r.args, r.types]).sort()).toEqual([
      ["0x1::coin::decimals", [], [COIN]],
      ["0x1::coin::symbol", [], [COIN]],
    ]);
  });

  it("accepts decimals reported as a string", async () => {
    const inner = withViews(pairAt(1));
    const adapter: EventAdapter = {
      ...inner.adapter,
      view: async (request, version, descriptor) => {
        if (request.function.endsWith("::decimals")) return ["6"];
        if (request.function.endsWith("::symbol")) return ["X"];
        return inner.adapter.view!(request, version, descriptor);
      },
    };
    const [record] = await fetch(transferSource(adapter, undefined, undefined, true));
    expect(record!.decimals).toBe(6);
  });

  it("reads each asset once across pages", async () => {
    const { adapter, requests } = infoViews([...pairAt(1), ...pairAt(2)]);
    const source = transferSource(adapter, undefined, undefined, true);
    await fetch(source, { fromVersion: 1n, toVersion: 1n, limit: 10 });
    await fetch(source, { fromVersion: 2n, limit: 10 });
    expect(requests).toHaveLength(2);
  });

  it("reads nothing with the option off", async () => {
    const { adapter, requests } = infoViews(pairAt(1));
    const [record] = await fetch(transferSource(adapter));
    expect(record).not.toHaveProperty("symbol");
    expect(requests).toEqual([]);
  });

  it("reads nothing for token records and adds no fields", async () => {
    const { adapter, requests } = infoViews([
      moved(1, 0, NFT, ALICE, BOB),
      legacy(2, 0, "Deposit", ALICE),
    ]);
    const records = await fetch(transferSource(adapter, undefined, undefined, true));
    expect(records).toHaveLength(2);
    for (const record of records) expect(record).not.toHaveProperty("symbol");
    expect(requests).toEqual([]);
  });

  it("reads nothing for records the account filter dropped", async () => {
    const { adapter, requests } = infoViews(pairAt(1));
    const records = await fetch(transferSource(adapter, undefined, () => Promise.resolve(CAROL), true));
    expect(records).toEqual([]);
    expect(requests).toEqual([]);
  });

  it("leaves the fields off when a view aborts, without asking again", async () => {
    const { adapter, requests } = infoViews([...pairAt(1), ...pairAt(2)], () => new StreamdError("aborted"));
    const records = await fetch(transferSource(adapter, undefined, undefined, true));
    expect(records).toHaveLength(2);
    expect(records[0]).not.toHaveProperty("symbol");
    expect(requests).toHaveLength(2);
  });

  it("fails the page on a network failure, and reads again next time", async () => {
    let failing = true;
    const { adapter, requests } = infoViews(pairAt(1), () =>
      failing ? new StreamdNetworkError("down", 503) : undefined,
    );
    const source = transferSource(adapter, undefined, undefined, true);
    await expect(fetch(source)).rejects.toBeInstanceOf(StreamdNetworkError);
    failing = false;
    const [record] = await fetch(source);
    expect(record!.symbol).toBe("APT");
    expect(requests.length).toBeGreaterThan(2);
  });

  it.each([
    ["a symbol that is not text", [7], [8]],
    ["no symbol", [], [8]],
    ["decimals that are not a number", ["APT"], ["x"]],
    ["no decimals", ["APT"], []],
    ["null decimals", ["APT"], [null]],
  ])("rejects %s", async (_name, symbol, decimals) => {
    const inner = withViews(pairAt(1));
    const adapter: EventAdapter = {
      ...inner.adapter,
      view: async (request, version, descriptor) => {
        if (request.function.endsWith("::symbol")) return symbol;
        if (request.function.endsWith("::decimals")) return decimals;
        return inner.adapter.view!(request, version, descriptor);
      },
    };
    await expect(fetch(transferSource(adapter, undefined, undefined, true))).rejects.toThrow("malformed");
  });
});

describe("formatAmount", () => {
  it.each([
    [0n, 8, "0"],
    [100000000n, 8, "1"],
    [150000000n, 8, "1.5"],
    [1n, 8, "0.00000001"],
    [1500000n, 8, "0.015"],
    [123456789n, 3, "123456.789"],
    [120n, 0, "120"],
    [-150n, 2, "-1.5"],
    [-1n, 3, "-0.001"],
    [2n ** 127n, 6, "170141183460469231731687303715884.105728"],
  ])("formats %p with %p decimals as %p", (amount, decimals, expected) => {
    expect(formatAmount(amount as bigint, decimals as number)).toBe(expected as string);
  });

  it("handles the widest decimals", () => {
    expect(formatAmount(1n, 255)).toBe(`0.${"0".repeat(254)}1`);
  });

  it.each([-1, 256, 1.5, Number.NaN, "8" as unknown as number, undefined as unknown as number])(
    "refuses decimals of %p",
    (decimals) => {
      expect(() => formatAmount(1n, decimals)).toThrow(StreamdError);
    },
  );

  it("refuses an amount that is not a bigint", () => {
    expect(() => formatAmount(1 as unknown as bigint, 8)).toThrow(StreamdError);
  });
});

describe("transfers with requestTimeout", () => {
  /** `withViews`, except that the first view read never settles. */
  function firstViewHangs() {
    const inner = withViews([fa(1, 0, "Withdraw", "a1", "100"), fa(1, 1, "Deposit", "b1", "100")]);
    let views = 0;
    const adapter: EventAdapter = {
      ...inner.adapter,
      view: (request, version, descriptor) =>
        ++views === 1 ? new Promise<never>(() => {}) : inner.adapter.view!(request, version, descriptor),
    };
    return { adapter, views: () => views };
  }

  it("fails a hung read and reads again on the next page", async () => {
    const { adapter } = firstViewHangs();
    const source = transferSource(adapter, undefined, undefined, false, 2);
    await expect(source.fetchEvents(TRANSFER_DESCRIPTOR, all)).rejects.toBeInstanceOf(StreamdNetworkError);
    expect((await source.fetchEvents(TRANSFER_DESCRIPTOR, all)).events).toHaveLength(1);
  });

  it("lets getTransfers retry past a hung read", async () => {
    const { adapter, views } = firstViewHangs();
    const client = new StreamdClient({ adapter, requestTimeout: 2 });
    const records = await client.getTransfers({ fromVersion: 0n, retries: 3 });
    expect(records.map((r) => r.data.amount)).toEqual([100n]);
    expect(views()).toBeGreaterThan(1);
  });

  it("does not let a retry join a read left hung by a slow page", async () => {
    const inner = withViews([fa(1, 0, "Withdraw", "a1", "100"), fa(1, 1, "Deposit", "b1", "100")]);
    let views = 0;
    let fetches = 0;
    const adapter: EventAdapter = {
      ...inner.adapter,
      // The first page arrives late, so its read starts close to the page's own timeout.
      fetchEvents: async (d, o) => {
        if (++fetches === 1) await new Promise((r) => setTimeout(r, 40));
        return inner.adapter.fetchEvents(d, o);
      },
      view: (request, version, descriptor) =>
        ++views === 1 ? new Promise<never>(() => {}) : inner.adapter.view!(request, version, descriptor),
    };
    const client = new StreamdClient({ adapter, requestTimeout: 50 });
    const errors: unknown[] = [];
    const record = await new Promise<StreamdEvent<Transfer> | undefined>((resolve) => {
      client.transfers(resolve, {
        fromVersion: 0n,
        pollInterval: 1,
        maxBackoff: 1,
        maxErrors: 2,
        onError: (err) => {
          errors.push(err);
          if (errors.length === 2) resolve(undefined);
        },
      });
    });
    client.destroy();
    expect(record?.data.amount).toBe(100n);
    expect(errors).toHaveLength(1);
  });

  it("starts no reads from a page abandoned while its fetch was still running", async () => {
    const inner = withViews([fa(1, 0, "Withdraw", "a1", "100"), fa(1, 1, "Deposit", "b1", "100")]);
    let fetches = 0;
    // The first page's fetch outlives its timeout; reads hang until just after it returns.
    // Timeout at 100, retry at about 101 whose fetch returns at about 181; the first returns at 160.
    const delays = [160, 80];
    const started = performance.now();
    const adapter: EventAdapter = {
      ...inner.adapter,
      fetchEvents: async (d, o) => {
        const delay = delays[fetches++] ?? 0;
        await new Promise((r) => setTimeout(r, delay));
        return inner.adapter.fetchEvents(d, o);
      },
      view: (request, version, descriptor) =>
        performance.now() - started < 170
          ? new Promise<never>(() => {})
          : inner.adapter.view!(request, version, descriptor),
    };
    const client = new StreamdClient({ adapter, requestTimeout: 100 });
    const errors: unknown[] = [];
    const record = await new Promise<StreamdEvent<Transfer> | undefined>((resolve) => {
      client.transfers(resolve, {
        fromVersion: 0n,
        pollInterval: 1,
        maxBackoff: 1,
        maxErrors: 2,
        onError: (err) => {
          errors.push(err);
          if (errors.length === 2) resolve(undefined);
        },
      });
    });
    client.destroy();
    expect(record?.data.amount).toBe(100n);
    expect(errors).toHaveLength(1);
  });

  it("starts no asset-info read from a page overtaken during its store reads", async () => {
    // Timeout at 100: the first page's store reads end at about 110, after the retry
    // started at about 101. Asset-info reads hang until 180, and the retry's begin after it.
    const inner = withViews([fa(1, 0, "Withdraw", "a1", "100"), fa(1, 1, "Deposit", "b1", "100")]);
    let fetches = 0;
    const started = performance.now();
    const adapter: EventAdapter = {
      ...inner.adapter,
      fetchEvents: async (d, o) => {
        await new Promise((r) => setTimeout(r, ++fetches === 1 ? 60 : 80));
        return inner.adapter.fetchEvents(d, o);
      },
      view: async (request, version, descriptor) => {
        if (/::(symbol|decimals)$/.test(request.function)) {
          if (performance.now() - started < 180) return new Promise<never>(() => {});
          return request.function.endsWith("symbol") ? ["APT"] : [8];
        }
        if (fetches === 1) await new Promise((r) => setTimeout(r, 50));
        return inner.adapter.view!(request, version, descriptor);
      },
    };
    const client = new StreamdClient({ adapter, requestTimeout: 100 });
    const errors: unknown[] = [];
    const record = await new Promise<StreamdEvent<Transfer> | undefined>((resolve) => {
      client.transfers(resolve, {
        fromVersion: 0n,
        withAssetInfo: true,
        pollInterval: 10,
        maxBackoff: 20,
        maxErrors: 2,
        onError: (err) => {
          errors.push(err);
          if (errors.length === 2) resolve(undefined);
        },
      });
    });
    client.destroy();
    expect(record?.data.symbol).toBe("APT");
    expect(errors).toHaveLength(1);
  });

  it("recovers a live subscription from a hung read", async () => {
    const { adapter } = firstViewHangs();
    const client = new StreamdClient({ adapter, requestTimeout: 2 });
    const record = await new Promise<StreamdEvent<Transfer>>((resolve) => {
      client.transfers(resolve, {
        fromVersion: 0n,
        pollInterval: 1,
        maxBackoff: 1,
        maxErrors: 10,
        onError: () => {},
      });
    });
    client.destroy();
    expect(record.data.amount).toBe(100n);
  });
});
