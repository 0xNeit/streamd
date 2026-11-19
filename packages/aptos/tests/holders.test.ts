import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, ViewRequest } from "@streamd/core";
import { holdersAt, requireFungibleAsset } from "../src/holders.js";
import type { FungibleActivityRow, FungibleStoreRow } from "../src/indexer.js";
import type { FungibleAssetIndexer, HoldersAtOptions } from "../src/types.js";

const ASSET = `0x${"5".repeat(64)}`;
const long = (n: number) => `0x${n.toString(16).padStart(64, "0")}`;
const OWNER = long(0xb0b);

function current(n: number, amount: bigint, lastVersion: bigint): FungibleStoreRow {
  return { store: long(n), owner: OWNER, amount, lastVersion };
}

function act(
  n: number,
  version: bigint,
  amount: bigint,
  type: "Deposit" | "Withdraw" | string = "Deposit",
  extra: Partial<FungibleActivityRow> = {},
  eventIndex = 0,
): FungibleActivityRow {
  return {
    version,
    eventIndex,
    store: long(n),
    owner: OWNER,
    type: type.includes("::") ? type : `0x1::fungible_asset::${type}`,
    amount,
    success: true,
    gasFee: false,
    ...extra,
  };
}

interface Fake {
  processed: bigint[];
  stores: FungibleStoreRow[];
  activities: FungibleActivityRow[];
  /** Stores the lookup finds although the walk did not list them. */
  later?: FungibleStoreRow[];
}

/** An in-memory Indexer paging as the real one does, recording each call. */
function fakeIndexer(state: Fake) {
  const calls: string[] = [];
  const indexer: FungibleAssetIndexer = {
    async getFungibleAssetVersion() {
      calls.push("version");
      return state.processed.length > 1 ? state.processed.shift()! : state.processed[0]!;
    },
    async listFungibleStores(_asset, after, limit) {
      calls.push("stores");
      return state.stores.filter((row) => after === undefined || row.store > after).slice(0, limit);
    },
    async getFungibleStores(_asset, ids) {
      calls.push("lookup");
      return [...state.stores, ...(state.later ?? [])].filter((row) => ids.includes(row.store));
    },
    async listFungibleActivities(_asset, from, to, limit, offset = 0) {
      calls.push(`activities ${from}-${to}+${offset}`);
      return state.activities.filter((row) => row.version >= from && row.version <= to).slice(offset, offset + limit);
    },
  };
  return { indexer, calls };
}

type ViewAnswer = (request: ViewRequest, version: bigint) => unknown[];

/** A view adapter answering by function name; `views` records each call. */
function viewAdapter(answers: Record<string, ViewAnswer> = {}) {
  const views: string[] = [];
  const defaults: Record<string, ViewAnswer> = {
    "0x1::coin::paired_coin": () => [{ vec: [] }],
    "0x1::fungible_asset::is_asset_type_dispatchable": () => [false],
  };
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    async view(request, version) {
      views.push(`${request.function}@${version}`);
      const answer = answers[request.function] ?? defaults[request.function];
      if (answer === undefined) throw new StreamdError(`unexpected view ${request.function}`);
      return answer(request, version);
    },
  };
  return { adapter, views };
}

async function snapshot(
  state: Fake,
  options: HoldersAtOptions = { verify: "none" },
  version = 100n,
  adapter = viewAdapter().adapter,
) {
  const fake = fakeIndexer(state);
  const result = await holdersAt({ indexer: fake.indexer, adapter, run: (task) => task() }, ASSET, version, options);
  return { result, calls: fake.calls };
}

describe("holdersAt fold", () => {
  it("undoes each store's changes after the version, up to its last version only", async () => {
    const { result } = await snapshot({
      processed: [200n],
      stores: [current(1, 50n, 150n), current(2, 30n, 90n), current(3, 0n, 160n)],
      activities: [
        act(1, 120n, 20n, "Deposit"),
        act(3, 130n, 40n, "Withdraw"),
        act(1, 150n, 5n, "Withdraw"),
        // After store 1's row: not reflected in it, so not undone.
        act(1, 170n, 1000n, "Deposit"),
      ],
    });
    expect(result.holders).toEqual([
      { store: long(3), owner: OWNER, balance: 40n },
      { store: long(1), owner: OWNER, balance: 35n },
      { store: long(2), owner: OWNER, balance: 30n },
    ]);
    expect(result.total).toBe(105n);
    expect(result.indexedVersion).toBe(200n);
    expect(result.verified).toBe("none");
  });

  it("drops stores that held nothing at the version and breaks ties by store", async () => {
    const { result } = await snapshot({
      processed: [200n],
      stores: [current(2, 7n, 50n), current(1, 7n, 50n), current(3, 9n, 110n)],
      activities: [act(3, 110n, 9n)],
    });
    expect(result.holders.map((h) => h.store)).toEqual([long(1), long(2)]);
  });

  it("reads activities from the version after the snapshot through the processor's version", async () => {
    const { calls } = await snapshot({ processed: [200n], stores: [], activities: [] });
    expect(calls).toEqual(["version", "stores", "version", "activities 101-200+0"]);
  });

  it("reads no activities when the version is the processor's", async () => {
    const { calls, result } = await snapshot({ processed: [100n], stores: [current(1, 5n, 100n)], activities: [] });
    expect(calls).toEqual(["version", "stores", "version"]);
    expect(result.total).toBe(5n);
  });

  it("re-reads a version split at a page edge whole", async () => {
    const activities = [
      ...Array.from({ length: 99 }, (_, i) => act(1, 101n + BigInt(i), 1n, "Deposit")),
      act(1, 300n, 1n, "Deposit", {}, 0),
      act(1, 300n, 1n, "Deposit", {}, 1),
      act(1, 301n, 1n, "Deposit"),
    ];
    const { result, calls } = await snapshot({ processed: [400n], stores: [current(1, 200n, 301n)], activities });
    expect(calls.filter((c) => c.startsWith("activities"))).toEqual(["activities 101-400+0", "activities 300-400+0"]);
    expect(result.holders[0]!.balance).toBe(200n - 102n);
  });

  it("reads a version holding more than a page by offset", async () => {
    const activities = [
      ...Array.from({ length: 250 }, (_, i) => act(1, 150n, 1n, "Deposit", {}, i)),
      act(1, 151n, 1n, "Deposit"),
    ];
    const { result, calls } = await snapshot({ processed: [400n], stores: [current(1, 300n, 151n)], activities });
    expect(calls.filter((c) => c.startsWith("activities"))).toEqual([
      "activities 101-400+0",
      "activities 150-150+100",
      "activities 150-150+200",
      "activities 151-400+0",
    ]);
    expect(result.holders[0]!.balance).toBe(49n);
  });

  it("pages current stores by storage_id", async () => {
    const stores = Array.from({ length: 150 }, (_, i) => current(i + 1, 1n, 10n));
    const { result, calls } = await snapshot({ processed: [100n], stores, activities: [] });
    expect(calls.filter((c) => c === "stores")).toHaveLength(2);
    expect(result.holders).toHaveLength(150);
  });

  it("folds a store seen only in activities from 0 when it has no current row", async () => {
    const { result, calls } = await snapshot({
      processed: [200n],
      stores: [],
      activities: [act(4, 120n, 30n, "Withdraw", { owner: long(0xa11ce) }), act(4, 130n, 5n, "Deposit")],
    });
    expect(calls).toContain("lookup");
    expect(result.holders).toEqual([{ store: long(4), owner: long(0xa11ce), balance: 25n }]);
  });

  it("drops a store created after the walk passed it", async () => {
    const { result } = await snapshot({
      processed: [200n, 220n],
      stores: [],
      later: [current(4, 30n, 210n)],
      activities: [act(4, 210n, 30n, "Deposit")],
    });
    expect(result.holders).toEqual([]);
  });

  it("rejects a store the walk missed although it changed before the walk began", async () => {
    const err = await snapshot({
      processed: [200n],
      stores: [],
      later: [current(4, 30n, 190n)],
      activities: [act(4, 150n, 30n, "Deposit")],
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
  });

  it("adds gas fee rows back, failed transactions' other rows aside", async () => {
    const { result } = await snapshot({
      processed: [200n],
      stores: [current(1, 10n, 150n)],
      activities: [
        act(1, 120n, 3n, "0x1::aptos_coin::GasFee", { gasFee: true, success: false }),
        act(1, 130n, 50n, "Deposit", { success: false }),
      ],
    });
    expect(result.holders[0]!.balance).toBe(13n);
  });

  it("skips a row of another type that moves nothing, and refuses one that moves an amount", async () => {
    const quiet = await snapshot({
      processed: [200n],
      stores: [current(1, 10n, 150n)],
      activities: [act(1, 120n, 0n, "Frozen")],
    });
    expect(quiet.result.total).toBe(10n);
    const err = await snapshot({
      processed: [200n],
      stores: [current(1, 10n, 150n)],
      activities: [act(1, 120n, 4n, "0x9::custom::Moved")],
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect((err as Error).message).toContain("0x9::custom::Moved");
  });

  it("rejects a fold that goes negative", async () => {
    const err = await snapshot({
      processed: [200n],
      stores: [current(1, 10n, 150n)],
      activities: [act(1, 120n, 11n, "Deposit")],
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as Error).message).toContain("negative");
  });

  it("refuses a version the Indexer has not processed", async () => {
    const err = await snapshot({ processed: [50n], stores: [], activities: [] }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("waits for the processor to reach the newest row it read", async () => {
    const { result, calls } = await snapshot({
      processed: [200n, 200n, 260n],
      stores: [current(1, 10n, 250n)],
      activities: [act(1, 250n, 10n, "Deposit")],
    });
    expect(calls.filter((c) => c === "version")).toHaveLength(3);
    expect(result.indexedVersion).toBe(260n);
    expect(result.holders).toEqual([]);
  });

  it("refuses coin types and APT before any request", () => {
    expect(() => requireFungibleAsset("0x1::aptos_coin::AptosCoin")).toThrow(StreamdError);
    expect(() => requireFungibleAsset("0xa")).toThrow("APT");
    expect(() => requireFungibleAsset("nope")).toThrow(StreamdError);
    expect(requireFungibleAsset("0x5555555555555555555555555555555555555555555555555555555555555555")).toBe(ASSET);
  });
});
