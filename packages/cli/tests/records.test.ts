import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Activity, PackageUpgrade, Sale, Swap } from "@streamd/aptos";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "@streamd/core";
import type { ActivityCommandOptions, SalesCommandOptions, SwapsCommandOptions, UpgradesCommandOptions } from "../src/args.js";
import { breakingChanges, runActivity, runSales, runSwaps, runUpgrades } from "../src/records.js";
import type { RecordsIO } from "../src/records.js";

const SWAP_TYPE = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3";
const SALE_TYPE = "0x584b50b999c78ade62f8359c91b5165ff390338d45f8e55969a04e65d76258c9::events::ListingFilledEvent";

function swap(version: number): StreamdEvent<Swap> {
  return {
    type: SWAP_TYPE,
    data: { protocol: "hyperion", poolId: "0xp", trader: "0xt", assetIn: "0xa", amountIn: 10n ** 20n + BigInt(version), assetOut: "0xb", amountOut: 7n },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

function sale(version: number): StreamdEvent<Sale> {
  return {
    type: SALE_TYPE,
    data: { marketplace: "wapal", kind: "listing", token: "0xk", seller: "0xs", buyer: "0xb", price: 10n ** 19n, royalty: 5n, fee: 2n },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

/** A client whose `follow` and `get` methods are plain events from `adapter`, recording what each call was given. */
function harness<T>(adapter: EventAdapter, type: string, names: { follow: string; get: string; narrowing: string[] }, signal = new AbortController().signal) {
  const descriptor = { kind: "module", eventType: type } as EventDescriptor;
  const inner = new StreamdClient({ adapter });
  const seen: Record<string, unknown>[] = [];
  const lines: string[] = [];
  const errors: string[] = [];
  const strip = (options: Record<string, unknown>) => {
    seen.push(options);
    return Object.fromEntries(Object.entries(options).filter(([k]) => !names.narrowing.includes(k)));
  };
  const client: Record<string, unknown> = {
    [names.follow]: (callback: (event: StreamdEvent<T>) => unknown, options: Record<string, unknown> = {}) =>
      inner.on(descriptor, callback as never, strip(options)),
    [names.get]: async (filter: Record<string, unknown> = {}) => inner.getEvents(descriptor, strip(filter)),
  };
  const io: RecordsIO<never> = { client: client as never, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal };
  const versions = () => lines.map((l) => (JSON.parse(l) as { version: string }).version);
  return { io, client, inner, seen, lines, errors, versions };
}

function swaps(adapter: EventAdapter, signal?: AbortSignal) {
  const h = harness<Swap>(adapter, SWAP_TYPE, { follow: "swaps", get: "getSwaps", narrowing: ["protocol", "asset"] }, signal);
  const run = async (options: Partial<SwapsCommandOptions>) => {
    const code = await runSwaps({ network: "mainnet", ...options }, h.io);
    h.inner.destroy();
    return code;
  };
  return { ...h, run };
}

function sales(adapter: EventAdapter) {
  const h = harness<Sale>(adapter, SALE_TYPE, { follow: "sales", get: "getSales", narrowing: ["marketplace", "token"] });
  const run = async (options: Partial<SalesCommandOptions>) => {
    const code = await runSales({ network: "mainnet", ...options }, h.io);
    h.inner.destroy();
    return code;
  };
  return { ...h, run };
}

const swapEvents = [swap(1), swap(2), swap(3), swap(4)];
const saleEvents = [sale(1), sale(2), sale(3)];

describe("runSwaps", () => {
  it("follows with the narrowing and listener options, printing bigints as decimal strings", async () => {
    const h = swaps(new MemoryAdapter(swapEvents));
    expect(await h.run({ protocol: ["hyperion", "tapp"], asset: "0xa", fromVersion: 2n, pollInterval: 1, batchSize: 50, limit: 2 })).toBe(0);
    expect(h.versions()).toEqual(["2", "3"]);
    expect(JSON.parse(h.lines[0]!).data).toEqual({
      protocol: "hyperion", poolId: "0xp", trader: "0xt", assetIn: "0xa", amountIn: "100000000000000000002", assetOut: "0xb", amountOut: "7",
    });
    const { signal, onError, ...given } = h.seen[0]!;
    expect(given).toEqual({ protocol: ["hyperion", "tapp"], asset: "0xa", fromVersion: 2n, pollInterval: 1, batchSize: 50 });
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(typeof onError).toBe("function");
  });

  it("passes nothing unset", async () => {
    const h = swaps(new MemoryAdapter(swapEvents));
    await h.run({ fromVersion: 1n, limit: 1, pollInterval: 1 });
    expect(Object.keys(h.seen[0]!).sort()).toEqual(["fromVersion", "onError", "pollInterval", "signal"]);
  });

  it("reads a range through getSwaps with order, limit and retries", async () => {
    const h = swaps(new MemoryAdapter(swapEvents));
    expect(await h.run({ protocol: ["cellana"], fromVersion: 1n, toVersion: 3n, order: "desc", limit: 2 })).toBe(0);
    expect(h.versions()).toEqual(["3", "2"]);
    const { signal, ...given } = h.seen[0]!;
    expect(given).toEqual({ protocol: ["cellana"], fromVersion: 1n, toVersion: 3n, orderBy: "desc", limit: 2, retries: 5 });
    expect(signal).toBeInstanceOf(AbortSignal);
  });

  it("saves its position to a cursor file and resumes from it", async () => {
    const file = join(mkdtempSync(join(tmpdir(), "streamd-swaps-")), "s.cursor");
    const controller = new AbortController();
    const first = swaps(new MemoryAdapter(swapEvents), controller.signal);
    const done = first.run({ fromVersion: 1n, pollInterval: 1, cursorFile: file });
    setTimeout(() => controller.abort(), 30);
    expect(await done).toBe(0);
    expect(first.versions()).toEqual(["1", "2", "3", "4"]);
    const cursor = readFileSync(file, "utf8").trim();
    const second = swaps(new MemoryAdapter([...swapEvents, swap(5)]));
    expect(await second.run({ limit: 1, pollInterval: 1, cursorFile: file })).toBe(0);
    expect(second.seen[0]!.cursor).toBe(cursor);
    expect(second.versions()).toEqual(["5"]);
  });

  it("exits 1 with the client's message when it refuses a protocol, following or over a range", async () => {
    const refuse = () => {
      throw new StreamdError("Unknown swap protocol: uniswap; expected one of hyperion, tapp");
    };
    const follow = swaps(new MemoryAdapter(swapEvents));
    follow.client.swaps = refuse;
    expect(await follow.run({ protocol: ["uniswap"] })).toBe(1);
    expect(follow.errors).toEqual(["streamd: Unknown swap protocol: uniswap; expected one of hyperion, tapp"]);
    const range = swaps(new MemoryAdapter(swapEvents));
    range.client.getSwaps = async () => refuse();
    expect(await range.run({ protocol: ["uniswap"], fromVersion: 1n, toVersion: "head" })).toBe(1);
    expect(range.errors).toEqual(["streamd: Unknown swap protocol: uniswap; expected one of hyperion, tapp"]);
  });
});

describe("runSales", () => {
  it("follows with the narrowing, printing bigints as decimal strings", async () => {
    const h = sales(new MemoryAdapter(saleEvents));
    expect(await h.run({ marketplace: ["wapal"], token: "0xk", fromVersion: 1n, pollInterval: 1, limit: 1 })).toBe(0);
    expect(JSON.parse(h.lines[0]!).data).toEqual({
      marketplace: "wapal", kind: "listing", token: "0xk", seller: "0xs", buyer: "0xb", price: "10000000000000000000", royalty: "5", fee: "2",
    });
    const { signal: _s, onError: _o, ...given } = h.seen[0]!;
    expect(given).toEqual({ marketplace: ["wapal"], token: "0xk", fromVersion: 1n, pollInterval: 1 });
  });

  it("reads a range through getSales, ascending by default", async () => {
    const h = sales(new MemoryAdapter(saleEvents));
    expect(await h.run({ fromVersion: 2n, toVersion: "head", order: undefined })).toBe(0);
    expect(h.versions()).toEqual(["2", "3"]);
    expect(h.seen[0]!.orderBy).toBe("asc");
    expect("marketplace" in h.seen[0]!).toBe(false);
  });

  it("exits 1 when a write fails", async () => {
    const h = sales(new MemoryAdapter(saleEvents));
    h.io.write = () => {
      throw new Error("disk full");
    };
    expect(await h.run({ fromVersion: 1n, toVersion: 3n })).toBe(1);
    expect(h.errors).toEqual(["streamd: disk full"]);
  });
});

function entry(version: number): StreamdEvent<Activity> {
  return {
    type: "0x1::transaction_fee::FeeStatement",
    data: { direction: "received", transfers: [], balanceChanges: [{ asset: "0xa", delta: 10n ** 19n }], events: [] },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

/** A client whose `activity` and `getActivity` read plain events, recording the address and options each call was given. */
function activity(adapter: EventAdapter) {
  const descriptor = { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" } as EventDescriptor;
  const inner = new StreamdClient({ adapter });
  const seen: { address: string; options: Record<string, unknown> }[] = [];
  const lines: string[] = [];
  const errors: string[] = [];
  const strip = (address: string, options: Record<string, unknown>) => {
    seen.push({ address, options });
    const { withAssetInfo: _w, ...rest } = options;
    return rest;
  };
  const client = {
    activity: (address: string, callback: never, options: Record<string, unknown> = {}) => inner.on(descriptor, callback, strip(address, options)),
    getActivity: async (address: string, filter: Record<string, unknown> = {}) => inner.getEvents(descriptor, strip(address, filter)),
  };
  const io: RecordsIO<never> = { client: client as never, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal: new AbortController().signal };
  const run = async (options: Partial<ActivityCommandOptions>) => {
    const code = await runActivity({ network: "mainnet", address: "alice.apt", ...options }, io);
    inner.destroy();
    return code;
  };
  return { run, seen, lines, errors, client };
}

const entries = [entry(1), entry(2), entry(3)];

describe("runActivity", () => {
  it("follows the address with withAssetInfo, printing bigints as decimal strings", async () => {
    const h = activity(new MemoryAdapter(entries));
    expect(await h.run({ withAssetInfo: true, fromVersion: 2n, pollInterval: 1, limit: 1 })).toBe(0);
    expect(JSON.parse(h.lines[0]!)).toMatchObject({ version: "2", data: { direction: "received", balanceChanges: [{ asset: "0xa", delta: "10000000000000000000" }] } });
    const { signal: _s, onError: _o, ...given } = h.seen[0]!.options;
    expect(h.seen[0]!.address).toBe("alice.apt");
    expect(given).toEqual({ withAssetInfo: true, fromVersion: 2n, pollInterval: 1 });
  });

  it("reads a range through getActivity, leaving withAssetInfo out when unset", async () => {
    const h = activity(new MemoryAdapter(entries));
    expect(await h.run({ address: "0x1", fromVersion: 1n, toVersion: 2n, order: "desc" })).toBe(0);
    expect(h.lines.map((l) => (JSON.parse(l) as { version: string }).version)).toEqual(["2", "1"]);
    const { signal: _s, ...given } = h.seen[0]!.options;
    expect(h.seen[0]!.address).toBe("0x1");
    expect(given).toEqual({ fromVersion: 1n, toVersion: 2n, orderBy: "desc", retries: 5 });
  });

  it("exits 1 with the client's message when it refuses the address", async () => {
    const h = activity(new MemoryAdapter(entries));
    h.client.getActivity = async () => {
      throw new StreamdError("activity needs an address or Aptos name");
    };
    expect(await h.run({ fromVersion: 1n, toVersion: "head" })).toBe(1);
    expect(h.errors).toEqual(["streamd: activity needs an address or Aptos name"]);
  });
});

const PUBLISH = "0x1::code::PublishPackage";
const fn = (name: string, params: string[] = []) => ({ name, visibility: "public", typeParameters: [], params, returns: [] });
const none = () => ({ added: [], changed: [], removed: [] });

function upgrade(version: number, change: "added" | "removed" | "changed" | "event" | "unsafe"): StreamdEvent<PackageUpgrade> {
  const data: PackageUpgrade = { address: "0x1", isUpgrade: true, events: none(), entryFunctions: none(), views: none() };
  if (change === "added") data.entryFunctions.added.push(fn("0x1::m::f"));
  if (change === "removed") data.entryFunctions.removed.push(fn("0x1::m::gone"));
  if (change === "changed") data.views.changed.push({ before: fn("0x1::m::v"), after: fn("0x1::m::v", ["u64"]) });
  if (change === "event") data.events.removed.push({ name: "0x1::m::E", abilities: [], typeParameters: [], fields: [] });
  if (change === "unsafe") data.entryFunctions.removed.push(fn("0x1::m::\u001b[31m"));
  return { type: PUBLISH, data, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

/** A client whose `upgrades` and `getUpgrades` read plain events, recording the address each call was given. */
function upgrades(adapter: EventAdapter) {
  const descriptor = { kind: "module", eventType: PUBLISH } as EventDescriptor;
  const inner = new StreamdClient({ adapter });
  const seen: { address: string; options: Record<string, unknown> }[] = [];
  const lines: string[] = [];
  const errors: string[] = [];
  const client = {
    upgrades: (address: string, callback: never, options: Record<string, unknown> = {}) => {
      seen.push({ address, options });
      return inner.on(descriptor, callback, options);
    },
    getUpgrades: async (address: string, filter: Record<string, unknown> = {}) => {
      seen.push({ address, options: filter });
      return inner.getEvents(descriptor, filter);
    },
  };
  const io: RecordsIO<never> = { client: client as never, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal: new AbortController().signal };
  const run = async (options: Partial<UpgradesCommandOptions>) => {
    const code = await runUpgrades({ network: "mainnet", address: "0x1", ...options }, io);
    inner.destroy();
    return code;
  };
  const versions = () => lines.map((l) => (JSON.parse(l) as { version: string }).version);
  return { run, seen, lines, errors, versions };
}

describe("breakingChanges", () => {
  it("is undefined for a publish that only adds", () => {
    expect(breakingChanges(upgrade(1, "added"))).toBeUndefined();
  });

  it("names removed and changed signatures across events, entry functions and views", () => {
    const record = upgrade(7, "removed");
    record.data.views.changed.push({ before: fn("0x1::m::v"), after: fn("0x1::m::v", ["u64"]) });
    record.data.events.removed.push({ name: "0x1::m::E", abilities: [], typeParameters: [], fields: [] });
    expect(breakingChanges(record)).toBe(
      'breaking upgrade at version 7: {"address":"0x1","removed":["0x1::m::E","0x1::m::gone"],"changed":["0x1::m::v"]}',
    );
  });

  it("JSON-encodes on-chain names, so no control character reaches stderr", () => {
    const line = breakingChanges(upgrade(1, "unsafe"))!;
    expect(line).toContain("\\u001b[31m");
    expect(/[\u0000-\u001f]/.test(line)).toBe(false);
  });
});

describe("runUpgrades", () => {
  it("reads an additive range through getUpgrades for the address and exits 0", async () => {
    const h = upgrades(new MemoryAdapter([upgrade(1, "added"), upgrade(2, "added")]));
    expect(await h.run({ address: "alice.apt", fromVersion: 1n, toVersion: "head" })).toBe(0);
    expect(h.versions()).toEqual(["1", "2"]);
    expect(JSON.parse(h.lines[0]!).data.entryFunctions.added[0].name).toBe("0x1::m::f");
    expect(h.seen[0]!.address).toBe("alice.apt");
    expect(h.errors).toEqual([]);
  });

  it("prints the whole range, then exits 1 when a printed publish was breaking", async () => {
    const h = upgrades(new MemoryAdapter([upgrade(1, "added"), upgrade(2, "changed"), upgrade(3, "event"), upgrade(4, "added")]));
    expect(await h.run({ fromVersion: 1n, toVersion: 4n })).toBe(1);
    expect(h.versions()).toEqual(["1", "2", "3", "4"]);
    expect(h.errors).toEqual([
      'streamd: breaking upgrade at version 2: {"address":"0x1","removed":[],"changed":["0x1::m::v"]}',
      'streamd: breaking upgrade at version 3: {"address":"0x1","removed":["0x1::m::E"],"changed":[]}',
    ]);
  });

  it("counts only printed publishes against a range's limit", async () => {
    const h = upgrades(new MemoryAdapter([upgrade(1, "added"), upgrade(2, "removed")]));
    expect(await h.run({ fromVersion: 1n, toVersion: 2n, limit: 1 })).toBe(0);
    expect(h.versions()).toEqual(["1"]);
  });

  it("following, prints a breaking publish and exits 1 at once", async () => {
    const h = upgrades(new MemoryAdapter([upgrade(1, "added"), upgrade(2, "removed"), upgrade(3, "added")]));
    expect(await h.run({ address: "0xabc", fromVersion: 1n, pollInterval: 1 })).toBe(1);
    expect(h.versions()).toEqual(["1", "2"]);
    expect(h.seen[0]!.address).toBe("0xabc");
    expect(h.errors).toEqual(['streamd: breaking upgrade at version 2: {"address":"0x1","removed":["0x1::m::gone"],"changed":[]}']);
  });
});
