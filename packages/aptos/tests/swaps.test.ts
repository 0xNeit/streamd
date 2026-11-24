import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, StreamdNetworkError, parseEvent } from "@streamd/core";
import type { AssetValue, EventAdapter, StreamdEvent, ViewRequest } from "@streamd/core";
import { swapDescriptor, swapSource, toSwap } from "../src/swaps.js";
import { testClient } from "./helpers.js";
import type { Swap } from "../src/types.js";

/**
 * Swap events from six mainnet transactions (versions 8028118058, 8049164541,
 * 8049164873, 8049168924, 8049170438 and 8049170625), read through
 * `FullnodeScanAdapter` and written by `serializeEvent`, `entryFunction` dropped.
 */
const LINES = readFileSync(join(import.meta.dir, "fixtures/swaps.jsonl"), "utf8");
const EVENTS = LINES.trim().split("\n").map((line) => parseEvent(line));

const long = (hex: string) => `0x${hex.slice(2).padStart(64, "0")}`;
const APT = long("0xa");
const USDC = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";
const USDT = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b";
const USD1 = "0x2b3be0a97a73c87ff62cbdd36837a9fb5bbd1d7f06a73b7ed62ec15c5326c1b8";
const APT_COIN = `${long("0x1")}::aptos_coin::AptosCoin`;
const ARB = "0x8a1203b361577fc93a28c46186206a6ee053af5e16c3731c9160e0313ba39234";

const at = (version: number, eventIndex: number): StreamdEvent => {
  const event = EVENTS.find((e) => e.version === BigInt(version) && e.eventIndex === eventIndex);
  if (event === undefined) throw new Error(`no fixture at ${version}:${eventIndex}`);
  return event;
};
const swapAt = (version: number, eventIndex: number) => toSwap(at(version, eventIndex))?.data;

describe("toSwap", () => {
  it("reads Hyperion's SwapEventV3", () => {
    expect(swapAt(8049170438, 10)).toEqual({
      protocol: "hyperion",
      poolId: "0xa7bb8c9b3215e29a3e2c2370dcbad9c71816d385e7863170b147243724b2da58",
      trader: "0xb847525a45c1b7497bbb438158f051b0f0ad8af3b2a4de188cd55285b30879a2",
      assetIn: "0x68844a0d7f2587e726ad0579f3d640865bb4162c08a4589eeda3f9689ec52a3d",
      amountIn: 4000n,
      assetOut: USDC,
      amountOut: 2601547n,
    });
  });

  it("reads Thala CLAMM in both directions, exact out net of the refund", () => {
    const pool = "0xa8a355df7d9e75ef16082da2a0bad62c173a054ab1e8eae0f0e26c828adaa4ef";
    // Sender reported in short form: one leading zero dropped.
    const trader = long("0x623d5561daf5a4cdcd234b0f9343016c53012236fe2e0926e1d2f7251191c33");
    expect(swapAt(8049170625, 6)).toEqual({
      protocol: "thala-clamm", poolId: pool, trader, assetIn: APT, amountIn: 55248600n, assetOut: USDC, amountOut: 267897n,
    });
    // zero_for_one false, exact_in false: withdrew 363832 and was refunded 1398.
    expect(swapAt(8049164541, 8)).toEqual({
      protocol: "thala-clamm", poolId: pool, trader, assetIn: USDC, amountIn: 362434n, assetOut: APT, amountOut: 74626900n,
    });
  });

  it("reads ThalaSwap v2 by idx_in and idx_out", () => {
    expect(swapAt(8049168924, 16)).toEqual({
      protocol: "thala-v2",
      poolId: "0xaf5f759ce4a2594cec4d1982160c846eb6ec2c70e7e8e6e2723e2917f13ac88c",
      trader: ARB,
      assetIn: USDT,
      amountIn: 961935n,
      assetOut: USD1,
      amountOut: 963999n,
    });
    expect(swapAt(8049168924, 21)).toMatchObject({ protocol: "thala-v2", assetIn: USD1, assetOut: USDC });
  });

  it("reads Cellana's coin types and fungible-asset addresses in long form", () => {
    expect(swapAt(8049164873, 26)).toEqual({
      protocol: "cellana",
      poolId: "0x234f0be57d6acfb2f0f19c17053617311a8d03c9ce358bdf9cd5c460e4a02b7c",
      trader: ARB,
      assetIn: "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC",
      amountIn: 4744486n,
      assetOut: APT_COIN,
      amountOut: 973755705n,
    });
    expect(swapAt(8028118058, 5)).toMatchObject({
      protocol: "cellana",
      assetIn: "0x2ebb2ccac5e027a87fa0e2e5f656a3a4238d6a48d93ec9b610d570fc0aa0df12",
      amountIn: 12000000000n,
      assetOut: APT_COIN,
      amountOut: 1185415n,
    });
  });

  it("keeps the event's identity and adds no trader without a sender", () => {
    const event = at(8049170438, 10);
    const record = toSwap(event)!;
    expect(record.version).toBe(8049170438n);
    expect(record.eventIndex).toBe(10);
    expect(record.transactionHash).toBe(event.transactionHash!);
    expect(record.timestamp).toBe(event.timestamp!);
    expect(record.type).toBe(event.type);
    const { sender: _sender, ...anonymous } = event;
    expect("trader" in toSwap(anonymous)!.data).toBe(false);
  });

  it("skips events that are not swaps or do not have their struct's shape", () => {
    const hyperion = at(8049170438, 10);
    const v2 = at(8049168924, 16);
    const clamm = at(8049170625, 6);
    const data = (event: StreamdEvent) => event.data as Record<string, unknown>;
    const broken = [
      { ...hyperion, type: "0x1::coin::CoinDeposit" },
      { ...hyperion, data: null },
      { ...hyperion, data: { ...data(hyperion), amount_in: "-1" } },
      { ...hyperion, data: { ...data(hyperion), from_token: "0xa" } },
      { ...v2, data: { ...data(v2), idx_in: "2" } },
      { ...v2, data: { ...data(v2), metadata: "0xa" } },
      { ...clamm, data: { ...data(clamm), zero_for_one: "true" } },
    ];
    for (const event of broken) expect(toSwap(event)).toBeUndefined();
  });
});

/**
 * Swap events from eight mainnet transactions (8049528130, 8049530377,
 * 8049576438, 8049645607, 8049648262, 8049659568, 8049659842, 8049696228),
 * captured through `FullnodeScanAdapter` with each generic struct named
 * without type arguments, written by `serializeEvent`, `entryFunction` dropped.
 */
const GENERIC = readFileSync(join(import.meta.dir, "fixtures/swaps-generic.jsonl"), "utf8")
  .trim()
  .split("\n")
  .map((line) => parseEvent(line));

describe("toSwap on generic-event DEXes and Tapp", () => {
  const LIQUIDSWAP_V05 = "0x0163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e";
  const THALA_V1 = "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af";
  const NULL = `${THALA_V1}::base_pool::Null`;
  const LZ_USDC = "0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC";
  const THAPT = "0xfaf4e633ae9eb31366c9ca24214231760926576c7b625313b3688b5e900731f6::staking::ThalaAPT";
  const DOODOO = "0x73eb84966be67e4697fc5ae75173ca6c35089e802650f75422ab49a8729704ec::coin::DooDoo";
  const LSD = "0x53a30a6e5936c0a4c5140daed34de39d17ca7fcae08f947c02e979cef98a3719::coin::LSD";
  const generic = (version: number, eventIndex: number) => {
    const event = GENERIC.find((e) => e.version === BigInt(version) && e.eventIndex === eventIndex);
    if (event === undefined) throw new Error(`no fixture at ${version}:${eventIndex}`);
    return event;
  };
  const swapOf = (version: number, eventIndex: number) => toSwap(generic(version, eventIndex))?.data;

  it("reads PancakeSwap's pair event, x in, naming the pair by its type", () => {
    expect(swapOf(8049528130, 2)).toEqual({
      protocol: "pancakeswap",
      poolId: `0xc7efb4076dbe143cbcd98cfaaa929ecfc8f299203dfff63b95ccb6bfe19850fa::swap::TokenPairMetadata<${APT_COIN}, ${LZ_USDC}>`,
      trader: "0x6fe8d6a9c2bae15c093a82cb767fa26e73d1766b75a0eb03394b0cd3c80d1816",
      assetIn: APT_COIN,
      amountIn: 1999666000n,
      assetOut: LZ_USDC,
      amountOut: 9747769n,
    });
  });

  it("reads LiquidSwap v0 and v0.5 pools, y in and x in, one record per hop", () => {
    expect(swapOf(8049530377, 3)).toEqual({
      protocol: "liquidswap",
      poolId:
        "0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::liquidity_pool::LiquidityPool" +
        `<${LZ_USDC}, ${APT_COIN}, 0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::curves::Uncorrelated>`,
      trader: "0xd0ace0a0d9ad410b18a124890f97cd16cce6f1b52bf869f21064b1d9946a5be8",
      assetIn: APT_COIN,
      amountIn: 999675000n,
      assetOut: LZ_USDC,
      amountOut: 4869051n,
    });
    // A three-hop route through v0.5, reported at 0x163d.. and read in long form.
    const route = [5, 10, 15].map((i) => swapOf(8049576438, i)!);
    expect(route.map((r) => [r.protocol, r.assetIn, r.amountIn, r.assetOut, r.amountOut])).toEqual([
      ["liquidswap", APT_COIN, 105219158n, DOODOO, 3547119804n],
      ["liquidswap", DOODOO, 3547119804n, LSD, 54743975650n],
      ["liquidswap", LSD, 54743975650n, LZ_USDC, 509996n],
    ]);
    expect(route[2]!.poolId).toBe(
      `${LIQUIDSWAP_V05}::liquidity_pool::LiquidityPool<${LSD}, ${LZ_USDC}, ${LIQUIDSWAP_V05}::curves::Uncorrelated>`,
    );
  });

  it("reads ThalaSwap v1 stable and weighted pools by index into their coins", () => {
    expect(swapOf(8049645607, 2)).toEqual({
      protocol: "thala-v1",
      poolId: `${THALA_V1}::stable_pool::StablePool<${THAPT}, ${APT_COIN}, ${NULL}, ${NULL}>`,
      trader: ARB,
      assetIn: APT_COIN,
      amountIn: 7100511n,
      assetOut: THAPT,
      amountOut: 7123764n,
    });
    const weighted = `${THALA_V1}::weighted_pool::WeightedPool<${LZ_USDC}, ${APT_COIN}, ${NULL}, ${NULL}, ` +
      `${THALA_V1}::weighted_pool::Weight_50, ${THALA_V1}::weighted_pool::Weight_50, ${NULL}, ${NULL}>`;
    expect(swapOf(8049659842, 4)).toMatchObject({ poolId: weighted, assetIn: APT_COIN, amountIn: 459588966n, assetOut: LZ_USDC, amountOut: 2189901n });
    expect(swapOf(8049696228, 15)).toMatchObject({ poolId: weighted, assetIn: LZ_USDC, amountIn: 2483471n, assetOut: APT_COIN, amountOut: 520140206n });
  });

  it("reads Tapp's Swapped in both directions", () => {
    const pool = "0x82e0b52f95ae57b35220726a32c3415919389aa5b8baa33a058d7125797535cc";
    expect(swapOf(8049648262, 20)).toEqual({
      protocol: "tapp",
      poolId: pool,
      trader: "0x7937d0aec1021bcac34cd217b6aea3b78197d9cf2ebad9bc4aa749db640115f8",
      assetIn: USDT,
      amountIn: 2128536n,
      assetOut: USDC,
      amountOut: 2126600n,
    });
    expect(swapOf(8049659568, 18)).toMatchObject({ poolId: pool, assetIn: USDC, amountIn: 4825036n, assetOut: USDT, amountOut: 4828402n });
  });

  it("skips pair events not one side in and the other out, Null slots and indexes out of range", () => {
    const pancake = generic(8049528130, 2);
    const liquid = generic(8049530377, 3);
    const thala = generic(8049645607, 2);
    const tapp = generic(8049648262, 20);
    const data = (event: StreamdEvent) => event.data as Record<string, unknown>;
    const broken = [
      { ...pancake, data: { ...data(pancake), amount_y_in: "5" } },
      { ...pancake, data: { ...data(pancake), amount_y_out: "0" } },
      { ...liquid, data: { ...data(liquid), x_in: "7" } },
      { ...liquid, data: { ...data(liquid), x_out: "-1" } },
      { ...liquid, type: liquid.type.replace(/<.*$/, "") },
      { ...thala, type: thala.type.replace(/<.*$/, "") },
      { ...thala, data: { ...data(thala), idx_in: "2" } },
      { ...thala, data: { ...data(thala), idx_out: "4" } },
      { ...tapp, data: { ...data(tapp), asset_in_index: "2" } },
      { ...tapp, data: { ...data(tapp), assets: "0xa" } },
    ];
    for (const event of broken) expect(toSwap(event)).toBeUndefined();
  });

  it("serves the generic records through getSwaps over a MemoryAdapter", async () => {
    const client = testClient({ adapter: new MemoryAdapter([...EVENTS, ...GENERIC]) });
    const thala = await client.getSwaps({ fromVersion: 0n, protocol: "thala-v1" });
    expect(thala.map((e) => `${e.version}:${e.eventIndex}`)).toEqual([
      "8049645607:2", "8049645607:4", "8049645607:6", "8049659842:4", "8049696228:15",
    ]);
    const coins = await client.getSwaps({ fromVersion: 8049500000n, asset: "0x1::aptos_coin::AptosCoin" });
    expect(new Set(coins.map((e) => e.data.protocol))).toEqual(new Set(["pancakeswap", "liquidswap", "thala-v1"]));
    client.destroy();
  });
});

describe("swapDescriptor", () => {
  it("lists every protocol's event type by default, and narrows to those named", () => {
    expect(swapDescriptor().eventType).toHaveLength(10);
    expect(swapDescriptor("cellana").eventType).toEqual([
      "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent",
    ]);
    expect(swapDescriptor(["hyperion", "thala-v2", "hyperion"]).eventType).toEqual([
      "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3",
      "0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5::pool::SwapEvent",
    ]);
  });

  it("names generic structs without type arguments, and both of ThalaSwap v1's pool kinds", () => {
    expect(swapDescriptor(["thala-v1", "pancakeswap"]).eventType).toEqual([
      "0xc7efb4076dbe143cbcd98cfaaa929ecfc8f299203dfff63b95ccb6bfe19850fa::swap::SwapEvent",
      "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af::stable_pool::SwapEvent",
      "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af::weighted_pool::SwapEvent",
    ]);
  });

  it("refuses an unknown protocol or an empty list", () => {
    for (const bad of ["uniswap", [], ["hyperion", "toString"], 7]) {
      expect(() => swapDescriptor(bad as never)).toThrow(StreamdError);
    }
  });
});

describe("swapSource", () => {
  const all = { fromVersion: 0n, limit: 100 };
  const records = async (asset?: string, options: Record<string, unknown> = all) => {
    const page = await swapSource(new MemoryAdapter(EVENTS), asset).fetchEvents(swapDescriptor(), options);
    return page.events as StreamdEvent<Swap>[];
  };

  it("gives one record per hop of a route, in event order", async () => {
    const route = (await records()).filter((e) => e.version === 8049168924n);
    expect(route.map((e) => [e.eventIndex, e.data.protocol, e.data.assetIn, e.data.assetOut])).toEqual([
      [12, "hyperion", APT, USDT],
      [16, "thala-v2", USDT, USD1],
      [21, "thala-v2", USD1, USDC],
      [37, "hyperion", USDC, APT],
    ]);
  });

  it("narrows by asset on either side, in any address form or as a coin type", async () => {
    const ids = (events: StreamdEvent[]) => events.map((e) => `${e.version}:${e.eventIndex}`);
    expect(ids(await records(USDT))).toEqual(["8049168924:12", "8049168924:16"]);
    expect(ids(await records("0xA"))).toEqual(ids(await records(APT)));
    expect(ids(await records("0x1::aptos_coin::AptosCoin"))).toEqual(["8028118058:5", "8049164873:26"]);
  });

  it("refuses a malformed asset", () => {
    expect(() => swapSource(new MemoryAdapter([]), "not an asset")).toThrow(StreamdError);
  });

  it("keeps the page's descending order", async () => {
    const page = await records(undefined, { ...all, orderBy: "desc" });
    expect(page.map((e) => e.version)).toEqual(EVENTS.map((e) => e.version).reverse());
  });

  it("passes the adapter's optional methods through", async () => {
    const source = swapSource(new MemoryAdapter(EVENTS));
    expect(await source.getHeadVersion!(swapDescriptor())).toBe(8049170625n);
  });
});

describe("AptosClient swaps", () => {
  const ids = (events: StreamdEvent[]) => events.map((e) => `${e.version}:${e.eventIndex}`);

  it("getSwaps reads a range, counting limit after asset and where", async () => {
    const client = testClient({ adapter: new MemoryAdapter(EVENTS) });
    expect(await client.getSwaps({ fromVersion: 8049164873n, toVersion: 8049164873n })).toHaveLength(3);
    const usdc = await client.getSwaps({ fromVersion: 0n, asset: USDC, limit: 2 });
    expect(ids(usdc)).toEqual(["8028118058:21", "8049164541:8"]);
    const big = await client.getSwaps({ fromVersion: 0n, where: (e) => (e.data as Swap).amountIn > 100_000_000n });
    expect(ids(big)).toEqual(["8028118058:5", "8049164873:13", "8049168924:12"]);
  });

  it("getSwaps narrows by protocol", async () => {
    const client = testClient({ adapter: new MemoryAdapter(EVENTS) });
    const thala = await client.getSwaps({ fromVersion: 0n, protocol: ["thala-clamm", "thala-v2"] });
    expect(new Set(thala.map((e) => e.data.protocol))).toEqual(new Set(["thala-clamm", "thala-v2"]));
    expect(thala).toHaveLength(5);
    await expect(client.getSwaps({ protocol: "sushi" as never })).rejects.toThrow(StreamdError);
  });

  it("swaps follows pushed swap events live until unsubscribed", async () => {
    const adapter = new MemoryAdapter([]);
    const client = testClient({ adapter });
    const seen: StreamdEvent<Swap>[] = [];
    let arrived = () => {};
    const sub = client.swaps(
      (event) => {
        seen.push(event);
        if (seen.length === 2) arrived();
      },
      { protocol: "thala-v2", pollInterval: 1 },
    );
    expect(client.listenerCount()).toBe(0);
    await sub.caughtUp;
    const done = new Promise<void>((resolve) => (arrived = resolve));
    adapter.push(...EVENTS.filter((e) => e.version === 8049168924n));
    await done;
    sub.unsubscribe();
    expect(seen.map((e) => [e.eventIndex, e.data.assetIn])).toEqual([
      [16, USDT],
      [21, USD1],
    ]);
    client.destroy();
  });

  it("swaps refuses a bad protocol or asset at the call", () => {
    const client = testClient({ adapter: new MemoryAdapter([]) });
    expect(() => client.swaps(() => {}, { protocol: [] })).toThrow(StreamdError);
    expect(() => client.swaps(() => {}, { asset: "0xnot" })).toThrow(StreamdError);
    client.destroy();
  });
});

describe("swaps withValue", () => {
  const POOL = "::pool_v3::current_tick_and_price";
  const APT_POOL = "0x925660b8618394809f89f8002e2926600c775221f43bf1919782b297a79400d8";
  const USDT_POOL = "0xd3894aca06d5f42b27c89e6f448114b3ed6a1ba07f992a58b2126c71dd83c127";
  // sqrt price 2^60 is 1/256 USDC base unit per octa; 2^64 is USDt at par.
  const SQRT: Record<string, bigint> = { [APT_POOL]: 1n << 60n, [USDT_POOL]: 1n << 64n };
  const value = (amount: bigint, numerator: bigint, denominator: bigint, source: AssetValue["source"], reference?: string): AssetValue => ({
    quote: USDC,
    decimals: 6,
    amount,
    price: { numerator, denominator },
    source,
    ...(reference !== undefined && { reference }),
  });

  function priced(events: StreamdEvent[], fail = false) {
    const memory = new MemoryAdapter(events);
    const calls: { pool: string; version: bigint }[] = [];
    const adapter: EventAdapter = {
      fetchEvents: (d, o) => memory.fetchEvents(d, o),
      getHeadVersion: () => memory.getHeadVersion(),
      view: async (request: ViewRequest, version: bigint) => {
        if (!request.function.endsWith(POOL)) throw new Error(`unexpected view ${request.function}`);
        const pool = String(request.functionArguments![0]);
        calls.push({ pool, version });
        if (fail) throw new StreamdNetworkError("rate limited", 429);
        return [0, SQRT[pool]!.toString()];
      },
    };
    return { adapter, calls };
  }

  it("values each side at the record's version, one pool read per source and version, leaving unpriced sides bare", async () => {
    const { adapter, calls } = priced(EVENTS);
    const route = await testClient({ adapter }).getSwaps({ fromVersion: 8049168924n, toVersion: 8049168924n, withValue: true });
    const [aptIn, usdtIn, usd1In, usdcIn] = route.map((r) => r.data);
    expect(aptIn!.valueIn).toEqual(value(aptIn!.amountIn / 256n, 1n, 256n, "pool", APT_POOL));
    expect(aptIn!.valueOut).toEqual(value(aptIn!.amountOut, 1n, 1n, "pool", USDT_POOL));
    expect(usdtIn!.valueIn).toEqual(value(usdtIn!.amountIn, 1n, 1n, "pool", USDT_POOL));
    expect(usdtIn!.valueOut).toBeUndefined();
    expect(usd1In!.valueIn).toBeUndefined();
    expect(usd1In!.valueOut).toEqual(value(usd1In!.amountOut, 1n, 1n, "par"));
    expect(usdcIn!.valueOut).toEqual(value(usdcIn!.amountOut / 256n, 1n, 256n, "pool", APT_POOL));
    expect(calls).toEqual([
      { pool: APT_POOL, version: 8049168924n },
      { pool: USDT_POOL, version: 8049168924n },
    ]);
  });

  it("values live swaps the same way", async () => {
    const { adapter } = priced(EVENTS.filter((e) => e.version === 8049170625n));
    const client = testClient({ adapter });
    const seen: Swap[] = [];
    const sub = client.swaps((e) => void seen.push(e.data), { fromVersion: 0n, protocol: "thala-clamm", withValue: true, pollInterval: 5 });
    await sub.caughtUp;
    sub.unsubscribe();
    expect(seen.map((s) => [s.valueIn?.amount, s.valueOut?.amount])).toEqual([[55248600n / 256n, 267897n]]);
    client.destroy();
  });

  it("fails the query when a pool read fails", async () => {
    const { adapter } = priced(EVENTS, true);
    await expect(testClient({ adapter }).getSwaps({ fromVersion: 0n, withValue: true })).rejects.toThrow("rate limited");
  });

  it("reads no prices without withValue, and refuses withValue without view", async () => {
    const { adapter, calls } = priced(EVENTS);
    const records = await testClient({ adapter }).getSwaps({ fromVersion: 0n });
    expect(records.some((r) => r.data.valueIn !== undefined || r.data.valueOut !== undefined)).toBe(false);
    expect(calls).toHaveLength(0);
    const client = testClient({ adapter: new MemoryAdapter(EVENTS) });
    await expect(client.getSwaps({ fromVersion: 0n, withValue: true })).rejects.toThrow("implements view");
    expect(() => client.swaps(() => {}, { withValue: true })).toThrow("implements view");
    client.destroy();
  });
});
