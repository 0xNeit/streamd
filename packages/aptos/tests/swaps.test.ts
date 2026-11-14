import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
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

describe("swapDescriptor", () => {
  it("lists every protocol's event type by default, and narrows to those named", () => {
    expect(swapDescriptor().eventType).toHaveLength(4);
    expect(swapDescriptor("cellana").eventType).toEqual([
      "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent",
    ]);
    expect(swapDescriptor(["hyperion", "thala-v2", "hyperion"]).eventType).toEqual([
      "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3",
      "0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5::pool::SwapEvent",
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
