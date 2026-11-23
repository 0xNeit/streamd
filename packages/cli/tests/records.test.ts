import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Sale, Swap } from "@streamd/aptos";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import type { EventAdapter, EventDescriptor, StreamdEvent } from "@streamd/core";
import type { SalesCommandOptions, SwapsCommandOptions } from "../src/args.js";
import { runSales, runSwaps } from "../src/records.js";
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
