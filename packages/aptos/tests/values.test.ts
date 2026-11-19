import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { ViewRequest } from "@streamd/core";
import { QUOTE, assetPricer, toAssetValue } from "../src/values.js";
import type { Price } from "../src/types.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const APT_POOL = "0x925660b8618394809f89f8002e2926600c775221f43bf1919782b297a79400d8";
const USDT = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b";
const USDT_POOL = "0xd3894aca06d5f42b27c89e6f448114b3ed6a1ba07f992a58b2126c71dd83c127";
const APT_USD = "0x03ae4db29ed4ae33d323568895aa00337e658e348b37509f5372ae51f0af00d5";
/** The APT/USDC pool's sqrt price on mainnet at version 8123186435. */
const HEAD_SQRT = "1208846870876777924";

const pyth = (fields: Partial<Price> = {}): Price => ({
  oracle: "pyth",
  feedId: APT_USD,
  price: 469183207n,
  confidence: 1n,
  exponent: -8,
  publishTime: 0n,
  observedAt: 10_000_000n,
  staleness: 10_000_000n,
  ...fields,
});

function pricer(options: { sqrt?: unknown; view?: () => Promise<unknown[]>; oracle?: Price | undefined } = {}) {
  const views: { request: ViewRequest; version: bigint }[] = [];
  const oracle: { feed: string; version: bigint }[] = [];
  const price = assetPricer(
    async (request, version) => {
      views.push({ request, version });
      return options.view?.() ?? [4294912788, options.sqrt ?? HEAD_SQRT];
    },
    async (feed, version) => {
      oracle.push({ feed, version });
      return "oracle" in options ? options.oracle : pyth();
    },
  );
  return { price, views, oracle };
}

const aborted = () => Promise.reject(new StreamdError("Move abort: object does not exist"));
/** After both pools were created; anything below 2321906214 is before them. */
const AFTER = 3_000_000_000n;
const BEFORE = 2_000_000_000n;

describe("assetPricer", () => {
  it("prices APT from the pool's sqrt price as s^2 / 2^128, reduced", async () => {
    const { price, views } = pricer();
    const quote = await price("0xa", 8123186435n);
    const s = BigInt(HEAD_SQRT);
    expect(quote!.price.numerator * (1n << 128n)).toBe(s * s * quote!.price.denominator);
    expect(quote!.source).toBe("pool");
    expect(quote!.reference).toBe(APT_POOL);
    // 0.0042944 USDC base units per octa: $0.429 per APT.
    expect(Number(quote!.price.numerator * 10_000_000n / quote!.price.denominator)).toBe(42944);
    expect(views).toEqual([
      {
        request: { function: "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::current_tick_and_price", typeArguments: [], functionArguments: [APT_POOL] },
        version: 8123186435n,
      },
    ]);
  });

  it("gives the APT coin and its fungible asset, in any form, one entry", async () => {
    const { price, views } = pricer();
    const quotes = await Promise.all(["0xa", long("a"), "0x1::aptos_coin::AptosCoin", `${long("1")}::aptos_coin::AptosCoin`].map((a) => price(a, AFTER)));
    expect(new Set(quotes.map((q) => JSON.stringify(q, (_k, v) => (typeof v === "bigint" ? `${v}` : v)))).size).toBe(1);
    expect(views.map((v) => v.request.functionArguments)).toEqual(Array(4).fill([APT_POOL]));
  });

  it("prices USDt from its own pool and USDC at par with no read", async () => {
    const { price, views } = pricer({ sqrt: (1n << 64n).toString() });
    expect(await price(USDT, AFTER)).toEqual({ price: { numerator: 1n, denominator: 1n }, source: "pool", reference: USDT_POOL });
    expect(await price(QUOTE, 1n)).toEqual({ price: { numerator: 1n, denominator: 1n }, source: "par" });
    expect(views).toHaveLength(1);
  });

  it("has no price for an asset outside the table, or a pool at sqrt price 0", async () => {
    const { price, views } = pricer({ sqrt: "0" });
    expect(await price(long("beef"), AFTER)).toBeUndefined();
    expect(await price("0x1::coin::Other", AFTER)).toBeUndefined();
    expect(views).toHaveLength(0);
    expect(await price("0xa", AFTER)).toBeUndefined();
  });

  it("reads Pyth, as USDC at par, before the pool was created, and the pool from its creation", async () => {
    const { price, oracle, views } = pricer();
    // $4.69183207 per APT is 469183207e-8 USD per 1e8 octas: 469183207 / 10^10 USDC base units per octa.
    expect(await price("0xa", 2321920812n)).toEqual({ price: { numerator: 469183207n, denominator: 10_000_000_000n }, source: "oracle", reference: APT_USD });
    expect(oracle).toEqual([{ feed: APT_USD, version: 2321920812n }]);
    expect(views).toHaveLength(0);
    expect((await price("0xa", 2321920813n))!.source).toBe("pool");
    expect(oracle).toHaveLength(1);
    expect(toAssetValue((await price("0xa", BEFORE))!, 100_000_000n).amount).toBe(4_691_832n);
  });

  it("reads USDt's pool from its own creation version", async () => {
    const { price } = pricer({ sqrt: (1n << 64n).toString() });
    expect((await price(USDT, 2321906213n))!.source).toBe("oracle");
    expect((await price(USDT, 2321906214n))!.source).toBe("pool");
  });

  it("scales an oracle price up when its exponent leaves no denominator", async () => {
    const { price } = pricer({ oracle: pyth({ price: 7n, exponent: 4 }) });
    expect((await price(USDT, BEFORE))!.price).toEqual({ numerator: 70_000n, denominator: 1n });
  });

  it("has no oracle price when stale either way, missing, not positive, or at an absurd exponent", async () => {
    for (const oracle of [pyth({ staleness: 60_000_001n }), pyth({ staleness: -60_000_001n }), undefined, pyth({ price: 0n }), pyth({ price: -5n }), pyth({ exponent: 2 ** 30 })]) {
      expect(await pricer({ oracle }).price("0xa", BEFORE)).toBeUndefined();
    }
    for (const staleness of [60_000_000n, -60_000_000n, -2_000_000n]) {
      expect(await pricer({ oracle: pyth({ staleness }) }).price("0xa", BEFORE)).toBeDefined();
    }
  });

  it("rejects on any failed pool read, an abort included, or a malformed result, without asking Pyth", async () => {
    for (const view of [() => Promise.reject(new StreamdNetworkError("rate limited", 429)), aborted]) {
      const failed = pricer({ view });
      await expect(failed.price("0xa", AFTER)).rejects.toThrow();
      expect(failed.oracle).toHaveLength(0);
    }
    for (const sqrt of ["-1", "1.5", "", 12]) {
      await expect(pricer({ sqrt }).price("0xa", AFTER)).rejects.toThrow("returned no sqrt price");
    }
    for (const result of [{}, [1, null], [1]]) {
      await expect(pricer({ view: async () => result as unknown[] }).price("0xa", AFTER)).rejects.toThrow("returned no sqrt price");
    }
  });

  it("runs each read through the run it is given", async () => {
    const ran: string[] = [];
    const { price } = pricer();
    const run = <T>(task: () => Promise<T>) => (ran.push("run"), task());
    await price("0xa", AFTER, run);
    await price("0xa", BEFORE, run);
    await price(QUOTE, AFTER, run);
    expect(ran).toEqual(["run", "run"]);
  });
});

describe("toAssetValue", () => {
  it("rounds toward zero, keeping the sign, in USDC base units", () => {
    const quote = { price: { numerator: 3n, denominator: 7n }, source: "pool" as const, reference: APT_POOL };
    expect(toAssetValue(quote, 10n)).toEqual({ quote: QUOTE, decimals: 6, amount: 4n, ...quote });
    expect(toAssetValue(quote, -10n).amount).toBe(-4n);
    expect(toAssetValue(quote, 0n).amount).toBe(0n);
  });
});
