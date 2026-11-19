import { StreamdNetworkError, UNSIGNED_INTEGER } from "@streamd/core";
import type { AssetValue, ViewRequest } from "@streamd/core";
import { assetKey, isViewAbort } from "./transfers.js";
import type { Price } from "./types.js";

/** Native USDC, the asset every value is quoted in. */
export const QUOTE = "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b";
const QUOTE_DECIMALS = 6;
const HYPERION = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c";
// Pyth stopped updating on Aptos in 2026, when it was seconds fresh at worst;
// a minute is far past that and still a price someone could have traded on.
const MAX_STALENESS = 60_000_000n;
const Q128 = 1n << 128n;

/**
 * A priced asset: a Hyperion pool whose token_a is the asset and token_b
 * USDC, and the Pyth feed for versions before the pool existed.
 */
interface Source {
  decimals: number;
  pool: string;
  feed: string;
}

const APT: Source = {
  decimals: 8,
  pool: "0x925660b8618394809f89f8002e2926600c775221f43bf1919782b297a79400d8",
  feed: "0x03ae4db29ed4ae33d323568895aa00337e658e348b37509f5372ae51f0af00d5",
};

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;

/**
 * Keyed as `assetKey` writes them, spelled out since `transfers.ts` imports
 * this module; each pool was the deepest for its pair when chosen.
 */
const SOURCES = new Map<string, Source | "par">([
  [long("a"), APT],
  [`${long("1")}::aptos_coin::AptosCoin`, APT],
  [
    "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b",
    {
      decimals: 6,
      pool: "0xd3894aca06d5f42b27c89e6f448114b3ed6a1ba07f992a58b2126c71dd83c127",
          feed: "0x2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b",
    },
  ],
  [QUOTE, "par"],
]);

/** A price: quote base units per base unit of the asset, and where it was read. */
export type Quote = Pick<AssetValue, "price" | "source" | "reference">;

/** The quote for an asset at a version, or `undefined` when nothing prices it then. */
export type Pricer = (asset: string, version: bigint) => Promise<Quote | undefined>;

const gcd = (a: bigint, b: bigint): bigint => (b === 0n ? (a < 0n ? -a : a) : gcd(b, a % b));

function fraction(numerator: bigint, denominator: bigint): AssetValue["price"] {
  const d = gcd(numerator, denominator);
  return { numerator: numerator / d, denominator: denominator / d };
}

/** Pyth's USD price, taken as USDC at par, per base unit; `undefined` unless positive and fresh. */
function oracleQuote(oracle: Price | undefined, source: Source): Quote | undefined {
  if (oracle === undefined || oracle.price <= 0n || oracle.staleness > MAX_STALENESS) return undefined;
  const shift = oracle.exponent + QUOTE_DECIMALS - source.decimals;
  const scale = 10n ** BigInt(Math.abs(shift));
  const price = shift >= 0 ? fraction(oracle.price * scale, 1n) : fraction(oracle.price, scale);
  return { price, source: "oracle", reference: source.feed };
}

/**
 * Prices assets in USDC at a version: a curated Hyperion pool's spot price by
 * `view`, or, when the pool did not exist then, Pyth's price by `pyth` if it
 * was fresh. A view or Pyth read failing for any other reason rejects.
 */
export function assetPricer(
  view: (request: ViewRequest, version: bigint) => Promise<unknown[]>,
  pyth: (feedId: string, version: bigint) => Promise<Price | undefined>,
): Pricer {
  return async (asset, version) => {
    const source = SOURCES.get(assetKey(asset));
    if (source === undefined) return undefined;
    if (source === "par") return { price: { numerator: 1n, denominator: 1n }, source: "par" };
    let result: unknown[];
    try {
      result = await view(
        { function: `${HYPERION}::pool_v3::current_tick_and_price`, typeArguments: [], functionArguments: [source.pool] },
        version,
      );
    } catch (err) {
      if (!isViewAbort(err)) throw err;
      return oracleQuote(await pyth(source.feed, version), source);
    }
    const raw = Array.isArray(result) ? result[1] : undefined;
    if (typeof raw !== "string" || !UNSIGNED_INTEGER.test(raw)) {
      throw new StreamdNetworkError(`Hyperion pool ${source.pool} at version ${version} returned no sqrt price`);
    }
    // sqrt_price is Q64.64 of token_b base units per token_a base unit.
    const squared = BigInt(raw) ** 2n;
    if (squared === 0n) return undefined;
    return { price: fraction(squared, Q128), source: "pool", reference: source.pool };
  };
}

/** `amount` of an asset at `quote`, in USDC base units rounded toward zero. */
export function valueOf(quote: Quote, amount: bigint): AssetValue {
  const { numerator, denominator } = quote.price;
  return { quote: QUOTE, decimals: QUOTE_DECIMALS, amount: (amount * numerator) / denominator, ...quote };
}
