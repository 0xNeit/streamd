import { StreamdNetworkError, UNSIGNED_INTEGER } from "@streamd/core";
import type { AssetValue, ViewRequest } from "@streamd/core";
import { assetKey } from "./transfers.js";
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
 * USDC, the version it was created at, and the Pyth feed read before then.
 */
interface Source {
  decimals: number;
  pool: string;
  createdAt: bigint;
  feed: string;
}

const APT: Source = {
  decimals: 8,
  pool: "0x925660b8618394809f89f8002e2926600c775221f43bf1919782b297a79400d8",
  createdAt: 2321920813n,
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
      createdAt: 2321906214n,
      feed: "0x2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b",
    },
  ],
  [QUOTE, "par"],
]);

/** A price: quote base units per base unit of the asset, and where it was read. */
export type PriceQuote = Pick<AssetValue, "price" | "source" | "reference">;

/** Runs one read, as a limiter does. */
export type Run = <T>(task: () => Promise<T>) => Promise<T>;

/** The quote for an asset at a version, or `undefined` when nothing prices it then; each read goes through `run`. */
export type Pricer = (asset: string, version: bigint, run?: Run) => Promise<PriceQuote | undefined>;

/** What prices `asset` (a pool address, or the quote itself), so assets sharing a source share a read; `undefined` when nothing does. */
export function priceKey(asset: string): string | undefined {
  const source = SOURCES.get(assetKey(asset));
  return source === "par" ? QUOTE : source?.pool;
}

const gcd = (a: bigint, b: bigint): bigint => (b === 0n ? (a < 0n ? -a : a) : gcd(b, a % b));

/** `numerator / denominator`, reduced. */
export function fraction(numerator: bigint, denominator: bigint): AssetValue["price"] {
  const d = gcd(numerator, denominator);
  return { numerator: numerator / d, denominator: denominator / d };
}

/**
 * Pyth's USD price, taken as USDC at par, per base unit; `undefined` unless
 * positive and within a minute of the block's time either way (ahead is
 * Pythnet's clock running before the chain's).
 */
function oracleQuote(oracle: Price | undefined, source: Source): PriceQuote | undefined {
  if (oracle === undefined || oracle.price <= 0n) return undefined;
  if (oracle.staleness > MAX_STALENESS || oracle.staleness < -MAX_STALENESS) return undefined;
  const shift = oracle.exponent + QUOTE_DECIMALS - source.decimals;
  // Pyth's exponents are single digits; a huge one would only cost memory.
  if (Math.abs(shift) > 38) return undefined;
  const scale = 10n ** BigInt(Math.abs(shift));
  const price = shift >= 0 ? fraction(oracle.price * scale, 1n) : fraction(oracle.price, scale);
  return { price, source: "oracle", reference: source.feed };
}

/**
 * Prices assets in USDC at a version: a curated Hyperion pool's spot price by
 * `view` from the version the pool was created at, and Pyth's price by
 * `pyth` before it, when fresh. Any failed read rejects.
 */
export function assetPricer(
  view: (request: ViewRequest, version: bigint) => Promise<unknown[]>,
  pyth: (feedId: string, version: bigint) => Promise<Price | undefined>,
): Pricer {
  return async (asset, version, run = (task) => task()) => {
    const source = SOURCES.get(assetKey(asset));
    if (source === undefined) return undefined;
    if (source === "par") return { price: { numerator: 1n, denominator: 1n }, source: "par" };
    if (version < source.createdAt) return oracleQuote(await run(() => pyth(source.feed, version)), source);
    const result = await run(() =>
      view({ function: `${HYPERION}::pool_v3::current_tick_and_price`, typeArguments: [], functionArguments: [source.pool] }, version),
    );
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
export function toAssetValue(quote: PriceQuote, amount: bigint): AssetValue {
  const { price, source, reference } = quote;
  const value: AssetValue = { quote: QUOTE, decimals: QUOTE_DECIMALS, amount: (amount * price.numerator) / price.denominator, price, source };
  if (reference !== undefined) value.reference = reference;
  return value;
}
