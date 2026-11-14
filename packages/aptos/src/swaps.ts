import { StreamdError, UNSIGNED_INTEGER, normalizeAddress } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, MoveEventType, StreamdEvent } from "@streamd/core";
import { assetKey, requireAsset } from "./transfers.js";
import type { Swap, SwapProtocol } from "./types.js";

type Fields = Record<string, unknown>;
type Decoded = Omit<Swap, "protocol" | "trader">;

const address = (field: unknown) => (typeof field === "string" ? normalizeAddress(field) : undefined);
/** An `Object<T>`, which the fullnode writes as `{ inner: address }`. */
const object = (field: unknown) =>
  typeof field === "object" && field !== null ? address((field as Fields).inner) : undefined;
const amount = (field: unknown) =>
  typeof field === "string" && UNSIGNED_INTEGER.test(field) ? BigInt(field) : undefined;

function swap(
  poolId: string | undefined,
  assetIn: string | undefined,
  assetOut: string | undefined,
  data: Fields,
): Decoded | undefined {
  const [amountIn, amountOut] = [amount(data.amount_in), amount(data.amount_out)];
  if (poolId === undefined || assetIn === undefined || assetOut === undefined) return undefined;
  if (amountIn === undefined || amountOut === undefined) return undefined;
  return { poolId, assetIn, amountIn, assetOut, amountOut };
}

/** Cellana names a coin by its type and a fungible asset by its metadata address. */
const cellanaAsset = (field: unknown) =>
  typeof field !== "string" ? undefined : field.includes("::") ? assetKey(field) : address(field);

/** A generic event's type arguments, split at its top-level commas. */
function typeArguments(type: string): string[] {
  const open = type.indexOf("<");
  if (open < 0 || !type.endsWith(">")) return [];
  const args: string[] = [];
  let depth = 0;
  let from = open + 1;
  for (let i = from; i < type.length - 1; i++) {
    if (type[i] === "<") depth++;
    else if (type[i] === ">") depth--;
    else if (type[i] === "," && depth === 0) {
      args.push(type.slice(from, i).trim());
      from = i + 1;
    }
  }
  args.push(type.slice(from, -1).trim());
  return args;
}

/** The pool resource a generic event names: the same type arguments on the pool's struct, in long form. */
const poolType = (type: string, struct: string) =>
  assetKey(`${type.slice(0, type.lastIndexOf("::", type.indexOf("<")))}::${struct}${type.slice(type.indexOf("<"))}`);

/**
 * A coin pair's swap from its four amounts: one side in and the other out,
 * the remaining two zero, as the router's swaps report them.
 */
function pairSwap(type: string, struct: string, xIn: unknown, xOut: unknown, yIn: unknown, yOut: unknown): Decoded | undefined {
  const [x, y] = typeArguments(type).map(assetKey);
  const [a, b, c, d] = [amount(xIn), amount(xOut), amount(yIn), amount(yOut)];
  if (x === undefined || y === undefined || a === undefined || b === undefined || c === undefined || d === undefined) return undefined;
  const poolId = poolType(type, struct);
  if (a > 0n && d > 0n && b === 0n && c === 0n) return { poolId, assetIn: x, amountIn: a, assetOut: y, amountOut: d };
  if (c > 0n && b > 0n && a === 0n && d === 0n) return { poolId, assetIn: y, amountIn: c, assetOut: x, amountOut: b };
  return undefined;
}

/** ThalaSwap v1 pads a pool's coin slots with this type. */
const THALA_V1 = "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af";
const THALA_V1_NULL = `${THALA_V1}::base_pool::Null`;

/** ThalaSwap v1: `idx_in`/`idx_out` pick among the first four type arguments, the pool's coins. */
function thalaV1(type: string, struct: string, d: Fields): Decoded | undefined {
  const coins = typeArguments(type).slice(0, 4).map(assetKey);
  const at = (index: unknown) => {
    const i = amount(index);
    const coin = i === undefined || i >= BigInt(coins.length) ? undefined : coins[Number(i)];
    return coin === THALA_V1_NULL ? undefined : coin;
  };
  return swap(poolType(type, struct), at(d.idx_in), at(d.idx_out), d);
}

interface Entry {
  protocol: SwapProtocol;
  /** As the fullnode reports it; written without type arguments for a generic struct, matching any. */
  type: MoveEventType;
  decode: (data: Fields, type: string) => Decoded | undefined;
}

/**
 * Each protocol's swap events and a decoder for their fields as the on-chain
 * struct declares them.
 */
const ENTRIES: readonly Entry[] = [
  /** `pool_v3::SwapEventV3 { pool_id: address, from_token, to_token: Object<Metadata>, amount_in, amount_out: u64, .. }` */
  {
    protocol: "hyperion",
    type: "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3",
    decode: (d) => swap(address(d.pool_id), object(d.from_token), object(d.to_token), d),
  },
  /** `pool::SwapEvent { pool_obj: Object<Pool>, metadata_0, metadata_1: Object<Metadata>, zero_for_one: bool, amount_in, amount_out: u64, .. }` */
  {
    protocol: "thala-clamm",
    type: "0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920::pool::SwapEvent",
    decode: (d) => {
      if (typeof d.zero_for_one !== "boolean") return undefined;
      const [zero, one] = [object(d.metadata_0), object(d.metadata_1)];
      return swap(object(d.pool_obj), d.zero_for_one ? zero : one, d.zero_for_one ? one : zero, d);
    },
  },
  /** `pool::SwapEvent { pool_obj: Object<Pool>, metadata: vector<Object<Metadata>>, idx_in, idx_out, amount_in, amount_out: u64, .. }` */
  {
    protocol: "thala-v2",
    type: "0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5::pool::SwapEvent",
    decode: (d) => indexed(object(d.pool_obj), d.metadata, object, d),
  },
  /** `liquidity_pool::SwapEvent { pool: address, from_token, to_token: String, amount_in, amount_out: u64 }` */
  {
    protocol: "cellana",
    type: "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent",
    decode: (d) => swap(address(d.pool), cellanaAsset(d.from_token), cellanaAsset(d.to_token), d),
  },
  /** `router::Swapped { pool_addr: address, assets: vector<address>, asset_in_index, asset_out_index, amount_in, amount_out: u64, .. }` */
  {
    protocol: "tapp",
    type: "0x487e905f899ccb6d46fdaec56ba1e0c4cf119862a16c409904b8c78fab1f5e8a::router::Swapped",
    decode: (d) => indexed(address(d.pool_addr), d.assets, address, { ...d, idx_in: d.asset_in_index, idx_out: d.asset_out_index }),
  },
  /** `liquidity_pool::SwapEvent<X, Y, Curve> { x_in, x_out, y_in, y_out: u64 }`, in the pool `LiquidityPool<X, Y, Curve>` */
  ...["0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12", "0x163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e"].map(
    (pkg): Entry => ({
      protocol: "liquidswap",
      type: `${pkg}::liquidity_pool::SwapEvent`,
      decode: (d, type) => pairSwap(type, "LiquidityPool", d.x_in, d.x_out, d.y_in, d.y_out),
    }),
  ),
  /** `swap::SwapEvent<X, Y> { user: address, amount_x_in, amount_y_in, amount_x_out, amount_y_out: u64 }`, in the pair `TokenPairMetadata<X, Y>` */
  {
    protocol: "pancakeswap",
    type: "0xc7efb4076dbe143cbcd98cfaaa929ecfc8f299203dfff63b95ccb6bfe19850fa::swap::SwapEvent",
    decode: (d, type) => pairSwap(type, "TokenPairMetadata", d.amount_x_in, d.amount_x_out, d.amount_y_in, d.amount_y_out),
  },
  /** `stable_pool::SwapEvent<A, B, C, D> { idx_in, idx_out, amount_in, amount_out: u64, .. }`, in `StablePool<A, B, C, D>` */
  {
    protocol: "thala-v1",
    type: `${THALA_V1}::stable_pool::SwapEvent`,
    decode: (d, type) => thalaV1(type, "StablePool", d),
  },
  /** `weighted_pool::SwapEvent<A, B, C, D, WA, WB, WC, WD> { idx_in, idx_out, amount_in, amount_out: u64, .. }`, in `WeightedPool<..>` */
  {
    protocol: "thala-v1",
    type: `${THALA_V1}::weighted_pool::SwapEvent`,
    decode: (d, type) => thalaV1(type, "WeightedPool", d),
  },
];

/** An asset list and `idx_in`/`idx_out` into it, as ThalaSwap v2 and Tapp report a swap. */
function indexed(poolId: string | undefined, list: unknown, read: (field: unknown) => string | undefined, d: Fields): Decoded | undefined {
  if (!Array.isArray(list)) return undefined;
  const at = (index: unknown) => {
    const i = amount(index);
    return i === undefined || i >= BigInt(list.length) ? undefined : read(list[Number(i)]);
  };
  return swap(poolId, at(d.idx_in), at(d.idx_out), d);
}

const PROTOCOL_NAMES = [...new Set(ENTRIES.map((e) => e.protocol))];
const BY_TYPE = new Map(ENTRIES.map((entry) => [entry.type as string, entry]));

/** The swap events of `protocol` (one or a list; every protocol when omitted), served by one scan. */
export function swapDescriptor(protocol?: SwapProtocol | readonly SwapProtocol[]): ModuleEventDescriptor {
  const chosen: unknown = protocol === undefined ? PROTOCOL_NAMES : typeof protocol === "string" ? [protocol] : protocol;
  if (!Array.isArray(chosen) || chosen.length === 0) {
    throw new StreamdError(`protocol must be one of ${PROTOCOL_NAMES.join(", ")}, or a non-empty list of them`);
  }
  for (const name of chosen as unknown[]) {
    if (typeof name !== "string" || !PROTOCOL_NAMES.includes(name as SwapProtocol)) {
      throw new StreamdError(`Unknown swap protocol: ${String(name)}; expected one of ${PROTOCOL_NAMES.join(", ")}`);
    }
  }
  return { kind: "module", eventType: ENTRIES.filter((e) => chosen.includes(e.protocol)).map((e) => e.type) };
}

/** One swap event as a record, or `undefined` when it is not a swap event of the shape its struct declares. */
export function toSwap(event: StreamdEvent): StreamdEvent<Swap> | undefined {
  const open = event.type.indexOf("<");
  const entry = BY_TYPE.get(open < 0 ? event.type : event.type.slice(0, open));
  if (entry === undefined || typeof event.data !== "object" || event.data === null) return undefined;
  const decoded = entry.decode(event.data as Fields, event.type);
  if (decoded === undefined) return undefined;
  const trader = typeof event.sender === "string" ? normalizeAddress(event.sender) : undefined;
  const data: Swap = { protocol: entry.protocol, ...decoded };
  if (trader !== undefined) data.trader = trader;
  return { ...event, data };
}

/**
 * `adapter` as a source of swap records: each page's swap events become
 * records, in the page's order, keeping those with `asset` on either side.
 */
export function swapSource(adapter: EventAdapter, asset?: string): EventAdapter {
  const wanted = asset === undefined ? undefined : requireAsset(asset);
  const source: EventAdapter = {
    fetchEvents: async (descriptor, options) => {
      const page = await adapter.fetchEvents(descriptor, options);
      const events = page.events
        .map(toSwap)
        .filter((e): e is StreamdEvent<Swap> => e !== undefined && (wanted === undefined || e.data.assetIn === wanted || e.data.assetOut === wanted));
      return { ...page, events };
    },
  };
  for (const name of ["getHeadVersion", "getEarliestVersion", "getBlock", "getBlockHeights", "view"] as const) {
    const method = adapter[name];
    if (method !== undefined) Object.assign(source, { [name]: method.bind(adapter) });
  }
  return source;
}
