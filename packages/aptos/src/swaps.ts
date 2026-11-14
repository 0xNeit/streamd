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

/**
 * Each protocol's swap event, as the fullnode reports its type, and a decoder
 * for its fields as the on-chain struct declares them.
 */
const PROTOCOLS: Record<SwapProtocol, { type: MoveEventType; decode: (data: Fields) => Decoded | undefined }> = {
  /** `pool_v3::SwapEventV3 { pool_id: address, from_token, to_token: Object<Metadata>, amount_in, amount_out: u64, .. }` */
  hyperion: {
    type: "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3",
    decode: (d) => swap(address(d.pool_id), object(d.from_token), object(d.to_token), d),
  },
  /** `pool::SwapEvent { pool_obj: Object<Pool>, metadata_0, metadata_1: Object<Metadata>, zero_for_one: bool, amount_in, amount_out: u64, .. }` */
  "thala-clamm": {
    type: "0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920::pool::SwapEvent",
    decode: (d) => {
      if (typeof d.zero_for_one !== "boolean") return undefined;
      const [zero, one] = [object(d.metadata_0), object(d.metadata_1)];
      return swap(object(d.pool_obj), d.zero_for_one ? zero : one, d.zero_for_one ? one : zero, d);
    },
  },
  /** `pool::SwapEvent { pool_obj: Object<Pool>, metadata: vector<Object<Metadata>>, idx_in, idx_out, amount_in, amount_out: u64, .. }` */
  "thala-v2": {
    type: "0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5::pool::SwapEvent",
    decode: (d) => {
      const metadata = d.metadata;
      if (!Array.isArray(metadata)) return undefined;
      const at = (index: unknown) => {
        const i = amount(index);
        return i === undefined || i >= BigInt(metadata.length) ? undefined : object(metadata[Number(i)]);
      };
      return swap(object(d.pool_obj), at(d.idx_in), at(d.idx_out), d);
    },
  },
  /** `liquidity_pool::SwapEvent { pool: address, from_token, to_token: String, amount_in, amount_out: u64 }` */
  cellana: {
    type: "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::SwapEvent",
    decode: (d) => swap(address(d.pool), cellanaAsset(d.from_token), cellanaAsset(d.to_token), d),
  },
};

const BY_TYPE = new Map<string, { protocol: SwapProtocol; decode: (data: Fields) => Decoded | undefined }>(Object.entries(PROTOCOLS).map(([protocol, { type, decode }]) => [type, { protocol: protocol as SwapProtocol, decode }]));

/** The swap events of `protocol` (one or a list; every protocol when omitted), served by one scan. */
export function swapDescriptor(protocol?: SwapProtocol | readonly SwapProtocol[]): ModuleEventDescriptor {
  const chosen = protocol === undefined ? Object.keys(PROTOCOLS) : typeof protocol === "string" ? [protocol] : protocol;
  if (!Array.isArray(chosen) || chosen.length === 0) {
    throw new StreamdError(`protocol must be one of ${Object.keys(PROTOCOLS).join(", ")}, or a non-empty list of them`);
  }
  const eventType = chosen.map((name: unknown) => {
    if (typeof name !== "string" || !Object.hasOwn(PROTOCOLS, name)) {
      throw new StreamdError(`Unknown swap protocol: ${String(name)}; expected one of ${Object.keys(PROTOCOLS).join(", ")}`);
    }
    return PROTOCOLS[name as SwapProtocol].type;
  });
  return { kind: "module", eventType: [...new Set(eventType)] };
}

/** One swap event as a record, or `undefined` when it is not a swap event of the shape its struct declares. */
export function toSwap(event: StreamdEvent): StreamdEvent<Swap> | undefined {
  const entry = BY_TYPE.get(event.type);
  if (entry === undefined || typeof event.data !== "object" || event.data === null) return undefined;
  const decoded = entry.decode(event.data as Fields);
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
