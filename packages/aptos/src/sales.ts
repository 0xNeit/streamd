import { StreamdError } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, MoveEventType, StreamdEvent } from "@streamd/core";
import { address, amount, namedTypes, object, pageSource, recordSource } from "./records.js";
import type { Fields } from "./records.js";
import { legacyTokenId, legacyTokenKey, requireAsset } from "./transfers.js";
import type { Sale, SaleKind, SaleMarketplace } from "./types.js";
import { pageValuer } from "./values.js";
import type { Pricer } from "./values.js";

type Decoded = Omit<Sale, "marketplace" | "kind">;

const WAPAL = "0x584b50b999c78ade62f8359c91b5165ff390338d45f8e55969a04e65d76258c9";
const TRADEPORT = "0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26";

function sale(token: string | undefined, seller: unknown, buyer: unknown, price: unknown): Decoded | undefined {
  const [from, to, paid] = [address(seller), address(buyer), amount(price)];
  if (token === undefined || from === undefined || to === undefined || paid === undefined) return undefined;
  return { token, seller: from, buyer: to, price: paid };
}

/** A Move `Option<T>`, which the fullnode writes as `{ vec: [] }` or `{ vec: [value] }`. */
const some = (field: unknown): unknown =>
  typeof field === "object" && field !== null && Array.isArray((field as Fields).vec) ? ((field as Fields).vec as unknown[])[0] : undefined;

/**
 * Wapal's `events::TokenMetadata { creator_address, collection_name, collection: Option<Object<Collection>>,
 * token_name, token: Option<Object<Token>>, property_version: Option<u64> }`: a token object, or a legacy
 * token by its id. Its fill events all carry `seller`, `purchaser`, `price`, `royalties` and `commission: u64`.
 */
function wapal(d: Fields): Decoded | undefined {
  const meta = d.token_metadata;
  if (typeof meta !== "object" || meta === null) return undefined;
  const m = meta as Fields;
  const version = amount(some(m.property_version));
  const token = object(some(m.token)) ?? (version === undefined ? undefined : legacyTokenKey(m.creator_address, m.collection_name, m.token_name, version));
  const [royalty, fee] = [amount(d.royalties), amount(d.commission)];
  const decoded = sale(token, d.seller, d.purchaser, d.price);
  if (decoded === undefined || royalty === undefined || fee === undefined) return undefined;
  const collection = object(some(m.collection));
  return { ...decoded, ...(collection === undefined ? {} : { collection }), royalty, fee };
}

/** Tradeport's events name a token object (`token: Object<Token>`) or, in its first modules, a legacy `token_id: TokenId`. */
const tradeportToken = (d: Fields) => object(d.token) ?? legacyTokenId(d.token_id);

interface Entry {
  marketplace: SaleMarketplace;
  kind: SaleKind;
  /** As the fullnode reports it. */
  type: MoveEventType;
  decode: (data: Fields) => Decoded | undefined;
}

/**
 * Each marketplace's fill events, all legacy handle events, and a decoder for
 * their fields as the on-chain struct declares them.
 */
const ENTRIES: readonly Entry[] = [
  { marketplace: "wapal", kind: "listing", type: `${WAPAL}::events::ListingFilledEvent`, decode: wapal },
  { marketplace: "wapal", kind: "token-offer", type: `${WAPAL}::events::TokenOfferFilledEvent`, decode: wapal },
  { marketplace: "wapal", kind: "collection-offer", type: `${WAPAL}::events::CollectionOfferFilledEvent`, decode: wapal },
  /** `listings_v2::BuyEvent { token: Object<Token>, price, seller, buyer, .. }` and `listings::BuyEvent { token_id: TokenId, price, owner, buyer, .. }` */
  ...["listings_v2", "listings"].map(
    (module): Entry => ({
      marketplace: "tradeport",
      kind: "listing",
      type: `${TRADEPORT}::${module}::BuyEvent`,
      decode: (d) => sale(tradeportToken(d), d.seller ?? d.owner, d.buyer, d.price),
    }),
  ),
  /** `biddings_v2::Accept{Token,Collection}BidEvent { token: Object<Token>, bid_buyer, bid_seller, price, .. }`, and `token_id: TokenId` in `biddings` */
  ...["biddings_v2", "biddings"].flatMap((module) =>
    (["token-offer", "collection-offer"] as const).map(
      (kind): Entry => ({
        marketplace: "tradeport",
        kind,
        type: `${TRADEPORT}::${module}::${kind === "token-offer" ? "AcceptTokenBidEvent" : "AcceptCollectionBidEvent"}`,
        decode: (d) => sale(tradeportToken(d), d.bid_seller, d.bid_buyer, d.price),
      }),
    ),
  ),
];

const BY_TYPE = new Map(ENTRIES.map((entry) => [entry.type as string, entry]));
const NAMED = ENTRIES.map((e) => ({ name: e.marketplace, type: e.type }));

/** The fill events of `marketplace` (one or a list; every marketplace when omitted), served by one scan. */
export function saleDescriptor(marketplace?: SaleMarketplace | readonly SaleMarketplace[]): ModuleEventDescriptor {
  return namedTypes(NAMED, marketplace, "marketplace", "marketplace");
}

/** One fill event as a record, or `undefined` when it is not a fill event of the shape its struct declares. */
export function toSale(event: StreamdEvent): StreamdEvent<Sale> | undefined {
  const entry = BY_TYPE.get(event.type);
  if (entry === undefined || typeof event.data !== "object" || event.data === null) return undefined;
  const decoded = entry.decode(event.data as Fields);
  return decoded === undefined ? undefined : { ...event, data: { marketplace: entry.marketplace, kind: entry.kind, ...decoded } };
}

/** `requireAsset`'s token forms, refusing coin types, which end in a struct name where a legacy id ends in its property version. */
function requireToken(token: unknown): string {
  let key: string | undefined;
  try {
    key = requireAsset(token);
  } catch {
    key = undefined;
  }
  if (key === undefined || (key.includes("::") && !/::\d+$/.test(key))) {
    throw new StreamdError(`token must be a token object address or a legacy token id: ${String(token)}`);
  }
  return key;
}

/**
 * `adapter` as a source of sale records, keeping only sales of `token` when
 * given. `pricer` adds `value` to Tradeport records, read as `pageValuer`
 * reads them: Tradeport settles only in APT, while Wapal's orders are generic
 * over a coin no event names, so its records stay unvalued.
 */
export function saleSource(adapter: EventAdapter, token?: string, pricer?: Pricer, requestTimeout?: number): EventAdapter {
  const wanted = token === undefined ? undefined : requireToken(token);
  const keep = (event: StreamdEvent) => {
    const record = toSale(event);
    return record === undefined || wanted === undefined || record.data.token === wanted ? record : undefined;
  };
  if (pricer === undefined) return recordSource(adapter, keep);
  if (typeof adapter.view !== "function") throw new StreamdError("withValue needs an adapter that implements view");
  const valuer = pageValuer(pricer, requestTimeout);
  return pageSource(adapter, (events) => {
    const value = valuer();
    const records = events.map(keep).filter((e): e is StreamdEvent<Sale> => e !== undefined);
    return Promise.all(
      records.map(async (record) => {
        if (record.data.marketplace !== "tradeport") return record;
        const worth = await value("0xa", record.data.price, record.version);
        return worth === undefined ? record : { ...record, data: { ...record.data, value: worth } };
      }),
    );
  });
}
