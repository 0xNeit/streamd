import { StreamdError } from "@streamd/core";
import type { EventAdapter, HandleEventDescriptor, StreamdEvent } from "@streamd/core";
import { malformed, recordSource, u } from "./records.js";
import type { Fields } from "./records.js";
import type { Price } from "./types.js";

/** Pyth's package on Aptos mainnet. */
export const PYTH = "0x7e783b349d3e89cf5931af376ebeadbfab855b3fa239b7ada8f5a92fbea6b387";
const UPDATE = `${PYTH}::event::PriceFeedUpdate`;

/**
 * Every Pyth price update, all feeds on one legacy handle: `PriceFeedUpdate`
 * is not a module event, and Pyth has no view functions.
 */
export const PYTH_DESCRIPTOR: HandleEventDescriptor = {
  kind: "handle",
  accountAddress: PYTH,
  eventHandleStruct: `${PYTH}::event::PriceFeedUpdateHandle`,
  fieldName: "event",
};

const FEED_ID = /^(0x)?[0-9a-fA-F]{64}$/;
const MICROS = 1_000_000n;

/** A Pyth price id, with or without `0x` and in any case, as `0x` and lowercase hex. */
export function requireFeedId(feedId: unknown): string {
  if (typeof feedId !== "string" || !FEED_ID.test(feedId)) {
    throw new StreamdError(`Not a Pyth price feed id: ${String(feedId)} (expected 32 bytes of hex)`);
  }
  return `0x${feedId.replace(/^0x/, "").toLowerCase()}`;
}

/** Move's `i64::I64 { negative, magnitude }`, with `-0` read as 0. */
function i64(field: unknown, event: StreamdEvent): bigint {
  const f = field as Fields | null;
  if (typeof f !== "object" || f === null || typeof f.negative !== "boolean") throw malformed(event);
  const magnitude = u(f.magnitude, event);
  return f.negative ? -magnitude : magnitude;
}

/** Pyth's `price::Price` as a `Price` seen at `observedAt` (microseconds). */
export function pythPrice(feedId: string, price: unknown, observedAt: bigint, event: StreamdEvent): Price {
  const p = price as Fields | null;
  if (typeof p !== "object" || p === null) throw malformed(event);
  const exponent = i64(p.expo, event);
  // Pyth's exponents are single digits; anything beyond an int32 is a misread.
  if (exponent < -(2n ** 31n) || exponent >= 2n ** 31n) throw malformed(event);
  const publishTime = u(p.timestamp, event) * MICROS;
  return {
    oracle: "pyth",
    feedId,
    price: i64(p.price, event),
    confidence: u(p.conf, event),
    exponent: Number(exponent),
    publishTime,
    observedAt,
    staleness: observedAt - publishTime,
  };
}

/** The id a `price_identifier` field holds, or a `malformed` throw. */
export function feedIdOf(identifier: unknown, event: StreamdEvent): string {
  const bytes = (identifier as Fields | null)?.bytes;
  if (typeof bytes !== "string" || !FEED_ID.test(bytes) || !bytes.startsWith("0x")) throw malformed(event);
  return bytes.toLowerCase();
}

/**
 * One `PriceFeedUpdate` as a record of its feed's price, staleness measured
 * against the update's own timestamp; `undefined` for any other event type.
 */
export function toPrice(event: StreamdEvent): StreamdEvent<Price> | undefined {
  if (event.type !== UPDATE) return undefined;
  const d = event.data as Fields | null;
  const feed = (typeof d === "object" && d !== null ? d.price_feed : undefined) as Fields | undefined;
  if (typeof feed !== "object" || feed === null) throw malformed(event);
  const data = pythPrice(feedIdOf(feed.price_identifier, event), feed.price, u(d!.timestamp, event), event);
  return { ...event, data };
}

/** `adapter` as a source of price records for `feeds` (normalised ids), in the page's order. */
export function priceSource(adapter: EventAdapter, feeds: ReadonlySet<string>): EventAdapter {
  return recordSource(adapter, (event) => {
    const record = toPrice(event);
    return record !== undefined && feeds.has(record.data.feedId) ? record : undefined;
  });
}
