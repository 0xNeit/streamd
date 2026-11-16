import { getAptosFullNode, postAptosFullNode, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, toStreamdError } from "@streamd/core";
import type { EventAdapter, HandleEventDescriptor, StreamdEvent } from "@streamd/core";
import { amount, malformed, recordSource } from "./records.js";
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

/** A u64 field, or `bad()`'s throw. */
function u64(field: unknown, bad: () => Error): bigint {
  const value = amount(field);
  if (value === undefined) throw bad();
  return value;
}

/** Move's `i64::I64 { negative, magnitude }`, with `-0` read as 0. */
function i64(field: unknown, bad: () => Error): bigint {
  const f = field as Fields | null;
  if (typeof f !== "object" || f === null || typeof f.negative !== "boolean") throw bad();
  const magnitude = u64(f.magnitude, bad);
  return f.negative ? -magnitude : magnitude;
}

/** Pyth's `price::Price` as a `Price` seen at `observedAt` (microseconds), or `bad()`'s throw. */
function pythPrice(feedId: string, price: unknown, observedAt: bigint, bad: () => Error): Price {
  const p = price as Fields | null;
  if (typeof p !== "object" || p === null) throw bad();
  const exponent = i64(p.expo, bad);
  // Pyth's exponents are single digits; anything beyond an int32 is a misread.
  if (exponent < -(2n ** 31n) || exponent >= 2n ** 31n) throw bad();
  const publishTime = u64(p.timestamp, bad) * MICROS;
  return {
    oracle: "pyth",
    feedId,
    price: i64(p.price, bad),
    confidence: u64(p.conf, bad),
    exponent: Number(exponent),
    publishTime,
    observedAt,
    staleness: observedAt - publishTime,
  };
}

/** The id a `price_identifier` field holds, or `bad()`'s throw. */
function feedIdOf(identifier: unknown, bad: () => Error): string {
  const bytes = (identifier as Fields | null)?.bytes;
  if (typeof bytes !== "string" || !FEED_ID.test(bytes) || !bytes.startsWith("0x")) throw bad();
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
  const bad = () => malformed(event);
  if (typeof feed !== "object" || feed === null) throw bad();
  const data = pythPrice(feedIdOf(feed.price_identifier, bad), feed.price, u64(d!.timestamp, bad), bad);
  return { ...event, data };
}

/** `adapter` as a source of price records for `feeds` (normalised ids), in the page's order. */
export function priceSource(adapter: EventAdapter, feeds: ReadonlySet<string>): EventAdapter {
  return recordSource(adapter, (event) => {
    const record = toPrice(event);
    return record !== undefined && feeds.has(record.data.feedId) ? record : undefined;
  });
}

/** Runs one fullnode read, as the client's limiter does. */
export type Run = <T>(task: () => Promise<T>) => Promise<T>;

/**
 * `priceAt` over `aptos`: the feed's entry in Pyth's `LatestPriceInfo` table at
 * `version`, observed at the timestamp of the block holding `version`. The
 * table's handle never changes once Pyth is initialised, so it is read once,
 * at the head, and read again only after a failure.
 */
export function pythPriceReader(aptos: Aptos, run: Run): (feedId: string, version: bigint) => Promise<Price | undefined> {
  let table: Promise<string> | undefined;
  const tableHandle = () =>
    (table ??= run(() => latestPriceTable(aptos)).catch((err: unknown) => {
      table = undefined;
      throw err;
    }));
  return async (feedId, version) => {
    const [info, observedAt] = await Promise.all([
      tableHandle().then((handle) => run(() => priceInfoAt(aptos, handle, feedId, version))),
      run(() => blockTimeAt(aptos, version)),
    ]);
    if (info === undefined) return undefined;
    const bad = () => new StreamdError(`Pyth's price of ${feedId} at version ${version} does not have its struct's fields`);
    const feed = (info as Fields | null)?.price_feed as Fields | undefined;
    if (typeof feed !== "object" || feed === null || feedIdOf(feed.price_identifier, bad) !== feedId) throw bad();
    return pythPrice(feedId, feed.price, observedAt, bad);
  };
}

async function latestPriceTable(aptos: Aptos): Promise<string> {
  let data: unknown;
  try {
    ({ data } = await getAptosFullNode<{}, Fields>({
      aptosConfig: aptos.config,
      originMethod: "priceAt",
      path: `accounts/${PYTH}/resource/${encodeURIComponent(`${PYTH}::state::LatestPriceInfo`)}`,
    }));
  } catch (err) {
    throw toStreamdError(err, "Pyth's price table");
  }
  const handle = ((data as Fields | null)?.data as { info?: { handle?: unknown } } | undefined)?.info?.handle;
  if (typeof handle !== "string" || !/^0x[0-9a-fA-F]{1,64}$/.test(handle)) throw new StreamdError("Fullnode returned no handle for Pyth's price table");
  return handle;
}

/** The feed's `PriceInfo` at `version`, or `undefined` when the table holds none for it then. */
async function priceInfoAt(aptos: Aptos, handle: string, feedId: string, version: bigint): Promise<unknown> {
  try {
    const { data } = await postAptosFullNode<object, Fields>({
      aptosConfig: aptos.config,
      originMethod: "priceAt",
      path: `tables/${handle}/item`,
      params: { ledger_version: version.toString() },
      body: {
        key_type: `${PYTH}::price_identifier::PriceIdentifier`,
        value_type: `${PYTH}::price_info::PriceInfo`,
        key: { bytes: feedId },
      },
    });
    return data;
  } catch (err) {
    const e = err as { status?: unknown; data?: { error_code?: unknown } } | null;
    if (e?.status === 404 && e.data?.error_code === "table_item_not_found") return undefined;
    throw toStreamdError(err, `Pyth's price of ${feedId} at version ${version}`);
  }
}

/** The timestamp, in microseconds, of the block holding `version`. */
async function blockTimeAt(aptos: Aptos, version: bigint): Promise<bigint> {
  let data: unknown;
  try {
    ({ data } = await getAptosFullNode<{}, Fields>({
      aptosConfig: aptos.config,
      originMethod: "priceAt",
      path: `blocks/by_version/${version}`,
    }));
  } catch (err) {
    throw toStreamdError(err, `the block at version ${version}`);
  }
  const time = amount((data as Fields | null)?.block_timestamp);
  if (time === undefined) throw new StreamdError(`Fullnode returned no block timestamp at version ${version}`);
  return time;
}
