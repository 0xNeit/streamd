import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { PYTH, PYTH_DESCRIPTOR, priceSource, requireFeedId, toPrice } from "../src/prices.js";

/**
 * The last five Pyth updates on Aptos mainnet (sequence numbers 630509387 to
 * 630509391, versions 7300078181 to 7302080573), read from the archive through
 * `FullnodeScanAdapter` and written by `serializeEvent`.
 */
const EVENTS = readFileSync(join(import.meta.dir, "fixtures/pyth.jsonl"), "utf8").trim().split("\n").map((line) => parseEvent(line));

const APT_USD = "0x03ae4db29ed4ae33d323568895aa00337e658e348b37509f5372ae51f0af00d5";
const USDT_USD = "0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a";
const UPDATE = `${PYTH}::event::PriceFeedUpdate`;

const withPrice = (price: Record<string, unknown>): StreamdEvent => {
  const event = structuredClone(EVENTS[0]!);
  const feed = (event.data as { price_feed: { price: Record<string, unknown> } }).price_feed;
  feed.price = { ...feed.price, ...price };
  return event;
};

describe("toPrice", () => {
  it("reads a PriceFeedUpdate exactly, times in microseconds", () => {
    expect(toPrice(EVENTS[0]!)?.data).toEqual({
      oracle: "pyth",
      feedId: APT_USD,
      price: 77820843n,
      confidence: 144294n,
      exponent: -8,
      publishTime: 1789999874000000n,
      observedAt: 1789999881121504n,
      staleness: 7121504n,
    });
    const last = toPrice(EVENTS[4]!)!;
    expect(last.version).toBe(7302080573n);
    expect(last.sequenceNumber).toBe(630509391n);
    expect(last.data.price).toBe(77353341n);
    expect(last.data.staleness).toBe(1790009907927720n - 1790009897000000n);
  });

  it("reads negative prices and exponents, and -0 as 0", () => {
    const negative = toPrice(withPrice({ price: { negative: true, magnitude: "5" }, expo: { negative: false, magnitude: "2" } }))!.data;
    expect([negative.price, negative.exponent]).toEqual([-5n, 2]);
    const zero = toPrice(withPrice({ price: { negative: true, magnitude: "0" }, expo: { negative: true, magnitude: "0" } }))!.data;
    expect(Object.is(zero.exponent, 0)).toBe(true);
    expect(zero.price).toBe(0n);
  });

  it("reports negative staleness when the publish time is after the update", () => {
    expect(toPrice(withPrice({ timestamp: "1789999882" }))!.data.staleness).toBe(-878496n);
  });

  it("skips other event types", () => {
    expect(toPrice({ ...EVENTS[0]!, type: `${PYTH}::event::Other` })).toBeUndefined();
  });

  it("refuses malformed payloads", () => {
    const bad = [
      withPrice({ price: { negative: "no", magnitude: "1" } }),
      withPrice({ price: { negative: false, magnitude: "-1" } }),
      withPrice({ conf: 5 }),
      withPrice({ timestamp: "1.5" }),
      withPrice({ expo: { negative: true, magnitude: "2147483649" } }),
      { ...EVENTS[0]!, data: { timestamp: "1" } },
      { ...EVENTS[0]!, data: null },
    ];
    const badId = structuredClone(EVENTS[0]!);
    (badId.data as { price_feed: { price_identifier: { bytes: string } } }).price_feed.price_identifier.bytes = "0x1234";
    for (const event of [...bad, badId]) expect(() => toPrice(event)).toThrow(StreamdError);
  });
});

describe("requireFeedId", () => {
  it("accepts 32 bytes of hex with or without 0x, in any case", () => {
    expect(requireFeedId(APT_USD)).toBe(APT_USD);
    expect(requireFeedId(APT_USD.slice(2).toUpperCase())).toBe(APT_USD);
  });

  it("refuses anything else", () => {
    for (const id of ["0x03ae", `${APT_USD}00`, `0x${"g".repeat(64)}`, "", 5, undefined]) {
      expect(() => requireFeedId(id)).toThrow("Not a Pyth price feed id");
    }
  });
});

describe("priceSource", () => {
  const read = async (feeds: string[]) => {
    const page = await priceSource(new MemoryAdapter(EVENTS), new Set(feeds)).fetchEvents(PYTH_DESCRIPTOR, { limit: 10 });
    return page.events.map((e) => [e.sequenceNumber, (e.data as { feedId: string }).feedId]);
  };

  it("keeps only the named feeds, in order", async () => {
    expect(await read([USDT_USD])).toEqual([[630509388n, USDT_USD], [630509390n, USDT_USD]]);
    expect((await read([APT_USD, USDT_USD])).map(([seq]) => seq)).toEqual([630509387n, 630509388n, 630509389n, 630509390n, 630509391n]);
  });

  it("drops events of other types on the page", async () => {
    const other = { ...EVENTS[0]!, type: UPDATE.replace("PriceFeedUpdate", "Other"), sequenceNumber: 1n, eventIndex: 1 };
    const page = await priceSource(new MemoryAdapter([other]), new Set([APT_USD])).fetchEvents(PYTH_DESCRIPTOR, { limit: 10 });
    expect(page.events).toEqual([]);
  });
});
