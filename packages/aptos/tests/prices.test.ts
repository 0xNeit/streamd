import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError, StreamdNetworkError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { PYTH, PYTH_DESCRIPTOR, priceSource, requireFeedId, toPrice } from "../src/prices.js";
import { testClient } from "./helpers.js";
import type { Price } from "../src/types.js";

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

describe("AptosClient.prices", () => {
  it("follows the named feeds as updates land, and refuses bad ids at the call", async () => {
    const adapter = new MemoryAdapter([]);
    const client = testClient({ adapter });
    const seen: StreamdEvent<Price>[] = [];
    let arrived = () => {};
    const sub = client.prices(USDT_USD.slice(2).toUpperCase(), (event) => {
      seen.push(event);
      if (seen.length === 2) arrived();
    }, { pollInterval: 1 });
    expect(client.listenerCount()).toBe(0);
    await sub.caughtUp;
    const done = new Promise<void>((resolve) => (arrived = resolve));
    adapter.push(...EVENTS);
    await done;
    sub.unsubscribe();
    expect(seen.map((e) => [e.sequenceNumber, e.data.feedId, e.data.price])).toEqual([
      [630509388n, USDT_USD, 99994148n],
      [630509390n, USDT_USD, 99994015n],
    ]);
    expect(() => client.prices("0x1", () => {})).toThrow("Not a Pyth price feed id");
    expect(() => client.prices([], () => {})).toThrow(StreamdError);
    client.destroy();
  });
});

const TABLE = "0xd1321c17eebcaceee2d54d5f6ea0f78dae846689935ef53d1f0c3cff9e2d6c49";
const INFO = {
  arrival_time: "1790009907",
  attestation_time: "1790009907",
  price_feed: {
    ema_price: { conf: "130848", expo: { magnitude: "8", negative: true }, price: { magnitude: "77521942", negative: false }, timestamp: "1790009897" },
    price: { conf: "152447", expo: { magnitude: "8", negative: true }, price: { magnitude: "77353341", negative: false }, timestamp: "1790009897" },
    price_identifier: { bytes: APT_USD },
  },
};

type Reply = { status?: number; data: unknown };

/** An SDK instance over a fake fullnode answering by path, recording each request. */
function fakeNode(route: (path: string, params: Record<string, unknown>, body: unknown) => Reply) {
  const requests: { path: string; params: Record<string, unknown>; body: unknown }[] = [];
  const client = {
    async provider(req: { url: string; params?: Record<string, unknown>; body?: unknown }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
      const params = { ...(req.params ?? {}) };
      requests.push({ path, params, body: req.body });
      const { status = 200, data } = route(path, params, req.body);
      return { status, statusText: status === 200 ? "OK" : "Error", data, headers: {} };
    },
  };
  const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, fullnode: "http://fake/v1", client: client as never, clientConfig: { http2: false } }));
  return { aptos, requests };
}

const RESOURCE = `accounts/${PYTH}/resource/${encodeURIComponent(`${PYTH}::state::LatestPriceInfo`)}`;

/** Pyth's table and blocks, with `item` answering the table read. */
const pyth = (item: (version: bigint, key: unknown) => Reply) => (path: string, params: Record<string, unknown>, body: unknown): Reply => {
  if (path === RESOURCE) return { data: { type: `${PYTH}::state::LatestPriceInfo`, data: { info: { handle: TABLE } } } };
  if (path === `tables/${TABLE}/item`) return item(BigInt(params.ledger_version as string), (body as { key: unknown }).key);
  const block = /^blocks\/by_version\/(\d+)$/.exec(path);
  if (block !== null) return { data: { block_height: "1", block_timestamp: "1794793906210481", first_version: block[1], last_version: block[1] } };
  throw new Error(`unexpected path ${path}`);
};

describe("AptosClient.priceAt", () => {
  it("reads the feed's table entry and the block at the version, the table handle once", async () => {
    const { aptos, requests } = fakeNode(pyth(() => ({ data: INFO })));
    const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
    const price = await client.priceAt(APT_USD.slice(2), 8070625397n);
    expect(price).toEqual({
      oracle: "pyth",
      feedId: APT_USD,
      price: 77353341n,
      confidence: 152447n,
      exponent: -8,
      publishTime: 1790009897000000n,
      observedAt: 1794793906210481n,
      staleness: 1794793906210481n - 1790009897000000n,
    });
    await client.priceAt(APT_USD, 8000000000n);
    const tableReads = requests.filter((r) => r.path === `tables/${TABLE}/item`);
    expect(tableReads.map((r) => [r.params.ledger_version, r.body])).toEqual([
      ["8070625397", { key_type: `${PYTH}::price_identifier::PriceIdentifier`, value_type: `${PYTH}::price_info::PriceInfo`, key: { bytes: APT_USD } }],
      ["8000000000", { key_type: `${PYTH}::price_identifier::PriceIdentifier`, value_type: `${PYTH}::price_info::PriceInfo`, key: { bytes: APT_USD } }],
    ]);
    expect(requests.filter((r) => r.path === RESOURCE)).toHaveLength(1);
    expect(requests.filter((r) => r.path.startsWith("blocks/by_version/")).map((r) => r.path)).toEqual([
      "blocks/by_version/8070625397",
      "blocks/by_version/8000000000",
    ]);
    client.destroy();
  });

  it("resolves undefined when the table held no entry for the feed", async () => {
    const { aptos } = fakeNode(pyth(() => ({ status: 404, data: { message: "not found", error_code: "table_item_not_found" } })));
    const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
    expect(await client.priceAt(USDT_USD, 5n)).toBeUndefined();
    client.destroy();
  });

  it("rejects a pruned or future version with the fullnode's status", async () => {
    for (const [status, code] of [[410, "version_pruned"], [404, "version_not_found"]] as const) {
      const { aptos } = fakeNode(pyth(() => ({ status, data: { message: "no", error_code: code } })));
      const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
      const err = await client.priceAt(APT_USD, 5n).catch((e) => e);
      expect(err).toBeInstanceOf(StreamdNetworkError);
      expect(err.statusCode).toBe(status);
      client.destroy();
    }
  });

  it("refuses an entry for another feed or without its fields", async () => {
    for (const info of [{ ...INFO, price_feed: { ...INFO.price_feed, price_identifier: { bytes: USDT_USD } } }, { arrival_time: "1" }]) {
      const { aptos } = fakeNode(pyth(() => ({ data: info })));
      const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
      await expect(client.priceAt(APT_USD, 5n)).rejects.toThrow("does not have its struct's fields");
      client.destroy();
    }
  });

  it("reads the table handle again after a failed read", async () => {
    let fail = true;
    const { aptos, requests } = fakeNode((path, params, body) => {
      if (path === RESOURCE && fail) return (fail = false), { status: 503, data: { message: "busy" } };
      return pyth(() => ({ data: INFO }))(path, params, body);
    });
    const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
    await expect(client.priceAt(APT_USD, 5n)).rejects.toThrow(StreamdNetworkError);
    expect((await client.priceAt(APT_USD, 5n))?.price).toBe(77353341n);
    expect(requests.filter((r) => r.path === RESOURCE)).toHaveLength(2);
    client.destroy();
  });

  it("refuses bad ids and versions before any request", async () => {
    const { aptos, requests } = fakeNode(pyth(() => ({ data: INFO })));
    const client = testClient({ aptos, adapter: new MemoryAdapter([]) });
    await expect(client.priceAt("0x1", 5n)).rejects.toThrow("Not a Pyth price feed id");
    await expect(client.priceAt(APT_USD, 5 as never)).rejects.toThrow("Not a valid ledger version");
    await expect(client.priceAt(APT_USD, -1n)).rejects.toThrow("Not a valid ledger version");
    expect(requests).toHaveLength(0);
    client.destroy();
  });
});
