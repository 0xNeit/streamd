import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MemoryAdapter, StreamdError, parseEvent } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { saleDescriptor, saleSource, toSale } from "../src/sales.js";
import { testClient } from "./helpers.js";
import type { Sale } from "../src/types.js";

/**
 * Fill events from ten mainnet transactions (versions 4297748442, 6873435660,
 * 6941966929, 6966070277, 7007867174, 7021757802, 7025333927, 7028105119,
 * 7028273582 and 7028351626), read from the archive node through
 * `FullnodeScanAdapter` and written by `serializeEvent`, `entryFunction` dropped.
 */
const LINES = readFileSync(join(import.meta.dir, "fixtures/sales.jsonl"), "utf8");
const EVENTS = LINES.trim().split("\n").map((line) => parseEvent(line));

const WAPAL = "0x584b50b999c78ade62f8359c91b5165ff390338d45f8e55969a04e65d76258c9";
const TRADEPORT = "0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26";
const long = (hex: string) => `0x${hex.slice(2).padStart(64, "0")}`;
const CELESTIALS = "0x591d0aa2e16ae589473b936785a5a7d945335a4321c8b13e9e0686295d173412::Celestials";

const at = (version: number, eventIndex: number): StreamdEvent => {
  const event = EVENTS.find((e) => e.version === BigInt(version) && e.eventIndex === eventIndex);
  if (event === undefined) throw new Error(`no fixture at ${version}:${eventIndex}`);
  return event;
};
const saleAt = (version: number, eventIndex: number) => toSale(at(version, eventIndex))?.data;
const ids = (events: StreamdEvent[]) => events.map((e) => `${e.version}:${e.eventIndex}`);

describe("toSale on Wapal", () => {
  it("reads a token object listing fill with royalty, fee and collection", () => {
    // Bought through Tradeport's aggregator: only Wapal's event, so a Wapal sale.
    expect(saleAt(7028273582, 5)).toEqual({
      marketplace: "wapal",
      kind: "listing",
      token: "0xa06de4f3ea9341fd862ca53258822e0ce937e4dee7e02a224c11288bf99ad401",
      collection: "0x0ebc4ec70862f02fd6b23e8f3f630141d492f62b372a0e2b1b1ac509525c27b2",
      seller: "0xc86c70464007270b9f7ec9d0ab8bd1e68ba89edb632c9dcdfafef1010796fbdf",
      buyer: "0x28e0532dde3b7aaf76e65004e5d38d744ed11abd90014ee28cd1f85c6547f8c0",
      price: 444_000_000n,
      royalty: 22_200_000n,
      fee: 6_660_000n,
    });
  });

  it("reads a legacy token listing fill by its token id, with no collection", () => {
    expect(saleAt(7025333927, 5)).toEqual({
      marketplace: "wapal",
      kind: "listing",
      token: `${CELESTIALS}::Celestials #82::0`,
      seller: "0xaa41a6d6dd8a3ba9c7e4876b2a74f67def7c8d9bd90fbfaa931227e31fdee0ee",
      buyer: "0xda55740a5f0c70d0e9991a1d89aacd9feac300d1bb6cfd49b304e81b6805fc2b",
      price: 235_000_000n,
      royalty: 12_925_000n,
      fee: 3_525_000n,
    });
  });

  it("reads collection and token offer fills, buyer the offer's maker", () => {
    expect(saleAt(7007867174, 4)).toEqual({
      marketplace: "wapal",
      kind: "collection-offer",
      token: "0xdaf33569885af3f96b10866acb39acddc0087e4e0217aa60d584d8ebab1b2e66",
      collection: "0x927043ce908830bcce52472a24fd58bd452a4707125bb146e6103247de5b42f3",
      seller: "0x9cc92405db37732776b7ceb30d7124503d3762e2fc3a61a3bfb37cef5d4bb922",
      buyer: "0x666a8c5168d3aa26fa80d70a2eac8232d99113d2938191f695a0b88e359fc3cd",
      price: 100_000n,
      royalty: 5_000n,
      fee: 1_500n,
    });
    expect(saleAt(4297748442, 4)).toMatchObject({
      marketplace: "wapal",
      kind: "token-offer",
      token: "0x6fc2ad3dd5e907df11e8fb9de8a5eb222393a687af19279fa4062e28c4ae8176",
      seller: "0x2315bb5e044d38e4d453052c896863a9fe6ac89197b28111b969d01eaa4483d6",
      buyer: "0x94b620adba658fd83ece2709e4c371bb04227618ddfa415793153374b5f7115e",
      price: 450_000_000n,
    });
  });
});

describe("toSale on Tradeport", () => {
  it("reads token object listings, bulk buys one record per fill, without royalty or fee", () => {
    const seller = "0xd3f9528f567db3534600e195076404f971a0d0ef7f156200a9efc74b4654ef11";
    const buyer = "0x28e0532dde3b7aaf76e65004e5d38d744ed11abd90014ee28cd1f85c6547f8c0";
    expect(saleAt(7028351626, 7)).toEqual({
      marketplace: "tradeport", kind: "listing", token: "0x7e8ad7ea5ebc754444b96971e5beaa2feacec5d3bf81499b79a185b1611526b0", seller, buyer, price: 395_000_000n,
    });
    expect(saleAt(7028351626, 15)).toEqual({
      marketplace: "tradeport", kind: "listing", token: "0x6a47fb229792109470e09892bf48d371b64767838dd572fbe997e7be548e6feb", seller, buyer, price: 375_000_000n,
    });
  });

  it("reads token and collection bids accepted, in long form", () => {
    expect(saleAt(6941966929, 4)).toEqual({
      marketplace: "tradeport",
      kind: "token-offer",
      token: "0x88a91af6c66499ae24591f56b1833c1f83ed450b129a8bd890a835c50eee231c",
      seller: "0x508186409aca05f3f88c09e1e8964503912184a9bab5a86322e1247bcbec7304",
      // Reported in short form: one leading zero dropped.
      buyer: long("0x98f228e1022db7d12e47510ff2c6d4326e908f14cdb48b41e243cf65a847207"),
      price: 1_000n,
    });
    expect(saleAt(7021757802, 6)).toEqual({
      marketplace: "tradeport",
      kind: "collection-offer",
      token: "0x8784f4b04373772f3d0b4bb4ddb4220c3436a3b9b1b1e7b5055a5b1f866d20bb",
      seller: "0x9cc92405db37732776b7ceb30d7124503d3762e2fc3a61a3bfb37cef5d4bb922",
      buyer: "0x638e6abbf63f4a419a46b614923bf65ea66cf6327f36f4bda3720209da2e1826",
      price: 100_000n,
    });
  });

  it("reads the legacy token modules by token id", () => {
    expect(saleAt(7028105119, 7)).toEqual({
      marketplace: "tradeport",
      kind: "listing",
      token: `${CELESTIALS}::Celestials #1393::0`,
      seller: "0x666a8c5168d3aa26fa80d70a2eac8232d99113d2938191f695a0b88e359fc3cd",
      buyer: "0x28e0532dde3b7aaf76e65004e5d38d744ed11abd90014ee28cd1f85c6547f8c0",
      price: 370_000_000n,
    });
    expect(saleAt(6873435660, 5)).toEqual({
      marketplace: "tradeport",
      kind: "token-offer",
      token: "0x043ec2cb158e3569842d537740fd53403e992b9e7349cc5d3dfaa5aff8faaef2::Bruh Bears::Bruh Bear #1482::1",
      seller: "0x1569f8d1321d56cc6613f3b825e3e4fd7a1fc1226c06ae19267fd42e5c24039f",
      buyer: "0x14cfffc3ce4c82f697d243a9a0db3f85dfefd1f3825012fc7ab9131f0b83ef21",
      price: 50_000_000n,
    });
    expect(saleAt(6966070277, 4)).toMatchObject({ marketplace: "tradeport", kind: "collection-offer", price: 700_000_000n });
  });
});

describe("toSale", () => {
  it("keeps the event's identity", () => {
    const event = at(7028351626, 7);
    const record = toSale(event)!;
    expect(record.version).toBe(event.version);
    expect(record.eventIndex).toBe(7);
    expect(record.type).toBe(`${TRADEPORT}::listings_v2::BuyEvent`);
    expect(record.transactionHash).toBe(event.transactionHash!);
    expect(record.guid).toEqual(event.guid!);
  });

  it("skips events that are not fills or do not have their struct's shape", () => {
    const listing = at(7028273582, 5);
    const data = listing.data as Record<string, unknown>;
    const meta = data.token_metadata as Record<string, unknown>;
    const broken = (patch: Record<string, unknown>) => toSale({ ...listing, data: { ...data, ...patch } });
    expect(toSale({ ...listing, type: `${WAPAL}::events::ListingPlacedEvent` })).toBeUndefined();
    expect(toSale({ ...listing, data: null })).toBeUndefined();
    expect(broken({ price: "-1" })).toBeUndefined();
    expect(broken({ royalties: undefined })).toBeUndefined();
    expect(broken({ purchaser: 7 })).toBeUndefined();
    expect(broken({ token_metadata: null })).toBeUndefined();
    expect(broken({ token_metadata: { ...meta, token: { vec: [] }, property_version: { vec: [] } } })).toBeUndefined();
    const buy = at(7028351626, 7);
    expect(toSale({ ...buy, data: { ...(buy.data as object), token: "0x1" } })).toBeUndefined();
  });
});

describe("saleDescriptor", () => {
  it("lists every marketplace's fill events by default, and narrows to those named", () => {
    expect(saleDescriptor().eventType).toHaveLength(9);
    const wapal = saleDescriptor("wapal").eventType as string[];
    expect(wapal).toEqual([
      `${WAPAL}::events::ListingFilledEvent`,
      `${WAPAL}::events::TokenOfferFilledEvent`,
      `${WAPAL}::events::CollectionOfferFilledEvent`,
    ]);
    expect(saleDescriptor(["tradeport"]).eventType).toHaveLength(6);
  });

  it("refuses an unknown marketplace or an empty list", () => {
    expect(() => saleDescriptor("topaz" as never)).toThrow(StreamdError);
    expect(() => saleDescriptor([])).toThrow(StreamdError);
    expect(() => saleDescriptor([7] as never)).toThrow(StreamdError);
  });
});

describe("saleSource", () => {
  const all = { fromVersion: 0n, limit: 100 };
  const records = async (token?: string, options: Record<string, unknown> = all) => {
    const page = await saleSource(new MemoryAdapter(EVENTS), token).fetchEvents(saleDescriptor(), options);
    return page.events as StreamdEvent<Sale>[];
  };

  it("gives a record per fill, in event order, across marketplaces in one transaction", async () => {
    const mixed = (await records()).filter((e) => e.version === 7025333927n);
    expect(mixed.map((e) => [e.eventIndex, e.data.marketplace, e.data.token])).toEqual([
      [5, "wapal", `${CELESTIALS}::Celestials #82::0`],
      [13, "tradeport", `${CELESTIALS}::Celestials #539::0`],
      [19, "wapal", `${CELESTIALS}::Celestials #1125::0`],
    ]);
    expect(await records()).toHaveLength(EVENTS.length);
  });

  it("narrows by token, an object address in any form or a legacy token id", async () => {
    const short = "0x88a91af6c66499ae24591f56b1833c1f83ed450b129a8bd890a835c50eee231c".toUpperCase().replace("0X", "0x");
    expect(ids(await records(short))).toEqual(["6941966929:4"]);
    expect(ids(await records(`${CELESTIALS}::Celestials #539::0`))).toEqual(["7025333927:13"]);
    expect(ids(await records(`${CELESTIALS}::Celestials #539::00`))).toEqual(["7025333927:13"]);
  });

  it("refuses a malformed token or a coin type", () => {
    expect(() => saleSource(new MemoryAdapter([]), "not a token")).toThrow(StreamdError);
    expect(() => saleSource(new MemoryAdapter([]), "0x1::aptos_coin::AptosCoin")).toThrow(StreamdError);
  });

  it("keeps the page's descending order and passes the optional reads through", async () => {
    const page = await records(undefined, { ...all, orderBy: "desc" });
    expect(page.map((e) => e.version)).toEqual(EVENTS.map((e) => e.version).reverse());
    expect(await saleSource(new MemoryAdapter(EVENTS)).getHeadVersion!(saleDescriptor())).toBe(7028351626n);
  });
});

describe("AptosClient sales", () => {
  it("getSales reads a range, counting limit after token and where", async () => {
    const client = testClient({ adapter: new MemoryAdapter(EVENTS) });
    expect(ids(await client.getSales({ fromVersion: 7028273582n, toVersion: 7028351626n }))).toEqual([
      "7028273582:5", "7028273582:11", "7028351626:7", "7028351626:15",
    ]);
    const token = "0x6a47fb229792109470e09892bf48d371b64767838dd572fbe997e7be548e6feb";
    expect(ids(await client.getSales({ fromVersion: 0n, token, limit: 1 }))).toEqual(["7028351626:15"]);
    const big = await client.getSales({ fromVersion: 0n, where: (e) => (e.data as Sale).price >= 500_000_000n, limit: 1 });
    expect(ids(big)).toEqual(["6966070277:4"]);
  });

  it("getSales narrows by marketplace and refuses an unknown one", async () => {
    const client = testClient({ adapter: new MemoryAdapter(EVENTS) });
    const wapal = await client.getSales({ fromVersion: 0n, marketplace: "wapal" });
    expect(wapal.map((e) => e.data.marketplace)).toEqual(Array(6).fill("wapal"));
    expect(await client.getSales({ fromVersion: 0n, marketplace: ["wapal", "tradeport"] })).toHaveLength(EVENTS.length);
    await expect(client.getSales({ marketplace: "topaz" as never })).rejects.toThrow(StreamdError);
  });

  it("sales follows pushed fills live until unsubscribed", async () => {
    const adapter = new MemoryAdapter([]);
    const client = testClient({ adapter });
    const seen: StreamdEvent<Sale>[] = [];
    let arrived = () => {};
    const sub = client.sales(
      (event) => {
        seen.push(event);
        if (seen.length === 2) arrived();
      },
      { marketplace: "tradeport", pollInterval: 1 },
    );
    expect(client.listenerCount()).toBe(0);
    await sub.caughtUp;
    const done = new Promise<void>((resolve) => (arrived = resolve));
    adapter.push(...EVENTS.filter((e) => e.version === 7025333927n || e.version === 7028105119n));
    await done;
    sub.unsubscribe();
    expect(seen.map((e) => [e.version, e.eventIndex, e.data.kind])).toEqual([
      [7025333927n, 13, "listing"],
      [7028105119n, 7, "listing"],
    ]);
    client.destroy();
  });

  it("sales refuses a bad marketplace or token at the call", () => {
    const client = testClient({ adapter: new MemoryAdapter([]) });
    expect(() => client.sales(() => {}, { marketplace: [] })).toThrow(StreamdError);
    expect(() => client.sales(() => {}, { token: "0xnot" })).toThrow(StreamdError);
    client.destroy();
  });
});
