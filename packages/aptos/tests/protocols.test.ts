import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient } from "@streamd/core";
import type { EventDescriptor } from "@streamd/core";
import { AAVE_DESCRIPTOR, ECHELON_DESCRIPTOR } from "../src/lending.js";
import * as protocols from "../src/protocols.js";
import { saleDescriptor } from "../src/sales.js";
import { swapDescriptor } from "../src/swaps.js";

// Decoded types whose modules declare no `#[event]` structs: each emits through an `EventHandle`.
const LEFT_OUT = [
  "0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12::liquidity_pool::SwapEvent",
  "0x163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e::liquidity_pool::SwapEvent",
  "0xc7efb4076dbe143cbcd98cfaaa929ecfc8f299203dfff63b95ccb6bfe19850fa::swap::SwapEvent",
  "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af::stable_pool::SwapEvent",
  "0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af::weighted_pool::SwapEvent",
  ...["ListingFilledEvent", "TokenOfferFilledEvent", "CollectionOfferFilledEvent"].map(
    (s) => `0x584b50b999c78ade62f8359c91b5165ff390338d45f8e55969a04e65d76258c9::events::${s}`,
  ),
  ...["listings_v2::BuyEvent", "listings::BuyEvent", "biddings_v2::AcceptTokenBidEvent", "biddings_v2::AcceptCollectionBidEvent", "biddings::AcceptTokenBidEvent", "biddings::AcceptCollectionBidEvent"].map(
    (s) => `0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26::${s}`,
  ),
];

const typesOf = (d: EventDescriptor): string[] => (d.kind !== "module" ? [] : typeof d.eventType === "string" ? [d.eventType] : [...d.eventType]);

describe("protocol catalog", () => {
  // Single-type descriptors only, so a module union cannot stand in for a missing descriptor.
  const catalog = new Set<string>(Object.values(protocols).flatMap((d) => ("kind" in d && typeof d.eventType === "string" ? [d.eventType] : [])));

  it("has a descriptor for every type the swap, sale and lending decoders read, but the handle events", () => {
    const decoded = [swapDescriptor(), saleDescriptor(), AAVE_DESCRIPTOR, ECHELON_DESCRIPTOR].flatMap(typesOf);
    expect(decoded.length).toBe(34);
    for (const type of decoded) expect(catalog.has(type) || LEFT_OUT.includes(type)).toBe(true);
    for (const type of LEFT_OUT) {
      expect(decoded).toContain(type);
      expect(catalog.has(type)).toBe(false);
    }
  });

  it("narrows event.data through a descriptor", async () => {
    const data = { pool_id: "0xa", from_token: { inner: "0xb" }, to_token: { inner: "0xc" }, amount_in: "5", amount_out: "4" };
    const client = new StreamdClient({
      adapter: new MemoryAdapter([{ type: "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3::SwapEventV3", data, version: 1n, sequenceNumber: 0n, eventIndex: 0 }]),
    });
    const [event] = await client.getEvents(protocols.SwapEventV3, { fromVersion: 0n });
    expect(event!.data.from_token.inner satisfies string).toBe("0xb");
    expect(event!.data.amount_in satisfies string).toBe("5");
    // @ts-expect-error SwapEventV3 has no such field
    void event!.data.no_such_field;
  });
});
