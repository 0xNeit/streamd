import { describe, expect, it } from "bun:test";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdError, StreamdNetworkError, type StreamdEvent } from "@streamd/core";
import { ownedBy } from "../src/ownership.js";
import { LONG, fakeNames, grpcError } from "./helpers.js";

const [ALICE, BOB, NFT, BAG, FIELD, POOL, PKG, COIN] = ["a11ce", "b0b", "f1", "ba9", "f1e1d", "9001", "2", "c01"].map(LONG);
const [ADDRESS, OBJECT, SHARED, IMMUTABLE, CONSENSUS] = [1, 2, 3, 4, 5];

/** NFT owned by alice; FIELD owned by BAG owned by alice; COIN consensus-owned by bob. */
const OWNERS: Record<string, { kind: number; address?: string } | Error> = {
  [NFT]: { kind: ADDRESS, address: "0xa11ce" },
  [FIELD]: { kind: OBJECT, address: BAG },
  [BAG]: { kind: OBJECT, address: ALICE },
  [COIN]: { kind: CONSENSUS, address: BOB },
  [POOL]: { kind: SHARED },
  [PKG]: { kind: IMMUTABLE },
};

function client(owners: Record<string, { kind: number; address?: string } | Error> = OWNERS, names: Parameters<typeof fakeNames>[0] = {}) {
  const reads: string[] = [];
  const { nameService, calls } = fakeNames(names);
  const ledgerService = {
    getObject: async ({ objectId }: { objectId?: string }) => {
      reads.push(objectId!);
      const owner = owners[objectId!];
      if (owner instanceof Error) throw owner;
      if (owner === undefined) throw grpcError("NOT_FOUND", `Object ${objectId} not found`);
      return { response: { object: { owner } } };
    },
  };
  return { sui: { ledgerService, nameService } as unknown as SuiGrpcClient, reads, lookups: calls.lookups };
}

const ev = (data: unknown, version = 1n) => ({ type: "0x2::kiosk::ItemListed", data, version, sequenceNumber: 0n, eventIndex: 0 }) as StreamdEvent;

describe("ownedBy", () => {
  it("matches an address- or consensus-owned object for its owner only", async () => {
    const { sui } = client();
    expect(await ownedBy(sui, ALICE)(ev({ id: "0xf1" }))).toBe(true);
    expect(await ownedBy(sui, "0xb0b")(ev({ id: NFT }))).toBe(false);
    expect(await ownedBy(sui, BOB)(ev({ object_id: COIN }))).toBe(true);
  });

  it("walks up objects to the wallet, and an object id can be the wallet", async () => {
    const { sui, reads } = client();
    expect(await ownedBy(sui, ALICE)(ev({ nft_id: FIELD }))).toBe(true);
    expect(reads).toEqual([FIELD, BAG]);
    expect(await ownedBy(sui, BAG)(ev({ nft_id: FIELD }))).toBe(true);
    expect(await ownedBy(sui, BOB)(ev({ nft_id: FIELD }))).toBe(false);
  });

  it("does not match shared, immutable, missing or too deep objects", async () => {
    const chain: Record<string, { kind: number; address: string }> = {};
    for (let i = 1; i <= 9; i++) chain[LONG(`d${i}`)] = { kind: OBJECT, address: i === 9 ? ALICE : LONG(`d${i + 1}`) };
    const { sui } = client({ ...OWNERS, ...chain });
    const alice = ownedBy(sui, ALICE);
    for (const id of [POOL, PKG, LONG("dead")]) expect(await alice(ev({ id }))).toBe(false);
    expect(await alice(ev({ id: LONG("d2") }))).toBe(true);
    expect(await alice(ev({ id: LONG("d1") }))).toBe(false);
  });

  it("reads nothing for an event naming no object, and checks fields in order", async () => {
    const { sui, reads } = client();
    const alice = ownedBy(sui, ALICE);
    expect(await alice(ev({ amount: "5", id: 7, object_id: "nope" }))).toBe(false);
    expect(await alice(ev(null))).toBe(false);
    expect(reads).toEqual([]);
    expect(await alice(ev({ id: POOL, object_id: NFT, nft_id: COIN }))).toBe(true);
    expect(reads).toEqual([POOL, NFT]);
    const custom = client();
    expect(await ownedBy(custom.sui, ALICE, { fields: ["item"] })(ev({ id: NFT, item: FIELD }))).toBe(true);
    expect(custom.reads).toEqual([FIELD, BAG]);
  });

  it("refuses bad options, wallets and clients", () => {
    const { sui } = client();
    expect(() => ownedBy(sui, ALICE, { fields: [] })).toThrow(StreamdError);
    expect(() => ownedBy(sui, ALICE, { fields: ["id", ""] })).toThrow(StreamdError);
    expect(() => ownedBy(sui, ALICE, { maxConcurrentRequests: 0 })).toThrow(StreamdError);
    expect(() => ownedBy(sui, "alice")).toThrow(StreamdError);
    expect(() => ownedBy({} as SuiGrpcClient, ALICE)).toThrow("needs a SuiGrpcClient");
    const noNames = { ledgerService: { getObject: async () => ({}) } } as unknown as SuiGrpcClient;
    expect(() => ownedBy(noNames, "alice.sui")).toThrow("needs a SuiGrpcClient");
  });

  it("caches per object and version", async () => {
    const { sui, reads } = client();
    const alice = ownedBy(sui, ALICE);
    await alice(ev({ id: NFT }, 5n));
    await alice(ev({ id: NFT }, 5n));
    await alice(ev({ id: NFT }, 6n));
    expect(reads).toEqual([NFT, NFT]);
  });

  it("resolves a name wallet once, retrying a failed resolution", async () => {
    let fail = true;
    const names = { get "alice.sui"() { return fail ? new Error("fetch failed") : ALICE; } } as unknown as Record<string, string>;
    const { sui, lookups } = client(OWNERS, names);
    const alice = ownedBy(sui, "alice.sui");
    await expect(alice(ev({ id: NFT }))).rejects.toBeInstanceOf(StreamdNetworkError);
    fail = false;
    expect(await alice(ev({ id: NFT }))).toBe(true);
    expect(await alice(ev({ id: NFT }, 2n))).toBe(true);
    expect(lookups).toEqual(["alice.sui", "alice.sui"]);
  });

  it("rejects a transport failure as a network error and does not cache it", async () => {
    const owners = { ...OWNERS, [NFT]: new Error("fetch failed") };
    const { sui, reads } = client(owners);
    const alice = ownedBy(sui, ALICE);
    await expect(alice(ev({ id: NFT }))).rejects.toBeInstanceOf(StreamdNetworkError);
    owners[NFT] = OWNERS[NFT]!;
    expect(await alice(ev({ id: NFT }))).toBe(true);
    expect(reads).toEqual([NFT, NFT]);
    const refused = client({ [NFT]: grpcError("INVALID_ARGUMENT", "bad id") });
    const err = await Promise.resolve(ownedBy(refused.sui, ALICE)(ev({ id: NFT }))).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });
});
