import { describe, expect, it } from "bun:test";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { primaryName, resolveName } from "../src/names.js";
import { LONG, fakeNames, grpcError } from "./helpers.js";

const ALICE = LONG("a11ce");
const BOB = LONG("b0b");

function client(names: Parameters<typeof fakeNames>[0], reverse?: Parameters<typeof fakeNames>[1]) {
  const { nameService, calls } = fakeNames(names, reverse);
  return { sui: { nameService } as unknown as SuiGrpcClient, calls };
}

/** Rejects with exactly a plain `StreamdError`, not its network subclass. */
async function plain(promise: Promise<unknown>, text: string) {
  const err = await promise.then(() => undefined, (e: unknown) => e);
  expect(err).toBeInstanceOf(StreamdError);
  expect(err).not.toBeInstanceOf(StreamdNetworkError);
  expect((err as Error).message).toContain(text);
}

describe("resolveName", () => {
  it("returns an address in long form without a read", async () => {
    const { sui, calls } = client({});
    expect(await resolveName(sui, "0xa11ce")).toBe(ALICE);
    expect(calls.lookups).toEqual([]);
  });

  it("resolves both name forms and subnames to the target in long form", async () => {
    const { sui, calls } = client({ "alice.sui": "0xa11ce", "@alice": "0xa11ce", "pay.alice.sui": "0xb0b" });
    expect(await resolveName(sui, "alice.sui")).toBe(ALICE);
    expect(await resolveName(sui, "@alice")).toBe(ALICE);
    expect(await resolveName(sui, "pay.alice.sui")).toBe(BOB);
    expect(calls.lookups).toEqual(["alice.sui", "@alice", "pay.alice.sui"]);
  });

  it("rejects what the name service says about the name as a plain StreamdError naming it", async () => {
    const { sui } = client({
      "expired.sui": grpcError("RESOURCE_EXHAUSTED", "name has expired"),
      "encoded.sui": grpcError("RESOURCE_EXHAUSTED", "name%20has%20expired"),
      "bad.sui": grpcError("INVALID_ARGUMENT", "invalid domain"),
      "devnet.sui": grpcError("UNIMPLEMENTED", "SuiNS not configured for this network"),
      "empty.sui": null,
    });
    await plain(resolveName(sui, "nobody.sui"), "nobody.sui is not registered");
    await plain(resolveName(sui, "expired.sui"), "has expired");
    await plain(resolveName(sui, "encoded.sui"), "has expired");
    await plain(resolveName(sui, "bad.sui"), "is malformed: invalid domain");
    await plain(resolveName(sui, "devnet.sui"), "no SuiNS");
    await plain(resolveName(sui, "empty.sui"), "empty.sui has no target address");
  });

  it("rejects rate limits and transport failures as StreamdNetworkError", async () => {
    const { sui } = client({ "busy.sui": grpcError("RESOURCE_EXHAUSTED", "slow down"), "down.sui": new Error("fetch failed") });
    const busy = await resolveName(sui, "busy.sui").catch((e: unknown) => e);
    expect(busy).toBeInstanceOf(StreamdNetworkError);
    expect((busy as StreamdNetworkError).statusCode).toBe(429);
    await expect(resolveName(sui, "down.sui")).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("refuses malformed input and a client without a name service before any read", async () => {
    const { sui, calls } = client({});
    for (const bad of ["alice", "alice.apt", "-a.sui", "0xnope", "", 7 as unknown as string]) {
      await expect(resolveName(sui, bad)).rejects.toThrow(StreamdError);
    }
    expect(calls.lookups).toEqual([]);
    await expect(resolveName({} as SuiGrpcClient, "alice.sui")).rejects.toThrow("needs a SuiGrpcClient");
  });
});

describe("primaryName", () => {
  it("returns the name when it points back, asking in long form", async () => {
    const { sui, calls } = client({ "alice.sui": ALICE }, { [ALICE]: "alice.sui" });
    expect(await primaryName(sui, "0xa11ce")).toBe("alice.sui");
    expect(calls.reverse).toEqual([ALICE]);
  });

  it("gives undefined with no forward read when there is none or it expired", async () => {
    const { sui, calls } = client({}, { [BOB]: grpcError("RESOURCE_EXHAUSTED", "name has expired") });
    expect(await primaryName(sui, ALICE)).toBeUndefined();
    expect(await primaryName(sui, BOB)).toBeUndefined();
    expect(calls.lookups).toEqual([]);
  });

  it("gives undefined when the name points elsewhere or no longer resolves", async () => {
    const { sui } = client({ "moved.sui": BOB }, { [ALICE]: "moved.sui", [BOB]: "gone.sui" });
    expect(await primaryName(sui, ALICE)).toBeUndefined();
    expect(await primaryName(sui, BOB)).toBeUndefined();
  });

  it("rejects a network failure on either read", async () => {
    const down = new Error("fetch failed");
    const { sui } = client({ "alice.sui": down }, { [ALICE]: "alice.sui", [BOB]: down });
    await expect(primaryName(sui, ALICE)).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(primaryName(sui, BOB)).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("rejects a network without SuiNS, and bad input before any read", async () => {
    const { sui, calls } = client({}, { [ALICE]: grpcError("UNIMPLEMENTED") });
    await plain(primaryName(sui, ALICE), "no SuiNS");
    await expect(primaryName(sui, "alice.sui")).rejects.toThrow(StreamdError);
    expect(calls.reverse).toEqual([ALICE]);
  });
});
