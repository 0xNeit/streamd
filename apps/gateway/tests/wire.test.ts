import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { fromWireError, parseDescriptor, parseFetchOptions, toJson, toWireError } from "../src/wire.js";

const COIN = "0x1::coin::CoinDeposit";

describe("parseDescriptor", () => {
  it("rebuilds each kind from its known fields only", () => {
    expect(parseDescriptor({ kind: "module", eventType: COIN, extra: 1 })).toEqual({ kind: "module", eventType: COIN });
    expect(parseDescriptor({ kind: "module", eventType: [COIN] })).toEqual({ kind: "module", eventType: [COIN] });
    expect(parseDescriptor({ kind: "handle", accountAddress: "0x1", creationNumber: "5" })).toEqual({
      kind: "handle",
      accountAddress: "0x1",
      creationNumber: 5n,
    });
    expect(
      parseDescriptor({ kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::B", fieldName: "e" }),
    ).toEqual({ kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::B", fieldName: "e" });
    expect(parseDescriptor({ kind: "account", accountAddress: "0x1", eventTypes: [COIN], received: true })).toEqual({
      kind: "account",
      accountAddress: "0x1",
      eventTypes: [COIN],
      received: true,
    });
    expect(parseDescriptor({ kind: "account", accountAddress: "0x1", received: false })).toEqual({
      kind: "account",
      accountAddress: "0x1",
    });
    expect(parseDescriptor({ kind: "resource", accountAddress: "0x1", resourceType: "0x1::account::Account" })).toEqual({
      kind: "resource",
      accountAddress: "0x1",
      resourceType: "0x1::account::Account",
    });
    expect(parseDescriptor({ kind: "object", objectId: "0xa" })).toEqual({ kind: "object", objectId: "0xa" });
  });

  it("refuses malformed input as a StreamdError", () => {
    for (const bad of [
      null,
      "module",
      { kind: "nope" },
      { kind: "module" },
      { kind: "module", eventType: [] },
      { kind: "module", eventType: "not a type" },
      { kind: "module", eventType: Array(101).fill(COIN) },
      { kind: "handle", accountAddress: "0x1", creationNumber: 5 },
      { kind: "handle", accountAddress: "0x1", creationNumber: "18446744073709551616" },
      { kind: "handle", accountAddress: "0x1", creationNumber: "1", fieldName: "e" },
      { kind: "handle", accountAddress: "0x1" },
      { kind: "account", accountAddress: "zz" },
      { kind: "account", accountAddress: "0x1", received: "yes" },
      { kind: "resource", accountAddress: "0x1", resourceType: "x" },
      { kind: "object", objectId: "" },
    ]) {
      expect(() => parseDescriptor(bad)).toThrow(StreamdError);
    }
  });

  it("round-trips through toJson", () => {
    const descriptor = { kind: "handle", accountAddress: "0x1", creationNumber: 7n } as const;
    expect(parseDescriptor(JSON.parse(toJson(descriptor)))).toEqual(descriptor);
  });
});

describe("parseFetchOptions", () => {
  it("keeps the known options, versions as bigints", () => {
    expect(
      parseFetchOptions({ cursor: "c", fromVersion: "1", toVersion: "2", limit: 10, offset: 0, orderBy: "desc", where: 1 }),
    ).toEqual({ cursor: "c", fromVersion: 1n, toVersion: 2n, limit: 10, offset: 0, orderBy: "desc" });
    expect(parseFetchOptions(undefined)).toEqual({});
  });

  it("refuses malformed options", () => {
    for (const bad of [[], { fromVersion: 1 }, { fromVersion: "-1" }, { limit: 0 }, { limit: 1001 }, { offset: -1 }, { orderBy: "up" }, { cursor: "" }]) {
      expect(() => parseFetchOptions(bad)).toThrow(StreamdError);
    }
  });
});

describe("wire errors", () => {
  const same = (text: string) => text;

  it("keeps a network error's status and retry hint, and calls input errors StreamdError", () => {
    const wire = toWireError(new StreamdNetworkError("slow down", 429, undefined, 2000), same);
    expect(wire).toEqual({ name: "StreamdNetworkError", message: "slow down", statusCode: 429, retryAfter: 2000 });
    expect(toWireError(new StreamdError("bad"), same).name).toBe("StreamdError");
    expect(toWireError(new Error("boom"), () => "<redacted>")).toEqual({ name: "Error", message: "<redacted>" });
  });

  it("maps a body back: StreamdError stays input, anything else is a retryable network error", () => {
    const input = fromWireError({ error: { name: "StreamdError", message: "bad" } }, 400);
    expect(input).toBeInstanceOf(StreamdError);
    expect(input).not.toBeInstanceOf(StreamdNetworkError);
    const network = fromWireError({ error: { name: "StreamdNetworkError", message: "gone", statusCode: 410 } }, 410);
    expect(network).toBeInstanceOf(StreamdNetworkError);
    expect((network as StreamdNetworkError).statusCode).toBe(410);
    const bare = fromWireError("not json", 502, 1000) as StreamdNetworkError;
    expect(bare.message).toBe("gateway answered 502");
    expect(bare.statusCode).toBe(502);
    expect(bare.retryAfter).toBe(1000);
  });
});
