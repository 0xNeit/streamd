import { describe, expect, it } from "bun:test";
import { MemoryAdapter } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiClient, networks } from "../src/index.js";
import type { SuiClientConfig } from "../src/index.js";
import { LONG, fakeLedger } from "./helpers.js";

const STAKE = `${LONG("3")}::validator::StakingRequestEvent` as MoveEventType;
const stakes = { kind: "module", eventType: STAKE } as const;

/** Exposes the protected client, to see which one the constructor kept. */
class Inspect extends SuiClient {
  get client(): SuiGrpcClient {
    return this.sui;
  }
}

describe("SuiClient", () => {
  it("serves events through a SuiAdapter over the given client", async () => {
    const { client } = fakeLedger([1, 2], [{ cp: 1n, ti: 1n, ei: 0, type: STAKE }]);
    const sui = new Inspect({ sui: client as unknown as SuiGrpcClient });
    expect(sui.client).toBe(client as unknown as SuiGrpcClient);
    const events = await sui.getEvents(stakes, { fromVersion: 0n });
    expect(events.map((e) => [e.version, e.type])).toEqual([[2n, STAKE]]);
  });

  it("takes a given adapter over the default", async () => {
    const { client, calls } = fakeLedger([1], []);
    const adapter = new MemoryAdapter([{ type: STAKE, data: {}, version: 7n, sequenceNumber: 0n, eventIndex: 0 }]);
    const sui = new SuiClient({ sui: client as unknown as SuiGrpcClient, adapter });
    expect((await sui.getEvents(stakes, { fromVersion: 0n })).map((e) => e.version)).toEqual([7n]);
    expect(calls.list).toEqual([]);
  });

  it("builds a SuiGrpcClient from options, such as a preset", () => {
    const sui = new Inspect({ sui: networks.testnet });
    expect(typeof sui.client.ledgerService.listEvents).toBe("function");
    expect(sui.client.network).toBe("testnet");
  });

  it.each([undefined, {}, { sui: null }, { sui: "mainnet" }])("refuses construction without sui: %p", (config) => {
    expect(() => new SuiClient(config as unknown as SuiClientConfig)).toThrow("requires `sui`");
  });
});
