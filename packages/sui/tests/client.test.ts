import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdAssertionError, StreamdError, StreamdTimeoutError, StreamdTransactionError } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiClient, networks } from "../src/index.js";
import type { SuiClientConfig } from "../src/index.js";
import { LONG, digest, fakeLedger, type FakeTransaction } from "./helpers.js";

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

describe("SuiClient transaction events", () => {
  const OK = digest(1);
  const FAILED = digest(2);
  const SWAP = `${LONG("abc")}::pool::Swap` as MoveEventType;
  const ALICE = LONG("a11ce");
  const TRANSACTIONS: FakeTransaction[] = [
    { digest: OK, cp: 1n, ti: 0n, events: [{ type: STAKE, sender: "0xa11ce", amount: "5" }, { type: SWAP, sender: "0xb0b" }, { type: STAKE, sender: "0xa11ce", amount: "9" }] },
    { digest: FAILED, cp: 1n, ti: 1n, error: "MoveAbort(m::z, 101) in command 1", events: [] },
  ];
  /** The OK transaction appears only after `hidden` lookups, as one being checkpointed would. */
  function client(hidden = 0) {
    const ledger = fakeLedger([1, 2], [], undefined, 0n, TRANSACTIONS);
    const read = ledger.client.ledgerService.getTransaction.bind(ledger.client.ledgerService);
    ledger.client.ledgerService.getTransaction = async (input) => {
      if (input.digest === OK && hidden-- > 0) throw Object.assign(new Error("not found"), { code: "NOT_FOUND" });
      return read(input);
    };
    const sui = new SuiClient({ sui: ledger.client as unknown as SuiGrpcClient, defaults: { pollInterval: 0 } });
    return { sui, calls: ledger.calls };
  }

  it("waits for the transaction, then narrows by a short-form type and by an account", async () => {
    using sui = client(2).sui;
    const stakes = await sui.eventsOf(OK, { kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType });
    expect(stakes.map((e) => [e.version, e.eventIndex, e.transactionHash])).toEqual([[1n, 0, OK], [1n, 2, OK]]);
    const alice = await sui.eventsOf(OK, { kind: "account", accountAddress: "0xa11ce", eventTypes: ["0x3::validator::StakingRequestEvent"] }, { decode: ["amount"] });
    expect(alice.map((e) => (e.data as { amount: bigint }).amount)).toEqual([5n, 9n]);
    expect(alice.every((e) => e.sender === ALICE)).toBe(true);
    expect((await sui.eventsOf(OK)).length).toBe(3);
  });

  it("rejects a failed transaction with StreamdTransactionError", async () => {
    using sui = client().sui;
    const err = await sui.eventsOf(FAILED).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: FAILED, version: 2n, vmStatus: "MoveAbort(m::z, 101) in command 1" });
  });

  it("times out on a digest never found", async () => {
    using sui = client().sui;
    const err = await sui.eventsOf(digest(9), undefined, { timeout: 250 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
  });

  it("refuses a hex hash, a handle and a resource descriptor before any lookup", async () => {
    const { sui, calls } = client();
    using _sui = sui;
    const refusals = [
      sui.eventsOf(`0x${"ab".repeat(32)}`),
      sui.eventsOf(OK, { kind: "handle", accountAddress: ALICE, creationNumber: 2n }),
      sui.eventsOf(OK, { kind: "resource", accountAddress: ALICE, resourceType: `${LONG("2")}::coin::Coin` }),
    ];
    for (const refusal of refusals) {
      const err = await refusal.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(StreamdError);
    }
    expect(() => sui.expectEvents("0xabc")).toThrow("Not a valid Sui transaction digest");
    expect(calls.transactions).toEqual([]);
  });

  it("asserts on the transaction's events with expectEvents", async () => {
    using sui = client(1).sui;
    const expectation = sui.expectEvents(OK);
    const swap = { kind: "module", eventType: SWAP } as const;
    expect((await expectation.toEmit(swap)).map((e) => e.eventIndex)).toEqual([1]);
    const err = await expectation.notToEmit({ kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect(err).toMatchObject({ transactionHash: OK });
    expect((err as StreamdAssertionError).events.length).toBe(3);
    await expect(expectation.toEmit({ kind: "handle", accountAddress: ALICE, creationNumber: 1n })).rejects.toThrow(StreamdError);
  });
});
