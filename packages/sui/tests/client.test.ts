import { describe, expect, it } from "bun:test";
import { MemoryAdapter, fromFunction, parseEvent, serializeEvent, StreamdAssertionError, StreamdError, StreamdTimeoutError, StreamdTransactionError } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { SuiAdapter, SuiClient, networks } from "../src/index.js";
import type { SuiClientConfig } from "../src/index.js";
import { LONG, digest, fakeLedger, fakeNames, ptb, withCalls, transactionBytes, withSimulator, withTransfers, type FakeListed, type FakeSimulation, type FakeTransaction } from "./helpers.js";

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

describe("SuiClient simulations", () => {
  const SWAP = `${LONG("abc")}::pool::Swap` as MoveEventType;
  const ALICE = LONG("a11ce");
  const client = (result: FakeSimulation) => {
    const { client: ledger } = fakeLedger([1, 2], []);
    const { client: simulating, requests } = withSimulator(ledger, result);
    return { sui: new SuiClient({ sui: simulating as unknown as SuiGrpcClient }), requests };
  };
  const EVENTS: FakeSimulation = {
    events: [
      { type: STAKE, sender: "0xa11ce", amount: "5" },
      { type: SWAP, sender: "0xa11ce" },
      { type: STAKE, sender: "0xb0b", amount: "9" },
    ],
  };

  it("previews events, narrowed by a short-form type and by an account", async () => {
    const { sui } = client(EVENTS);
    const bytes = await transactionBytes();
    const stakesShort = { kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType } as const;
    expect((await sui.previewEvents(bytes, stakesShort)).map((e) => [e.eventIndex, e.version, e.transactionHash])).toEqual([
      [0, 3n, undefined],
      [2, 3n, undefined],
    ]);
    const byAlice = await sui.previewEvents(bytes, { kind: "account", accountAddress: "0xa11ce", eventTypes: [STAKE] }, { decode: ["amount"] });
    expect(byAlice.map((e) => (e.data as { amount: bigint }).amount)).toEqual([5n]);
    expect(byAlice[0]?.sender).toBe(ALICE);
  });

  it("rejects a failed simulation with StreamdTransactionError", async () => {
    const { sui } = client({ error: "MoveAbort(m::z, 10) in command 1", events: [] });
    const err = await sui.previewEvents(await transactionBytes()).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: undefined, version: 3n, vmStatus: "MoveAbort(m::z, 10) in command 1" });
  });

  it("refuses bad bytes, a handle and a resource descriptor before any call", async () => {
    const { sui, requests } = client(EVENTS);
    const bytes = await transactionBytes();
    await expect(sui.previewEvents(new Uint8Array([1, 2, 3]))).rejects.toThrow(StreamdError);
    await expect(sui.previewEvents(bytes, { kind: "handle", accountAddress: ALICE, creationNumber: 1n })).rejects.toThrow(StreamdError);
    await expect(sui.previewEvents(bytes, { kind: "resource", accountAddress: ALICE, resourceType: "0x2::coin::Coin" })).rejects.toThrow(StreamdError);
    expect(() => sui.expectPreview("AAEC" as unknown as Uint8Array)).toThrow(StreamdError);
    await expect(sui.expectPreview(bytes).toEmit({ kind: "handle", accountAddress: ALICE, creationNumber: 1n })).rejects.toThrow(StreamdError);
    expect(requests).toEqual([]);
  });

  it("asserts on a simulation with expectPreview, one simulation per assertion", async () => {
    const { sui, requests } = client(EVENTS);
    const expectation = sui.expectPreview(await transactionBytes(), { decode: ["amount"] });
    expect(requests).toHaveLength(0);
    const swaps = await expectation.toEmit({ kind: "module", eventType: `0xabc::pool::Swap` as MoveEventType });
    expect(swaps.map((e) => e.eventIndex)).toEqual([1]);
    const err = await expectation.notToEmit({ kind: "module", eventType: "0x3::validator::StakingRequestEvent" as MoveEventType }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect((err as StreamdAssertionError).transactionHash).toBeUndefined();
    expect((err as StreamdAssertionError).events).toHaveLength(3);
    expect(requests).toHaveLength(2);
  });
});

describe("SuiClient transfers and balances", () => {
  const SUI = `${LONG("2")}::sui::SUI`;
  const USDC = `${LONG("dba")}::usdc::USDC`;
  const [ALICE, BOB] = [LONG("a11ce"), LONG("b0b")];
  const SIZES = [1, 3, 2];
  const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
  function client(listed: FakeListed[], balances?: Map<string, bigint>, extra: Partial<SuiClientConfig> = {}) {
    const ledger = fakeLedger(SIZES, []);
    const wired = withTransfers(ledger.client, SIZES, listed, balances, new Map([[SUI, { symbol: "SUI", decimals: 9 }]]));
    return { sui: new SuiClient({ sui: wired.client as unknown as SuiGrpcClient, ...extra }), calls: wired.calls };
  }
  const LISTED: FakeListed[] = [
    { cp: 1n, ti: 0n, gas: 1n, changes: [[ALICE, SUI, -11n], [BOB, SUI, 10n]] },
    { cp: 1n, ti: 1n, changes: [[BOB, USDC, -7n], [ALICE, USDC, 7n]] },
    { cp: 2n, ti: 0n, changes: [[ALICE, SUI, -3n], [BOB, SUI, 3n]] },
  ];

  it("returns paired records over a range, with limit, where and asset info", async () => {
    const { sui } = client(LISTED);
    const all = await sui.getTransfers({ fromVersion: 0n });
    expect(all.map((e) => [e.version, e.data.asset, e.data.amount])).toEqual([[1n, SUI, 10n], [2n, USDC, 7n], [4n, SUI, 3n]]);
    const usdc = await sui.getTransfers({ fromVersion: 0n, where: (e) => e.type === USDC });
    expect(usdc.map((e) => e.data)).toEqual([{ asset: USDC, amount: 7n, from: BOB, to: ALICE }]);
    expect(await sui.getTransfers({ fromVersion: 0n, limit: 1 })).toHaveLength(1);
    const info = await sui.getTransfers({ fromVersion: 0n, asset: "0x2::sui::SUI", withAssetInfo: true });
    expect(info.map((e) => [e.data.symbol, e.data.decimals])).toEqual([["SUI", 9], ["SUI", 9]]);
    const noInfo = await sui.getTransfers({ fromVersion: 0n, asset: USDC, withAssetInfo: true });
    expect(noInfo[0]!.data.symbol).toBeUndefined();
  });

  it("follows new transactions live, is not a listener, and stops on unsubscribe", async () => {
    const listed: FakeListed[] = [];
    const { sui } = client(listed);
    const seen: bigint[] = [];
    const sub = sui.transfers((e) => void seen.push(e.data.amount), { fromVersion: 0n, pollInterval: 5, account: "0xb0b" });
    expect(sui.listenerCount()).toBe(0);
    await wait(20);
    listed.push(...LISTED);
    await wait(40);
    expect(seen).toEqual([10n, 7n, 3n]);
    sub.unsubscribe();
    expect(sub.active).toBe(false);
  });

  it("reads SUI balances by default, reports a change, and keys by long-form type", async () => {
    const balances = new Map([[`${ALICE}:${SUI}`, 5n]]);
    const { sui, calls } = client([], balances);
    const changes: bigint[] = [];
    using wallet = sui.balances("0xa11ce", { pollInterval: 5, onChange: (_, c) => void changes.push(c.data.balance) });
    expect(await wallet.ready).toEqual(new Map([[SUI, 5n]]));
    expect(calls.balances[0]).toBe(`${ALICE}:${SUI}`);
    balances.set(`${ALICE}:${SUI}`, 8n);
    await wait(30);
    expect(changes).toEqual([8n]);
    expect(wallet.get("0x2::sui::SUI")).toBe(8n);
    expect(sui.listenerCount()).toBe(0);
  });

  it("refuses a bad account, asset or coin type at the call", async () => {
    const { sui } = client([]);
    expect(() => sui.transfers(() => {}, { account: "nope" })).toThrow(StreamdError);
    expect(() => sui.transfers(() => {}, { asset: "nope" })).toThrow("coin type");
    await expect(sui.getTransfers({ account: "nope" })).rejects.toThrow(StreamdError);
    expect(() => sui.balances("nope")).toThrow(StreamdError);
    expect(() => sui.balances(ALICE, { asset: "nope" })).toThrow("coin type");
    expect(() => sui.balances(ALICE, { asset: [] })).toThrow("empty");
  });

  it("takes SuiNS names for account and balances, resolving each once", async () => {
    const CAROL = LONG("ca401");
    const listed: FakeListed[] = [...LISTED, { cp: 2n, ti: 1n, changes: [[CAROL, SUI, -2n], [ALICE, SUI, 2n]] }];
    const wired = withTransfers(fakeLedger(SIZES, []).client, SIZES, listed, new Map([[`${CAROL}:${SUI}`, 4n]]), new Map());
    const names = fakeNames({ "carol.sui": "0xca401", "@carol": CAROL });
    const sui = new SuiClient({ sui: { ...wired.client, nameService: names.nameService } as unknown as SuiGrpcClient });
    const carols = await sui.getTransfers({ fromVersion: 0n, account: "carol.sui" });
    expect(carols.map((e) => e.data)).toEqual([{ asset: SUI, amount: 2n, from: CAROL, to: ALICE }]);
    expect(names.calls.lookups).toEqual(["carol.sui"]);
    const seen: bigint[] = [];
    const sub = sui.transfers((e) => void seen.push(e.data.amount), { fromVersion: 0n, pollInterval: 5, account: "@carol" });
    await wait(30);
    sub.unsubscribe();
    expect(seen).toEqual([2n]);
    using wallet = sui.balances("carol.sui", { pollInterval: 5 });
    expect(await wallet.ready).toEqual(new Map([[SUI, 4n]]));
    expect(wired.calls.balances[0]).toBe(`${CAROL}:${SUI}`);
    expect(names.calls.lookups).toEqual(["carol.sui", "@carol", "carol.sui"]);
    expect(() => sui.transfers(() => {}, { account: "carol" })).toThrow(StreamdError);
  });

  it("reads transfers through a SuiAdapter over sui when given another adapter", async () => {
    const { sui, calls } = client(LISTED, undefined, { adapter: new MemoryAdapter([]) });
    expect(await sui.getTransfers({ fromVersion: 0n })).toHaveLength(3);
    expect(calls.list.length).toBeGreaterThan(0);
  });
});

describe("SuiClient entry functions", () => {
  it("filters events by the call that emitted them, and keeps the call through serialisation", async () => {
    const [A, B] = [LONG("a"), LONG("b")];
    const events = [
      { cp: 1n, ti: 0n, ei: 0, type: STAKE, pkg: A, module: "pool" },
      { cp: 1n, ti: 1n, ei: 0, type: STAKE, pkg: B, module: "vault" },
    ];
    const ptbs = {
      D1t0: ptb([{ package: A, module: "pool", function: "swap", arguments: [{ kind: 1 }] }]),
      D1t1: ptb([{ package: B, module: "vault", function: "deposit" }]),
    };
    const { client } = withCalls(fakeLedger([1, 2], events).client, ptbs);
    using sui = new SuiClient({ sui: client as unknown as SuiGrpcClient });
    const found = await sui.getEvents(stakes, { fromVersion: 0n, where: fromFunction("0xa::pool::swap") });
    expect(found.map((e) => e.entryFunction)).toEqual([{ function: `${A}::pool::swap`, typeArguments: [], arguments: ["GasCoin"] }]);
    expect(parseEvent(serializeEvent(found[0]!))).toEqual(found[0]!);
  });
});

describe("SuiClient object descriptors", () => {
  const SIZES = [1, 3, 2];
  const POOL = LONG("9001");
  const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const pool = { kind: "object" as const, objectId: "0x9001" };

  it("delivers an object's changes to getEvents and on, and refuses it for eventsOf", async () => {
    const listed: FakeListed[] = [
      { cp: 1n, ti: 1n, changes: [], objects: [{ id: POOL, version: 3n, value: "x" }] },
      { cp: 2n, ti: 0n, changes: [], objects: [{ id: LONG("9002"), version: 1n }] },
    ];
    const wired = withTransfers(fakeLedger(SIZES, []).client, SIZES, listed);
    const sui = new SuiClient({ sui: wired.client as unknown as SuiGrpcClient });
    expect((await sui.getEvents(pool, { fromVersion: 0n })).map((e) => [e.version, e.data])).toEqual([[2n, { value: "x" }]]);
    const seen: unknown[] = [];
    const sub = sui.on(pool, (e) => void seen.push(e.data), { fromVersion: 3n, pollInterval: 5 });
    expect(sui.listenerCount({ kind: "object", objectId: POOL })).toBe(1);
    listed.push({ cp: 2n, ti: 1n, changes: [], objects: [{ id: POOL }] });
    await wait(40);
    expect(seen).toEqual([null]);
    sub.unsubscribe();
    await expect(sui.eventsOf(digest(1), pool)).rejects.toThrow("not object changes");
  });
});

describe("SuiClient object state", () => {
  const POOL = LONG("9001");
  const json = { kind: { oneofKind: "structValue" as const, structValue: { fields: { value: { kind: { oneofKind: "stringValue" as const, stringValue: "after" } } } } } };

  it("attaches objects as the event's transaction left them, and still refuses views", async () => {
    const { client } = fakeLedger([1, 2], [{ cp: 1n, ti: 1n, ei: 0, type: STAKE }]);
    client.ledgerService.getTransaction = async () => ({
      response: {
        transaction: {
          effects: { changedObjects: [{ objectId: POOL, outputState: 2, outputVersion: 4n }] },
          objects: { objects: [{ objectId: POOL, version: 4n, json }] },
        } as never,
      },
    });
    const reader = new SuiAdapter(client);
    const adapter = Object.assign(
      new MemoryAdapter([{ type: STAKE, data: {}, version: 2n, sequenceNumber: 0n, eventIndex: 0, transactionHash: digest(2) }]),
      { readObjects: reader.readObjects.bind(reader) },
    );
    const sui = new SuiClient({ sui: client as unknown as SuiGrpcClient, adapter });
    const [event] = await sui.getEvents(stakes, { fromVersion: 0n, withState: () => ({ objects: ["0x9001"] }) });
    expect(event!.state).toEqual([{ value: "after" }]);
    const plain = new SuiClient({ sui: client as unknown as SuiGrpcClient });
    await expect(
      plain.getEvents(stakes, { fromVersion: 0n, withState: () => ({ function: "0x2::clock::timestamp_ms" }) }),
    ).rejects.toThrow("withState needs an adapter that implements view");
  });
});
