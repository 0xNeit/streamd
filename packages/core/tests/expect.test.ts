import { describe, it, expect } from "bun:test";
import { StreamdClient } from "../src/client.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { RoutingAdapter } from "../src/adapters/routing.js";
import { whereField } from "../src/filter.js";
import {
  StreamdAssertionError,
  StreamdError,
  StreamdTimeoutError,
  StreamdTransactionError,
} from "../src/errors.js";
import type { EventAdapter, StreamdEvent, TransactionEvents } from "../src/types.js";
import type { PublicKey, SimpleTransaction } from "@aptos-labs/ts-sdk";

const HASH = `0x${"cd".repeat(32)}`;
const PING = "0x1::test::Ping";
const PONG = "0x1::test::Pong";
const MISSING = "0x1::test::Missing";

function ev(eventIndex: number, type: string, amount: string): StreamdEvent {
  return {
    type,
    data: { amount, big: 7n },
    version: 9n,
    sequenceNumber: 0n,
    eventIndex,
    transactionHash: HASH,
  };
}

const EVENTS = [ev(0, PING, "10"), ev(1, PONG, "20"), ev(2, PING, "30")];

function lookup(result: TransactionEvents | undefined) {
  const calls: { hash: string; descriptor: unknown }[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], hasMore: false }),
    async getTransactionEvents(hash, descriptor) {
      calls.push({ hash, descriptor });
      return result;
    },
  };
  return { adapter, calls };
}

const committed = (events: StreamdEvent[]): TransactionEvents => ({
  version: 9n,
  success: true,
  vmStatus: "Executed successfully",
  events,
});

function client(adapter: EventAdapter) {
  return new StreamdClient({ adapter, defaults: { pollInterval: 5 } });
}

async function failure(promise: Promise<unknown>): Promise<StreamdAssertionError> {
  const err = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(err).toBeInstanceOf(StreamdAssertionError);
  return err as StreamdAssertionError;
}

describe("expectEvents", () => {
  it("toEmit resolves with the matching events in transaction order", async () => {
    const { adapter, calls } = lookup(committed(EVENTS));
    const tx = client(adapter).expectEvents(HASH);
    const pings = await tx.toEmit({ kind: "module", eventType: PING });
    expect(pings).toEqual([EVENTS[0]!, EVENTS[2]!]);
    expect(calls).toEqual([{ hash: HASH, descriptor: { kind: "module", eventType: PING } }]);
  });

  it("applies decode before where", async () => {
    const { adapter } = lookup(committed(EVENTS));
    const tx = client(adapter).expectEvents(HASH, { decode: ["amount"] });
    const big = await tx.toEmit({ kind: "module", eventType: PING }, whereField("amount", 30n));
    expect(big.map((e) => e.eventIndex)).toEqual([2]);
    expect((big[0]!.data as { amount: unknown }).amount).toBe(30n);
  });

  it("toEmit rejects listing every emitted event, undecoded", async () => {
    const { adapter } = lookup(committed(EVENTS));
    const tx = client(adapter).expectEvents(HASH, { decode: ["amount"] });
    const err = await failure(tx.toEmit({ kind: "module", eventType: MISSING }));
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.transactionHash).toBe(HASH);
    expect(err.events).toEqual(EVENTS);
    expect(err.message).toBe(
      `Expected transaction ${HASH} to emit ["module",["${MISSING}"]], but it emitted 3 event(s):\n` +
        `  [0] ${PING} {"amount":"10","big":"7"}\n` +
        `  [1] ${PONG} {"amount":"20","big":"7"}\n` +
        `  [2] ${PING} {"amount":"30","big":"7"}`,
    );
  });

  it("names a where predicate and an empty transaction", async () => {
    const { adapter } = lookup(committed([]));
    const err = await failure(
      client(adapter).expectEvents(HASH).toEmit({ kind: "module", eventType: PING }, () => true),
    );
    expect(err.message).toBe(
      `Expected transaction ${HASH} to emit ["module",["${PING}"]] matching where, but it emitted no events`,
    );
  });

  it("notToEmit resolves when nothing matches and rejects naming the matches", async () => {
    const { adapter } = lookup(committed(EVENTS));
    const tx = client(adapter).expectEvents(HASH);
    await tx.notToEmit({ kind: "module", eventType: MISSING });
    await tx.notToEmit({ kind: "module", eventType: PING }, whereField("amount", "99"));
    const err = await failure(tx.notToEmit({ kind: "module", eventType: PING }));
    expect(err.message).toStartWith(
      `Expected transaction ${HASH} not to emit ["module",["${PING}"]], but 2 matched (event index 0, 2); ` +
        "it emitted 3 event(s):\n",
    );
    expect(err.events).toEqual(EVENTS);
  });

  it("awaits an async where, rejecting with its reason", async () => {
    const { adapter } = lookup(committed(EVENTS));
    const tx = client(adapter).expectEvents(HASH);
    const last = await tx.toEmit({ kind: "module", eventType: PING }, async (e) => e.eventIndex === 2);
    expect(last).toEqual([EVENTS[2]!]);
    await tx.notToEmit({ kind: "module", eventType: PING }, async () => false);
    const boom = new Error("lookup failed");
    await expect(
      tx.toEmit({ kind: "module", eventType: PING }, () => Promise.reject(boom)),
    ).rejects.toBe(boom);
  });

  it("looks nothing up until an assertion runs", () => {
    const { adapter, calls } = lookup(committed(EVENTS));
    client(adapter).expectEvents(HASH);
    expect(calls).toHaveLength(0);
  });

  it("refuses a bad hash or timeout at once, and a bad descriptor before any lookup", async () => {
    const { adapter, calls } = lookup(committed(EVENTS));
    const c = client(adapter);
    expect(() => c.expectEvents("0x12")).toThrow(StreamdError);
    expect(() => c.expectEvents(HASH, { timeout: -1 })).toThrow(StreamdError);
    const handle = {
      kind: "handle",
      accountAddress: "0x1",
      eventHandleStruct: "0x1::a::B",
      fieldName: "f",
    } as const;
    await expect(c.expectEvents(HASH).toEmit(handle)).rejects.toThrow("creationNumber");
    expect(calls).toHaveLength(0);
  });

  it("rejects a failed transaction and a timeout as eventsOf does", async () => {
    const failed = lookup({ version: 9n, success: false, vmStatus: "Move abort", events: [] });
    await expect(
      client(failed.adapter).expectEvents(HASH).toEmit({ kind: "module", eventType: PING }),
    ).rejects.toBeInstanceOf(StreamdTransactionError);
    const pending = lookup(undefined);
    const slow = client(pending.adapter).expectEvents(HASH, { timeout: 20 });
    await expect(slow.notToEmit({ kind: "module", eventType: PING })).rejects.toBeInstanceOf(StreamdTimeoutError);
  });

  it("routes each lookup by its assertion's descriptor, over MemoryAdapter", async () => {
    const memory = new MemoryAdapter(EVENTS);
    const routing = new RoutingAdapter({ module: memory });
    const tx = client(routing).expectEvents(HASH);
    expect(await tx.toEmit({ kind: "module", eventType: PONG })).toEqual([EVENTS[1]!]);
  });
});

describe("expectPreview", () => {
  const TRANSACTION = { rawTransaction: {} } as unknown as SimpleTransaction;
  const KEY = {} as PublicKey;
  const ran: TransactionEvents = { version: 12n, success: true, vmStatus: "Executed successfully", events: EVENTS };
  function simulating(result: TransactionEvents = ran) {
    const calls: { key: unknown; descriptor: unknown }[] = [];
    const adapter: EventAdapter = {
      fetchEvents: async () => ({ events: [], hasMore: false }),
      async simulateTransactionEvents(simulation, descriptor) {
        calls.push({ key: simulation.signerPublicKey, descriptor });
        return result;
      },
    };
    return { client: new StreamdClient({ adapter }), calls };
  }
  const pings = { kind: "module", eventType: PING } as const;
  const missing = { kind: "module", eventType: MISSING } as const;

  it("resolves toEmit with the matches, and rejects listing every simulated event when none match", async () => {
    const { client, calls } = simulating();
    const expectation = client.expectPreview(TRANSACTION, { signerPublicKey: KEY });
    expect(calls).toEqual([]);
    expect((await expectation.toEmit(pings)).map((e) => e.eventIndex)).toEqual([0, 2]);
    const err = (await expectation.toEmit(missing).catch((e: unknown) => e)) as StreamdAssertionError;
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect(err.transactionHash).toBeUndefined();
    expect(err.events).toEqual(EVENTS);
    expect(err.message).toStartWith(`Expected the simulated transaction to emit ["module",["${MISSING}"]], but`);
    expect(err.message).toContain("it emitted 3 event(s)");
    expect(calls).toEqual([
      { key: KEY, descriptor: pings },
      { key: KEY, descriptor: missing },
    ]);
  });

  it("resolves notToEmit when nothing matches, and rejects naming the matches", async () => {
    const { client } = simulating();
    await client.expectPreview(TRANSACTION).notToEmit(missing);
    const err = (await client.expectPreview(TRANSACTION).notToEmit(pings).catch((e: unknown) => e)) as Error;
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect(err.message).toContain("not to emit");
    expect(err.message).toContain("2 matched (event index 0, 2)");
  });

  it("applies decode before where", async () => {
    const { client } = simulating();
    const big = await client
      .expectPreview(TRANSACTION, { decode: ["amount"] })
      .toEmit(pings, (e) => (e.data as { amount: bigint }).amount > 15n);
    expect(big.map((e) => e.eventIndex)).toEqual([2]);
  });

  it("rejects with StreamdTransactionError when the simulation fails, and refuses an adapter that cannot simulate", async () => {
    const { client } = simulating({ ...ran, success: false, vmStatus: "Move abort 0x1::coin: 0x10006" });
    const err = (await client.expectPreview(TRANSACTION).toEmit(pings).catch((e: unknown) => e)) as StreamdTransactionError;
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err.vmStatus).toBe("Move abort 0x1::coin: 0x10006");
    const plain = new StreamdClient({ adapter: new MemoryAdapter([]) });
    await expect(plain.expectPreview(TRANSACTION).toEmit(pings)).rejects.toThrow("simulateTransactionEvents");
  });
});
