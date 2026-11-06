import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { defineEvent } from "../src/define.js";
import type { EventDescriptor, StreamdEvent } from "../src/types.js";

type Equals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;

interface Deposit {
  amount: string;
}

const deposit = defineEvent<Deposit>("0x1::coin::DepositEvent");

function ev(version: number): StreamdEvent {
  return {
    type: "0x1::coin::DepositEvent",
    data: { amount: String(version) },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

describe("defineEvent", () => {
  it("returns a plain module descriptor", () => {
    expect(deposit).toEqual({ kind: "module", eventType: "0x1::coin::DepositEvent" });
    expect(Reflect.ownKeys(deposit)).toEqual(["kind", "eventType"]);
  });

  it("keeps a list of event types as one descriptor", () => {
    const both = defineEvent<Deposit>(["0x1::a::A", "0x1::b::B"]);
    expect(both).toEqual({ kind: "module", eventType: ["0x1::a::A", "0x1::b::B"] });
  });

  it("infers data from the descriptor in client methods", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1), ev(2)]) });

    const events = await client.getEvents(deposit, { fromVersion: 0n });
    const inferred: Equals<typeof events, StreamdEvent<Deposit>[]> = true;
    expect(inferred).toBe(true);
    expect(events.map((e) => e.data.amount)).toEqual(["1", "2"]);

    const first = await client.once(deposit, { fromVersion: 0n });
    const once: Equals<typeof first.data, Deposit> = true;
    expect(once).toBe(true);
    expect(first.data.amount).toBe("1");

    for await (const page of client.getEventPages(deposit, { fromVersion: 0n })) {
      const paged: Equals<typeof page, StreamdEvent<Deposit>[]> = true;
      expect(paged).toBe(true);
    }

    const iter = client.stream(deposit, { fromVersion: 0n });
    const streamed: Equals<typeof iter, AsyncIterableIterator<StreamdEvent<Deposit>>> = true;
    expect(streamed).toBe(true);
    await iter.return?.();

    const sub = client.on(deposit, (e) => {
      const on: Equals<typeof e.data, Deposit> = true;
      void on;
    });
    const batch = client.onBatch(deposit, (es) => {
      const onBatch: Equals<typeof es, StreamdEvent<Deposit>[]> = true;
      void onBatch;
    });
    sub.unsubscribe();
    batch.unsubscribe();
    client.destroy();
  });

  it("types a merged list by its shared payload, or an explicit union", async () => {
    interface Withdraw {
      to: string;
    }
    const withdraw = defineEvent<Withdraw>("0x1::coin::WithdrawEvent");
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });

    const same = await client.getEvents([deposit, deposit]);
    const one: Equals<typeof same, StreamdEvent<Deposit>[]> = true;
    expect(one).toBe(true);

    const mixed = await client.getEvents<Deposit | Withdraw>([deposit, withdraw]);
    const union: Equals<typeof mixed, StreamdEvent<Deposit | Withdraw>[]> = true;
    expect(union).toBe(true);
    expect(mixed).toHaveLength(1);
  });

  it("leaves plain descriptors as before", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([ev(1)]) });
    const plain: EventDescriptor = { kind: "module", eventType: "0x1::coin::DepositEvent" };

    const untyped = await client.getEvents(plain, { fromVersion: 0n });
    const unknownData: Equals<typeof untyped, StreamdEvent<unknown>[]> = true;
    const explicit = await client.getEvents<Deposit>(plain, { fromVersion: 0n });
    const explicitData: Equals<typeof explicit, StreamdEvent<Deposit>[]> = true;
    expect([unknownData, explicitData]).toEqual([true, true]);
    expect(explicit[0]?.data.amount).toBe("1");

    const widened: EventDescriptor = deposit;
    expect(widened).toBe(deposit);
  });

  it("rejects an explicit type argument that contradicts the descriptor", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter([]) });
    // @ts-expect-error a Deposit descriptor cannot deliver { other: number }
    await client.getEvents<{ other: number }>(deposit, { fromVersion: 0n });
  });
});
