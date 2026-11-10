import { describe, it, expect } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { defineEvent, defineEvents } from "../src/define.js";
import { StreamdError } from "../src/errors.js";
import type { EventDescriptor, EventOf, EventStream, StreamdEvent } from "../src/types.js";

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
    const streamed: Equals<typeof iter, EventStream<Deposit>> = true;
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

describe("defineEvents", () => {
  const DEPOSIT = "0x1::coin::DepositEvent";
  const WITHDRAW = "0x1::coin::WithdrawEvent";

  interface Withdrawal {
    to: string;
  }

  const both = defineEvents({
    [DEPOSIT]: defineEvent<Deposit>(DEPOSIT),
    [WITHDRAW]: defineEvent<Withdrawal>(WITHDRAW),
  } as {
    "0x1::coin::DepositEvent": ReturnType<typeof defineEvent<Deposit>>;
    "0x1::coin::WithdrawEvent": ReturnType<typeof defineEvent<Withdrawal>>;
  });

  const events: StreamdEvent[] = [
    { type: DEPOSIT, data: { amount: "5" }, version: 1n, sequenceNumber: 0n, eventIndex: 0 },
    { type: WITHDRAW, data: { to: "0xb" }, version: 2n, sequenceNumber: 0n, eventIndex: 0 },
  ];

  it("returns a plain module descriptor over the types", () => {
    expect(both).toEqual({ kind: "module", eventType: [DEPOSIT, WITHDRAW] });
  });

  it("refuses an empty set and anything that would make the types lie", () => {
    const one = defineEvent<Deposit>(DEPOSIT);
    const bad: unknown[] = [
      {},
      undefined,
      { [WITHDRAW]: one },
      { [DEPOSIT]: defineEvent<Deposit>([DEPOSIT, WITHDRAW]) },
      { [DEPOSIT]: { kind: "account", accountAddress: "0x1" } },
      { [DEPOSIT]: null },
      { "not a type": defineEvent<Deposit>("not a type" as never) },
    ];
    for (const input of bad) {
      expect(() => defineEvents(input as never)).toThrow(StreamdError);
    }
  });

  it("delivers every type and narrows on event.type in each public call", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    const amounts: string[] = [];
    const targets: string[] = [];

    type Both = EventOf<typeof both extends EventDescriptor<infer T> ? T : never>;
    const read = (event: Both) => {
      // @ts-expect-error a field of one member is not readable before narrowing
      void event.data.amount;
      if (event.type === DEPOSIT) amounts.push(event.data.amount);
      else targets.push(event.data.to);
    };

    const seen = new Promise<void>((resolve) => {
      let count = 0;
      const sub = client.on(
        both,
        (event) => {
          read(event);
          if (++count === 2) (sub.unsubscribe(), resolve());
        },
        { pollInterval: 1 },
      );
    });
    await seen;

    await new Promise<void>((resolve) => {
      const sub = client.onBatch(
        both,
        (batch) => {
          batch.forEach(read);
          sub.unsubscribe();
          resolve();
        },
        { pollInterval: 1 },
      );
    });

    read(await client.once(both, { pollInterval: 1 }));
    (await client.getEvents(both)).forEach(read);
    for await (const page of client.getEventPages(both)) page.forEach(read);
    for await (const event of client.iterateEvents(both)) read(event);

    const stream = client.stream(both, { pollInterval: 1 });
    read((await stream.next()).value!);
    await stream.return?.(undefined);

    const reduction = client.reduce(
      both,
      (total: number, event) => total + (event.type === DEPOSIT ? Number(event.data.amount) : event.data.to.length),
      0,
      { pollInterval: 1 },
    );
    await reduction.ready;
    expect(reduction.value).toBe(5 + 3);
    reduction.unsubscribe();

    client.destroy();
    expect(amounts.every((a) => a === "5")).toBe(true);
    expect(amounts.length).toBeGreaterThanOrEqual(6);
    expect(targets.every((t) => t === "0xb")).toBe(true);
  });

  it("leaves every other payload's event type exactly as it was", () => {
    const plain: Equals<EventOf<Deposit>, StreamdEvent<Deposit>> = true;
    const unknownData: Equals<EventOf<unknown>, StreamdEvent<unknown>> = true;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const anyData: Equals<EventOf<any>, StreamdEvent<any>> = true;
    const union: Equals<EventOf<Deposit | Withdrawal>, StreamdEvent<Deposit | Withdrawal>> = true;
    expect([plain, unknownData, anyData, union]).toEqual([true, true, true, true]);
  });

  it("infers the literal keys from an object literal", () => {
    const direct = defineEvents({
      "0x1::a::A": defineEvent<{ a: string }>("0x1::a::A"),
      "0x1::b::B": defineEvent<{ b: number }>("0x1::b::B"),
    });
    type Direct = EventOf<typeof direct extends EventDescriptor<infer T> ? T : never>;
    const check = (event: Direct): string | number =>
      event.type === "0x1::a::A" ? event.data.a : event.data.b;
    expect(check({ type: "0x1::b::B", data: { b: 7 }, version: 1n, sequenceNumber: 0n, eventIndex: 0 })).toBe(7);
  });
});
