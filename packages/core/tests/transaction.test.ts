import { describe, expect, it } from "bun:test";
import {
  RequestLimiter,
  StreamdAssertionError,
  StreamdTimeoutError,
  StreamdTransactionError,
  eventsOf,
  expectEvents,
} from "../src/index.js";
import type { EventAdapter, MoveEventType, StreamdEvent, TransactionEvents } from "../src/index.js";

const A = "0x1::m::A" as MoveEventType;
const B = "0x1::m::B" as MoveEventType;
const ev = (type: string, eventIndex: number, amount = "1"): StreamdEvent => ({
  type,
  data: { amount },
  version: 5n,
  sequenceNumber: 0n,
  eventIndex,
});

/** Answers `undefined` for the first `misses` lookups, then `found`; ids are not hex, as on Sui. */
function lookups(misses: number, found: TransactionEvents | undefined) {
  const seen: string[] = [];
  const adapter: EventAdapter = {
    fetchEvents: async () => ({ events: [], nextCursor: undefined, hasMore: false }),
    async getTransactionEvents(id) {
      seen.push(id);
      return seen.length > misses ? found : undefined;
    },
  };
  return { adapter, seen };
}

const ID = "4fQ2txNotHex";
const limiter = new RequestLimiter();

describe("eventsOf in core", () => {
  it("waits through missing lookups, then narrows by descriptor, decode and where", async () => {
    const { adapter, seen } = lookups(2, {
      version: 5n,
      success: true,
      vmStatus: "success",
      events: [ev(A, 0, "3"), ev(B, 1), ev(A, 2, "9")],
    });
    const events = await eventsOf(adapter, limiter, 0, ID, { kind: "module", eventType: A }, {
      decode: ["amount"],
      where: (e) => (e.data as { amount: bigint }).amount > 5n,
    });
    expect(events.map((e) => [e.eventIndex, (e.data as { amount: bigint }).amount])).toEqual([[2, 9n]]);
    expect(seen).toEqual([ID, ID, ID]);
  });

  it("rejects a failed transaction with StreamdTransactionError", async () => {
    const { adapter } = lookups(0, { version: 7n, success: false, vmStatus: "MoveAbort 101", events: [] });
    const err = await eventsOf(adapter, limiter, 0, ID, undefined, {}).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTransactionError);
    expect(err).toMatchObject({ transactionHash: ID, version: 7n, vmStatus: "MoveAbort 101" });
  });

  it("times out on a transaction never found", async () => {
    const { adapter } = lookups(0, undefined);
    const err = await eventsOf(adapter, limiter, 0, ID, undefined, { timeout: 250 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdTimeoutError);
    expect((err as StreamdTimeoutError).timeout).toBe(250);
  });

  it("expectEvents asserts over the same lookup", async () => {
    const { adapter } = lookups(0, { version: 5n, success: true, vmStatus: "success", events: [ev(A, 0)] });
    const expectation = expectEvents(adapter, limiter, 0, ID, {});
    expect((await expectation.toEmit({ kind: "module", eventType: A })).map((e) => e.eventIndex)).toEqual([0]);
    const err = await expectation.notToEmit({ kind: "module", eventType: A }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdAssertionError);
    expect((err as StreamdAssertionError).transactionHash).toBe(ID);
  });
});
