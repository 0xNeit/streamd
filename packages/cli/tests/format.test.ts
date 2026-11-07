import { describe, expect, it } from "bun:test";
import { formatEvent } from "../src/format.js";
import type { StreamdEvent } from "@streamd/core";

function ev(overrides: Partial<StreamdEvent> = {}): StreamdEvent {
  return {
    type: "0x1::coin::DepositEvent",
    data: { amount: "100" },
    version: 42n,
    sequenceNumber: 7n,
    eventIndex: 1,
    ...overrides,
  };
}

describe("formatEvent", () => {
  it("serialises bigint fields as decimal strings in a fixed order", () => {
    const line = formatEvent(
      ev({
        transactionHash: "0xabc",
        timestamp: 1_700_000_000_000_000n,
        sender: "0x1",
      }),
    );
    expect(line).toBe(
      '{"type":"0x1::coin::DepositEvent","data":{"amount":"100"},"version":"42",' +
        '"sequenceNumber":"7","eventIndex":1,"transactionHash":"0xabc",' +
        '"timestamp":"1700000000000000","sender":"0x1"}',
    );
  });

  it("prints entryFunction after every other field", () => {
    const line = formatEvent(
      ev({ sender: "0x1", entryFunction: { function: "0x1::coin::transfer", typeArguments: [], arguments: ["0x2", "5"] } }),
    );
    expect(line).toBe(
      '{"type":"0x1::coin::DepositEvent","data":{"amount":"100"},"version":"42",' +
        '"sequenceNumber":"7","eventIndex":1,"sender":"0x1",' +
        '"entryFunction":{"function":"0x1::coin::transfer","typeArguments":[],"arguments":["0x2","5"]}}',
    );
  });

  it("serialises bigints nested inside data", () => {
    const line = formatEvent(
      ev({ data: { amount: 2n ** 70n, list: [1n, { deep: 3n }] } }),
    );
    expect(JSON.parse(line).data).toEqual({
      amount: "1180591620717411303424",
      list: ["1", { deep: "3" }],
    });
  });

  it("omits optional fields that are absent", () => {
    const parsed = JSON.parse(formatEvent(ev()));
    expect(Object.keys(parsed)).toEqual([
      "type",
      "data",
      "version",
      "sequenceNumber",
      "eventIndex",
    ]);
  });

  it("produces exactly one line", () => {
    const line = formatEvent(ev({ data: { text: "a\nb" } }));
    expect(line.includes("\n")).toBe(false);
  });
});
