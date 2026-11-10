import { describe, expect, it } from "bun:test";
import { formatEvent, prettyEvent } from "../src/format.js";
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

describe("prettyEvent", () => {
  const SENDER = `0xabcd${"0".repeat(56)}1234`;
  const full = { timestamp: 1_700_000_000_000_000n, sender: SENDER };

  it("prints UTC time, short type, short sender and payload, two spaces apart", () => {
    expect(prettyEvent(ev(full))).toBe(
      `2023-11-14 22:13:20  ${"coin::DepositEvent".padEnd(32)}  0xabcd\u20261234  {"amount":"100"}`,
    );
  });

  it("aligns events whatever they carry", () => {
    const column = (line: string) => line.indexOf("{");
    const events = [ev(full), ev(), ev({ ...full, sender: "0x1" }), ev({ ...full, type: "0x1::a::B" })];
    expect(new Set(events.map((e) => column(prettyEvent(e)))).size).toBe(1);
  });

  it("prints a dash for an absent or unrepresentable time and an absent sender", () => {
    expect(prettyEvent(ev())).toStartWith(`${"-".padEnd(19)}  `);
    expect(prettyEvent(ev())).toContain(`  ${"-".padEnd(11)}  {`);
    expect(prettyEvent(ev({ timestamp: 10n ** 30n }))).toStartWith(`${"-".padEnd(19)}  `);
  });

  it("clips a long type and payload", () => {
    const type = "0x1::coin::CoinDeposit<0x1::aptos_coin::AptosCoin, 0x1::other::Thing>";
    const line = prettyEvent(ev({ type, data: { memo: "x".repeat(200) } }));
    expect(line).toContain(`  coin::CoinDeposit<0x1::aptos_co\u2026  `);
    expect(line.length).toBe(19 + 2 + 32 + 2 + 11 + 2 + 80);
    expect(line).toEndWith("\u2026");
  });

  it("does not clip text exactly at the width", () => {
    const memo = "x".repeat(80 - '{"m":""}'.length);
    expect(prettyEvent(ev({ data: { m: memo } }))).toEndWith(`"}`);
  });

  it("prints bigints as decimals and absent or null data as null", () => {
    expect(prettyEvent(ev({ data: { amount: 5n } }))).toEndWith('{"amount":"5"}');
    expect(prettyEvent(ev({ data: null }))).toEndWith("null");
    expect(prettyEvent(ev({ data: undefined }))).toEndWith("null");
  });

  it("keeps control characters from reaching the terminal", () => {
    const line = prettyEvent(
      ev({ type: "0x1::a\u001b[2J::B", sender: "\u009b31m", data: { memo: "\u009b2J\u0007" } }),
    );
    expect(line).not.toMatch(/[\u0000-\u001f\u007f-\u009f]/);
  });
});
