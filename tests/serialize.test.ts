import { describe, expect, it } from "bun:test";
import { StreamdError } from "../src/errors.js";
import { parseEvent, serializeEvent } from "../src/serialize.js";
import type { StreamdEvent } from "../src/types.js";

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

function line(fields: Record<string, unknown>): string {
  return JSON.stringify({ type: "0x1::a::B", version: "1", sequenceNumber: "0", eventIndex: 0, ...fields });
}

describe("serializeEvent / parseEvent", () => {
  it("round-trips every top-level field, decoding named data fields", () => {
    const u128 = 2n ** 128n - 1n;
    const event = ev({
      data: { amount: u128, store: "0xa" },
      transactionHash: "0xabc",
      timestamp: 1_700_000_000_000_000n,
      sender: "0x1",
    });
    const parsed = parseEvent(serializeEvent(event), { decode: ["amount"] });
    expect(parsed).toEqual(event);
    expect(typeof parsed.version).toBe("bigint");
  });

  it("round-trips an event without optional fields or data", () => {
    const event = ev({ data: undefined });
    const parsed = parseEvent(serializeEvent(event));
    expect(parsed).toEqual(event);
    expect(Object.keys(parsed)).not.toContain("timestamp");
    expect(Object.keys(parsed)).not.toContain("transactionHash");
    expect(Object.keys(parsed)).not.toContain("sender");
  });

  it("leaves data bigints as strings unless named, and nested ones always", () => {
    const text = serializeEvent(ev({ data: { amount: 5n, nested: { deep: 3n } } }));
    expect(parseEvent(text).data).toEqual({ amount: "5", nested: { deep: "3" } });
    expect(parseEvent(text, { decode: ["amount"] }).data).toEqual({
      amount: 5n,
      nested: { deep: "3" },
    });
  });

  it("ignores extra keys", () => {
    const parsed = parseEvent(line({ extra: true }));
    expect(Object.keys(parsed)).not.toContain("extra");
  });

  it("rejects text that is not JSON, with the parse error as cause", () => {
    try {
      parseEvent("{nope");
      throw new Error("expected a throw");
    } catch (err) {
      expect(err).toBeInstanceOf(StreamdError);
      expect((err as StreamdError).cause).toBeInstanceOf(SyntaxError);
    }
  });

  it.each([
    ["an array", "[]", "object"],
    ["null", "null", "object"],
    ["a missing type", line({ type: undefined }), '"type"'],
    ["an empty type", line({ type: "" }), '"type"'],
    ["a numeric version", line({ version: 1 }), '"version"'],
    ["a negative version", line({ version: "-1" }), '"version"'],
    ["a missing sequenceNumber", line({ sequenceNumber: undefined }), '"sequenceNumber"'],
    ["a fractional eventIndex", line({ eventIndex: 1.5 }), '"eventIndex"'],
    ["a negative eventIndex", line({ eventIndex: -1 }), '"eventIndex"'],
    ["a string eventIndex", line({ eventIndex: "0" }), '"eventIndex"'],
    ["a numeric transactionHash", line({ transactionHash: 1 }), '"transactionHash"'],
    ["a hex timestamp", line({ timestamp: "0x10" }), '"timestamp"'],
    ["a numeric sender", line({ sender: 1 }), '"sender"'],
  ])("rejects %s", (_name, text, field) => {
    expect(() => parseEvent(text)).toThrow(StreamdError);
    expect(() => parseEvent(text)).toThrow(field);
  });

  it("rejects a decode field that is not a decimal string", () => {
    const text = line({ data: { amount: "abc" } });
    expect(() => parseEvent(text, { decode: ["amount"] })).toThrow(StreamdError);
  });

  it("rejects a decode option that is not a list of names", () => {
    expect(() => parseEvent(line({}), { decode: [""] })).toThrow(StreamdError);
    expect(() =>
      parseEvent(line({}), { decode: "amount" as unknown as string[] }),
    ).toThrow(StreamdError);
  });
});

describe("public surface", () => {
  it("exports serializeEvent and parseEvent from the package entry", async () => {
    const entry = await import("../src/index.js");
    expect(entry.serializeEvent).toBe(serializeEvent);
    expect(entry.parseEvent).toBe(parseEvent);
  });
});
