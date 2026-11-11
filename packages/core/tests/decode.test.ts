import { describe, it, expect } from "bun:test";
import { fieldDecoder } from "../src/decode.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { StreamdClient } from "../src/client.js";
import { StreamdError } from "../src/errors.js";
import type { StreamdEvent } from "../src/types.js";

function ev(data: unknown): StreamdEvent {
  return { type: "0x1::test::Event", data, version: 7n, sequenceNumber: 0n, eventIndex: 2 };
}

describe("fieldDecoder", () => {
  it("returns undefined when no fields are named", () => {
    expect(fieldDecoder(undefined)).toBeUndefined();
    expect(fieldDecoder([])).toBeUndefined();
  });

  it("rejects an option that is not a list of non-empty names", () => {
    expect(() => fieldDecoder("amount" as unknown as string[])).toThrow(StreamdError);
    expect(() => fieldDecoder([""])).toThrow(StreamdError);
    expect(() => fieldDecoder([1 as unknown as string])).toThrow(StreamdError);
  });

  it("turns named decimal strings into bigint, u128-sized included", () => {
    const decode = fieldDecoder(["amount", "supply"])!;
    const u128Max = "340282366920938463463374607431768211455";
    const out = decode(ev({ amount: "100", supply: u128Max, store: "0x1" }));

    expect(out.data).toEqual({ amount: 100n, supply: BigInt(u128Max), store: "0x1" });
    expect(out.version).toBe(7n);
  });

  it("keeps bigint values and leaves absent fields absent", () => {
    const decode = fieldDecoder(["amount", "fee"])!;
    const out = decode(ev({ amount: 5n }));

    expect(out.data).toEqual({ amount: 5n });
    expect(Object.hasOwn(out.data as object, "fee")).toBe(false);
  });

  it("returns the event untouched when no named field is present or data is not an object", () => {
    const decode = fieldDecoder(["amount"])!;
    for (const data of [{ other: "1" }, null, "1", ["1"]]) {
      const input = ev(data);
      expect(decode(input)).toBe(input);
    }
  });

  it("throws a StreamdError for a named field that is not an unsigned integer string", () => {
    const decode = fieldDecoder(["amount"])!;
    for (const value of [1, "-1", "0x10", "1.5", "", "abc", { value: "1" }, null]) {
      expect(() => decode(ev({ amount: value }))).toThrow(/"amount" of 7:2/);
    }
  });

  it("does not mutate the input event", () => {
    const input = ev({ amount: "9" });
    const out = fieldDecoder(["amount"])!(input);

    expect(input.data).toEqual({ amount: "9" });
    expect(out).not.toBe(input);
  });
});

describe("fieldDecoder paths", () => {
  const data = {
    amount: "1",
    pool: { reserve: "340282366920938463463374607431768211455", name: "p" },
    legs: [{ amount: "2", to: "0xa" }, { amount: "3", to: "0xb" }],
    fees: ["4", "5"],
    limit: { vec: ["6"] },
    none: { vec: [] },
    grid: [["7"], ["8", "9"]],
  };

  it("converts nested fields, array wildcards, nested wildcards and Option values", () => {
    const decode = fieldDecoder(["amount", "pool.reserve", "legs.*.amount", "fees.*", "limit.vec.*", "none.vec.*", "grid.*.*"])!;
    expect(decode(ev(data)).data).toEqual({
      amount: 1n,
      pool: { reserve: 2n ** 128n - 1n, name: "p" },
      legs: [{ amount: 2n, to: "0xa" }, { amount: 3n, to: "0xb" }],
      fees: [4n, 5n],
      limit: { vec: [6n] },
      none: { vec: [] },
      grid: [[7n], [8n, 9n]],
    });
  });

  it("skips a key absent at any depth, and returns the event itself when nothing changes", () => {
    const event = ev({ pool: { name: "p" }, legs: [{ to: "0xa" }] });
    const decode = fieldDecoder(["pool.reserve", "missing.deep.path", "legs.*.amount"])!;
    expect(decode(event).data).toEqual({ pool: { name: "p" }, legs: [{ to: "0xa" }] });
    const untouched = ev({ pool: { name: "p" } });
    expect(fieldDecoder(["pool.reserve", "x.y"])!(untouched)).toBe(untouched);
  });

  it("rejects a present value that does not fit its path, naming the path", () => {
    const cases: [string, unknown][] = [
      ["pool.reserve", { pool: "flat" }],
      ["fees.*", { fees: { a: "1" } }],
      ["legs.*.amount", { legs: [{ amount: "-1" }] }],
      ["legs.*.amount", { legs: ["2"] }],
      ["pool.reserve", { pool: { reserve: 5 } }],
    ];
    for (const [path, value] of cases) {
      expect(() => fieldDecoder([path])!(ev(value))).toThrow(
        `Cannot decode field "${path}" of 7:2 as an unsigned integer`,
      );
    }
  });

  it("refuses malformed paths when the option is read", () => {
    for (const path of ["a..b", ".a", "a.", "*", "*.a"]) {
      expect(() => fieldDecoder([path])).toThrow(StreamdError);
    }
  });

  it("copies only along converted paths and never mutates the input", () => {
    const input = structuredClone(data);
    const event = ev(input);
    const out = fieldDecoder(["legs.*.amount"])!(event).data as typeof data;
    expect(input).toEqual(data);
    expect(out.pool).toBe(input.pool);
    expect(out.fees).toBe(input.fees);
    expect(out.legs).not.toBe(input.legs);
  });
});

describe("nested decode through the client", () => {
  const events = [1, 2, 3].map((n) => ({
    ...ev({ pool: { reserve: String(10n ** 20n * BigInt(n)) } }),
    version: BigInt(n),
  }));
  const big = (e: StreamdEvent) => (e.data as { pool: { reserve: bigint } }).pool.reserve > 10n ** 20n;
  const d = { kind: "module", eventType: "0x1::test::Event" } as const;

  it("applies a nested path before where, in queries and listeners", async () => {
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    const queried = await client.getEvents(d, { decode: ["pool.reserve"], where: big });
    expect(queried.map((e) => (e.data as { pool: { reserve: bigint } }).pool.reserve)).toEqual([
      2n * 10n ** 20n,
      3n * 10n ** 20n,
    ]);
    const first = await client.once(d, { fromVersion: 0n, decode: ["pool.reserve"], where: big });
    expect(first.version).toBe(2n);
    client.destroy();
  });
});
