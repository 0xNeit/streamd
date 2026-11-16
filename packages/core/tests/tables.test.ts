import { describe, expect, it } from "bun:test";
import { StreamdError, eventTableRows, requireEventTables } from "../src/index.js";
import type { EventColumn, EventColumnType, EventTable, StreamdEvent } from "../src/index.js";

const col = (name: string, type: EventColumnType, path: (string | number)[] = [name], nullable = false): EventColumn => ({
  name,
  type,
  path,
  nullable,
});

const DEPOSIT: EventTable = {
  eventType: "0x1::fungible_asset::Deposit",
  table: "fungible_asset_deposit",
  generic: false,
  columns: [col("store", "address"), col("amount", "u64")],
};

const COIN: EventTable = {
  eventType: "0x1::coin::CoinDeposit",
  table: "coin_coin_deposit",
  generic: true,
  columns: [col("amount", "u64")],
};

const ev = (type: string, data: unknown, over: Partial<StreamdEvent> = {}): StreamdEvent => ({
  type,
  data,
  version: 10n,
  sequenceNumber: 0n,
  eventIndex: 2,
  ...over,
});

const one = (table: EventTable, data: unknown) => eventTableRows([table], [ev(table.eventType, data)])[0]!.values.slice(5);

describe("eventTableRows", () => {
  it("fills the standard columns, normalising the sender and lowercasing a hex hash", () => {
    const [row] = eventTableRows([DEPOSIT], [
      ev(DEPOSIT.eventType, { store: "0xA", amount: "5" }, { transactionHash: "0xABC", timestamp: 7n, sender: "0x1" }),
    ]);
    expect(row!.table.table).toBe("fungible_asset_deposit");
    expect(row!.values).toEqual([10n, 2, "0xabc", 7n, `0x${"0".repeat(63)}1`, `0x${"0".repeat(63)}a`, "5"]);
  });

  it("leaves absent standard values null and keeps a non-hex hash as it is", () => {
    const [row] = eventTableRows([DEPOSIT], [ev(DEPOSIT.eventType, { store: "0x1", amount: "1" }, { transactionHash: "AbC9" })]);
    expect(row!.values.slice(2, 5)).toEqual(["AbC9", null, null]);
  });

  it("finds the table of a generic event by its base type and stores the type arguments", () => {
    const type = "0x1::coin::CoinDeposit<0x1::coin::Wrapped<0x1::aptos_coin::AptosCoin>>";
    const [row] = eventTableRows([DEPOSIT, COIN], [ev(type, { amount: "3" })]);
    expect(row!.table.table).toBe("coin_coin_deposit");
    expect(row!.values.slice(5)).toEqual(["0x1::coin::Wrapped<0x1::aptos_coin::AptosCoin>", "3"]);
  });

  it("matches event types whatever the address's form", () => {
    const long = `0x${"0".repeat(63)}1::fungible_asset::Deposit`;
    expect(eventTableRows([DEPOSIT], [ev(long, { store: "0x1", amount: "1" })])[0]!.table.table).toBe("fungible_asset_deposit");
  });

  it("refuses an event with no table", () => {
    expect(() => eventTableRows([DEPOSIT], [ev("0x1::fungible_asset::Withdraw", {})])).toThrow(/No event table/);
  });

  it("stores integers up to 32 bits as numbers and wider ones as exact decimal strings", () => {
    const table: EventTable = {
      eventType: "0x2::m::E",
      table: "m_e",
      generic: false,
      columns: [col("a", "u8"), col("b", "u32"), col("c", "i32"), col("d", "u256"), col("e", "i64"), col("f", "i128"), col("g", "u64")],
    };
    const data = { a: 255, b: 4294967295, c: -2147483648, d: (2n ** 256n - 1n).toString(), e: "-9223372036854775808", f: 5n, g: "007" };
    expect(one(table, data)).toEqual([255, 4294967295, -2147483648, (2n ** 256n - 1n).toString(), "-9223372036854775808", "5", "7"]);
  });

  it("refuses integers out of range or of the wrong form", () => {
    const cases: [EventColumnType, unknown][] = [
      ["u8", 256], ["u8", -1], ["u8", 1.5], ["u16", "x"], ["i8", 128], ["i8", -129], ["u32", 2 ** 32],
      ["u64", (2n ** 64n).toString()], ["u64", "-1"], ["u64", "1e3"], ["u64", true], ["i64", (2n ** 63n).toString()],
      ["u256", (2n ** 256n).toString()], ["i256", (-(2n ** 255n) - 1n).toString()], ["u64", 2 ** 60],
    ];
    for (const [type, v] of cases) {
      const table: EventTable = { eventType: "0x2::m::E", table: "m_e", generic: false, columns: [col("v", type)] };
      expect(() => one(table, { v })).toThrow(`Column m_e.v of event 10:2 expects a ${type}`);
    }
  });

  it("stores booleans, addresses, text and JSON, and refuses the wrong kinds", () => {
    const table: EventTable = {
      eventType: "0x2::m::E",
      table: "m_e",
      generic: false,
      columns: [col("flag", "bool"), col("who", "address"), col("name", "text"), col("legs", "json"), col("whole", "json", [])],
    };
    const data = { flag: false, who: "0xBEEF", name: "a\u0000b", legs: [{ x: 1n }] };
    const values = eventTableRows([table], [ev("0x2::m::E", data)], (s) => s.replaceAll("\0", "?"))[0]!.values.slice(5);
    expect(values).toEqual([false, `0x${"0".repeat(60)}beef`, "a?b", '[{"x":"1"}]', '{"flag":false,"who":"0xBEEF","name":"a?b","legs":[{"x":"1"}]}']);
    for (const [name, bad] of [["flag", "true"], ["who", "beef"], ["who", 1], ["name", 2]] as const) {
      expect(() => one(table, { ...data, [name]: bad })).toThrow(StreamdError);
    }
  });

  it("reads nested paths, stores missing nullable values as null and refuses missing required ones", () => {
    const table: EventTable = {
      eventType: "0x2::m::E",
      table: "m_e",
      generic: false,
      columns: [col("maybe", "u64", ["maybe", "vec", 0], true), col("obj", "address", ["obj", "inner"])],
    };
    expect(one(table, { maybe: { vec: ["9"] }, obj: { inner: "0x3" } })).toEqual(["9", `0x${"0".repeat(63)}3`]);
    expect(one(table, { maybe: { vec: [] }, obj: { inner: "0x3" } })).toEqual([null, `0x${"0".repeat(63)}3`]);
    expect(() => one(table, { maybe: { vec: [] }, obj: {} })).toThrow("Column m_e.obj of event 10:2 has no value");
    expect(() => one(table, { maybe: null, obj: null })).toThrow("has no value");
  });

  it("refuses versions, timestamps and event indexes a column cannot hold", () => {
    const data = { store: "0x1", amount: "1" };
    expect(() => eventTableRows([DEPOSIT], [ev(DEPOSIT.eventType, data, { version: 2n ** 63n })])).toThrow(/version/);
    expect(() => eventTableRows([DEPOSIT], [ev(DEPOSIT.eventType, data, { timestamp: -1n })])).toThrow(/timestamp/);
    expect(() => eventTableRows([DEPOSIT], [ev(DEPOSIT.eventType, data, { eventIndex: 2 ** 31 })])).toThrow(/eventIndex/);
  });
});

describe("requireEventTables", () => {
  const base = (over: Partial<EventTable>): EventTable[] => [{ ...DEPOSIT, ...over }];

  it("returns one frozen copy per list", () => {
    const tables = [DEPOSIT, COIN];
    const copy = requireEventTables(tables);
    expect(copy).toEqual(tables);
    expect(requireEventTables(tables)).toBe(copy);
    expect(Object.isFrozen(copy[0]!.columns[0]!.path)).toBe(true);
  });

  it("refuses every malformed spec", () => {
    const bad: unknown[] = [
      [],
      base({ table: "Upper" }),
      base({ table: "a-b" }),
      base({ table: `t${"x".repeat(63)}` }),
      base({ table: "streamd_events" }),
      base({ table: "sqlite_master" }),
      base({ table: 'x"; DROP TABLE y; --' }),
      base({ eventType: "0x1::fungible_asset::Deposit<u8>" }),
      base({ eventType: "fungible_asset::Deposit" as never }),
      base({ generic: "no" as never }),
      base({ columns: [col("version", "u64")] }),
      base({ columns: [col("type_arguments", "text")] }),
      base({ columns: [col("a", "u64"), col("a", "u8")] }),
      base({ columns: [col("Amount", "u64")] }),
      base({ columns: [col(`c${"x".repeat(63)}`, "u64")] }),
      base({ columns: [col("a", "float" as never)] }),
      base({ columns: [col("a", "u64", [""])] }),
      base({ columns: [col("a", "u64", [-1])] }),
      base({ columns: [{ name: "a", type: "u64", path: ["a"] } as never] }),
      [DEPOSIT, { ...DEPOSIT, table: "other" }],
      [DEPOSIT, { ...COIN, table: DEPOSIT.table }],
      [DEPOSIT, { ...DEPOSIT, eventType: `0x${"0".repeat(63)}1::fungible_asset::Deposit`, table: "other" }],
    ];
    for (const tables of bad) {
      expect(() => requireEventTables(tables as EventTable[])).toThrow(/Invalid event tables/);
    }
  });

  it("accepts names of exactly 63 bytes", () => {
    const name = `t${"x".repeat(62)}`;
    expect(() => requireEventTables(base({ table: name, columns: [col(name, "u64")] }))).not.toThrow();
  });
});
