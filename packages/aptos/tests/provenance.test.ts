import { describe, it, expect } from "bun:test";
import { StreamdError } from "@streamd/core";
import type { StreamdEvent, TransactionEvents } from "@streamd/core";
import { decodeProperties, toProvenance } from "../src/provenance.js";

const long = (short: string) => `0x${short.replace(/^0x/, "").padStart(64, "0")}`;
const TOKEN = long("70c");
const OTHER = long("0e1");
const ALICE = long("a11ce");
const BOB = long("b0b");
const COLLECTION = long("c011");

function ev(type: string, data: unknown, eventIndex = 0, extra: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type, data, version: 10n, sequenceNumber: 0n, eventIndex, ...extra };
}

function txn(events: StreamdEvent[], changes: TransactionEvents["changes"] = [], success = true): TransactionEvents {
  return { version: 10n, success, vmStatus: "Executed successfully", events, changes };
}

const actions = (found: TransactionEvents) => toProvenance(TOKEN, found).data.actions;
const prop = (key: string, type: number, value: string) => ({ key, value: { type, value } });
const propertyMap = (...entries: unknown[]) => ({ inner: { data: entries } });

describe("decodeProperties", () => {
  it("decodes each of the ten property types from its BCS bytes", () => {
    const longText = "a".repeat(200);
    expect(
      decodeProperties(
        propertyMap(
          prop("bool", 0, "0x01"),
          prop("u8", 1, "0x07"),
          prop("u16", 2, "0x0201"),
          prop("u32", 3, "0x01000000"),
          prop("u64", 4, "0xffffffffffffffff"),
          prop("u128", 5, `0x01${"00".repeat(15)}`),
          prop("u256", 6, `0x${"00".repeat(31)}80`),
          prop("address", 7, `0x${"00".repeat(31)}0b`),
          prop("bytes", 8, "0x02abcd"),
          prop("text", 9, "0x0668c3a96c6c6f"),
          prop("long", 9, `0xc801${"61".repeat(200)}`),
        ),
      ),
    ).toEqual([
      { key: "bool", type: "bool", value: true },
      { key: "u8", type: "u8", value: 7n },
      { key: "u16", type: "u16", value: 0x0102n },
      { key: "u32", type: "u32", value: 1n },
      { key: "u64", type: "u64", value: 2n ** 64n - 1n },
      { key: "u128", type: "u128", value: 1n },
      { key: "u256", type: "u256", value: 2n ** 255n },
      { key: "address", type: "address", value: long("0b") },
      { key: "bytes", type: "vector<u8>", value: "0xabcd" },
      { key: "text", type: "0x1::string::String", value: "héllo" },
      { key: "long", type: "0x1::string::String", value: longText },
    ]);
    expect(decodeProperties(propertyMap())).toEqual([]);
  });

  it("refuses a value off its type, an unknown tag, and a map off the struct's shape", () => {
    for (const entry of [
      prop("k", 4, "0x01020304050607"),
      prop("k", 0, "0x02"),
      prop("k", 7, "0x0b"),
      prop("k", 9, "0x05616263"),
      prop("k", 9, "0x01ff"),
      prop("k", 8, "0x"),
      prop("k", 10, "0x00"),
      prop("k", 1, "07"),
      prop("k", 1, "0x0"),
      { key: 1, value: { type: 1, value: "0x07" } },
      { key: "k" },
      null,
    ]) {
      expect(() => decodeProperties(propertyMap(entry))).toThrow(StreamdError);
    }
    for (const map of [null, {}, { inner: {} }, { inner: { data: "x" } }]) {
      expect(() => decodeProperties(map)).toThrow(StreamdError);
    }
  });
});

describe("toProvenance", () => {
  it("reads mints, from each mint event, for this object only", () => {
    const guid = { creationNumber: 3n, accountAddress: "0xc011" };
    expect(
      actions(
        txn([
          ev("0x4::collection::Mint", { collection: "0xc011", index: { value: "1" }, token: "0x70c" }, 0),
          ev("0x4::collection::MintEvent", { index: "1", token: "0x70c" }, 1, { guid }),
          ev("0x4::collection::ConcurrentMintEvent", { collection_addr: "0xc011", index: { value: "1" }, token: "0x70c" }, 2),
          ev("0x4::collection::Mint", { collection: "0xc011", index: { value: "2" }, token: "0x0e1" }, 3),
        ]),
      ),
    ).toEqual([
      { kind: "mint", collection: COLLECTION },
      { kind: "mint", collection: COLLECTION },
      { kind: "mint", collection: COLLECTION },
    ]);
  });

  it("reads transfers from object::Transfer and the legacy TransferEvent", () => {
    expect(
      actions(
        txn([
          ev("0x1::object::TransferEvent", { object: "0x70c", from: "0xa11ce", to: "0xb0b" }, 0),
          ev("0x1::object::Transfer", { object: TOKEN, from: BOB, to: ALICE }, 1),
          ev("0x1::object::Transfer", { object: OTHER, from: BOB, to: ALICE }, 2),
        ]),
      ),
    ).toEqual([
      { kind: "transfer", from: ALICE, to: BOB },
      { kind: "transfer", from: BOB, to: ALICE },
    ]);
  });

  it("reads mutations of a token and of a collection, legacy ones by their handle's account", () => {
    const mine = { creationNumber: 4n, accountAddress: "0x70c" };
    const theirs = { creationNumber: 4n, accountAddress: "0x0e1" };
    const change = { mutated_field_name: "uri", old_value: "a", new_value: "b" };
    expect(
      actions(
        txn([
          ev("0x4::token::Mutation", { token_address: "0x70c", ...change }, 0),
          ev("0x4::token::MutationEvent", change, 1, { guid: mine }),
          ev("0x4::token::MutationEvent", change, 2, { guid: theirs }),
          ev("0x4::collection::Mutation", { collection: { inner: "0x70c" }, ...change }, 3),
          ev("0x4::collection::MutationEvent", { mutated_field_name: "uri" }, 4, { guid: mine }),
        ]),
      ),
    ).toEqual([
      { kind: "mutation", field: "uri", oldValue: "a", newValue: "b" },
      { kind: "mutation", field: "uri", oldValue: "a", newValue: "b" },
      { kind: "mutation", field: "uri", oldValue: "a", newValue: "b" },
      { kind: "mutation", field: "uri" },
    ]);
  });

  it("reads burns from each burn event, and a deleted ObjectCore with no burn event as a burn", () => {
    const guid = { creationNumber: 3n, accountAddress: "0xc011" };
    const deleted = [{ address: TOKEN, type: "0x1::object::ObjectCore", data: null }];
    expect(
      actions(
        txn(
          [
            ev("0x4::collection::Burn", { collection: "0xc011", index: "1", token: "0x70c", previous_owner: "0xb0b" }, 0),
            ev("0x4::collection::BurnEvent", { index: "1", token: "0x70c" }, 1, { guid }),
            ev("0x4::collection::ConcurrentBurnEvent", { collection_addr: "0xc011", index: "1", token: "0x70c" }, 2),
          ],
          deleted,
        ),
      ),
    ).toEqual([
      { kind: "burn", collection: COLLECTION, previousOwner: BOB },
      { kind: "burn", collection: COLLECTION },
      { kind: "burn", collection: COLLECTION },
    ]);
    expect(actions(txn([], deleted))).toEqual([{ kind: "burn" }]);
    expect(actions(txn([], [{ address: OTHER, type: "0x1::object::ObjectCore", data: null }]))).toEqual([]);
  });

  it("adds the PropertyMap the transaction wrote, after the events, and keeps only changes at the object", () => {
    const core = { address: TOKEN, type: "0x1::object::ObjectCore", data: { owner: BOB } };
    const map = { address: TOKEN, type: "0x4::property_map::PropertyMap", data: propertyMap(prop("level", 4, "0x0500000000000000")) };
    const elsewhere = { address: OTHER, type: "0x4::property_map::PropertyMap", data: propertyMap() };
    const record = toProvenance(TOKEN, txn([ev("0x1::object::Transfer", { object: TOKEN, from: ALICE, to: BOB })], [elsewhere, core, map]));
    expect(record.data).toEqual({
      object: TOKEN,
      success: true,
      actions: [
        { kind: "transfer", from: ALICE, to: BOB },
        { kind: "properties", properties: [{ key: "level", type: "u64", value: 5n }] },
      ],
      changes: [core, map],
    });
    const removed = { address: TOKEN, type: "0x4::property_map::PropertyMap", data: null };
    expect(actions(txn([], [removed]))).toEqual([]);
  });

  it("keeps the first event's envelope, or a bare one when the transaction emitted none", () => {
    const fee = ev("0x1::transaction_fee::FeeStatement", {}, 0, { transactionHash: "0xab", sender: ALICE, timestamp: 5n });
    const record = toProvenance(TOKEN, txn([fee, ev("0x1::object::Transfer", { object: TOKEN, from: ALICE, to: BOB }, 1)]));
    expect(record).toMatchObject({ type: fee.type, version: 10n, eventIndex: 0, transactionHash: "0xab", sender: ALICE, timestamp: 5n });
    const failed = toProvenance(TOKEN, { version: 12n, success: false, vmStatus: "Move abort", events: [] });
    expect(failed).toEqual({
      type: "0x1::object::ObjectCore",
      version: 12n,
      sequenceNumber: 0n,
      eventIndex: 0,
      data: { object: TOKEN, success: false, actions: [], changes: [] },
    });
  });

  it("refuses a framework event off its struct's shape, and ignores every other event", () => {
    for (const bad of [
      ev("0x1::object::Transfer", { object: TOKEN, from: "nope", to: BOB }),
      ev("0x4::collection::Mint", { collection: 5, token: TOKEN }),
      ev("0x4::collection::MintEvent", { index: "1", token: TOKEN }),
      ev("0x4::token::Mutation", { token_address: TOKEN, mutated_field_name: 3 }),
      ev("0x4::collection::Mutation", { collection: "0xc011", mutated_field_name: "uri" }),
    ]) {
      expect(() => toProvenance(TOKEN, txn([bad]))).toThrow(StreamdError);
    }
    expect(actions(txn([ev("0xbad::object::Transfer", { object: TOKEN, from: "x", to: "y" })]))).toEqual([]);
  });
});
