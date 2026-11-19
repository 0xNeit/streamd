import { describe, expect, it } from "bun:test";
import { parseModuleAbi, parseMoveType } from "../src/abi.js";

describe("parseMoveType", () => {
  it("parses primitives, signed integers and type parameters", () => {
    expect(parseMoveType("u64")).toEqual({ kind: "primitive", name: "u64" });
    expect(parseMoveType("i128")).toEqual({ kind: "primitive", name: "i128" });
    expect(parseMoveType("address")).toEqual({ kind: "primitive", name: "address" });
    expect(parseMoveType("T12")).toEqual({ kind: "param", index: 12 });
  });

  it("parses vectors and nested generic struct tags", () => {
    expect(parseMoveType("vector<vector<u8>>")).toEqual({
      kind: "vector",
      of: { kind: "vector", of: { kind: "primitive", name: "u8" } },
    });
    expect(parseMoveType("0x1::option::Option<0xab::m::Pair<u8, T0>>")).toEqual({
      kind: "struct",
      address: "0x1",
      module: "option",
      name: "Option",
      args: [
        {
          kind: "struct",
          address: "0xab",
          module: "m",
          name: "Pair",
          args: [
            { kind: "primitive", name: "u8" },
            { kind: "param", index: 0 },
          ],
        },
      ],
    });
  });

  it("rejects what it cannot parse", () => {
    for (const bad of [
      "",
      "u7",
      "vector",
      "vector<u8, u8>",
      "u8<u8>",
      "&u64",
      "|u64|u64",
      "0x1::m",
      "0xzz::m::S",
      "0x1::m::S<",
      "0x1::m::S<u8>>",
      "0x1::m::S; evil",
      `${"vector<".repeat(40)}u8${">".repeat(40)}`,
    ]) {
      expect(() => parseMoveType(bad)).toThrow("Unsupported Move type");
    }
  });
});

const module = (structs: unknown[]) => ({
  bytecode: "0x",
  abi: { address: "0x1", name: "m", friends: [], exposed_functions: [], structs },
});

const struct = (extra: Record<string, unknown> = {}) => ({
  name: "S",
  is_native: false,
  is_event: true,
  is_enum: false,
  abilities: ["drop", "store"],
  generic_type_params: [],
  fields: [{ name: "amount", type: "u64" }],
  variants: [],
  ...extra,
});

describe("parseModuleAbi", () => {
  it("validates and normalizes a module response", () => {
    const abi = parseModuleAbi(
      module([
        struct({ generic_type_params: [{ constraints: [] }] }),
        struct({
          name: "E",
          is_event: false,
          is_enum: true,
          abilities: ["key"],
          fields: [],
          variants: [{ name: "V1", fields: [{ name: "_0", type: "bool" }] }],
        }),
      ]),
    );
    expect(abi).toEqual({
      address: "0x1",
      name: "m",
      structs: [
        {
          name: "S",
          isEvent: true,
          isEnum: false,
          isResource: false,
          typeParams: 1,
          fields: [{ name: "amount", type: { kind: "primitive", name: "u64" } }],
          variants: [],
        },
        {
          name: "E",
          isEvent: false,
          isEnum: true,
          isResource: true,
          typeParams: 0,
          fields: [],
          variants: [{ name: "V1", fields: [{ name: "_0", type: { kind: "primitive", name: "bool" } }] }],
        },
      ],
      views: [],
    });
  });

  it("treats a missing is_event, is_enum or variants as a plain struct", () => {
    const { is_event: _e, is_enum: _n, variants: _v, ...old } = struct();
    const [parsed] = parseModuleAbi(module([old])).structs;
    expect(parsed).toMatchObject({ isEvent: false, isEnum: false, variants: [] });
  });

  it("keeps an unparseable field type as unsupported rather than failing the module", () => {
    const [parsed] = parseModuleAbi(
      module([struct({ fields: [{ name: "f", type: "|u64|u64 has copy" }] })]),
    ).structs;
    expect(parsed!.fields[0]!.type).toEqual({ kind: "unsupported", text: "|u64|u64 has copy" });
  });

  it("rejects malformed responses and names that could inject source", () => {
    const cases: unknown[] = [
      null,
      {},
      { abi: { address: "1", name: "m", structs: [] } },
      { abi: { address: "0x1", name: "m-x", structs: [] } },
      { abi: { address: "0x1", name: "m" } },
      module([struct({ name: "S {} evil" })]),
      module([struct({ fields: [{ name: "a: 1; b", type: "u8" }] })]),
      module([struct({ fields: [{ name: "a" }] })]),
      module([struct({ fields: undefined })]),
      module([struct({ is_event: "yes" })]),
      module([struct({ generic_type_params: undefined })]),
      module([struct({ variants: [{ name: "V\"", fields: [] }] })]),
    ];
    for (const json of cases) {
      expect(() => parseModuleAbi(json)).toThrow("malformed module ABI");
    }
  });
});

describe("parseModuleAbi views", () => {
  const module = (functions: unknown) => ({ abi: { address: "0x1", name: "m", structs: [], exposed_functions: functions } });
  const fn = { name: "f", is_view: true, generic_type_params: [{}], params: ["address", "&signer"] };

  it("keeps views, with their type parameters and parsed arguments", () => {
    expect(parseModuleAbi(module([fn, { ...fn, name: "g", is_view: false }])).views).toEqual([
      {
        name: "f",
        typeParams: 1,
        params: [{ kind: "primitive", name: "address" }, { kind: "unsupported", text: "&signer" }],
      },
    ]);
  });

  it("treats a missing list as none", () => {
    expect(parseModuleAbi({ abi: { address: "0x1", name: "m", structs: [] } }).views).toEqual([]);
  });

  it("refuses malformed entries", () => {
    expect(() => parseModuleAbi(module("x"))).toThrow("bad function list");
    expect(() => parseModuleAbi(module([{ ...fn, name: "bad name" }]))).toThrow("function");
    expect(() => parseModuleAbi(module([{ ...fn, params: "x" }]))).toThrow("parameter list");
    expect(() => parseModuleAbi(module([{ ...fn, params: [1] }]))).toThrow("bad parameter");
  });
});
