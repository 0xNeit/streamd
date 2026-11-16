import { StreamdError } from "@streamd/core";
import type { FieldAbi, MoveType, StructAbi } from "./abi.js";
import type { Dialect } from "./codegen.js";

type StructType = Extract<MoveType, { kind: "struct" }>;

// Every helper draws its edge cases a quarter of the time, so 0, the maximum and
// empty values turn up in a few hundred draws rather than never.
const HELPERS = {
  bits: [
    "function streamdBits(rng: StreamdRng, bits: number): bigint {",
    "  let v = 0n;",
    "  for (let i = 0; i < bits; i += 32) v = (v << 32n) | BigInt(rng() >>> 0);",
    "  return v & ((1n << BigInt(bits)) - 1n);",
    "}",
  ].join("\n"),
  int: [
    "function streamdInt(rng: StreamdRng, bits: number, signed: boolean): bigint {",
    "  const top = 1n << BigInt(signed ? bits - 1 : bits);",
    "  if (rng() % 4 === 0) return [signed ? -top : 0n, top - 1n, 0n, 1n][rng() % 4]!;",
    "  const v = (streamdBits(rng, bits) % top) >> BigInt(rng() % bits);",
    "  return signed && rng() % 2 === 1 ? -v - 1n : v;",
    "}",
  ].join("\n"),
  text: [
    "function streamdText(rng: StreamdRng, alphabet: string): string {",
    "  const chars = Array.from(alphabet);",
    "  let out = \"\";",
    "  for (let n = rng() % 4 === 0 ? 0 : 1 + (rng() % 8); n > 0; n--) out += chars[rng() % chars.length]!;",
    "  return out;",
    "}",
  ].join("\n"),
  string: "const streamdString = (rng: StreamdRng) => streamdText(rng, \"abzAZ09 _-.éß✓中😀\");",
  ascii: "const streamdAscii = (rng: StreamdRng) => streamdText(rng, \"abzAZ09 _-.:/\");",
  typeName: "const streamdTypeName = (rng: StreamdRng) =>\n  `${streamdBits(rng, 256).toString(16).padStart(64, \"0\")}::m${rng() % 10}::T${rng() % 10}`;",
  address: "",
  bytes: "",
  vector: [
    "const streamdVector = <T>(rng: StreamdRng, item: (rng: StreamdRng) => T): T[] =>",
    "  Array.from({ length: rng() % 4 === 0 ? 0 : 1 + (rng() % 4) }, () => item(rng));",
  ].join("\n"),
  option: [
    "const streamdOption = <T>(rng: StreamdRng, value: (rng: StreamdRng) => T): { vec: [] | [T] } =>",
    "  rng() % 2 === 0 ? { vec: [] } : { vec: [value(rng)] };",
  ].join("\n"),
} as const;
export type ArbitraryHelper = keyof typeof HELPERS;

const REQUIRES: Partial<Record<ArbitraryHelper, ArbitraryHelper[]>> = {
  int: ["bits"],
  address: ["bits"],
  typeName: ["bits"],
  string: ["text"],
  ascii: ["text"],
};

/** A byte vector's length: empty a quarter of the time, else 1 to 32. */
export const BYTE_LENGTH = "rng() % 4 === 0 ? 0 : 1 + (rng() % 32)";

const RNG = [
  "/** A source of uint32 values; `streamdRng(seed)` is one, and any function returning uint32s works. */",
  "export type StreamdRng = () => number;",
  "",
  "/** A seeded `StreamdRng` (mulberry32): the same seed gives the same values in every runtime. */",
  "export function streamdRng(seed: number): StreamdRng {",
  "  let s = seed >>> 0;",
  "  return () => {",
  "    s = (s + 0x6d2b79f5) >>> 0;",
  "    let t = Math.imul(s ^ (s >>> 15), s | 1);",
  "    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);",
  "    return (t ^ (t >>> 14)) >>> 0;",
  "  };",
  "}",
].join("\n");

const EVENT = [
  "/** A `StreamdEvent` of `descriptor`'s one event type carrying `data`, for `MemoryAdapter`. */",
  "export function streamdEvent<T>(descriptor: ModuleEventDescriptor<T>, data: T, version: bigint, eventIndex = 0): StreamdEvent<T> {",
  "  if (typeof descriptor.eventType !== \"string\") throw new TypeError(\"streamdEvent takes a descriptor of one event type\");",
  "  return { type: descriptor.eventType, data, version, sequenceNumber: 0n, eventIndex };",
  "}",
].join("\n");

/** What a generator of a framework struct's value can call on. */
export interface ArbitraryContext {
  args: MoveType[];
  /** A call of a shared helper with `args`, `rng` by default. */
  use: (helper: ArbitraryHelper, args?: string) => string;
  /** An expression for a random value of `t`, a type argument, with `rng` in scope. */
  inner: (t: MoveType) => string;
}

/** The parts of a generated struct a generator is written from. */
export interface ArbitraryTarget {
  qualified: string;
  type: string;
  generator: string;
  struct: StructAbi;
}

// An opaque builtin such as Object<T> generates nothing from T, so its generator would go unused.
const mentions = (t: MoveType, index: number, d: Dialect): boolean =>
  (t.kind === "param" && t.index === index) ||
  (t.kind === "vector" && mentions(t.of, index, d)) ||
  (t.kind === "struct" && !d.builtin(t)?.opaque && t.args.some((a) => mentions(a, index, d)));

// A literal `__proto__:` key, quoted or not, sets the prototype instead of making a field.
const propertyName = (key: string) =>
  key === "__proto__" ? '["__proto__"]' : /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);

/** Renders generators for one file, recording which helpers they call. */
export function arbitraryWriter(d: Dialect, generatorOf: (t: StructType) => string) {
  const used = new Set<ArbitraryHelper>();
  const mark = (h: ArbitraryHelper): void => {
    used.add(h);
    for (const r of REQUIRES[h] ?? []) mark(r);
  };
  const use = (h: ArbitraryHelper, args = "rng") => (mark(h), `streamd${h.charAt(0).toUpperCase()}${h.slice(1)}(${args})`);
  const lambda = (t: MoveType) => `(rng) => ${value(t)}`;

  const value = (t: MoveType): string => {
    switch (t.kind) {
      case "primitive": {
        if (t.name === "bool") return "rng() % 2 === 1";
        if (t.name === "address" || t.name === "signer") return use("address");
        const bits = Number(t.name.slice(1));
        const int = use("int", `rng, ${bits}, ${t.name.startsWith("i")}`);
        return bits <= 32 ? `Number(${int})` : `String(${int})`;
      }
      case "param":
        return `t${t.index}(rng)`;
      case "vector":
        if (t.of.kind === "primitive" && t.of.name === "u8") return use("bytes");
        return use("vector", `rng, ${lambda(t.of)}`);
      case "struct": {
        const known = d.builtin(t);
        if (known !== undefined) return known.arbitrary({ args: t.args, use, inner: value });
        return `${generatorOf(t)}(${["rng", ...t.args.map(lambda)].join(", ")})`;
      }
      case "unsupported":
        throw new StreamdError(`Unsupported Move type: ${t.text}`);
    }
  };

  const object = (fields: FieldAbi[], indent: string, variant?: string) => {
    const props = fields.map((f) => `${propertyName(f.name)}: ${value(f.type)}`);
    if (variant !== undefined) props.unshift(`${propertyName(d.variantKey)}: ${JSON.stringify(variant)}`);
    return props.length === 0 ? "{}" : `{\n${props.map((p) => `${indent}  ${p},\n`).join("")}${indent}}`;
  };

  const render = ({ qualified, type, generator, struct }: ArbitraryTarget): string => {
    const all = struct.isEnum ? struct.variants.flatMap((v) => v.fields) : struct.fields;
    const usesRng = struct.isEnum ? struct.variants.length > 0 : all.length > 0;
    const typeParams = Array.from({ length: struct.typeParams }, (_, i) => `T${i}`);
    const params = [
      `${usesRng ? "" : "_"}rng: StreamdRng`,
      ...typeParams.map((p, i) => `${all.some((f) => mentions(f.type, i, d)) ? "" : "_"}t${i}: (rng: StreamdRng) => ${p}`),
    ];
    const generics = typeParams.length === 0 ? "" : `<${typeParams.join(", ")}>`;
    const head = [
      `/** A random \`${qualified}\` in the node's JSON form. */`,
      `export function ${generator}${generics}(${params.join(", ")}): ${type}${generics} {`,
    ];
    if (!struct.isEnum) return [...head, `  return ${object(struct.fields, "  ")};`, "}"].join("\n");
    if (struct.variants.length === 0) {
      return [...head, `  throw new TypeError(${JSON.stringify(`${qualified} has no variants`)});`, "}"].join("\n");
    }
    const cases = struct.variants.map((v, i) =>
      `    ${i === struct.variants.length - 1 ? "default" : `case ${i}`}:\n      return ${object(v.fields, "      ", v.name)};`,
    );
    return [...head, `  switch (rng() % ${struct.variants.length}) {`, ...cases, "  }", "}"].join("\n");
  };

  /** The rng, `streamdEvent` when the file has events, and the helpers the rendered generators call. */
  const preamble = (withEvents: boolean): string[] => {
    const source = (h: ArbitraryHelper) => (h === "address" ? d.arbitraryAddress : h === "bytes" ? d.arbitraryBytes : HELPERS[h]);
    const helpers = (Object.keys(HELPERS) as ArbitraryHelper[]).filter((h) => used.has(h)).map(source);
    return [RNG, ...(withEvents ? [EVENT] : []), ...(helpers.length > 0 ? [helpers.join("\n")] : [])];
  };

  return { render, preamble };
}
