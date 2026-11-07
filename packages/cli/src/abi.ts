import { StreamdError } from "@streamd/core";

const PRIMITIVES = [
  "bool", "address", "signer",
  "u8", "u16", "u32", "u64", "u128", "u256",
  "i8", "i16", "i32", "i64", "i128", "i256",
] as const;

export type Primitive = (typeof PRIMITIVES)[number];

/** A field type from a module ABI, parsed. */
export type MoveType =
  | { kind: "primitive"; name: Primitive }
  | { kind: "vector"; of: MoveType }
  | { kind: "param"; index: number }
  | { kind: "struct"; address: string; module: string; name: string; args: MoveType[] }
  /** A type this parser does not know, such as a function type; fails only if generated. */
  | { kind: "unsupported"; text: string };

export interface FieldAbi {
  name: string;
  type: MoveType;
}

export interface StructAbi {
  name: string;
  isEvent: boolean;
  isEnum: boolean;
  typeParams: number;
  fields: FieldAbi[];
  variants: { name: string; fields: FieldAbi[] }[];
}

export interface ModuleAbi {
  /** As the fullnode reports it: short for special addresses, long otherwise. */
  address: string;
  name: string;
  structs: StructAbi[];
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const ADDRESS = /^0x[0-9a-fA-F]{1,64}$/;
const MAX_DEPTH = 32;

/** Parses an ABI type string such as `0x1::option::Option<vector<u8>>`. */
export function parseMoveType(text: string): MoveType {
  let pos = 0;
  const fail = (): never => {
    throw new StreamdError(`Unsupported Move type: ${text.slice(0, 200)}`);
  };
  const skipSpace = () => {
    while (text[pos] === " ") pos++;
  };
  const word = (pattern: RegExp): string => {
    const match = pattern.exec(text.slice(pos));
    if (match === null) fail();
    pos += match![0].length;
    return match![0];
  };
  const expect = (literal: string) => {
    if (!text.startsWith(literal, pos)) fail();
    pos += literal.length;
  };

  const type = (depth: number): MoveType => {
    if (depth > MAX_DEPTH) fail();
    skipSpace();
    const head = word(/^[A-Za-z0-9_]+/);
    if (text.startsWith("::", pos)) {
      if (!ADDRESS.test(head)) fail();
      expect("::");
      const module = word(/^[A-Za-z_][A-Za-z0-9_]*/);
      expect("::");
      const name = word(/^[A-Za-z_][A-Za-z0-9_]*/);
      return { kind: "struct", address: head, module, name, args: typeArgs(depth) };
    }
    if (head === "vector") {
      const args = typeArgs(depth);
      if (args.length !== 1) fail();
      return { kind: "vector", of: args[0]! };
    }
    if ((PRIMITIVES as readonly string[]).includes(head)) {
      return { kind: "primitive", name: head as Primitive };
    }
    const param = /^T(\d{1,3})$/.exec(head);
    if (param === null) fail();
    return { kind: "param", index: Number(param![1]) };
  };

  const typeArgs = (depth: number): MoveType[] => {
    if (text[pos] !== "<") return [];
    pos++;
    const args = [type(depth + 1)];
    skipSpace();
    while (text[pos] === ",") {
      pos++;
      args.push(type(depth + 1));
      skipSpace();
    }
    expect(">");
    return args;
  };

  const result = type(0);
  if (pos !== text.length) fail();
  return result;
}

function malformed(what: string): never {
  throw new StreamdError(`Fullnode returned a malformed module ABI: ${what}`);
}

function identifier(value: unknown, what: string): string {
  if (typeof value !== "string" || !IDENTIFIER.test(value)) malformed(`bad ${what} name`);
  return value as string;
}

function fields(value: unknown, owner: string): FieldAbi[] {
  if (!Array.isArray(value)) malformed(`${owner} has no field list`);
  return (value as unknown[]).map((field) => {
    const { name, type } = (field ?? {}) as { name?: unknown; type?: unknown };
    const fieldName = identifier(name, `field of ${owner}`);
    if (typeof type !== "string") malformed(`field ${owner}.${fieldName} has no type`);
    let parsed: MoveType;
    try {
      parsed = parseMoveType(type as string);
    } catch {
      parsed = { kind: "unsupported", text: (type as string).slice(0, 200) };
    }
    return { name: fieldName, type: parsed };
  });
}

function struct(value: unknown): StructAbi {
  const raw = (value ?? {}) as Record<string, unknown>;
  const name = identifier(raw.name, "struct");
  const isEnum = raw.is_enum ?? false;
  // Nodes from before module events omit is_event; treat that as false.
  const isEvent = raw.is_event ?? false;
  if (typeof isEnum !== "boolean" || typeof isEvent !== "boolean") {
    malformed(`struct ${name} has non-boolean flags`);
  }
  if (!Array.isArray(raw.generic_type_params)) malformed(`struct ${name} has no type params`);
  const variants = raw.variants ?? [];
  if (!Array.isArray(variants)) malformed(`struct ${name} has a bad variant list`);
  return {
    name,
    isEvent: isEvent as boolean,
    isEnum: isEnum as boolean,
    typeParams: (raw.generic_type_params as unknown[]).length,
    fields: fields(raw.fields, name),
    variants: (variants as unknown[]).map((variant) => {
      const v = (variant ?? {}) as { name?: unknown; fields?: unknown };
      const variantName = identifier(v.name, `variant of ${name}`);
      return { name: variantName, fields: fields(v.fields, `${name}::${variantName}`) };
    }),
  };
}

/** Validates a fullnode `/accounts/{address}/module/{name}` response. */
export function parseModuleAbi(json: unknown): ModuleAbi {
  const abi = (json as { abi?: unknown } | null)?.abi as Record<string, unknown> | undefined;
  if (abi === undefined || abi === null || typeof abi !== "object") malformed("no abi");
  const { address, name, structs } = abi!;
  if (typeof address !== "string" || !ADDRESS.test(address)) malformed("bad address");
  if (!Array.isArray(structs)) malformed("no struct list");
  return {
    address: address as string,
    name: identifier(name, "module"),
    structs: (structs as unknown[]).map(struct),
  };
}
