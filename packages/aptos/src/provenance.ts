import { StreamdError } from "@streamd/core";
import type { ResourceChange, StreamdEvent, TransactionEvents } from "@streamd/core";
import { addr, malformed, object, type Fields } from "./records.js";
import type { Provenance, ProvenanceAction, TokenProperty } from "./types.js";

const PROPERTY_MAP = "0x4::property_map::PropertyMap";
const OBJECT_CORE = "0x1::object::ObjectCore";
/** Indexed by the type tag `0x4::property_map` stores beside each value. */
const PROPERTY_TYPES = ["bool", "u8", "u16", "u32", "u64", "u128", "u256", "address", "vector<u8>", "0x1::string::String"];

const utf8 = new TextDecoder("utf-8", { fatal: true });

const toHex = (bytes: Uint8Array) => `0x${Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")}`;
const fromHex = (hex: string) => Uint8Array.from(hex.slice(2).match(/../g) ?? [], (b) => parseInt(b, 16));

/** An unsigned little-endian integer of `bytes`. */
function littleEndian(bytes: Uint8Array): bigint {
  let value = 0n;
  for (let i = bytes.length - 1; i >= 0; i--) value = (value << 8n) | BigInt(bytes[i]!);
  return value;
}

/** A BCS `vector<u8>`: a ULEB128 length, then exactly that many bytes. */
function bcsBytes(bytes: Uint8Array): Uint8Array | undefined {
  let length = 0;
  let at = 0;
  for (let shift = 0; at < bytes.length && shift <= 28; shift += 7) {
    const byte = bytes[at++]!;
    length += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) return at + length === bytes.length ? bytes.subarray(at) : undefined;
  }
  return undefined;
}

function decodeValue(type: string, bytes: Uint8Array): TokenProperty["value"] | undefined {
  const width = { u8: 1, u16: 2, u32: 4, u64: 8, u128: 16, u256: 32 }[type];
  if (width !== undefined) return bytes.length === width ? littleEndian(bytes) : undefined;
  if (type === "bool") return bytes.length === 1 && bytes[0]! <= 1 ? bytes[0] === 1 : undefined;
  if (type === "address") return bytes.length === 32 ? toHex(bytes) : undefined;
  const inner = bcsBytes(bytes);
  if (inner === undefined) return undefined;
  if (type === "vector<u8>") return toHex(inner);
  try {
    return utf8.decode(inner);
  } catch {
    return undefined;
  }
}

/** Every property of a `PropertyMap` value, as the fullnode writes it; anything off the struct's shape throws. */
export function decodeProperties(map: unknown): TokenProperty[] {
  const entries = ((map as Fields | null)?.inner as Fields | undefined)?.data;
  if (!Array.isArray(entries)) throw new StreamdError("PropertyMap does not have its struct's fields");
  return entries.map((entry: unknown) => {
    const { key, value } = (entry ?? {}) as Fields;
    const { type: tag, value: hex } = (value ?? {}) as Fields;
    const type = typeof tag === "number" ? PROPERTY_TYPES[tag] : undefined;
    const decoded =
      typeof key === "string" && type !== undefined && typeof hex === "string" && /^0x([0-9a-fA-F]{2})*$/.test(hex)
        ? decodeValue(type, fromHex(hex))
        : undefined;
    if (decoded === undefined) {
      throw new StreamdError(`PropertyMap entry ${typeof key === "string" ? JSON.stringify(key.slice(0, 80)) : ""} does not decode`);
    }
    return { key: key as string, type: type!, value: decoded };
  });
}

/** The object a framework event names, and what it did, or `undefined` for any other event. */
function actionOf(event: StreamdEvent): [string, ProvenanceAction] | undefined {
  const d = (event.data ?? {}) as Fields;
  const handle = () => addr(event.guid?.accountAddress, event);
  switch (event.type) {
    case "0x1::object::Transfer":
    case "0x1::object::TransferEvent":
      return [addr(d.object, event), { kind: "transfer", from: addr(d.from, event), to: addr(d.to, event) }];
    case "0x4::collection::Mint":
      return [addr(d.token, event), { kind: "mint", collection: addr(d.collection, event) }];
    case "0x4::collection::ConcurrentMintEvent":
      return [addr(d.token, event), { kind: "mint", collection: addr(d.collection_addr, event) }];
    case "0x4::collection::MintEvent":
      return [addr(d.token, event), { kind: "mint", collection: handle() }];
    case "0x4::collection::Burn":
      return [addr(d.token, event), { kind: "burn", collection: addr(d.collection, event), previousOwner: addr(d.previous_owner, event) }];
    case "0x4::collection::ConcurrentBurnEvent":
      return [addr(d.token, event), { kind: "burn", collection: addr(d.collection_addr, event) }];
    case "0x4::collection::BurnEvent":
      return [addr(d.token, event), { kind: "burn", collection: handle() }];
    case "0x4::token::Mutation":
      return [addr(d.token_address, event), mutation(d, event)];
    case "0x4::token::MutationEvent":
      return [handle(), mutation(d, event)];
    case "0x4::collection::Mutation": {
      const collection = object(d.collection);
      if (collection === undefined) throw malformed(event);
      return [collection, mutation(d, event)];
    }
    case "0x4::collection::MutationEvent":
      return [handle(), mutation(d, event)];
    default:
      return undefined;
  }
}

function mutation(d: Fields, event: StreamdEvent): ProvenanceAction {
  const { mutated_field_name: field, old_value: oldValue, new_value: newValue } = d;
  const text = (v: unknown) => v === undefined || typeof v === "string";
  if (typeof field !== "string" || !text(oldValue) || !text(newValue)) throw malformed(event);
  return {
    kind: "mutation",
    field,
    ...(oldValue !== undefined && { oldValue: oldValue as string }),
    ...(newValue !== undefined && { newValue: newValue as string }),
  };
}

/**
 * The record of one transaction that touched `object` (long form): the
 * actions its framework events name the object in, a `properties` action
 * when it wrote the object's `PropertyMap`, a `burn` when it deleted the
 * `ObjectCore` with no burn event, and every change at the object.
 */
export function toProvenance(object: string, found: TransactionEvents): StreamdEvent<Provenance> {
  const actions: ProvenanceAction[] = [];
  for (const event of found.events) {
    const named = actionOf(event);
    if (named !== undefined && named[0] === object) actions.push(named[1]);
  }
  const changes: ResourceChange[] = (found.changes ?? []).filter((c) => c.address === object);
  // A write set holds at most one change per resource.
  const properties = changes.find((c) => c.type === PROPERTY_MAP && c.data !== null);
  if (properties !== undefined) actions.push({ kind: "properties", properties: decodeProperties(properties.data) });
  if (changes.some((c) => c.type === OBJECT_CORE && c.data === null) && !actions.some((a) => a.kind === "burn")) {
    actions.push({ kind: "burn" });
  }
  const first = found.events[0];
  const envelope = first ?? { type: OBJECT_CORE, version: found.version, sequenceNumber: 0n, eventIndex: 0 };
  return { ...envelope, data: { object, success: found.success, actions, changes } };
}
