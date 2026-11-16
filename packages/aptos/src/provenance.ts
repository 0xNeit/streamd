import { DEFAULT_LIMIT, StreamdError, StreamdNetworkError, normalizeAddress, parseCursor } from "@streamd/core";
import type { EventAdapter, EventPage, ResourceChange, ResourceDescriptor, StreamdEvent, TransactionEvents } from "@streamd/core";
import { addr, malformed, object, type Fields } from "./records.js";
import type { Provenance, ProvenanceAction, TokenProperty, TransactionLister } from "./types.js";

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
 * actions its framework events name the object in, a `burn` when it deleted
 * the `ObjectCore` with no burn event, every change at the object, and the
 * `PropertyMap` it wrote, decoded.
 */
export function toProvenance(object: string, found: TransactionEvents): StreamdEvent<Provenance> {
  const actions: ProvenanceAction[] = [];
  for (const event of found.events) {
    const named = actionOf(event);
    if (named !== undefined && named[0] === object) actions.push(named[1]);
  }
  const changes: ResourceChange[] = (found.changes ?? []).filter((c) => c.address === object);
  // A write set holds at most one change per resource.
  const map = changes.find((c) => c.type === PROPERTY_MAP && c.data !== null);
  if (changes.some((c) => c.type === OBJECT_CORE && c.data === null) && !actions.some((a) => a.kind === "burn")) {
    actions.push({ kind: "burn" });
  }
  const first = found.events[0];
  const envelope = first ?? { type: OBJECT_CORE, version: found.version, sequenceNumber: 0n, eventIndex: 0 };
  const data: Provenance = { object, success: found.success, actions, changes };
  if (map !== undefined) data.properties = decodeProperties(map.data);
  return { ...envelope, data };
}

/** The most versions one Indexer listing returns. */
const LISTING = 100;

/**
 * The descriptor provenance queries and subscriptions run under. Only the
 * provenance source reads it, and a `resource` descriptor routes its
 * by-version reads to the fullnode side of a `RoutingAdapter`.
 */
export function provenanceDescriptor(object: string): ResourceDescriptor {
  return { kind: "resource", accountAddress: object, resourceType: OBJECT_CORE };
}

async function withNames(
  record: StreamdEvent<Provenance>,
  name: (address: string) => Promise<string | null>,
): Promise<StreamdEvent<Provenance>> {
  const addresses = new Set<string | undefined>([record.sender === undefined ? undefined : normalizeAddress(record.sender)]);
  for (const action of record.data.actions) {
    if (action.kind === "transfer") addresses.add(action.from).add(action.to);
    if (action.kind === "burn") addresses.add(action.previousOwner);
  }
  addresses.delete(undefined);
  const names = await Promise.all([...addresses].map(async (a) => [a!, await name(a!)] as const));
  return { ...record, data: { ...record.data, names: Object.fromEntries(names) } };
}

/**
 * Pages of provenance records for `object` (long form): each page lists up
 * to `limit` versions from `lister`, from the cursor, `fromVersion` or its
 * processed version, through `toVersion` or that version, and reads each
 * one through `adapter.getTransactionEventsByVersion`, one at a time, so an
 * archive behind it answers pruned ones. The page fetch holds the client's
 * slot, whose `requestTimeout` bounds the whole page; the reads themselves
 * go straight to `adapter`. The cursor is the next version to list.
 */
export function provenanceSource(
  lister: TransactionLister,
  adapter: EventAdapter,
  object: string,
  name?: (address: string) => Promise<string | null>,
): EventAdapter {
  if (typeof adapter.getTransactionEventsByVersion !== "function") {
    throw new StreamdError("provenance needs an adapter that implements getTransactionEventsByVersion");
  }
  const descriptor = provenanceDescriptor(object);
  // Pages are read one at a time, so one still reading when the next starts
  // was given up on (timed out or aborted), and it reads no more.
  let page = new AbortController();
  const source: EventAdapter = {
    fetchEvents: async (_descriptor, options): Promise<EventPage> => {
      // A network error, so a caller still holding the page retries rather than stops.
      page.abort(new StreamdNetworkError("Superseded by a later page"));
      const { signal } = (page = new AbortController());
      if (options.offset !== undefined) throw new StreamdError("provenance takes no offset; start at fromVersion or a cursor");
      const wanted = options.limit ?? DEFAULT_LIMIT;
      if (!(wanted > 0)) return { events: [], hasMore: false };
      const head = await lister.getHeadVersion();
      const start = options.cursor !== undefined ? parseCursor(options.cursor) : (options.fromVersion ?? head);
      const end = options.toVersion !== undefined && options.toVersion < head ? options.toVersion : head;
      if (start > end) return { events: [], nextCursor: start.toString(), hasMore: false };
      const limit = Math.min(wanted, LISTING);
      const versions = await lister.listTransactionVersions(object, start, end, limit);
      const events: StreamdEvent<Provenance>[] = [];
      for (const version of versions) {
        signal.throwIfAborted();
        const found = await adapter.getTransactionEventsByVersion!(version, descriptor);
        // The Indexer listed it, so a node without it is behind and will catch up.
        if (found === undefined) throw new StreamdNetworkError(`Transaction ${version} is not on the node yet`);
        const record = toProvenance(object, found);
        events.push(name === undefined ? record : await withNames(record, name));
      }
      const next = versions.length < limit ? end + 1n : versions[versions.length - 1]! + 1n;
      return { events, nextCursor: next.toString(), hasMore: next <= end, throughVersion: next - 1n };
    },
    getHeadVersion: () => lister.getHeadVersion(),
  };
  if (adapter.getBlock !== undefined) source.getBlock = (_d, height) => adapter.getBlock!(descriptor, height);
  if (adapter.getBlockHeights !== undefined) source.getBlockHeights = () => adapter.getBlockHeights!(descriptor);
  return source;
}
