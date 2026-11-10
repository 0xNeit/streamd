import { getAptosFullNode } from "@aptos-labs/ts-sdk";
import type { Aptos, MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { StreamdError } from "./errors.js";
import { MOVE_IDENTIFIER, normalizeAddress } from "./filter.js";
import { findHandles, handleParts } from "./handles.js";
import type {
  CreationNumberEventDescriptor,
  HandleEventDescriptor,
  ModuleEventDescriptor,
  ResourceDescriptor,
} from "./types.js";
import { toStreamdError } from "./adapters/fullnode.js";
import {
  handlePath,
  requireAddress,
  requireEventType,
  requireResourceType,
} from "./adapters/validate.js";

/**
 * - `event`: the struct declares `#[event]`.
 * - `legacy`: no `#[event]`, but `drop` and `store`, so a legacy `EventHandle`
 *   can emit it and the module scan still sees it.
 * - `not-event`: lacks `drop` or `store`, so no event can carry it.
 * - `missing-module`: no such module is published at the address.
 * - `missing-struct`: the module declares no such struct.
 * - `handle`: the `EventHandle` exists on the account.
 * - `missing-resource`: the account holds no resource of the handle's struct.
 * - `not-handle`: that resource has no such field, or it is not an `EventHandle`.
 * - `missing-handle`: nothing on the account holds that creation number.
 * - `resource`: the account holds the resource.
 * - `not-held`: a valid resource type the account does not hold now.
 * - `not-resource`: the struct lacks `key`, so no account can hold it.
 */
export type EventTypeStatus =
  | "event"
  | "legacy"
  | "not-event"
  | "missing-module"
  | "missing-struct"
  | "handle"
  | "missing-resource"
  | "not-handle"
  | "missing-handle"
  | "resource"
  | "not-held"
  | "not-resource";

export interface EventTypeCheck {
  /** The Move type checked; for a handle by creation number, that number. */
  eventType: string;
  status: EventTypeStatus;
}

export interface DescriptorCheck {
  /** True when every status is `event`, `legacy`, `handle`, `resource` or `not-held`. */
  ok: boolean;
  /** One entry per distinct type, in descriptor order. */
  types: EventTypeCheck[];
}

interface StructRef {
  eventType: string;
  address: string;
  module: string;
  struct: string;
}

function parseType(value: unknown): StructRef {
  const eventType = requireEventType(value);
  const [address = "", module = "", struct = ""] = eventType.split("<", 1)[0]!.split("::");
  if (!MOVE_IDENTIFIER.test(module) || !MOVE_IDENTIFIER.test(struct)) {
    throw new StreamdError(`Not a valid Move event type: ${eventType}`);
  }
  return { eventType, address: requireAddress(address), module, struct };
}

const CAN_DELIVER: ReadonlySet<EventTypeStatus> = new Set([
  "event",
  "legacy",
  "handle",
  "resource",
  "not-held",
]);

function checked(eventType: string, status: EventTypeStatus): DescriptorCheck {
  return { ok: CAN_DELIVER.has(status), types: [{ eventType, status }] };
}

function findStruct(module: MoveModuleBytecode, struct: string) {
  return module.abi!.structs.find((s) => s?.name === struct);
}

function requireAbilities(abilities: unknown, struct: string): string[] {
  if (!Array.isArray(abilities)) {
    throw new StreamdError(`Fullnode returned a malformed ABI for struct ${struct}`);
  }
  return abilities;
}

function statusOf(module: MoveModuleBytecode | undefined, struct: string): EventTypeStatus {
  if (module === undefined) return "missing-module";
  const found = findStruct(module, struct);
  if (found === undefined) return "missing-struct";
  const abilities = requireAbilities(found.abilities, struct);
  // Nodes from before module events omit is_event; treat that as false.
  const isEvent: unknown = found.is_event ?? false;
  if (typeof isEvent !== "boolean") {
    throw new StreamdError(`Fullnode returned a malformed ABI for struct ${struct}`);
  }
  if (isEvent) return "event";
  return abilities.includes("drop") && abilities.includes("store") ? "legacy" : "not-event";
}

/** A read that treats 404 as absent. */
async function readNode<T extends {}>(aptos: Aptos, path: string, label: string): Promise<T | undefined> {
  try {
    const { data } = await getAptosFullNode<{}, T>({
      aptosConfig: aptos.config,
      originMethod: "checkDescriptor",
      path,
    });
    return data;
  } catch (err) {
    if ((err as { status?: unknown } | null)?.status === 404) return undefined;
    throw toStreamdError(err, label);
  }
}

async function fetchModule(
  aptos: Aptos,
  address: string,
  module: string,
): Promise<MoveModuleBytecode | undefined> {
  // Not aptos.getAccountModule: it memoizes by address and name across every
  // Aptos instance, so a check on one network could answer from another.
  const data = await readNode<MoveModuleBytecode>(
    aptos,
    `accounts/${address}/module/${module}`,
    `module ${address}::${module}`,
  );
  if (data !== undefined && !Array.isArray(data?.abi?.structs)) {
    throw new StreamdError(`Fullnode returned module ${address}::${module} without an ABI`);
  }
  return data;
}

async function fetchResource(
  aptos: Aptos,
  address: string,
  type: string,
): Promise<Record<string, unknown> | undefined> {
  const label = `resource ${type} at ${address}`;
  const body = await readNode<{ data?: unknown }>(
    aptos,
    `accounts/${address}/resource/${encodeURIComponent(type)}`,
    label,
  );
  if (body === undefined) return undefined;
  if (typeof body?.data !== "object" || body.data === null) {
    throw new StreamdError(`Fullnode returned ${label} without data`);
  }
  return body.data as Record<string, unknown>;
}

async function checkModule(
  aptos: Aptos,
  descriptor: ModuleEventDescriptor,
): Promise<DescriptorCheck> {
  const list = Array.isArray(descriptor.eventType)
    ? descriptor.eventType
    : [descriptor.eventType];
  if (list.length === 0) {
    throw new StreamdError("Module descriptor requires at least one eventType");
  }
  const refs = [...new Set(list)].map(parseType);
  const modules = new Map<string, MoveModuleBytecode | undefined>();
  const types: EventTypeCheck[] = [];
  for (const { eventType, address, module, struct } of refs) {
    const key = `${address}::${module}`;
    if (!modules.has(key)) modules.set(key, await fetchModule(aptos, address, module));
    types.push({ eventType, status: statusOf(modules.get(key), struct) });
  }
  return { ok: types.every((t) => CAN_DELIVER.has(t.status)), types };
}

async function checkResource(
  aptos: Aptos,
  descriptor: ResourceDescriptor,
): Promise<DescriptorCheck> {
  const address = requireAddress(descriptor.accountAddress);
  const type = requireResourceType(descriptor.resourceType);
  const { address: home, module, struct } = parseType(type);
  const abi = await fetchModule(aptos, home, module);
  if (abi === undefined) return checked(type, "missing-module");
  const found = findStruct(abi, struct);
  if (found === undefined) return checked(type, "missing-struct");
  if (!requireAbilities(found.abilities, struct).includes("key")) {
    return checked(type, "not-resource");
  }
  return checked(type, (await fetchResource(aptos, address, type)) ? "resource" : "not-held");
}

async function checkHandleField(
  aptos: Aptos,
  descriptor: HandleEventDescriptor,
): Promise<DescriptorCheck> {
  const address = requireAddress(descriptor.accountAddress);
  const type = requireResourceType(descriptor.eventHandleStruct);
  const data = await fetchResource(aptos, address, type);
  if (data === undefined) return checked(type, "missing-resource");
  return checked(type, handleParts(data[descriptor.fieldName]) ? "handle" : "not-handle");
}

async function checkCreationNumber(
  aptos: Aptos,
  descriptor: CreationNumberEventDescriptor,
): Promise<DescriptorCheck> {
  const address = requireAddress(descriptor.accountAddress);
  const target = normalizeAddress(address);
  let resources: { data?: unknown }[];
  try {
    resources = await aptos.getAccountResources({ accountAddress: address });
  } catch (err) {
    if ((err as { status?: unknown } | null)?.status !== 404) {
      throw toStreamdError(err, "account resources");
    }
    resources = [];
  }
  for (const resource of resources) {
    for (const { guid } of findHandles(resource.data)) {
      if (
        guid.creationNumber === descriptor.creationNumber &&
        normalizeAddress(guid.accountAddress) === target
      ) {
        return checked(String(descriptor.creationNumber), "handle");
      }
    }
  }
  return checked(String(descriptor.creationNumber), "missing-handle");
}

/**
 * Whether a descriptor names something on chain that can deliver: each struct
 * of a `module` descriptor, a `handle`'s `EventHandle`, or a `resource`'s type.
 * Only the outer struct of a generic type is checked, not its type arguments.
 * `account` descriptors are refused: there is nothing to check but the address.
 */
export async function checkDescriptor(
  aptos: Aptos,
  descriptor:
    | ModuleEventDescriptor
    | HandleEventDescriptor
    | CreationNumberEventDescriptor
    | ResourceDescriptor,
): Promise<DescriptorCheck> {
  switch ((descriptor as { kind?: unknown } | null)?.kind) {
    case "module":
      return checkModule(aptos, descriptor as ModuleEventDescriptor);
    case "resource":
      return checkResource(aptos, descriptor as ResourceDescriptor);
    case "handle":
      // Validates either form before any request.
      handlePath(descriptor as HandleEventDescriptor);
      return "creationNumber" in descriptor && descriptor.creationNumber !== undefined
        ? checkCreationNumber(aptos, descriptor as CreationNumberEventDescriptor)
        : checkHandleField(aptos, descriptor as HandleEventDescriptor);
    default:
      throw new StreamdError("checkDescriptor checks module, handle and resource descriptors");
  }
}
