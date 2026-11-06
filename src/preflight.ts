import { getAptosFullNode } from "@aptos-labs/ts-sdk";
import type { Aptos, MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { StreamdError } from "./errors.js";
import { MOVE_IDENTIFIER } from "./filter.js";
import type { ModuleEventDescriptor } from "./types.js";
import { toStreamdError } from "./adapters/fullnode.js";
import { requireAddress, requireEventType } from "./adapters/validate.js";

/**
 * - `event`: the struct declares `#[event]`.
 * - `legacy`: no `#[event]`, but `drop` and `store`, so a legacy `EventHandle`
 *   can emit it and the module scan still sees it.
 * - `not-event`: lacks `drop` or `store`, so no event can carry it.
 * - `missing-module`: no such module is published at the address.
 * - `missing-struct`: the module declares no such struct.
 */
export type EventTypeStatus =
  | "event"
  | "legacy"
  | "not-event"
  | "missing-module"
  | "missing-struct";

export interface EventTypeCheck {
  eventType: string;
  status: EventTypeStatus;
}

export interface DescriptorCheck {
  /** True when every type is `event` or `legacy`. */
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

function statusOf(module: MoveModuleBytecode | undefined, struct: string): EventTypeStatus {
  if (module === undefined) return "missing-module";
  const found = module.abi!.structs.find((s) => s?.name === struct);
  if (found === undefined) return "missing-struct";
  const abilities: unknown = found.abilities;
  // Nodes from before module events omit is_event; treat that as false.
  const isEvent: unknown = found.is_event ?? false;
  if (typeof isEvent !== "boolean" || !Array.isArray(abilities)) {
    throw new StreamdError(`Fullnode returned a malformed ABI for struct ${struct}`);
  }
  if (isEvent) return "event";
  return abilities.includes("drop") && abilities.includes("store") ? "legacy" : "not-event";
}

async function fetchModule(
  aptos: Aptos,
  address: string,
  module: string,
): Promise<MoveModuleBytecode | undefined> {
  let data: MoveModuleBytecode;
  try {
    // Not aptos.getAccountModule: it memoizes by address and name across every
    // Aptos instance, so a check on one network could answer from another.
    ({ data } = await getAptosFullNode<{}, MoveModuleBytecode>({
      aptosConfig: aptos.config,
      originMethod: "checkDescriptor",
      path: `accounts/${address}/module/${module}`,
    }));
  } catch (err) {
    if ((err as { status?: unknown } | null)?.status === 404) return undefined;
    throw toStreamdError(err, `module ${address}::${module}`);
  }
  if (!Array.isArray(data?.abi?.structs)) {
    throw new StreamdError(`Fullnode returned module ${address}::${module} without an ABI`);
  }
  return data;
}

/**
 * Whether each event type in a `module` descriptor names a struct published
 * on chain that an event can carry. Only the outer struct of a generic type is
 * checked, not its type arguments. One request per distinct module.
 */
export async function checkDescriptor(
  aptos: Aptos,
  descriptor: ModuleEventDescriptor,
): Promise<DescriptorCheck> {
  if ((descriptor as { kind?: unknown } | null)?.kind !== "module") {
    throw new StreamdError("checkDescriptor checks module descriptors only");
  }
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
  return {
    ok: types.every((t) => t.status === "event" || t.status === "legacy"),
    types,
  };
}
