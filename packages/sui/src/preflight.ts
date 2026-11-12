import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { normalizeStructTag, parseStructTag } from "@mysten/sui/utils";
import { StreamdError } from "@streamd/core";
import type { AccountEventDescriptor, ModuleEventDescriptor } from "@streamd/core";
import { networkError, suiAddress } from "./adapter.js";

type PackageService = SuiGrpcClient["movePackageService"];

// The protobuf `Ability` enum's values.
const COPY = 1;
const DROP = 2;

/**
 * - `event`: the struct or enum has `copy` and `drop`, which is all
 *   `sui::event::emit` asks; Sui has no `#[event]` attribute.
 * - `not-event`: it lacks `copy` or `drop`, so no event can carry it.
 * - `missing-package`: no package is published at the address.
 * - `missing-module`: the package has no such module.
 * - `missing-struct`: the module declares no such struct or enum.
 * - `defined-elsewhere`: the address is a later version of an upgraded
 *   package; an earlier version introduced the type, and events name it by
 *   that version's id, `definingId`.
 */
export type SuiEventTypeStatus =
  | "event"
  | "not-event"
  | "missing-package"
  | "missing-module"
  | "missing-struct"
  | "defined-elsewhere";

export interface SuiEventTypeCheck {
  /** The type checked, in the long form Sui events carry. */
  eventType: string;
  status: SuiEventTypeStatus;
  /** For `defined-elsewhere`: the package id events name the type by. */
  definingId?: string;
}

export interface SuiDescriptorCheck {
  /** True when every status is `event`. */
  ok: boolean;
  /** One entry per distinct type, in descriptor order. */
  types: SuiEventTypeCheck[];
}

interface Datatype {
  name?: string;
  definingId?: string;
  abilities?: unknown;
}
type Modules = Map<string, Datatype[]>;

function parseType(value: unknown): { eventType: string; address: string; module: string; name: string } {
  try {
    const eventType = normalizeStructTag(value as string);
    const { address, module, name } = parseStructTag(eventType);
    return { eventType, address, module, name };
  } catch {
    throw new StreamdError(`Not a valid Sui event type: ${String(value)}`);
  }
}

function packageService(sui: SuiGrpcClient): PackageService {
  const service = (sui as { movePackageService?: Partial<PackageService> } | null)?.movePackageService;
  if (typeof service?.getPackage !== "function") {
    throw new StreamdError("Checking a Sui descriptor needs a SuiGrpcClient");
  }
  return service as PackageService;
}

/** A package's datatypes by module, or `undefined` when there is no package at `address`. */
async function readPackage(service: PackageService, address: string): Promise<Modules | undefined> {
  let modules: unknown;
  try {
    modules = (await service.getPackage({ packageId: address })).response.package?.modules;
  } catch (err) {
    // The id is validated, so INVALID_ARGUMENT means the object there is not a package.
    const code = (err as { code?: unknown } | null)?.code;
    if (code === "NOT_FOUND" || code === "INVALID_ARGUMENT") return undefined;
    networkError(err);
  }
  if (!Array.isArray(modules)) throw new StreamdError(`Sui node returned package ${address} without modules`);
  return new Map(
    modules.map((m: { name?: string; datatypes?: unknown }) => {
      if (!Array.isArray(m?.datatypes)) throw new StreamdError(`Sui node returned a malformed module in package ${address}`);
      return [m.name ?? "", m.datatypes as Datatype[]];
    }),
  );
}

function statusOf(modules: Modules | undefined, address: string, module: string, name: string): Omit<SuiEventTypeCheck, "eventType"> {
  if (modules === undefined) return { status: "missing-package" };
  const datatypes = modules.get(module);
  if (datatypes === undefined) return { status: "missing-module" };
  const found = datatypes.find((d) => d?.name === name);
  if (found === undefined) return { status: "missing-struct" };
  if (!Array.isArray(found.abilities)) throw new StreamdError(`Sui node returned a malformed datatype ${address}::${module}::${name}`);
  const definingId = found.definingId === undefined ? address : suiAddress(found.definingId);
  if (definingId !== address) return { status: "defined-elsewhere", definingId };
  return { status: found.abilities.includes(COPY) && found.abilities.includes(DROP) ? "event" : "not-event" };
}

async function checkTypes(sui: SuiGrpcClient, list: readonly unknown[]): Promise<SuiEventTypeCheck[]> {
  const refs = list.map(parseType);
  const service = packageService(sui);
  const packages = new Map<string, Modules | undefined>();
  const seen = new Set<string>();
  const types: SuiEventTypeCheck[] = [];
  for (const { eventType, address, module, name } of refs) {
    if (seen.has(eventType)) continue;
    seen.add(eventType);
    if (!packages.has(address)) packages.set(address, await readPackage(service, address));
    types.push({ eventType, ...statusOf(packages.get(address), address, module, name) });
  }
  return types;
}

const result = (types: SuiEventTypeCheck[], ok = true): SuiDescriptorCheck => ({
  ok: ok && types.every((t) => t.status === "event"),
  types,
});

/**
 * Whether each type a descriptor names can be emitted as a Sui event, read
 * from its package through the node's `MovePackageService`, one read per
 * package. Only the outer datatype of a generic type is checked. An `account`
 * descriptor's address is only validated (Sui has no account object), and its
 * `eventTypes` checked like a `module` descriptor's. `handle` and `resource`
 * descriptors are refused: Sui has neither; so is `object`, which names no type.
 */
export async function checkDescriptor(
  sui: SuiGrpcClient,
  descriptor: ModuleEventDescriptor | AccountEventDescriptor,
): Promise<SuiDescriptorCheck> {
  switch ((descriptor as { kind?: unknown } | null)?.kind) {
    case "module": {
      const { eventType } = descriptor as ModuleEventDescriptor;
      const list = Array.isArray(eventType) ? eventType : [eventType];
      if (list.length === 0) throw new StreamdError("Module descriptor requires at least one eventType");
      return result(await checkTypes(sui, list));
    }
    case "account": {
      const { accountAddress, eventTypes } = descriptor as AccountEventDescriptor;
      suiAddress(accountAddress);
      if (eventTypes === undefined) return result([]);
      if (!Array.isArray(eventTypes)) throw new StreamdError("Account descriptor eventTypes must be an array");
      // An empty list matches nothing.
      return result(await checkTypes(sui, eventTypes), eventTypes.length > 0);
    }
    default:
      throw new StreamdError("checkDescriptor checks module and account descriptors: Sui has no event handles or account resources, and an object descriptor names no type");
  }
}
