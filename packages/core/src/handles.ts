import type { Aptos } from "@aptos-labs/ts-sdk";
import type { EventGuid, HandleEventDescriptor } from "./types.js";
import { toStreamdError } from "./adapters/fullnode.js";
import { resolveName } from "./names.js";

export interface EventHandleInfo {
  descriptor: HandleEventDescriptor;
  /** Events emitted on the handle so far, from its `counter`. */
  count: bigint;
  /**
   * The handle's guid, for a `CreationNumberEventDescriptor`. Its address is as
   * the node reports it; compare with `StreamdEvent.guid` through `normalizeAddress`.
   */
  guid: EventGuid;
}

interface HandleParts {
  counter: string;
  guid: EventGuid;
}

/** The `EventHandle` this value is, if it is one. */
export function handleParts(value: unknown): HandleParts | undefined {
  const v = value as {
    counter?: unknown;
    guid?: { id?: { addr?: unknown; creation_num?: unknown } };
  } | null;
  const id = v?.guid?.id;
  return typeof v?.counter === "string" &&
    /^\d+$/.test(v.counter) &&
    typeof id?.addr === "string" &&
    typeof id.creation_num === "string" &&
    /^\d+$/.test(id.creation_num)
    ? {
        counter: v.counter,
        guid: { creationNumber: BigInt(id.creation_num), accountAddress: id.addr },
      }
    : undefined;
}

/** Every `EventHandle` anywhere inside a resource's data. */
export function* findHandles(value: unknown): Generator<HandleParts> {
  const parts = handleParts(value);
  if (parts !== undefined) {
    yield parts;
  } else if (typeof value === "object" && value !== null) {
    for (const child of Object.values(value)) yield* findHandles(child);
  }
}

/**
 * Every `EventHandle` stored directly on a field of one of the account's
 * resources, as a `handle` descriptor ready for `on()` or `getEvents()`.
 *
 * Handles nested inside a struct field are skipped: the struct and field
 * form only addresses top-level fields. A nested handle is still readable by
 * creation number, from its guid in the resource data.
 *
 * `address` may be an Aptos name, resolved first through `resolveName`.
 */
export async function listEventHandles(
  aptos: Aptos,
  address: string,
): Promise<EventHandleInfo[]> {
  const accountAddress = await resolveName(aptos, address);
  let resources;
  try {
    resources = await aptos.getAccountResources({ accountAddress });
  } catch (err) {
    throw toStreamdError(err, "account resources");
  }
  const out: EventHandleInfo[] = [];
  for (const resource of resources) {
    for (const [fieldName, value] of Object.entries(resource.data ?? {})) {
      const parts = handleParts(value);
      if (parts === undefined) continue;
      out.push({
        descriptor: {
          kind: "handle",
          accountAddress,
          eventHandleStruct: resource.type,
          fieldName,
        },
        count: BigInt(parts.counter),
        guid: parts.guid,
      });
    }
  }
  return out;
}
