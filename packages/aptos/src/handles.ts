import type { Aptos } from "@aptos-labs/ts-sdk";
import type { CreationNumberEventDescriptor, EventGuid, HandleEventDescriptor } from "@streamd/core";
import { toStreamdError } from "@streamd/core";
import { normalizeAddress } from "@streamd/core";
import { resolveName } from "@streamd/core";

export interface EventHandleInfo {
  /** By struct and field for a top-level handle, by creation number for a nested one. */
  descriptor: HandleEventDescriptor | CreationNumberEventDescriptor;
  /** The resource holding the handle. */
  resourceType: string;
  /** Field names from the resource's top level to the handle; array positions as strings. */
  path: string[];
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

/** Every `EventHandle` anywhere inside a resource's data, with where it sits. */
export function* findHandles(
  value: unknown,
  path: string[] = [],
): Generator<HandleParts & { path: string[] }> {
  const parts = handleParts(value);
  if (parts !== undefined) {
    yield { ...parts, path };
  } else if (typeof value === "object" && value !== null) {
    for (const [key, child] of Object.entries(value)) yield* findHandles(child, [...path, key]);
  }
}

/**
 * Every `EventHandle` on the account's resources, at any depth, as a descriptor
 * ready for `on()` or `getEvents()`. The struct and field form addresses only
 * top-level fields, so a nested handle comes back by creation number.
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
    for (const { counter, guid, path } of findHandles(resource.data ?? {})) {
      out.push({
        descriptor:
          path.length === 1
            ? { kind: "handle", accountAddress, eventHandleStruct: resource.type, fieldName: path[0]! }
            : {
                kind: "handle",
                accountAddress: normalizeAddress(guid.accountAddress) ?? guid.accountAddress,
                creationNumber: guid.creationNumber,
              },
        resourceType: resource.type,
        path,
        count: BigInt(counter),
        guid,
      });
    }
  }
  return out;
}
