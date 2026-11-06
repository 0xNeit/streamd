import type { Aptos } from "@aptos-labs/ts-sdk";
import type { HandleEventDescriptor } from "./types.js";
import { toStreamdError } from "./adapters/fullnode.js";
import { requireAddress } from "./adapters/validate.js";

export interface EventHandleInfo {
  descriptor: HandleEventDescriptor;
  /** Events emitted on the handle so far, from its `counter`. */
  count: bigint;
}

function handleCounter(value: unknown): string | undefined {
  const v = value as {
    counter?: unknown;
    guid?: { id?: { addr?: unknown; creation_num?: unknown } };
  } | null;
  const id = v?.guid?.id;
  return typeof v?.counter === "string" &&
    /^\d+$/.test(v.counter) &&
    typeof id?.addr === "string" &&
    typeof id.creation_num === "string"
    ? v.counter
    : undefined;
}

/**
 * Every `EventHandle` stored directly on a field of one of the account's
 * resources, as a `handle` descriptor ready for `on()` or `getEvents()`.
 *
 * Handles nested inside a struct field are skipped: the fullnode's handle
 * events endpoint only addresses top-level fields, so no descriptor could
 * read them.
 */
export async function listEventHandles(
  aptos: Aptos,
  address: string,
): Promise<EventHandleInfo[]> {
  const accountAddress = requireAddress(address);
  let resources;
  try {
    resources = await aptos.getAccountResources({ accountAddress });
  } catch (err) {
    throw toStreamdError(err, "account resources");
  }
  const out: EventHandleInfo[] = [];
  for (const resource of resources) {
    for (const [fieldName, value] of Object.entries(resource.data ?? {})) {
      const counter = handleCounter(value);
      if (counter === undefined) continue;
      out.push({
        descriptor: {
          kind: "handle",
          accountAddress,
          eventHandleStruct: resource.type,
          fieldName,
        },
        count: BigInt(counter),
      });
    }
  }
  return out;
}
