import type { Aptos } from "@aptos-labs/ts-sdk";
import { generateEventTypes, parseModuleAbi } from "@streamd/codegen";
import type { LoadModule, ModuleRef } from "@streamd/codegen";
import { StreamdError, toStreamdError } from "@streamd/core";

/** The module an event type is declared in, type arguments ignored; the type must already be checked. */
export function moduleOf(eventType: string): ModuleRef {
  const [address = "", module = ""] = eventType.split("<", 1)[0]!.split("::");
  return { address, module };
}

/** Reads module ABIs from the fullnode `aptos` points at. */
export function moduleLoader(aptos: Aptos): LoadModule {
  return async (address, module) => {
    let response: unknown;
    try {
      response = await aptos.getAccountModule({ accountAddress: address, moduleName: module });
    } catch (err) {
      if ((err as { status?: unknown } | null)?.status === 404) throw new StreamdError(`Module ${address}::${module} not found`);
      throw toStreamdError(err, `module ${address}::${module}`);
    }
    return parseModuleAbi(response);
  };
}

/** What `streamd codegen <address>::<module>` writes for the event type's module. */
export function generateFor(aptos: Aptos, eventType: string): Promise<string> {
  return generateEventTypes([moduleOf(eventType)], moduleLoader(aptos));
}
