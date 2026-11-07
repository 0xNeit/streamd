import { Network, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError } from "./errors.js";
import { normalizeAddress } from "./filter.js";
import { toStreamdError } from "./adapters/fullnode.js";
import { requireAddress } from "./adapters/validate.js";

const LABEL = "[a-z\\d][a-z\\d-]{1,61}[a-z\\d]";
const ANS_NAME = new RegExp(`^(?:${LABEL}\\.)?${LABEL}(?:\\.apt)?$`);
// The networks the SDK knows an ANS router for; on any other it refuses before a request.
const ANS_NETWORKS: readonly string[] = [Network.MAINNET, Network.TESTNET, Network.LOCAL];

/**
 * The long-form address of `value` when it is one, `undefined` when it is a
 * well-formed Aptos name; anything else throws. `0x` always means an address,
 * so a mistyped one is refused rather than looked up as a name.
 */
export function addressOrName(value: string): string | undefined {
  if (typeof value === "string" && value.startsWith("0x")) return requireAddress(value);
  if (typeof value !== "string" || !ANS_NAME.test(value)) {
    throw new StreamdError(`Not a valid Aptos address or name: ${String(value).slice(0, 80)}`);
  }
  return undefined;
}

/** Throws unless `aptos` can resolve names, so callers can refuse one at construction. */
export function requireNameResolver(aptos: Aptos): void {
  if (typeof (aptos as { getTargetAddress?: unknown } | null)?.getTargetAddress !== "function") {
    throw new StreamdError("Resolving an Aptos name needs an Aptos instance");
  }
}

/**
 * The long-form address an Aptos name (`alice.apt`, `sub.alice.apt`) points
 * to, read once through the ANS router at the node's head; an address is
 * returned in long form with no read.
 *
 * An unregistered or expired name, or one with no target, rejects with a
 * `StreamdError`. Only Aptos mainnet serves names: testnet's router is gone
 * and Movement has none, so names reject there too.
 */
export async function resolveName(aptos: Aptos, nameOrAddress: string): Promise<string> {
  const address = addressOrName(nameOrAddress);
  if (address !== undefined) return address;
  requireNameResolver(aptos);
  const network = aptos.config?.network;
  if (network !== undefined && !ANS_NETWORKS.includes(network)) {
    throw new StreamdError(`Aptos name ${nameOrAddress} cannot be resolved: no ANS on network ${network}`);
  }
  let target;
  try {
    target = await aptos.getTargetAddress({ name: nameOrAddress });
  } catch (err) {
    if ((err as { status?: unknown } | null)?.status === 400) {
      throw new StreamdError(
        `Aptos name ${nameOrAddress} could not be resolved: ${err instanceof Error ? err.message : "unknown error"}`,
      );
    }
    throw toStreamdError(err, `the Aptos name ${nameOrAddress}`);
  }
  const resolved = target === undefined ? undefined : normalizeAddress(target.toString());
  if (resolved === undefined) {
    throw new StreamdError(`Aptos name ${nameOrAddress} is unregistered, expired or has no target address`);
  }
  return resolved;
}
