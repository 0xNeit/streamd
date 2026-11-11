import { Network, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { normalizeAddress } from "@streamd/core";
import { toStreamdError } from "@streamd/core";
import { requireAddress } from "@streamd/core";

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

function requireAns(aptos: Aptos, subject: string): void {
  const network = aptos.config?.network;
  if (network !== undefined && !ANS_NETWORKS.includes(network)) {
    throw new StreamdError(`${subject} cannot be resolved: no ANS on network ${network}`);
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
  requireAns(aptos, `Aptos name ${nameOrAddress}`);
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

/**
 * The name `address` has set as its primary one, as `alice.apt`, or `undefined`
 * when it has none. Read once at the node's head, and resolved forward, since a
 * reverse record can outlive what it names: a name that no longer points back
 * here counts as none. A failed read rejects rather than returning `undefined`.
 */
export async function primaryName(aptos: Aptos, address: string): Promise<string | undefined> {
  const target = requireAddress(address);
  if (typeof (aptos as { getPrimaryName?: unknown } | null)?.getPrimaryName !== "function") {
    throw new StreamdError("Looking up a primary name needs an Aptos instance");
  }
  requireAns(aptos, `The primary name of ${target}`);
  let name;
  try {
    name = await aptos.getPrimaryName({ address: target });
  } catch (err) {
    if ((err as { status?: unknown } | null)?.status === 400) {
      throw new StreamdError(
        `The primary name of ${target} could not be read: ${err instanceof Error ? err.message : "unknown error"}`,
      );
    }
    throw toStreamdError(err, `the primary name of ${target}`);
  }
  if (!name) return undefined;
  const full = `${name}.apt`;
  try {
    return (await resolveName(aptos, full)) === target ? full : undefined;
  } catch (err) {
    if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) return undefined;
    throw err;
  }
}
