import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { isValidSuiNSName } from "@mysten/sui/utils";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { networkError, suiAddress } from "./adapter.js";

type NameService = SuiGrpcClient["nameService"];

// The node percent-encodes the status message on some transports; the SDK's own check accepts both.
const EXPIRED = /^name(?: |%20)has(?: |%20)expired$/;

/**
 * The long-form address of `value` when it is one, `undefined` when it is a
 * well-formed SuiNS name (`alice.sui`, `@alice`, `sub.alice.sui`, `sub@alice`);
 * anything else throws. `0x` always means an address, so a mistyped one is
 * refused rather than looked up as a name.
 */
export function addressOrName(value: string): string | undefined {
  if (typeof value === "string" && value.startsWith("0x")) return suiAddress(value);
  if (typeof value !== "string" || !isValidSuiNSName(value)) {
    throw new StreamdError(`Not a valid Sui address or SuiNS name: ${String(value).slice(0, 80)}`);
  }
  return undefined;
}

function nameService(sui: SuiGrpcClient): NameService {
  const service = (sui as { nameService?: Partial<NameService> } | null)?.nameService;
  if (typeof service?.lookupName !== "function" || typeof service.reverseLookupName !== "function") {
    throw new StreamdError("Resolving a SuiNS name needs a SuiGrpcClient");
  }
  return service as NameService;
}

function codeOf(err: unknown): { code: unknown; text: string } {
  const { code, message } = (err ?? {}) as { code?: unknown; message?: unknown };
  return { code, text: typeof message === "string" ? message : "" };
}

/** No record: unregistered, or expired, which the node reports as `RESOURCE_EXHAUSTED`. */
function isMiss(err: unknown): boolean {
  const { code, text } = codeOf(err);
  return code === "NOT_FOUND" || (code === "RESOURCE_EXHAUSTED" && EXPIRED.test(text));
}

/** What the name service says about the name itself is a plain `StreamdError`; anything else is the network's. */
function nameError(err: unknown, subject: string): never {
  const { code, text } = codeOf(err);
  const reason =
    code === "NOT_FOUND"
      ? "is not registered"
      : code === "RESOURCE_EXHAUSTED" && EXPIRED.test(text)
        ? "has expired"
        : code === "INVALID_ARGUMENT"
          ? `is malformed: ${text}`
          : code === "UNIMPLEMENTED"
            ? "cannot be resolved: this network has no SuiNS"
            : undefined;
  if (reason !== undefined) throw new StreamdError(`${subject} ${reason}`);
  networkError(err);
}

/**
 * The long-form address a SuiNS name points to, read once through the node's
 * name service at its head; an address is returned in long form with no read.
 *
 * An unregistered or expired name, one with no target address, and any name
 * on a network without SuiNS (devnet) reject with a `StreamdError`; a node or
 * transport failure with a `StreamdNetworkError`.
 */
export async function resolveName(sui: SuiGrpcClient, nameOrAddress: string): Promise<string> {
  const address = addressOrName(nameOrAddress);
  if (address !== undefined) return address;
  const service = nameService(sui);
  const subject = `SuiNS name ${nameOrAddress}`;
  let target: string | undefined;
  try {
    target = (await service.lookupName({ name: nameOrAddress })).response.record?.targetAddress;
  } catch (err) {
    nameError(err, subject);
  }
  if (!target) throw new StreamdError(`${subject} has no target address`);
  return suiAddress(target);
}

/**
 * The name `address` has set as its default one, as `alice.sui`, or
 * `undefined` when it has none. Read once at the node's head, and resolved
 * forward, since a reverse record can outlive what it names: a name that no
 * longer points back here counts as none. A failed read rejects rather than
 * returning `undefined`.
 */
export async function primaryName(sui: SuiGrpcClient, address: string): Promise<string | undefined> {
  const target = suiAddress(address);
  const service = nameService(sui);
  let name: string | undefined;
  try {
    name = (await service.reverseLookupName({ address: target })).response.record?.name;
  } catch (err) {
    if (isMiss(err)) return undefined;
    nameError(err, `The primary name of ${target}`);
  }
  if (!name) return undefined;
  try {
    return (await resolveName(sui, name)) === target ? name : undefined;
  } catch (err) {
    if (err instanceof StreamdError && !(err instanceof StreamdNetworkError)) return undefined;
    throw err;
  }
}
