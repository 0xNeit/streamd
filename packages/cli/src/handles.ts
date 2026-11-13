import type { EventHandleInfo } from "@streamd/aptos";
import type { HandlesOptions } from "./args.js";
import { message } from "./format.js";

export interface HandlesIO {
  /** `listEventHandles` over the endpoint's node. */
  list(address: string): Promise<EventHandleInfo[]>;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborted when stdout closes, which ends the command with exit code 0. */
  signal: AbortSignal;
}

const PLAIN = /^[A-Za-z0-9_:.,/@=+-]+$/;

function quote(arg: string): string {
  return PLAIN.test(arg) ? arg : `'${arg.replaceAll("'", `'\\''`)}'`;
}

/** The `tail` flags that build the same descriptor, as one shell-ready string. */
export function handleFlags(descriptor: EventHandleInfo["descriptor"]): string {
  const args =
    "creationNumber" in descriptor
      ? ["--account", descriptor.accountAddress, "--creation-number", String(descriptor.creationNumber)]
      : ["--account", descriptor.accountAddress, "--handle", descriptor.eventHandleStruct, "--field", descriptor.fieldName];
  return args.map(quote).join(" ");
}

export function handleLine(info: EventHandleInfo): string {
  const { resourceType, path, count, guid, descriptor } = info;
  const record = { resourceType, path, count, guid, descriptor, flags: handleFlags(descriptor) };
  return `${JSON.stringify(record, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v))}\n`;
}

/**
 * Prints one JSON line per event handle on the account and resolves to the
 * exit code: 0 when listed (none is fine) or the output closed, 1 when the
 * read failed or a write failed.
 */
export async function runHandles(options: HandlesOptions, io: HandlesIO): Promise<number> {
  let handles: EventHandleInfo[];
  try {
    handles = await io.list(options.address);
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  try {
    for (const info of handles) await io.write(handleLine(info));
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}
