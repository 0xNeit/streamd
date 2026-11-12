import type { ModuleEventDescriptor } from "@streamd/core";
import type { CheckOptions } from "./args.js";
import { message } from "./format.js";

/** Either chain's `checkDescriptor` result; `definingId` is Sui's, for `defined-elsewhere`. */
export interface TypeChecks {
  ok: boolean;
  types: readonly { eventType: string; status: string; definingId?: string }[];
}

export interface CheckIO {
  check(descriptor: ModuleEventDescriptor): Promise<TypeChecks>;
  write(line: string): void;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
}

// The longest Aptos status, "missing-struct", plus one space; Sui's can be longer.
const STATUS_WIDTH = 15;

/** Prints each type's status and resolves to the exit code: 0 when all can be emitted, else 1. */
export async function runCheck(options: CheckOptions, io: CheckIO): Promise<number> {
  let result: TypeChecks;
  try {
    result = await io.check(options.descriptor);
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  const width = Math.max(STATUS_WIDTH, ...result.types.map((t) => t.status.length + 1));
  for (const { eventType, status, definingId } of result.types) {
    io.write(`${status.padEnd(width)}${eventType}${definingId === undefined ? "" : ` ${definingId}`}\n`);
  }
  return result.ok ? 0 : 1;
}
