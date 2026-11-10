import type { DescriptorCheck, ModuleEventDescriptor } from "@streamd/core";
import type { CheckOptions } from "./args.js";
import { message } from "./format.js";

export interface CheckIO {
  check(descriptor: ModuleEventDescriptor): Promise<DescriptorCheck>;
  write(line: string): void;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
}

// The longest status, "missing-struct", plus one space.
const STATUS_WIDTH = 15;

/** Prints each type's status and resolves to the exit code: 0 when all can be emitted, else 1. */
export async function runCheck(options: CheckOptions, io: CheckIO): Promise<number> {
  let result: DescriptorCheck;
  try {
    result = await io.check(options.descriptor);
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  for (const { eventType, status } of result.types) {
    io.write(`${status.padEnd(STATUS_WIDTH)}${eventType}\n`);
  }
  return result.ok ? 0 : 1;
}
