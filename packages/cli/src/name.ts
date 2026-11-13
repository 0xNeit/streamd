import type { NameOptions } from "./args.js";
import type { Chain } from "./chain.js";
import { message } from "./format.js";

export interface NameIO extends Pick<Chain, "resolveName" | "primaryName"> {
  /** Writes the output line; a returned promise is awaited. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborted when stdout closes, which ends the command with exit code 0. */
  signal: AbortSignal;
}

/**
 * Resolves a name to its address, or an address (anything starting `0x`, as
 * the libraries read it) to its primary name, prints `{"name","address"}` as
 * one JSON line, and resolves to the exit code: 0 when printed, `name` null
 * for an address with no primary name, or when the output closed; 1 when the
 * chain refused or failed the lookup, or the write failed.
 */
export async function runName(options: NameOptions, io: NameIO): Promise<number> {
  const { value } = options;
  let record: { name: string | null; address: string };
  try {
    if (value.startsWith("0x")) {
      const address = await io.resolveName(value);
      record = { name: (await io.primaryName(address)) ?? null, address };
    } else {
      record = { name: value, address: await io.resolveName(value) };
    }
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  try {
    await io.write(`${JSON.stringify(record)}\n`);
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
}
