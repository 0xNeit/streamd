import { formatExplanation } from "@streamd/aptos";
import type { AptosClient, TransactionExplanation } from "@streamd/aptos";
import type { ExplainCommandOptions } from "./args.js";
import { message } from "./format.js";

export interface ExplainIO {
  client: Pick<AptosClient, "explain">;
  /** Writes the output; a returned promise is awaited, for backpressure. */
  write(text: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  signal: AbortSignal;
}

function json(explanation: TransactionExplanation): string {
  return `${JSON.stringify(explanation, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v))}\n`;
}

/**
 * Prints what a transaction did and resolves to the process exit code: 0 when
 * it succeeded (or the output closed), 1 when it failed (still printed), was
 * not found, a read failed or a write failed, 130 when interrupted while waiting.
 */
export async function runExplain(options: ExplainCommandOptions, io: ExplainIO): Promise<number> {
  const { timeout } = options;
  let explanation: TransactionExplanation;
  try {
    explanation = await io.client.explain(options.hash, {
      signal: io.signal,
      ...(timeout !== undefined && { timeout }),
    });
  } catch (err) {
    if (io.signal.aborted) return 130;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  try {
    await io.write(options.json ? json(explanation) : formatExplanation(explanation));
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  if (explanation.success) return 0;
  io.error(`streamd: transaction failed at version ${explanation.version}: ${explanation.vmStatus}`);
  return 1;
}
