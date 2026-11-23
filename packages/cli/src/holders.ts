import type { AptosClient, FungibleAssetIndexer, FungibleHolder, HolderSnapshot } from "@streamd/aptos";
import type { HoldersCommandOptions } from "./args.js";
import { message } from "./format.js";

export interface HoldersIO {
  client: Pick<AptosClient, "holdersAt">;
  indexer: FungibleAssetIndexer;
  /** Writes the output; a returned promise is awaited, for backpressure. */
  write(text: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Reports the snapshot's summary line, without a trailing newline. */
  summary(line: string): void;
  signal: AbortSignal;
}

/** One holder as a JSON line, the balance a decimal string. */
export function holderLine({ store, owner, balance }: FungibleHolder): string {
  return `${JSON.stringify({ store, owner, balance: balance.toString() })}\n`;
}

/** One RFC 4180 record: a field holding a comma, quote, CR or LF is quoted with its quotes doubled, and the record ends CRLF. */
export function csvRow(fields: readonly string[]): string {
  return `${fields.map((f) => (/[",\r\n]/.test(f) ? `"${f.replace(/"/g, '""')}"` : f)).join(",")}\r\n`;
}

/** The snapshot without its holder list, which is the output, as `{"snapshot":{...}}`. */
export function summaryLine({ holders, ...rest }: HolderSnapshot): string {
  const snapshot = { ...rest, holders: holders.length };
  return JSON.stringify({ snapshot }, (_key, v: unknown) => (typeof v === "bigint" ? v.toString() : v));
}

/**
 * Lists an asset's holders at a version, then reports the summary, resolving
 * to the exit code: 0 when printed (or the output closed), 1 when the
 * snapshot or a write failed, 130 when interrupted first.
 */
export async function runHolders(options: HoldersCommandOptions, io: HoldersIO): Promise<number> {
  let snapshot: HolderSnapshot;
  try {
    snapshot = await io.client.holdersAt(options.asset, options.at, {
      indexer: io.indexer,
      ...(options.verify !== undefined && { verify: options.verify }),
      ...(options.maxRequests !== undefined && { maxRequests: options.maxRequests }),
      retries: 5,
      signal: io.signal,
    });
  } catch (err) {
    if (io.signal.aborted) return 130;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  const csv = options.format === "csv";
  try {
    if (csv) await io.write(csvRow(["store", "owner", "balance"]));
    for (const holder of snapshot.holders) {
      await io.write(csv ? csvRow([holder.store, holder.owner, holder.balance.toString()]) : holderLine(holder));
    }
  } catch (err) {
    if (io.signal.aborted) return 0;
    io.error(`streamd: ${message(err)}`);
    return 1;
  }
  io.summary(summaryLine(snapshot));
  return 0;
}
