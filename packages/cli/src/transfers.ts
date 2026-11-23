import type { AptosClient } from "@streamd/aptos";
import type { TransfersOptions } from "./args.js";
import { fromArray, runRecords } from "./records.js";
import type { RecordsIO } from "./records.js";

/** The client is an `AptosClient` or a `SuiClient`, which take the same options here. */
export type TransfersIO = RecordsIO<
  Pick<AptosClient, "transfers" | "getTransfers"> & Partial<Pick<AptosClient, "iterateTransfers">>
>;

/**
 * Prints transfer records as JSON lines and resolves to the process exit
 * code, as `runRecords` does.
 */
export function runTransfers(options: TransfersOptions, io: TransfersIO): Promise<number> {
  const { asset, account, withAssetInfo } = options;
  const narrowing = {
    ...(asset !== undefined && { asset }),
    ...(account !== undefined && { account }),
    ...(withAssetInfo && { withAssetInfo }),
  };
  const { client } = io;
  return runRecords(options, {
    follow: (callback, listener) => client.transfers(callback, { ...narrowing, ...listener }),
    range: (filter) =>
      client.iterateTransfers !== undefined
        ? client.iterateTransfers({ ...narrowing, ...filter })
        : fromArray(client.getTransfers({ ...narrowing, ...filter })),
  }, io);
}
