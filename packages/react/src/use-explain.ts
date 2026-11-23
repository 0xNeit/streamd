import { useEffect, useRef, useState } from "react";
import type { StreamdClient } from "@streamd/core";
import type { ExplainOptions, TransactionExplanation } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** A client that explains transactions: an `AptosClient`, or one shaped like it. */
export interface ExplainClient extends StreamdClient {
  explain(target: string, options?: ExplainOptions): Promise<TransactionExplanation>;
}

/**
 * `explain` options for a committed hash, minus `signal`, which the hook
 * aborts on unmount and on input change, and the simulation-only signer keys.
 */
export interface UseExplainOptions
  extends Omit<ExplainOptions, "signal" | "signerPublicKey" | "secondarySignersPublicKeys" | "feePayerPublicKey"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: ExplainClient;
}

export interface UseExplainResult {
  /** The explanation, once the transaction committed; a failed one has `success: false`. */
  data: TransactionExplanation | undefined;
  /** Why there is none: the transaction was not found in time, or a read failed. */
  error: Error | undefined;
  /** True while waiting for the transaction and reading its details. */
  loading: boolean;
}

/**
 * `client.explain(hash, options)` for a component: reads after mount, again
 * whenever the hash or options change by value, aborting the read it replaces.
 * A `null` or `undefined` hash reads nothing.
 */
export function useExplain(transactionHash: string | null | undefined, options: UseExplainOptions = {}): UseExplainResult {
  const { client: explicit, ...filter } = options;
  const client = useClientWith<ExplainClient>("explain", explicit);
  const latest = useRef(filter);
  latest.current = filter;
  const key = valueKey([transactionHash, filter]);
  const [finished, setFinished] = useState<{ key: string; client: ExplainClient; result: UseExplainResult }>();

  useEffect(() => {
    setFinished((prev) => (prev === undefined ? prev : undefined));
    if (transactionHash == null) return;
    const controller = new AbortController();
    const finish = (result: UseExplainResult) => {
      if (!controller.signal.aborted) setFinished({ key, client, result });
    };
    const live = { ...withLatestFunctions(filter, () => latest.current), signal: controller.signal };
    let read: Promise<TransactionExplanation>;
    try {
      read = client.explain(transactionHash, live);
    } catch (err) {
      read = Promise.reject(err);
    }
    read.then(
      (data) => finish({ data, error: undefined, loading: false }),
      (err: unknown) => finish({ data: undefined, error: toError(err), loading: false }),
    );
    return () => controller.abort();
    // `key` stands for the hash and options by value.
  }, [client, key]);

  if (finished?.key === key && finished.client === client) return finished.result;
  return { data: undefined, error: undefined, loading: transactionHash != null };
}
