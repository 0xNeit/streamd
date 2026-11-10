import { useEffect, useRef, useState } from "react";
import {
  StreamdTransactionError,
  type EventDescriptor,
  type EventsOfOptions,
  type StreamdClient,
  type StreamdEvent,
} from "@streamd/core";
import { useStreamdClient } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** `eventsOf` options, minus `signal`: the hook aborts on unmount and on input change. */
export interface UseTransactionEventsOptions<TData = unknown> extends Omit<EventsOfOptions, "signal"> {
  /** Keep only the events this descriptor matches. */
  descriptor?: EventDescriptor<TData>;
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: StreamdClient;
}

export interface UseTransactionEventsResult<TData> {
  /** The transaction's matching events, once it has committed successfully. */
  data: StreamdEvent<TData>[] | undefined;
  /** Why there are no events: the transaction failed, was not found in time, or a lookup failed. */
  error: Error | undefined;
  /** True while waiting for the transaction. */
  loading: boolean;
  /** The chain's status for a transaction that committed and failed. */
  vmStatus: string | undefined;
}

/**
 * `client.eventsOf(hash, descriptor, options)` for a component: waits after
 * mount, again whenever the hash or options change by value, aborting the wait
 * it replaces. A `null` or `undefined` hash waits for nothing.
 */
export function useTransactionEvents<TData = unknown>(
  transactionHash: string | null | undefined,
  options: UseTransactionEventsOptions<TData> = {},
): UseTransactionEventsResult<TData> {
  const { client: explicit, descriptor, ...filter } = options;
  const client = useStreamdClient(explicit);
  const latest = useRef(filter);
  latest.current = filter;
  const key = valueKey([transactionHash, descriptor, filter]);
  const [finished, setFinished] = useState<{
    key: string;
    client: StreamdClient;
    result: UseTransactionEventsResult<TData>;
  }>();

  useEffect(() => {
    if (transactionHash == null) return;
    const controller = new AbortController();
    const finish = (result: UseTransactionEventsResult<TData>) => {
      if (!controller.signal.aborted) setFinished({ key, client, result });
    };
    const live = { ...withLatestFunctions(filter, () => latest.current), signal: controller.signal };
    client.eventsOf<TData>(transactionHash, descriptor, live).then(
      (data) => finish({ data, error: undefined, loading: false, vmStatus: undefined }),
      (err: unknown) =>
        finish({
          data: undefined,
          error: toError(err),
          loading: false,
          vmStatus: err instanceof StreamdTransactionError ? err.vmStatus : undefined,
        }),
    );
    return () => controller.abort();
    // `key` stands for the hash, descriptor and options by value.
  }, [client, key]);

  if (finished?.key === key && finished.client === client) return finished.result;
  return { data: undefined, error: undefined, loading: transactionHash != null, vmStatus: undefined };
}
