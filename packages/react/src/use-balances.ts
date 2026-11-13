import { useEffect, useRef, useState } from "react";
import type { Balances, BalancesOptions, StreamdClient } from "@streamd/core";
import { useClientWith } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** A client that follows balances: an `AptosClient`, a `SuiClient`, or one shaped like them. */
export interface BalancesClient extends StreamdClient {
  balances(address: string, options?: BalancesOptions): Balances;
}

/** `balances` options, minus `signal`: the hook stops it on unmount and on input change. */
export interface UseBalancesOptions extends Omit<BalancesOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: BalancesClient;
}

/** `loading` until the first read finishes; `stopped` once it fails, polling gives up, or the input is refused. */
export type UseBalancesStatus = "idle" | "loading" | "live" | "stopped";

export interface UseBalancesResult {
  /** Every balance known so far, by asset in long form; empty until the first read finishes. */
  balances: ReadonlyMap<string, bigint>;
  /** The last error, cleared when a balance next changes. */
  error: Error | undefined;
  status: UseBalancesStatus;
}

const EMPTY: ReadonlyMap<string, bigint> = new Map();

/**
 * `client.balances(address, options)` for a component: restarts only when the
 * address or options change by value, and re-renders only when a balance
 * changes. A `null`, `undefined` or empty address waits.
 */
export function useBalances(
  address: string | null | undefined,
  options: UseBalancesOptions = {},
): UseBalancesResult {
  const { client: explicit, ...listener } = options;
  const client = useClientWith<BalancesClient>("balances", explicit);
  const latest = useRef(listener);
  latest.current = listener;
  const key = valueKey([address, listener]);
  const [state, setState] = useState<UseBalancesResult>({ balances: EMPTY, error: undefined, status: "idle" });

  useEffect(() => {
    if (address == null || address === "") {
      setState({ balances: EMPTY, error: undefined, status: "idle" });
      return;
    }
    let current = true;
    let handle: Balances | undefined;
    const opts = withLatestFunctions(listener, () => latest.current);
    const fail = (err: unknown) => {
      if (current) setState((s) => ({ ...s, error: toError(err), status: "stopped" }));
    };
    setState({ balances: EMPTY, error: undefined, status: "loading" });
    try {
      handle = client.balances(address, {
        ...opts,
        onChange: (balances, change) => {
          opts.onChange?.(balances, change);
          if (current) setState({ balances, error: undefined, status: "live" });
        },
        onError: (err) => {
          opts.onError?.(err);
          if (!current) return;
          const stopped = handle !== undefined && !handle.active;
          setState((s) => ({ ...s, error: err, status: stopped ? "stopped" : s.status }));
        },
      });
    } catch (err) {
      opts.onError?.(toError(err));
      fail(err);
      return;
    }
    handle.ready.then(
      (balances) => {
        if (current) setState((s) => (s.status === "live" ? s : { balances, error: undefined, status: "live" }));
      },
      fail,
    );
    return () => {
      current = false;
      handle.unsubscribe();
    };
    // `key` stands for `address` and the options by value.
  }, [client, key]);

  return state;
}
