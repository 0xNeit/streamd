import { useEffect, useRef, useState } from "react";
import type { StreamdClient } from "@streamd/core";
import type { LendingPosition, LendingProtocol, Positions, PositionsOptions } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** A client that keeps lending positions: an `AptosClient`, or one shaped like it. */
export interface PositionsClient extends StreamdClient {
  positions(protocol: LendingProtocol, options?: PositionsOptions): Positions;
}

/** `positions` options, minus `signal`: the hook stops it on unmount and on input change. */
export interface UsePositionsOptions extends Omit<PositionsOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: PositionsClient;
}

/** `loading` until the start reads finish; `stopped` once they fail, the monitor stops, or the input is refused. */
export type UsePositionsStatus = "idle" | "loading" | "live" | "stopped";

export interface UsePositionsResult {
  /** Every tracked position by long-form account; empty until the start reads finish. */
  positions: ReadonlyMap<string, LendingPosition>;
  /** The last error, cleared when a position next changes. */
  error: Error | undefined;
  status: UsePositionsStatus;
}

const EMPTY: ReadonlyMap<string, LendingPosition> = new Map();

/**
 * `client.positions(protocol, options)` for a component: restarts only when
 * the protocol or options change by value, and re-renders when a position
 * changes. A `null` or `undefined` protocol waits.
 */
export function usePositions(
  protocol: LendingProtocol | null | undefined,
  options: UsePositionsOptions = {},
): UsePositionsResult {
  const { client: explicit, ...listener } = options;
  const client = useClientWith<PositionsClient>("positions", explicit);
  const latest = useRef(listener);
  latest.current = listener;
  const key = valueKey([protocol, listener]);
  const [state, setState] = useState<UsePositionsResult>({ positions: EMPTY, error: undefined, status: "idle" });

  useEffect(() => {
    if (protocol == null) {
      setState({ positions: EMPTY, error: undefined, status: "idle" });
      return;
    }
    let current = true;
    let handle: Positions | undefined;
    const opts = withLatestFunctions(listener, () => latest.current);
    const fail = (err: unknown) => {
      if (current) setState((s) => ({ ...s, error: toError(err), status: "stopped" }));
    };
    // `Positions.value` is updated in place, so each render gets its own copy.
    const show = () => current && setState({ positions: new Map(handle!.value), error: undefined, status: "live" });
    setState({ positions: EMPTY, error: undefined, status: "loading" });
    try {
      handle = client.positions(protocol, {
        ...opts,
        onChange: (position, event) => {
          opts.onChange?.(position, event);
          show();
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
    handle.ready.then(show, fail);
    return () => {
      current = false;
      handle.unsubscribe();
    };
    // `key` stands for `protocol` and the options by value.
  }, [client, key]);

  return state;
}
