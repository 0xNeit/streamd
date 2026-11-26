import { useEffect, useRef, useState } from "react";
import type { StreamdEvent } from "@streamd/core";
import { toError, valueKey, withLatestFunctions } from "./key.js";

/** A running monitor, as `proposals()`, `stakePools()` and `multisig()` return one. */
export interface RunningMonitor<V> {
  readonly value: V;
  readonly ready: Promise<unknown>;
  readonly active: boolean;
  unsubscribe(): void;
}

/** The callbacks every monitor takes, which the shared effect wraps. */
export interface MonitorCallbacks<R> {
  onChange?: (record: R, event: StreamdEvent) => void;
  onError?: (error: Error) => void;
}

/** `loading` until the start reads finish; `stopped` once they fail, the monitor stops, or the input is refused. */
export type MonitorStatus = "idle" | "loading" | "live" | "stopped";

export interface MonitorState<S> {
  value: S;
  error: Error | undefined;
  status: MonitorStatus;
}

/** How one hook starts its monitor and copies its state, which the monitor updates in place. */
export interface MonitorSpec<H extends RunningMonitor<unknown>, R, S, O extends MonitorCallbacks<R>> {
  /** The client the monitor runs on; a different one restarts it. */
  client: object;
  /** False while the hook waits on a missing input. */
  enabled: boolean;
  /** The hook's inputs besides the options, compared by value. */
  inputs: unknown;
  options: O;
  start(options: O): H;
  /** The whole state, copied, once `ready` resolves. */
  snapshot(handle: H): S;
  /** The state after one change, copying only what it touched. */
  apply(previous: S, record: R): S;
  empty: S;
}

/**
 * The lifecycle shared by the monitor hooks: starts on mount, restarts only
 * when the inputs or options change by value, calls the latest callbacks,
 * and unsubscribes on unmount. `handle` holds the running monitor.
 */
export function useMonitor<H extends RunningMonitor<unknown>, R, S, O extends MonitorCallbacks<R>>(
  spec: MonitorSpec<H, R, S, O>,
): MonitorState<S> & { handle: { current: H | undefined } } {
  const latest = useRef(spec);
  latest.current = spec;
  const handle = useRef<H | undefined>(undefined);
  const key = valueKey([spec.enabled, spec.inputs, spec.options]);
  const [state, setState] = useState<MonitorState<S>>({ value: spec.empty, error: undefined, status: "idle" });

  useEffect(() => {
    const { enabled, empty } = latest.current;
    if (!enabled) {
      setState({ value: empty, error: undefined, status: "idle" });
      return;
    }
    let current = true;
    let running: H | undefined;
    const opts = withLatestFunctions(latest.current.options, () => latest.current.options);
    const fail = (err: unknown) => {
      if (current) setState((s) => ({ ...s, error: toError(err), status: "stopped" }));
    };
    setState({ value: empty, error: undefined, status: "loading" });
    try {
      running = latest.current.start({
        ...opts,
        onChange: (record: R, event: StreamdEvent) => {
          opts.onChange?.(record, event);
          if (current) setState((s) => ({ value: latest.current.apply(s.value, record), error: undefined, status: "live" }));
        },
        onError: (err: Error) => {
          opts.onError?.(err);
          if (!current) return;
          const stopped = running !== undefined && !running.active;
          setState((s) => ({ ...s, error: err, status: stopped ? "stopped" : s.status }));
        },
      });
    } catch (err) {
      opts.onError?.(toError(err));
      fail(err);
      return;
    }
    handle.current = running;
    running.ready.then(() => {
      if (current) setState({ value: latest.current.snapshot(running!), error: undefined, status: "live" });
    }, fail);
    return () => {
      current = false;
      if (handle.current === running) handle.current = undefined;
      running!.unsubscribe();
    };
    // `key` stands for the inputs and options by value.
  }, [spec.client, key]);

  return { ...state, handle };
}
