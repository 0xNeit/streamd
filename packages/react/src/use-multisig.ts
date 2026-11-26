import { useCallback } from "react";
import { StreamdError, type StreamdClient } from "@streamd/core";
import type { Multisig, MultisigOptions, MultisigPreview, MultisigPreviewOptions, MultisigState } from "@streamd/aptos";
import { useClientWith } from "./context.js";
import { useMonitor, type MonitorStatus } from "./monitor.js";

/** A client that follows multisig accounts: an `AptosClient`, or one shaped like it. */
export interface MultisigClient extends StreamdClient {
  multisig(address: string, options?: MultisigOptions): Multisig;
}

/** `multisig` options, minus `signal`: the hook stops it on unmount and on input change. */
export interface UseMultisigOptions extends Omit<MultisigOptions, "signal"> {
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: MultisigClient;
}

export interface UseMultisigResult {
  /** The account's state, a copy per change; undefined until the start read finishes. */
  state: MultisigState | undefined;
  /** The last error, cleared when the state next changes. */
  error: Error | undefined;
  status: MonitorStatus;
  /** Simulates a pending transaction through the running monitor; stable across renders. */
  preview(sequenceNumber: bigint, options?: MultisigPreviewOptions): Promise<MultisigPreview>;
}

/**
 * `client.multisig(address, options)` for a component, restarting only when
 * the address or options change by value. A `null` or `undefined` address waits.
 */
export function useMultisig(address: string | null | undefined, options: UseMultisigOptions = {}): UseMultisigResult {
  const { client: explicit, ...listener } = options;
  const client = useClientWith<MultisigClient>("multisig", explicit);
  const { value, error, status, handle } = useMonitor({
    client,
    enabled: address != null,
    inputs: address,
    options: listener,
    start: (opts: MultisigOptions) => client.multisig(address!, opts),
    snapshot: (running: Multisig) => structuredClone(running.value),
    apply: (_previous: MultisigState | undefined, state: MultisigState) => structuredClone(state),
    empty: undefined as MultisigState | undefined,
  });
  const preview = useCallback((sequenceNumber: bigint, previewOptions?: MultisigPreviewOptions) => {
    const running = handle.current;
    if (running === undefined) return Promise.reject(new StreamdError("useMultisig has no running monitor to preview with"));
    return running.preview(sequenceNumber, previewOptions);
  }, [handle]);
  return { state: value, error, status, preview };
}
