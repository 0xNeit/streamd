import { useCallback, useEffect, useRef, useState } from "react";
import {
  StreamdError,
  StreamdTransactionError,
  type EventDescriptor,
  type EventOf,
  type EventsOfOptions,
} from "@streamd/core";
import { useClientWith } from "./context.js";
import { toError } from "./key.js";
import type { TransactionEventsClient } from "./use-transaction-events.js";

/** A client that simulates and waits for transactions: an `AptosClient`, a `SuiClient`, or one shaped like them. */
export interface SubmitTransactionClient<TTransaction> extends TransactionEventsClient {
  previewEvents<TData = unknown>(
    transaction: TTransaction,
    descriptor?: EventDescriptor<TData>,
    options?: Pick<EventsOfOptions, "decode" | "where">,
  ): Promise<EventOf<TData>[]>;
}

/** What `signAndSubmit` may resolve with: a hash, or an object carrying it as `hash` (Aptos) or `digest` (Sui). */
export type SubmittedTransaction = string | { hash: string } | { digest: string };

export interface UseSubmitTransactionOptions<TTransaction, TData = unknown>
  extends Pick<EventsOfOptions, "decode" | "where" | "timeout"> {
  /**
   * Signs and submits `transaction`, through a wallet or a local signer. The
   * signal aborts when the component unmounts or `reset()` is called.
   */
  signAndSubmit: (transaction: TTransaction, context: { signal: AbortSignal }) => Promise<SubmittedTransaction>;
  /** Keep only the events this descriptor matches, in the preview and the result. */
  descriptor?: EventDescriptor<TData>;
  /** `false` skips the simulation and signs at once. Default `true`. */
  preview?: boolean;
  /**
   * Submit even when the simulation fails, keeping its reason as
   * `previewError`. Default `false`: a failed simulation stops the run
   * before anything is signed.
   */
  submitOnPreviewFailure?: boolean;
  /** Use this client instead of the nearest `StreamdProvider`'s. */
  client?: SubmitTransactionClient<TTransaction>;
}

export type UseSubmitTransactionStatus = "idle" | "previewing" | "signing" | "confirming" | "confirmed" | "failed";

export interface SubmitTransactionState<TData> {
  status: UseSubmitTransactionStatus;
  /** The events the simulation says the transaction will emit. */
  preview: EventOf<TData>[] | undefined;
  /** Why the simulation failed. */
  previewError: Error | undefined;
  /** The submitted transaction's hash (a Sui digest on Sui), once `signAndSubmit` resolves. */
  hash: string | undefined;
  /** The committed transaction's matching events, once `confirmed`. */
  events: EventOf<TData>[] | undefined;
  /** Why the run `failed`: the preview, the signature, the transaction, or the wait. */
  error: Error | undefined;
  /** The chain's status for a transaction that committed and failed, or for the failed simulation that stopped the run. */
  vmStatus: string | undefined;
}

export interface UseSubmitTransactionResult<TTransaction, TData> extends SubmitTransactionState<TData> {
  /**
   * Preview, sign and confirm `transaction`. Resolves with the run's final
   * state and never rejects; while a run is going, returns its promise and
   * starts nothing, so a `signAndSubmit` must not await it.
   */
  submit: (transaction: TTransaction) => Promise<SubmitTransactionState<TData>>;
  /** Abandon any run and return to `idle`. A transaction already submitted stays submitted. */
  reset: () => void;
}

function idle<TData>(): SubmitTransactionState<TData> {
  return {
    status: "idle",
    preview: undefined,
    previewError: undefined,
    hash: undefined,
    events: undefined,
    error: undefined,
    vmStatus: undefined,
  };
}

/**
 * Preview, sign, submit and confirm a transaction from a component: simulates
 * it through `client.previewEvents`, hands it to `signAndSubmit`, then waits
 * through `client.eventsOf`. Every failure of a run is a `failed` state, never
 * a throw; a client without the methods it needs throws during render.
 * Options are read when `submit` is called.
 */
export function useSubmitTransaction<TTransaction, TData = unknown>(
  options: UseSubmitTransactionOptions<TTransaction, TData>,
): UseSubmitTransactionResult<TTransaction, TData> {
  const client = useClientWith<SubmitTransactionClient<TTransaction>>("eventsOf", options.client);
  if (options.preview !== false && typeof client.previewEvents !== "function") {
    throw new StreamdError("useSubmitTransaction needs a client with previewEvents, or preview: false");
  }
  const latest = useRef({ options, client });
  latest.current = { options, client };
  const [state, setState] = useState(idle<TData>);
  const running = useRef<{ controller: AbortController; promise: Promise<SubmitTransactionState<TData>> }>(undefined);

  const submit = useCallback((transaction: TTransaction) => {
    if (running.current) return running.current.promise;
    const controller = new AbortController();
    const { options, client } = latest.current;
    // Started a tick later, so a signAndSubmit that calls submit synchronously already finds this run.
    const promise = Promise.resolve()
      .then(() => run(transaction, client, options, controller.signal, setState))
      .finally(() => {
        if (running.current?.controller === controller) running.current = undefined;
      });
    running.current = { controller, promise };
    return promise;
  }, []);

  const abandon = useCallback(() => {
    const abandoned = running.current;
    running.current = undefined;
    abandoned?.controller.abort();
    return abandoned !== undefined;
  }, []);

  const reset = useCallback(() => {
    abandon();
    setState(idle<TData>());
  }, [abandon]);

  // Also runs when <Activity> hides the component, keeping its state: a run cut short there
  // must not stay shown as in progress, nor hide a hash already submitted.
  useEffect(
    () => () => {
      if (!abandon()) return;
      setState((shown) =>
        shown.hash === undefined
          ? idle<TData>()
          : { ...shown, status: "failed", error: new StreamdError("Stopped waiting for the transaction: the component was hidden") },
      );
    },
    [abandon],
  );

  return { ...state, submit, reset };
}

async function run<TTransaction, TData>(
  transaction: TTransaction,
  client: SubmitTransactionClient<TTransaction>,
  options: UseSubmitTransactionOptions<TTransaction, TData>,
  signal: AbortSignal,
  publish: (state: SubmitTransactionState<TData>) => void,
): Promise<SubmitTransactionState<TData>> {
  const { descriptor, decode, where, timeout } = options;
  let state = idle<TData>();
  if (signal.aborted) return state;
  const step = (patch: Partial<SubmitTransactionState<TData>>) => {
    state = { ...state, ...patch };
    if (!signal.aborted) publish(state);
    return state;
  };
  const fail = (err: unknown, patch: Partial<SubmitTransactionState<TData>> = {}) =>
    step({ status: "failed", error: toError(err), vmStatus: vmStatusOf(err), ...patch });
  try {
    if (options.preview !== false) {
      step({ status: "previewing" });
      try {
        step({ preview: await client.previewEvents(transaction, descriptor, { decode, where }) });
      } catch (err) {
        if (signal.aborted) return state;
        if (!options.submitOnPreviewFailure) return fail(err, { previewError: toError(err) });
        step({ previewError: toError(err) });
      }
      // previewEvents takes no signal, so an abandoned run is caught here, before anything is signed.
      if (signal.aborted) return state;
    }
    step({ status: "signing" });
    const hash = hashOf(await options.signAndSubmit(transaction, { signal }));
    step({ status: "confirming", hash });
    if (signal.aborted) return state;
    const events = await client.eventsOf<TData>(hash, descriptor, { decode, where, timeout, signal });
    return step({ status: "confirmed", events });
  } catch (err) {
    return signal.aborted ? state : fail(err);
  }
}

function vmStatusOf(err: unknown): string | undefined {
  return err instanceof StreamdTransactionError ? err.vmStatus : undefined;
}

function hashOf(submitted: unknown): string {
  const value =
    typeof submitted === "object" && submitted !== null
      ? ((submitted as { hash?: unknown }).hash ?? (submitted as { digest?: unknown }).digest)
      : submitted;
  if (typeof value !== "string" || value === "") {
    throw new StreamdError("signAndSubmit must resolve with a transaction hash, or an object with a hash or digest");
  }
  return value;
}
