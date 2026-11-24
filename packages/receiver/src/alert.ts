import { StreamdError, parseAlert } from "@streamd/core";
import type { AlertPayload } from "@streamd/core";
import { receiverCore, reply } from "./receiver.js";
import type { SharedOptions } from "./receiver.js";
import type { DedupStore } from "./store.js";

export interface AlertReceiverOptions extends SharedOptions {
  /** The secret `streamd alert --secret` signs with. Unsigned deliveries are refused. */
  secret: string;
  /**
   * Called once per alert `id`. The id is recorded as handled only once this
   * resolves; a throw or rejection answers 500, so `streamd alert` sends it again.
   */
  onAlert(alert: AlertPayload, request: Request): void | PromiseLike<void>;
  /** Where handled alert ids are kept. Default a `MemoryDedupStore`. */
  store?: DedupStore;
  /**
   * Prefixes every id the store sees, as `<namespace>/<id>`. Rule names repeat
   * across rule files, so give each `streamd alert` sharing a receiver or store its own.
   */
  namespace?: string;
  /** The largest body read, in bytes; a larger one answers 413. Default 4 MiB. */
  maxBodyBytes?: number;
  /** Told why a delivery answered 400 or 500. */
  onError?(error: unknown, request: Request): void;
}

/**
 * A `Request` to `Response` handler for `streamd alert` deliveries, threshold
 * and anomaly alerts alike. It checks the request as `createReceiver` does,
 * then reads the body with `parseAlert` (400 naming the refused field) and
 * hands `onAlert` each alert whose `id` its store has not seen.
 */
export function createAlertReceiver(options: AlertReceiverOptions): (request: Request) => Promise<Response> {
  const { onAlert } = options;
  const core = receiverCore("createAlertReceiver", options);
  if (typeof onAlert !== "function") throw new StreamdError("createAlertReceiver: onAlert must be a function");

  return async (request) => {
    const body = await core.body(request);
    if (body instanceof Response) return body;
    let alert: AlertPayload;
    try {
      alert = parseAlert(new TextDecoder("utf-8", { fatal: true }).decode(body));
    } catch (err) {
      core.report(err, request);
      return reply(400, err instanceof StreamdError ? err.message : "Cannot parse alert: not UTF-8");
    }
    return core.deliver([core.keyOf(alert.id)], () => onAlert(alert, request), request);
  };
}
