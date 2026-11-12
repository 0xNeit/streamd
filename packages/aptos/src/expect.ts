import type { EventAdapter, EventExpectation } from "@streamd/core";
import type { ExpectPreviewOptions } from "./types.js";
import type { RequestLimiter } from "@streamd/core";
import { transactionExpectation } from "@streamd/core";
import { simulatedEvents } from "./transaction.js";

/** The assertions behind `AptosClient.expectPreview`; each one simulates the transaction. */
export function expectPreview(
  adapter: EventAdapter,
  limiter: RequestLimiter,
  transaction: unknown,
  options: ExpectPreviewOptions,
): EventExpectation {
  return transactionExpectation(
    "the simulated transaction",
    undefined,
    (descriptor) => simulatedEvents(adapter, limiter, transaction, descriptor, options),
    options,
  );
}
