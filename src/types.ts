import type { Aptos, AptosConfig } from "@aptos-labs/ts-sdk";

// ---- Event identification ----

/** Modern module event: `${address}::${module}::${struct}` */
export interface ModuleEventDescriptor {
  kind: "module";
  /** Full type string, e.g. "0x1::coin::WithdrawEvent" */
  eventType: `${string}::${string}::${string}`;
}

/** Legacy EventHandle event, identified by account + handle + field */
export interface HandleEventDescriptor {
  kind: "handle";
  accountAddress: string;
  eventHandleStruct: string;
  fieldName: string;
}

/**
 * Every event emitted by transactions the account *sent*.
 *
 * Scoped to one account's history rather than the whole chain, so it stays
 * cheap over deep history. Note the asymmetry: this sees only transactions
 * this account signed. Events where the account is merely a recipient live in
 * the *sender's* transaction and will not appear here — see the class docs on
 * `FullnodeScanAdapter`.
 */
export interface AccountEventDescriptor {
  kind: "account";
  /** Account whose transaction history is walked. */
  accountAddress: string;
  /** Restrict to these Move event types; omit for every event. */
  eventTypes?: string[];
}

export type EventDescriptor =
  | ModuleEventDescriptor
  | HandleEventDescriptor
  | AccountEventDescriptor;

// ---- Parsed event ----

export interface StreamdEvent<TData = unknown> {
  /** The Move type string */
  type: string;
  /** Decoded event data */
  data: TData;
  /** Transaction version that emitted this event */
  version: bigint;
  /**
   * Sequence number. Meaningful for legacy handle events; module events
   * frequently report 0, so it is not a unique key on its own.
   */
  sequenceNumber: bigint;
  /**
   * Position of this event within its transaction. Combined with `version`
   * this uniquely identifies an event, which `sequenceNumber` alone does not.
   *
   * Handle events are the exception: their endpoint omits the position, so
   * `FullnodeScanAdapter` sets this to the handle's sequence number instead,
   * which keeps the pair unique and stable across pages.
   */
  eventIndex: number;
}

// ---- Filters ----

/**
 * Client-side test applied to each event after it is fetched.
 *
 * Used for anything the chain APIs can't filter on — most importantly the
 * account inside a module event's `data`. See `involvesAddress`.
 */
export type EventPredicate = (event: StreamdEvent) => boolean;

export interface EventFilter {
  /** Start from this version (inclusive) */
  fromVersion?: bigint;
  /** End at this version (inclusive) */
  toVersion?: bigint;
  /** Maximum events to return, counted *after* `where` is applied */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
  /**
   * Keep only events satisfying this predicate. Applied client-side after
   * fetching, so it narrows results but not the work done to find them.
   *
   * Because `limit` counts matches, a restrictive predicate keeps the walk
   * going until it fills — potentially across an account's whole history.
   * Pair it with `maxPages` when you would rather cap the work than wait.
   */
  where?: EventPredicate;
  /**
   * Stop after this many adapter pages, even if `limit` is unmet. Unbounded
   * when omitted. Use it to put a ceiling on a filtered walk.
   */
  maxPages?: number;
}

/**
 * What an adapter receives on each fetch.
 *
 * `where` is deliberately absent: predicates are functions, so they cannot be
 * pushed down to a remote data source. Query and poller apply them to results.
 */
export interface FetchOptions extends Omit<EventFilter, "where" | "maxPages"> {
  orderBy?: "asc" | "desc";
  /**
   * Opaque resume token returned by this adapter's previous page. When
   * present it takes precedence over `fromVersion`.
   */
  cursor?: string;
}

/**
 * One page of results.
 *
 * `nextCursor` is what makes streaming correct: it reports how far the
 * adapter actually advanced, which is not the same as where the last
 * matching event was. A page that matched nothing still moves the cursor,
 * so a subscription cannot stall re-reading the same window.
 */
export interface EventPage {
  events: StreamdEvent[];
  /** Opaque, adapter-defined position to resume from. */
  nextCursor?: string;
  /** True when more data is immediately available (drives catch-up polling). */
  hasMore: boolean;
}

// ---- Listener options ----

export interface ListenerOptions {
  /** Polling interval in ms (default: 1000) */
  pollInterval?: number;
  /** Maximum events to deliver per poll (default: 25) */
  batchSize?: number;
  /** Start from this version; undefined = chain head */
  fromVersion?: bigint;
  /**
   * Resume from a token a previous subscription reported as `cursor`. It wins
   * over `fromVersion`, and is only meaningful with the same descriptor and
   * adapter that produced it.
   */
  cursor?: string;
  /**
   * Max consecutive errors before auto-stopping (default: 10). An adapter
   * error that is a `StreamdError` but not a `StreamdNetworkError` is invalid
   * input and stops the subscription on the first occurrence.
   */
  maxErrors?: number;
  /**
   * Deliver only events satisfying this predicate. Applied client-side after
   * fetching. See `involvesAddress`.
   */
  where?: EventPredicate;
  /**
   * Receives polling failures and errors thrown by `where` or the callback.
   * A throw from it does not stop polling: it is rethrown on a microtask and
   * surfaces as an uncaught error.
   */
  onError?: (error: Error) => void;
}

// ---- Client config ----

export interface StreamdConfig {
  /** An existing Aptos SDK instance, or AptosConfig to create one */
  aptos: Aptos | AptosConfig;
  /** Default polling interval in ms (default: 1000) */
  defaultPollInterval?: number;
  /**
   * Event source. Defaults to `FullnodeScanAdapter`, which reads from a
   * fullnode and therefore only sees unpruned recent history. Supply your
   * own adapter to query a self-hosted processor or third-party indexer —
   * see the `EventAdapter` docs for the contract.
   */
  adapter?: EventAdapter;
}

// ---- Subscription handle ----

export interface Subscription {
  /** Unique ID for this subscription */
  id: string;
  /** Stop listening and clean up */
  unsubscribe(): void;
  /** Whether the subscription is active */
  readonly active: boolean;
}

// ---- Callback ----

/**
 * Called once per delivered event. It is not awaited: an async callback's
 * rejection is reported through `onError`, but the next event does not wait.
 */
export type EventCallback<TData = unknown> = (
  event: StreamdEvent<TData>,
) => void;

// ---- Adapter interface ----

/**
 * The extension point for event sources.
 *
 * Aptos has no hosted API for querying module events by type — the Indexer's
 * `events` table was removed — so anything needing deep history must bring
 * its own index. Implement this interface against your data source and pass
 * it as `StreamdConfig.adapter`.
 *
 * Implementations must:
 *  - return events in ascending `version` order when `orderBy` is `"asc"`;
 *  - honour `cursor` in preference to `fromVersion` when both are present;
 *  - return a `nextCursor` that advances even when `events` is empty, so
 *    callers can page past regions containing no matches;
 *  - set `hasMore` when another page is immediately available.
 *
 * Throw `StreamdNetworkError` for failures worth retrying. Any other
 * `StreamdError` means the request itself is wrong, and a live subscription
 * stops on it rather than retrying.
 */
export interface EventAdapter {
  fetchEvents(
    descriptor: EventDescriptor,
    options: FetchOptions,
  ): Promise<EventPage>;
}
