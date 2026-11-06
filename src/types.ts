// Kept in the emitted declarations so consumers whose `lib` predates
// explicit resource management still have `Symbol.dispose` (TypeScript 5.2+).
/// <reference lib="esnext.disposable" preserve="true" />
import type { Aptos, AptosConfig } from "@aptos-labs/ts-sdk";

// ---- Event identification ----

/** Move struct tag identifying a module event: `${address}::${module}::${struct}` */
export type MoveEventType = `${string}::${string}::${string}`;

declare const eventData: unique symbol;

/**
 * Modern module event: one `${address}::${module}::${struct}` or a list of
 * them. `TData` is the payload type `defineEvent` attaches, so client methods
 * infer `event.data` from the descriptor.
 */
export interface ModuleEventDescriptor<TData = unknown> {
  kind: "module";
  /**
   * Type-only carrier for `TData`; never present at runtime. A method
   * signature so it compares bivariantly: a plain `EventDescriptor` still
   * accepts an explicit type argument, while unrelated payload types clash.
   */
  [eventData]?(data: TData): void;
  /**
   * One Move event type or a non-empty list of them (e.g.
   * `"0x1::coin::WithdrawEvent"` or
   * `["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::Deposit"]`),
   * served by a single transaction scan on `FullnodeScanAdapter`.
   */
  eventType: MoveEventType | readonly MoveEventType[];
}

/** Legacy EventHandle event, identified by account + handle + field */
export interface HandleEventDescriptor {
  kind: "handle";
  accountAddress: string;
  eventHandleStruct: string;
  fieldName: string;
  creationNumber?: never;
}

/**
 * Legacy EventHandle event, identified by the handle's guid: the account that
 * created the handle and its creation number (`EventGuid`). Reaches handles
 * the struct and field form cannot, such as one nested inside a struct field.
 */
export interface CreationNumberEventDescriptor {
  kind: "handle";
  accountAddress: string;
  creationNumber: bigint;
  eventHandleStruct?: never;
  fieldName?: never;
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

export type EventDescriptor<TData = unknown> =
  | ModuleEventDescriptor<TData>
  | HandleEventDescriptor
  | CreationNumberEventDescriptor
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
  /**
   * Hex hash of the transaction that emitted this event, when the event
   * source had the transaction in hand (`module` and `account` paths on
   * `FullnodeScanAdapter`). Omitted on `handle` events, whose endpoint returns
   * events without their transactions.
   */
  transactionHash?: string;
  /**
   * Transaction timestamp in microseconds since the Unix epoch (the fullnode's
   * `u64` `timestamp`), when the event source had the transaction in hand and
   * the transaction carries one. Omitted on `handle` events and on genesis
   * transactions.
   */
  timestamp?: bigint;
  /**
   * Address that signed the emitting transaction, as the source reports it,
   * when the source had the transaction in hand and it names one (user
   * transactions on the `module` and `account` paths of `FullnodeScanAdapter`).
   * Omitted on `handle` events and on system transactions.
   */
  sender?: string;
  /**
   * The `EventHandle` that emitted this event, when the source reports one.
   * Module events report the zero guid, which names no handle, so they carry
   * none.
   */
  guid?: EventGuid;
}

/** An `EventHandle`'s on-chain id: the account that created it and its creation number. */
export interface EventGuid {
  creationNumber: bigint;
  /** As the source reports it, not normalized. */
  accountAddress: string;
}

// ---- Filters ----

/**
 * Client-side test applied to each event after it is fetched.
 *
 * Used for anything the chain APIs can't filter on, most importantly fields
 * or addresses inside a module event's `data`. See `whereField`,
 * `involvesAddress`, `ofType`, `ofModule`, `every`, `some`, and `not`.
 */
export type EventPredicate = (event: StreamdEvent) => boolean;

export interface EventFilter {
  /**
   * Start from this version (inclusive). Negative counts back from the head,
   * read once from `EventAdapter.getHeadVersion`: `-1000n` starts 1000
   * versions before it, clamped at 0. `"earliest"` starts at the oldest
   * version the source still serves, read once from
   * `EventAdapter.getEarliestVersion`.
   */
  fromVersion?: bigint | "earliest";
  /**
   * End at this version (inclusive). `"head"` ends at the head as it stood
   * when the query started, read once from `EventAdapter.getHeadVersion`, so
   * a walk over a busy type ends instead of chasing new transactions.
   */
  toVersion?: bigint | "head";
  /**
   * Start at the first version of this block height (inclusive), read once
   * from `EventAdapter.getBlock`. Cannot be combined with `fromVersion`.
   */
  fromBlock?: bigint;
  /**
   * End at the last version of this block height (inclusive), read once from
   * `EventAdapter.getBlock`. Cannot be combined with `toVersion`.
   */
  toBlock?: bigint;
  /**
   * Start at the first block stamped at or after this time, in microseconds
   * since the Unix epoch (the unit of `StreamdEvent.timestamp`), searched once
   * through `EventAdapter.getBlockHeights` and `getBlock`. Before the oldest
   * served block clamps to it; in the future starts at the head. Cannot be
   * combined with `fromVersion` or `fromBlock`.
   */
  fromTime?: bigint;
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
   * Top-level `data` fields whose decimal strings (u64, u128, u256) become
   * `bigint`, before `where` runs. An event without the field is left as is;
   * one holding anything else rejects the query with a `StreamdError`.
   */
  decode?: readonly string[];
  /**
   * Stop after this many adapter pages, even if `limit` is unmet. Unbounded
   * when omitted. Use it to put a ceiling on a filtered walk.
   */
  maxPages?: number;
  /**
   * `"desc"` returns the last `limit` matches of the range, newest first.
   * Adapters only page forward, so the whole range is still walked: bound it
   * with `fromVersion` (a negative one works) or `maxPages`, and end it with
   * `toVersion: "head"` on a type that keeps emitting.
   */
  orderBy?: "asc" | "desc";
  /**
   * Retry a failed adapter call up to this many times (default 0), waiting
   * 1s, 2s, 4s... (capped at 60s) between attempts. Only failures worth
   * retrying are: a `StreamdError` that is not a `StreamdNetworkError` rejects
   * at once. A `StreamdNetworkError` carrying `retryAfter` waits that long
   * instead, capped at 60s. The query rejects with the last attempt's error.
   */
  retries?: number;
  /**
   * Aborting rejects `getEvents()` or the pending/next `getEventPages()` page
   * with the signal's `reason` and stops fetching further pages.
   */
  signal?: AbortSignal;
  /**
   * Called once when the walk finishes, with the cursor to pass as
   * `ListenerOptions.cursor` so a subscription on the same descriptor and
   * client resumes exactly where the query ended. `undefined` means there is
   * no exact resume point: no page was read, `limit` cut the last page short,
   * `toVersion` or `toBlock` bounded the walk (follow with `fromVersion` past
   * the bound instead), or the adapter gave no cursor. Not called when the
   * query rejects or a `getEventPages`/`iterateEvents` loop is left early.
   */
  onCursor?: (cursor: string | undefined) => void;
}

/**
 * What an adapter receives on each fetch.
 *
 * `where` is deliberately absent: predicates are functions, so they cannot be
 * pushed down to a remote data source. Query and poller apply them to results.
 */
export interface FetchOptions
  extends Omit<
    EventFilter,
    | "fromVersion"
    | "toVersion"
    | "fromBlock"
    | "fromTime"
    | "toBlock"
    | "where"
    | "decode"
    | "maxPages"
    | "retries"
    | "signal"
    | "onCursor"
  > {
  /** Start from this version (inclusive); always absolute by the time an adapter sees it. */
  fromVersion?: bigint;
  /** End at this version (inclusive); always absolute by the time an adapter sees it. */
  toVersion?: bigint;
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
  /**
   * True only when this exact request will always return this exact page,
   * which is what lets `CachingAdapter` reuse it. Only the adapter can say so,
   * since only it can read its cursor; leave it unset when unsure.
   */
  final?: boolean;
  /**
   * The highest ledger version the source has finished examining for this
   * descriptor: resuming from `nextCursor` returns nothing at or below it,
   * and nothing at or below it is missing from this or earlier pages of the
   * walk. Never decreases across a walk. Events on this same page may sit
   * above it, such as when a page ends partway through a transaction's
   * events. Leave it unset when the source cannot say.
   */
  throughVersion?: bigint;
}

// ---- Listener options ----

export interface ListenerOptions {
  /** Polling interval in ms (default: 1000) */
  pollInterval?: number;
  /** Maximum events to deliver per poll (default: 25) */
  batchSize?: number;
  /**
   * Start from this version; undefined = chain head. Negative counts back from
   * the head, read once on the first poll from `EventAdapter.getHeadVersion`:
   * `-1000n` starts 1000 versions before it, clamped at 0. `"earliest"` starts
   * at the oldest version the source still serves, read once on the first
   * poll from `EventAdapter.getEarliestVersion`.
   */
  fromVersion?: bigint | "earliest";
  /**
   * Start at the first block stamped at or after this time, in microseconds
   * since the Unix epoch, searched once on the first poll through
   * `EventAdapter.getBlockHeights` and `getBlock`. Before the oldest served
   * block clamps to it; in the future starts at the head. Cannot be combined
   * with `fromVersion`.
   */
  fromTime?: bigint;
  /**
   * Resume from a token a previous subscription reported as `cursor`, or one a
   * query passed to `EventFilter.onCursor`. It wins over `fromVersion` and
   * `fromTime`, and is only meaningful with the same descriptor and adapter
   * that produced it.
   */
  cursor?: string;
  /**
   * Max consecutive errors before auto-stopping (default: 10). An adapter
   * error that is a `StreamdError` but not a `StreamdNetworkError` is invalid
   * input and stops the subscription on the first occurrence.
   */
  maxErrors?: number;
  /**
   * Maximum exponential backoff delay in ms between failed polls (default:
   * 60000, valid range 0..2147483647). A `StreamdNetworkError` carrying
   * `retryAfter` waits that long instead, capped at this value, and still
   * counts toward `maxErrors`.
   */
  maxBackoff?: number;
  /**
   * Sliding-window capacity for deduplicating seen events by
   * `(version, eventIndex)` (default: 1000). Set to `0` to disable client-side
   * deduplication.
   */
  dedupWindow?: number;
  /**
   * Deliver only events satisfying this predicate. Applied client-side after
   * fetching. See `whereField`, `involvesAddress`, `every`, `some`, and `not`.
   */
  where?: EventPredicate;
  /**
   * Top-level `data` fields whose decimal strings (u64, u128, u256) become
   * `bigint`, before `where` runs. An event without the field is left as is;
   * one holding anything else is skipped and reported through `onError`,
   * without counting toward `maxErrors`.
   */
  decode?: readonly string[];
  /**
   * Receives polling failures and errors thrown by `where` or the callback,
   * overriding `StreamdConfig.onError` for this subscription. A throw from it
   * does not stop polling: it is rethrown on a microtask and surfaces as an
   * uncaught error.
   */
  onError?: (error: Error) => void;
  /**
   * Called once, the first time a poll delivers its whole page and the adapter
   * reports nothing more waiting (`hasMore: false`). Not awaited; a throw goes
   * to `onError` and does not count toward `maxErrors`.
   */
  onCaughtUp?: () => void;
  /**
   * Aborting stops the subscription as `unsubscribe()` does, without calling
   * `onError`. Already aborted, it never polls. `once()` and `stream()` also
   * reject with the signal's `reason`.
   */
  signal?: AbortSignal;
}

/** Options for `on()` and `onBatch()`, the calls that return a `Subscription`. */
export interface SubscriptionOptions extends ListenerOptions {
  /**
   * The subscription's `id` instead of a generated one: a non-empty string no
   * other live subscription on this client holds. Free again once the
   * subscription ends.
   */
  id?: string;
}

export interface OnceOptions extends ListenerOptions {
  /**
   * Milliseconds to wait before unsubscribing and rejecting with
   * `StreamdTimeoutError` (0 to 2147483647).
   */
  timeout?: number;
}

/** Folds one event into the state; synchronous, and should not mutate `state`. */
export type Reducer<S, TData = unknown> = (state: S, event: StreamdEvent<TData>) => S;

/**
 * Options for `reduce()`. History options shape the backfill, live options
 * the subscription that follows it, and `where`/`decode` apply to both.
 * Anything that would leave no exact point to follow from (`limit`,
 * `offset`, `toVersion`, `toBlock`, `orderBy`, `onCursor`, `cursor`) is
 * refused.
 */
export interface ReduceOptions<S = unknown, TData = unknown>
  extends Pick<EventFilter, "fromBlock" | "maxPages" | "retries">,
    Pick<
      ListenerOptions,
      | "fromVersion"
      | "fromTime"
      | "where"
      | "decode"
      | "pollInterval"
      | "batchSize"
      | "maxErrors"
      | "maxBackoff"
      | "dedupWindow"
      | "onError"
      | "onCaughtUp"
      | "signal"
    > {
  /**
   * Called after each live event is folded in, with the new state. Not called
   * during the backfill, whose result `ready` resolves with. A throw goes to
   * `onError` and does not stop the reduction.
   */
  onChange?: (state: S, event: StreamdEvent<TData>) => void;
}

/** A running `reduce()`: the reduced value, kept current as events arrive. */
export interface Reduction<S> {
  /** The reduction of every event folded so far; the last good one once stopped. */
  readonly value: S;
  /**
   * Resolves with the backfilled value once the history is folded and live
   * following has started. Rejects if the backfill fails, the reducer throws,
   * or the reduction is stopped or aborted first. Never an unhandled
   * rejection, so it is safe to ignore.
   */
  readonly ready: Promise<S>;
  /** False once stopped: unsubscribed, aborted, failed, or polling gave up. */
  readonly active: boolean;
  /** Stop folding, whether still backfilling or following. */
  unsubscribe(): void;
  /** `unsubscribe()`, for `using`. */
  [Symbol.dispose](): void;
}

/**
 * Listener options a client can set once for every `on`, `onBatch`, `once` and
 * `stream` call. Start position, filters, callbacks and `signal` are per
 * subscription; `StreamdConfig.onError` is the client-wide error handler.
 */
export type ListenerDefaults = Pick<
  ListenerOptions,
  "pollInterval" | "batchSize" | "maxErrors" | "maxBackoff" | "dedupWindow" | "decode"
>;

// ---- Client config ----

export interface StreamdConfig {
  /**
   * An existing Aptos SDK instance, or AptosConfig to create one. Required
   * when `adapter` is omitted; optional when a custom `adapter` (such as
   * `MemoryAdapter`) is supplied.
   */
  aptos?: Aptos | AptosConfig;
  /** Default polling interval in ms (default: 1000) */
  defaultPollInterval?: number;
  /**
   * Fallback error handler for subscriptions (`on`, `once`, `stream`) that do
   * not set their own `options.onError`. Receives polling failures and errors
   * thrown by `where` or the callback; a throw from it is rethrown on a
   * microtask without stopping polling.
   */
  onError?: (error: Error) => void;
  /**
   * Event source. Defaults to `FullnodeScanAdapter`, which reads from a
   * fullnode and therefore only sees unpruned recent history. Supply
   * `MemoryAdapter` to test handlers offline, compose sources with
   * `RoutingAdapter` and `FallbackAdapter`, or implement `EventAdapter` to
   * query a self-hosted processor or third-party indexer (see `EventAdapter`
   * docs for the contract).
   */
  adapter?: EventAdapter;
  /**
   * Most adapter calls (`fetchEvents`, `getHeadVersion`, `getEarliestVersion`, `getBlock`, `getBlockHeights`) this client has in
   * flight at once, across all subscriptions and queries; a positive integer.
   * Further calls wait in order. Omit for no limit.
   */
  maxConcurrentRequests?: number;
  /**
   * Listener options for every subscription this client creates. A call's own
   * value for a field wins; one it leaves `undefined` takes this one. A call's
   * `decode` replaces this list rather than adding to it. `pollInterval` here
   * wins over `defaultPollInterval`. Checked when the client is constructed.
   */
  defaults?: ListenerDefaults;
}

// ---- Subscription handle ----

export interface SubscriptionStatus {
  /** Whether the subscription is currently active. */
  readonly active: boolean;
  /**
   * The adapter's opaque resume token after the last finished page, or
   * `undefined` before the first page finishes (unless seeded via
   * `ListenerOptions.cursor`).
   */
  readonly cursor: string | undefined;
  /**
   * Highest `event.version` seen across events processed by this subscription,
   * or `undefined` before any event has been seen.
   */
  readonly lastVersion: bigint | undefined;
  /**
   * Consecutive polling failures since the subscription started or last
   * finished a poll. Resets to `0` after a poll finishes its page.
   */
  readonly consecutiveErrors: number;
  /**
   * Epoch timestamp in milliseconds (`Date.now()`) when the subscription last
   * finished a poll without a polling error, or `undefined` before the first
   * successful poll.
   */
  readonly lastSuccessAt: number | undefined;
}

export interface Subscription {
  /** Unique ID for this subscription */
  id: string;
  /** Stop listening and clean up */
  unsubscribe(): void;
  /** `unsubscribe()`, so `using sub = client.on(...)` stops it at scope exit. */
  [Symbol.dispose](): void;
  /**
   * Stop polling without ending the subscription; `resume()` carries on from
   * the same cursor and dedup window. No-op unless active.
   */
  pause(): void;
  /** Undo `pause()`. No-op unless paused: it never revives an ended subscription. */
  resume(): void;
  /** Whether the subscription is polling; `false` while paused */
  readonly active: boolean;
  /** Whether the subscription is paused; `false` once it has ended */
  readonly paused: boolean;
  /**
   * Where to resume: pass it back as `ListenerOptions.cursor` after a restart.
   * Undefined before the first page; still readable after unsubscribing.
   */
  readonly cursor: string | undefined;
  /**
   * Snapshot of the subscription's polling state (`active`, `cursor`,
   * `lastVersion`, `consecutiveErrors`, `lastSuccessAt`). Still readable after
   * unsubscribing or stopping.
   */
  readonly status: SubscriptionStatus;
}

// ---- Callback ----

/**
 * Called once per delivered event, in order. A returned promise is awaited
 * before the next event and before the cursor moves past the page, so a slow
 * handler slows polling down. A rejection is reported through `onError` and
 * delivery carries on, as for a synchronous throw. A handler that never
 * settles stops delivery for good, with nothing reported; `unsubscribe()`
 * still works.
 *
 * Typed `void` rather than `void | Promise<void>` on purpose: `void` accepts
 * async functions and also expression bodies such as `(e) => list.push(e)`,
 * which the union would reject.
 */
export type EventCallback<TData = unknown> = (
  event: StreamdEvent<TData>,
) => void;

/**
 * Called once per poll with that page's new events that pass `where`, in
 * order; never with an empty array. Otherwise as `EventCallback`: a returned
 * promise is awaited before the cursor moves past the page, and a throw or
 * rejection goes to `onError` without counting toward `maxErrors`.
 */
export type BatchCallback<TData = unknown> = (
  events: StreamdEvent<TData>[],
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
  /**
   * The latest version this source can serve for `descriptor`. Optional; a
   * negative `fromVersion` (relative to the head) needs it.
   */
  getHeadVersion?(descriptor: EventDescriptor): Promise<bigint>;
  /**
   * The oldest version this source can still serve for `descriptor`, below
   * which history is pruned. Optional; `fromVersion: "earliest"` needs it.
   */
  getEarliestVersion?(descriptor: EventDescriptor): Promise<bigint>;
  /**
   * The block at `height` on the chain serving `descriptor`. Optional;
   * `fromBlock`/`toBlock` on a query need it.
   */
  getBlock?(descriptor: EventDescriptor, height: bigint): Promise<BlockInfo>;
  /**
   * The oldest and newest block heights this source can serve for
   * `descriptor`. Optional; `fromTime` needs it, together with `getBlock`.
   */
  getBlockHeights?(descriptor: EventDescriptor): Promise<BlockHeights>;
}

/** Block heights an adapter can serve, both inclusive, as `EventAdapter.getBlockHeights` reports them. */
export interface BlockHeights {
  oldest: bigint;
  newest: bigint;
}

/** A block's position in the ledger, as `EventAdapter.getBlock` reports it. */
export interface BlockInfo {
  height: bigint;
  /** First transaction version in the block (inclusive). */
  firstVersion: bigint;
  /** Last transaction version in the block (inclusive). */
  lastVersion: bigint;
  /** Block time in microseconds since the Unix epoch. */
  timestamp: bigint;
}
