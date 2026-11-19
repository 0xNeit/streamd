export { StreamdClient, checkTimeout } from "./client.js";
export { CachingAdapter } from "./adapters/caching.js";
export { FallbackAdapter } from "./adapters/fallback.js";
export { ArchiveFallbackAdapter } from "./adapters/archive.js";
export { PoolAdapter } from "./adapters/pool.js";
export type { PoolAdapterOptions } from "./adapters/pool.js";
export { withHooks } from "./adapters/hooks.js";
export { adapterMetrics } from "./adapters/metrics.js";
export { withTracing } from "./adapters/tracing.js";
export { toPrometheus } from "./prometheus.js";
export { MemoryAdapter } from "./adapters/memory.js";
export { pageJson, parsePage, recordingAdapter, replayAdapter } from "./adapters/recording.js";
export { RoutingAdapter } from "./adapters/routing.js";
export { TabSharedAdapter } from "./adapters/tabs.js";
export { EventPoller } from "./poller.js";
export { EventQuery, withRetries } from "./query.js";
export { CursorManager, eventId } from "./cursor.js";
export { checkAdapter } from "./conformance.js";
export { compareAdapters } from "./compare.js";
export { defineEvent, defineEvents } from "./define.js";
export type { TaggedEvents } from "./define.js";
export { RequestLimiter, markDescending, markSelfTimed, supportsDescending } from "./limiter.js";
export { SharedFetches } from "./shared.js";
// Building blocks the chain packages use for their own client methods.
export { UNSIGNED_INTEGER, fieldDecoder } from "./decode.js";
export { descriptorKey, onAbort, raceAbort, redactSecrets, resolveFromVersion, sleep } from "./utils.js";
export { parseEvent, serializeEvent } from "./serialize.js";
export { STANDARD_COLUMNS, eventTableRows, requireEventTables } from "./tables.js";
export type { EventTableRow } from "./tables.js";
export { count, groupBy, latest, sum } from "./reducers.js";
export type { Fold } from "./reducers.js";
export { startReduction } from "./reduce.js";
export type { ReduceHost } from "./reduce.js";
export { WEBHOOK_SIGNATURE_HEADER, verifyWebhook } from "./webhook.js";
export { formatAmount, startBalances } from "./balances.js";
export type { BalanceReader, BalancesHost } from "./balances.js";
export { lazyResolve, ownedByPredicate } from "./ownership.js";
export type { OwnedByHost } from "./ownership.js";
export {
  committedEvents,
  committedTransaction,
  eventsAt,
  eventsOf,
  expectEvents,
  expectPreview,
  narrower,
  previewEvents,
  simulatedEvents,
  transactionExpectation,
} from "./transaction.js";
export {
  StreamdAssertionError,
  StreamdError,
  StreamdNetworkError,
  StreamdPollingError,
  StreamdTimeoutError,
  StreamdTransactionError,
  parseRetryAfter,
  toStreamdError,
} from "./errors.js";

export {
  every,
  filterEvents,
  fromFunction,
  fromSender,
  involvesAddress,
  normalizeAddress,
  not,
  ofModule,
  ofType,
  some,
  whereCompare,
  whereField,
  whereOneOf,
} from "./filter.js";
export type { CompareOp, OneOfValue } from "./filter.js";
export { changesOnly } from "./changes.js";

// Input checks for adapter authors; the chain packages build their adapters on them.
export {
  DEFAULT_LIMIT,
  handlePath,
  isObject,
  moduleTypeFilter,
  parseCursor,
  requireAddress,
  requireEventType,
  requireLedgerVersion,
  requireResourceType,
  requireTransactionHash,
  requireObjectRequest,
  requireViewRequest,
  validateOffset,
} from "./adapters/validate.js";

export type {
  AdapterCheckName,
  AdapterCheckOptions,
  AdapterCheckReport,
  AdapterCheckResult,
} from "./conformance.js";
export type {
  AdapterCompareRange,
  AdapterComparison,
  EventDifference,
  EventDifferenceField,
} from "./compare.js";
export type { CachingAdapterOptions, PageStore } from "./adapters/caching.js";
export type {
  BroadcastChannelLike,
  LockManagerLike,
  TabSharedAdapterOptions,
} from "./adapters/tabs.js";
export type {
  AdapterMethod,
  AdapterMetrics,
  AdapterMetricsSnapshot,
  LatencyPercentiles,
  MethodMetrics,
} from "./adapters/metrics.js";
export type { PrometheusOptions, PrometheusSource } from "./prometheus.js";
export type { SpanAttributes, SpanLike, TracerLike } from "./adapters/tracing.js";
export type {
  AdapterHooks,
  AdapterRequest,
  AdapterResponse,
} from "./adapters/hooks.js";
export type {
  RecordedCall,
  RecordedMethod,
  Recording,
  RecordingAdapter,
} from "./adapters/recording.js";
export type { RoutingAdapterRoutes } from "./adapters/routing.js";
export type { ParseEventOptions } from "./serialize.js";
export type {
  FieldPath,
  FieldValue,
  InvolvesAddressOptions,
} from "./filter.js";

export type {
  AccountEventDescriptor,
  AssetValue,
  BalanceChange,
  Balances,
  BalancesOptions,
  Transfer,
  BatchCallback,
  TransactionCallback,
  TransactionFilter,
  TransactionGroup,
  Watch,
  WatchCallback,
  WatchOptions,
  BlockHeights,
  BlockInfo,
  CreationNumberEventDescriptor,
  EntryFunctionCall,
  EventAdapter,
  EventCallback,
  EventDescriptor,
  EventFilter,
  EventGuid,
  EventPage,
  EventPredicate,
  EventColumn,
  EventColumnType,
  EventTable,
  SyncEventPredicate,
  EventStream,
  EventExpectation,
  EventsAtOptions,
  EventsOfOptions,
  ExpectEventsOptions,
  FetchOptions,
  HandleEventDescriptor,
  ListenerDefaults,
  ListenerOptions,
  ModuleEventDescriptor,
  MoveEventType,
  OnceOptions,
  ReduceOptions,
  Reducer,
  Reduction,
  Replay,
  ReplayOptions,
  ResourceDescriptor,
  ObjectDescriptor,
  CloseOptions,
  EventOf,
  DescriptorData,
  DescriptorEvent,
  TaggedEvent,
  TaggedModuleEventDescriptor,
  StreamdConfig,
  StreamdEvent,
  Subscription,
  SubscriptionOptions,
  ObjectRequest,
  StateRequest,
  StateRequestBuilder,
  NameOptions,
  SubscriptionError,
  SubscriptionInfo,
  SubscriptionStatus,
  ResourceChange,
  TransactionEvents,
  TransactionSimulation,
  ViewRequest,
} from "./types.js";
