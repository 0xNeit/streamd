# @streamd/core

Chain-neutral event subscription and historical query engine for `streamd`.

## What it provides

- `StreamdClient` for live polling (`on`, `onBatch`, `once`, `stream`), historical paging (`getEvents`, `getEventPages`, `iterateEvents`), reductions (`reduce`), transaction grouping (`onTransaction`, `getTransactions`), watch lists (`watch`), and replay (`replay`).
- Adapters and wrappers (`MemoryAdapter`, `RoutingAdapter`, `FallbackAdapter`, `ArchiveFallbackAdapter`, `PoolAdapter`, `CachingAdapter`, `TabSharedAdapter`, `withHooks`, `adapterMetrics`, `withTracing`, `recordingAdapter`, `replayAdapter`).
- Client-side predicates (`whereField`, `whereOneOf`, `whereCompare`, `ofType`, `ofModule`, `fromSender`, `fromFunction`, `involvesAddress`, `changesOnly`, `every`, `some`, `not`), `fieldDecoder`, `serializeEvent`/`parseEvent`, and `verifyWebhook`.

For chain-specific clients (`AptosClient`, `SuiClient`), stores, and full documentation, see the [streamd documentation](https://github.com/0xNeit/streamd#readme).
