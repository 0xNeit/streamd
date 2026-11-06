# streamd

Event subscriptions and historical queries for blockchains, starting with Aptos.

Subscribe to events as they land, or query them historically, with cursors,
deduplication, backoff and client-side filtering handled for you.

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "streamd";

const streamd = new StreamdClient({
  aptos: new Aptos(new AptosConfig({ network: Network.MAINNET })),
});

streamd.on(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  (event) => console.log(event.version, event.transactionHash, event.data),
);
```

## Install

Not yet published to npm. To use it now, build from source:

```sh
git clone https://github.com/0xNeit/streamd.git
cd streamd
bun install
bun run build
```

`@aptos-labs/ts-sdk` (>= 1.0.0) is a peer dependency. The package ships ESM,
CommonJS and TypeScript types.

## Usage

### Listen for new events

```ts
const sub = streamd.on(
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  (event) => console.log(event),
  {
    pollInterval: 3000,  // ms between polls (default 1000)
    batchSize: 25,       // events fetched per poll (default 25)
    maxErrors: 10,       // consecutive failures before stopping (default 10)
    maxBackoff: 60_000,  // max backoff delay in ms on failures (default 60000)
    dedupWindow: 1000,   // seen-event dedup window size, 0 to disable (default 1000)
    onError: (err) => console.error(err),
  },
);

console.log(sub.status);
// { active, cursor, lastVersion, consecutiveErrors, lastSuccessAt }

sub.unsubscribe();
```

`sub.status` returns a snapshot of the subscription's polling state: `active`
(`boolean`), `cursor` (`string | undefined`, after the last finished page),
`lastVersion` (`bigint | undefined`, highest `event.version` seen so far),
`consecutiveErrors` (`number`, reset to `0` after each successful poll), and
`lastSuccessAt` (`number | undefined`, `Date.now()` in ms when the last poll
finished). Like `sub.cursor`, `sub.status` stays readable after
`unsubscribe()`, `destroy()`, abort, or auto-stop.

Handlers may be `async`: each one is awaited before the next event, and polling waits
for the page to finish, so a slow handler slows the subscription down instead of piling
up work. A rejected handler is reported through `onError` and the next event is still
delivered. A handler that never settles stops delivery with nothing reported, so give
long-running work its own timeout; `unsubscribe()` still works.

Listening polls under the hood. When the source reports more data is waiting,
the poller fetches again immediately instead of waiting for the next interval,
deduplicating seen `(version, eventIndex)` pairs across a sliding window of
`dedupWindow` events (default `1000`, or `0` to disable client-side
deduplication). On errors it backs off exponentially up to `maxBackoff` ms
(default `60000`), and stops after `maxErrors` failures in a row. Invalid
input, such as a malformed address or cursor, stops it at once, since retrying
cannot fix it. Pass `onError` to `new StreamdClient({ aptos, onError })` to set
a client-wide fallback for any `on()`, `once()`, or `stream()` call that omits
its own `onError`.

### Pause and resume

```ts
sub.pause();   // stop polling; the subscription stays open
sub.paused;    // true
sub.resume();  // poll again at once, from the same cursor
```

Pausing keeps the cursor, the dedup window, and the error count, so `resume()` picks up
with the page it held and skips events already delivered. A handler already running when
you pause finishes, the rest of its page waits for `resume()`, and a fetch in flight is
discarded. While paused, `active` (and `sub.status.active`) is `false`. `unsubscribe()`,
`destroy()`, or the `signal` aborting still ends a paused subscription, and `resume()` never
revives one that has ended, including after `maxErrors`.

### Caught-up signal

```ts
streamd.on(descriptor, handle, {
  fromVersion: 3_000_000_000n,
  onCaughtUp: () => console.log("backfill done, now live"),
});
```

`onCaughtUp` is called once, the first time a poll delivers its whole page and the adapter
reports nothing more waiting. Started from history, that is after the backfill; started at
the head, after the first successful poll. It is not called again, even after a pause and
resume. A throw from it goes to `onError` without counting toward `maxErrors`. `once()` and
`stream()` accept it too.

### Batch delivery

```ts
const sub = streamd.onBatch(descriptor, async (events) => {
  await db.insertMany(events); // one write per poll
}, { batchSize: 100 });
```

`onBatch` takes the same options as `on()` and returns the same `Subscription`, but calls
your handler once per poll with that page's new events that pass `where`, in order, at most
`batchSize` of them. A page with nothing new is skipped rather than delivered as `[]`. The
handler is awaited before the cursor moves past the page, so a crash or a stop mid-batch
redelivers the whole page on resume. A throw or rejection goes to `onError` without counting
toward `maxErrors`, and polling moves on to the next page.

### Iterate live events

```ts
for await (const event of streamd.stream(descriptor, { where: involvesAddress(wallet) })) {
  await handle(event);
  if (done) break; // leaving the loop unsubscribes
}
```

`stream()` takes the same options as `on()`. Nothing is fetched ahead of your loop: the
next poll waits until the body asks for the following event, so a slow body slows the
subscription down. The loop throws if polling gives up (after `maxErrors`, or at once on
invalid input); other errors go to `onError`. `destroy()` ends any open loop. One loop
per stream: the stream subscribes when the loop first asks for an event.

### Resume after a restart

Save a subscription's `cursor` when you stop, and pass it back to `on()` to pick up
where you left off instead of at the chain head:

```ts
process.on("SIGTERM", async () => {
  streamd.destroy();
  await saveCursor(sub.cursor); // undefined if no page was fetched yet
});

const saved = await loadCursor();
const sub = streamd.on(descriptor, handle, { cursor: saved });
```

The cursor is the adapter's own token, so it only works with the same descriptor and
adapter that produced it, and it wins over `fromVersion`. It moves only once every handler
on a page has settled, and the dedup window is not saved, so events from the page in
flight when the process stopped arrive again after a restart: make your handler
idempotent on `(version, eventIndex)`.

### Start relative to the head

```ts
// the last 10,000 versions, then live
streamd.on(descriptor, handle, { fromVersion: -10_000n });

// history from 1,000 versions back
const recent = await streamd.getEvents(descriptor, { fromVersion: -1_000n });
```

A negative `fromVersion` counts back from the chain head, as a negative block tag does in
ethers: `-10_000n` starts at `head - 10_000`, and never below version 0. The head is read
once, from the adapter's `getHeadVersion`, when the subscription first polls or the query
fetches its first page; a `cursor` still wins and skips that read. `once()`, `stream()`
and `onBatch()` accept it too. An adapter without `getHeadVersion` is rejected as invalid
input.

### Start at the earliest available version

```ts
// everything the fullnode still holds, then live
streamd.on(descriptor, handle, { fromVersion: "earliest" });

const all = await streamd.getEvents(descriptor, { fromVersion: "earliest", maxPages: 100 });
```

`fromVersion: "earliest"` starts at the oldest version the source can still serve. On
`FullnodeScanAdapter` that is the ledger's `oldest_ledger_version`: history below it is
pruned. It is read once, from the adapter's `getEarliestVersion`, at the same point a
relative start reads the head, and a `cursor` still wins and skips the read. `once()`,
`stream()` and `onBatch()` accept it too. An adapter without `getEarliestVersion` is
rejected as invalid input, and so is any `fromVersion` that is neither a `bigint` nor
`"earliest"`. A module-event scan from the earliest version walks every transaction since,
so bound a query with `limit` or `maxPages`.

### Start at a time

```ts
// microseconds since the Unix epoch, the unit of event.timestamp
const oneHourAgo = BigInt(Date.now() - 3_600_000) * 1000n;

streamd.on(descriptor, handle, { fromTime: oneHourAgo });
const lastHour = await streamd.getEvents(descriptor, { fromTime: oneHourAgo, maxPages: 50 });
```

`fromTime` starts at the first block stamped at or after that time. It is resolved to a
version once, when a query starts or on a subscription's first poll, by searching blocks
through the adapter's `getBlockHeights` and `getBlock`: on mainnet that is about 9 to 13
block lookups, which counts against the public fullnode's anonymous rate limit. A time
before the oldest unpruned block starts at that block; a time in the future starts at the
head. It cannot be combined with `fromVersion` or `fromBlock`, and a `cursor` still wins
and skips the search. A failed lookup is retried under a query's `retries`, and on a
subscription it is a polling error like any other, so the next poll searches again.
`once()`, `stream()` and `onBatch()` accept it too.

### Wait for one event

```ts
const event = await streamd.once(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  { timeout: 10_000 }, // optional, ms
);
```

The promise rejects with `StreamdTimeoutError` if `timeout` elapses first, if
polling gives up after `maxErrors`, or straight away on invalid input.

### Query history

```ts
const events = await streamd.getEvents(
  {
    kind: "account",
    accountAddress: "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d",
  },
  { fromVersion: 1_000_000n, limit: 50 },
);

// Or page through large result sets
for await (const page of streamd.getEventPages(descriptor, { limit: 1000 }, 100)) {
  handle(page);
}

// Or one event at a time
for await (const event of streamd.iterateEvents(descriptor, { fromVersion: 1_000_000n })) {
  handle(event);
  if (done) break; // nothing further is fetched
}
```

`iterateEvents` takes the same arguments as `getEventPages` and yields the same events,
one per step. A page is fetched only once the previous one is used up, and leaving the loop
stops the walk. Aborting `signal` rejects the next event, even mid-page.

For the most recent events first, pass `orderBy: "desc"`:

```ts
const latest = await streamd.getEvents(descriptor, {
  fromVersion: -100_000n,
  orderBy: "desc",
  limit: 10,
});
```

This returns the last `limit` matches of the range, newest first. Adapters only page
forward, so the whole range is still walked: bound it with `fromVersion` or `maxPages`.
`getEventPages` yields the same events in `pageSize` chunks once the walk ends.

To ride out a flaky node, pass `retries`:

```ts
const events = await streamd.getEvents(descriptor, { fromVersion: 1_000_000n, retries: 3 });
```

A failed page is retried up to `retries` times, waiting 1s, 2s, 4s and so on (capped at
60s) between attempts, then the query rejects with the last error. Only network failures
are retried: any other `StreamdError` means the request itself is wrong and rejects at once.
`signal` cancels a wait.

### Query by block range

```ts
const inBlocks = await streamd.getEvents(descriptor, {
  fromBlock: 1_141_400_000n,
  toBlock: 1_141_400_010n,
});
```

`fromBlock` and `toBlock` bound a query by block height, both inclusive: the query runs
from the first version of `fromBlock` to the last version of `toBlock`. Each is looked up
once, from the adapter's `getBlock`, before the first page, and `retries` covers the
lookups too. Either may be used alone, alongside the other side's version bound
(`fromBlock` with `toVersion`, or `fromVersion` with `toBlock`). Setting both `fromBlock`
and `fromVersion`, or both `toBlock` and `toVersion`, is rejected as invalid input, as is a
negative height or a `fromBlock` above `toBlock`. `orderBy: "desc"` and `iterateEvents`
work over the resulting range. `FullnodeScanAdapter` reads blocks from the fullnode, so a
pruned block fails with a `StreamdNetworkError` (status 410). Listeners take versions only.

> [!NOTE]
> Renamed in 0.x: `getLogs` is now `getEvents` and `getLogsIterator` is now
> `getEventPages`. Arguments and results are unchanged.

### Cancel with AbortSignal

Every listener and query takes a standard `signal`:

```ts
const controller = new AbortController();

streamd.on(descriptor, handle, { signal: controller.signal });
const events = await streamd.getEvents(descriptor, {
  fromVersion: 1_000_000n,
  signal: AbortSignal.timeout(5000),
});

controller.abort();
```

Aborting `on()` unsubscribes quietly without calling `onError`. Aborting
`once()`, `stream()`, `getEvents()`, `getEventPages()` or `iterateEvents()` stops polling or
paging and rejects with `signal.reason`. An already-aborted signal never
fetches.

### Limit concurrent requests

```ts
const streamd = new StreamdClient({ aptos, maxConcurrentRequests: 4 });
```

At most that many adapter calls run at once across every subscription and query on the
client; the rest wait their turn in order. `fetchEvents`, the `getHeadVersion` read behind
a negative `fromVersion`, the `getEarliestVersion` read behind `"earliest"`, and the
`getBlock` and `getBlockHeights` lookups behind `fromBlock`/`toBlock` and `fromTime` all count. The unit is one adapter call, not one HTTP request:
a module-event scan on `FullnodeScanAdapter` may make several requests per call. A call
still waiting when its subscription stops or pauses, or its query's `signal` aborts, is
dropped without reaching the adapter. Omit the option for no limit.

### Manage listeners

```ts
streamd.on(descriptor, handle);
streamd.listenerCount(descriptor);   // 1
streamd.off(descriptor, handle);     // stop that one listener
streamd.removeAllListeners();        // stop every on() and onBatch() subscription
```

As in ethers, these work by descriptor and callback, so you need not keep every
`Subscription`. Descriptors match by value: `"T"` and `["T"]` are the same module
descriptor, list order does not matter, and `0x1` equals its long form. `off(descriptor,
callback)` stops the earliest matching subscription; without a callback it stops all on that
descriptor, as `removeAllListeners(descriptor)` does. `listenerCount()` counts paused
subscriptions but not ended ones. `once()` and `stream()` are not listeners here: they end
with their promise or loop, or `destroy()`.

### Clean up

```ts
streamd.destroy(); // stops every active subscription
```

## Choosing events

Every call takes an event descriptor:

| `kind` | Matches | Example |
|---|---|---|
| `module` | Module events of one or more Move types | `{ kind: "module", eventType: "0x1::coin::WithdrawEvent" }` or `{ kind: "module", eventType: ["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::Deposit"] }` |
| `handle` | A legacy `EventHandle` on an account | `{ kind: "handle", accountAddress, eventHandleStruct, fieldName }` |
| `account` | Events from transactions an account **sent** | `{ kind: "account", accountAddress, eventTypes?: [...] }` |

When `module.eventType` is an array of Move types, `FullnodeScanAdapter` serves
all of them in a single transaction scan per page, delivering matches in
`(version, eventIndex)` order.

`account` only sees transactions the account signed. If someone else's
transaction deposits into the account, that event belongs to the sender's
transaction and won't appear.

Each delivered `StreamdEvent` carries `type`, `data`, `version` (`bigint`),
`sequenceNumber` (`bigint`), and `eventIndex` (`number`). Where the source
transaction is in hand (`module` and `account` descriptors on
`FullnodeScanAdapter`), events also carry `transactionHash` (`string`),
`timestamp` (`bigint`, microseconds since the Unix epoch), and `sender`
(`string`, the address that signed the transaction). `handle` events come from
an endpoint that omits the surrounding transaction, so all three are
`undefined` there. `timestamp` is `undefined` on genesis transactions, and
`sender` on system transactions such as block metadata, which no account signs.

### Discover event handles

`listEventHandles(aptos, address)` reads an account's resources and returns
each `EventHandle` it finds as a ready-to-use `handle` descriptor, with the
number of events emitted on it so far:

```ts
import { Aptos } from "@aptos-labs/ts-sdk";
import { listEventHandles } from "streamd";

const handles = await listEventHandles(new Aptos(), "0x1");
// [{ descriptor: { kind: "handle", accountAddress, eventHandleStruct: "0x1::block::BlockResource", fieldName: "new_block_events" }, count: 1165098930n }, ...]

const blocks = handles.find((h) => h.descriptor.fieldName === "new_block_events");
if (blocks) client.on(blocks.descriptor, (event) => console.log(event.data));
```

It takes an `Aptos` instance rather than living on the client, because a
client may run on any adapter. Only handles stored directly on a resource field
are listed: the fullnode's handle endpoint cannot address one nested inside a
struct field, so those are skipped.

### Typed events

`defineEvent<T>(eventType)` returns a `module` descriptor that carries its
payload type, so every client method infers `event.data` from it:

```ts
import { defineEvent } from "streamd";

const Deposit = defineEvent<{ amount: string }>("0x1::coin::DepositEvent");

client.on(Deposit, (event) => console.log(event.data.amount));
const recent = await client.getEvents(Deposit, { fromVersion: -1000n });
```

The type is your claim about the decoded JSON, not a runtime check: `u64`
values still arrive as strings unless you name them in `decode` (below), in
which case describe them as `bigint`. At runtime the descriptor is the same
plain object you would write by hand.

### Decode integers

Move `u64`, `u128` and `u256` values arrive as decimal strings. Name the
top-level `data` fields you want as `bigint`:

```ts
const Deposit = defineEvent<{ store: string; amount: bigint }>(
  "0x1::fungible_asset::Deposit",
);

client.on(Deposit, (event) => console.log(event.data.amount + 1n), {
  decode: ["amount"],
  where: whereField("amount", 1_000_000n),
});
const big = await client.getEvents(Deposit, { decode: ["amount"], limit: 10 });
```

`decode` works on `on`, `onBatch`, `once`, `stream`, `getEvents`, `iterateEvents` and
`getEventPages`, and runs before `where`, so predicates see the decoded
values. An event without the field is left alone. A named field holding
anything other than an unsigned decimal string is an error: a subscription
reports it through `onError` and skips the event, and a query rejects.

### Serialise events

JSON has no `bigint`, so `JSON.stringify` throws on a `StreamdEvent`. Use
`serializeEvent` and `parseEvent` to store or send events and read them back:

```ts
import { parseEvent, serializeEvent } from "streamd";

const text = serializeEvent(event); // one line, the same format as `streamd tail`
const back = parseEvent(text, { decode: ["amount"] });
back.version; // bigint again
```

`version`, `sequenceNumber` and `timestamp` always come back as `bigint`.
Inside `data`, every `bigint` is written as a decimal string, and only the
top-level fields named in `decode` are turned back into `bigint`, by the same
rule as the `decode` option; deeper ones stay strings. Malformed input throws
a `StreamdError`.

## Filtering

Chain APIs can't filter on what's inside an event. `where` filters on your
side, after fetching:

```ts
import { involvesAddress, whereField, ofType, ofModule, fromSender, every, some, not } from "streamd";

streamd.on(
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  (event) => console.log(event),
  { where: whereField("store", myStore) },
);
```

- `whereField(path, value)` matches events whose `data` has `value` at `path`
  (a dot-separated string such as `"store.owner"` or `"holders.0.owner"`, or an
  array of keys and indices such as `["store", "owner"]` or `["key.with.dots"]`).
  When `value` is an address, `0x1` and `0x000...01` count as the same address.
- `involvesAddress(address, { fields?, maxDepth? })` matches events whose
  payload mentions the address anywhere. `0x1` and `0x000…01` count as the
  same address.
- `ofType(...types)` matches exact event types.
- `ofModule(...modules)` matches every event declared in a module, written
  `address::module` (`ofModule("0x1::coin")` matches `0x1::coin::CoinDeposit`
  and `0x1::coin::CoinStore<...>`). `0x1` and `0x000...01` count as the same
  address.
- `fromSender(address)` matches events whose transaction `address` signed, so
  it narrows a `module` subscription to one wallet's own transactions. It
  needs `sender` on the event; events without one never match. `0x1` and
  `0x000...01` count as the same address.
- `every(...predicates)`, `some(...predicates)`, and `not(predicate)` combine
  and negate filters.
- Any `(event) => boolean` function works too.

`limit` counts events *after* filtering, so a filter that rarely matches keeps
the query reading pages until it fills. Set `maxPages` to cap that work.

## Data sources and their limits

By default streamd reads from a fullnode with `FullnodeScanAdapter`:

- **Handle events** use the dedicated fullnode endpoint. Exact and cheap.
- **Account events** walk that account's own transaction history. Cheap even
  far back.
- **Module events** have no by-type endpoint on Aptos, so the adapter scans
  transactions and keeps the matches. That's fine for following the chain
  head, but not for deep history: fullnodes prune old data, and a rare event
  type means scanning every transaction in between.

For deep history, implement `EventAdapter` against your own index (a
self-hosted processor or third-party indexer) and pass it in:

```ts
const streamd = new StreamdClient({ aptos, adapter: myIndexAdapter });
```

An adapter has one required method, `fetchEvents(descriptor, options)`, returning
`{ events, nextCursor, hasMore }`, an optional `getHeadVersion(descriptor)` that
a negative `fromVersion` needs, an optional `getEarliestVersion(descriptor)` that
`fromVersion: "earliest"` needs, an optional `getBlock(descriptor, height)` that
`fromBlock`/`toBlock` need, and an optional `getBlockHeights(descriptor)` that `fromTime`
needs alongside `getBlock`. See the `EventAdapter` type for the full contract.

To check a custom adapter against that contract, run `checkAdapter` from any
test runner or a plain script. It walks a few pages (`maxPages`, default 3),
then makes two more fetches and one call to each optional method present, and
reports a result per rule (`page-shape`, `order`, `cursor-advance`,
`cursor-precedence`, `foreign-cursor`, `head-version`, ...) instead of
throwing. It only reads, but against a real source those reads are network
calls:

```ts
import { checkAdapter } from "streamd";

const report = await checkAdapter(
  myIndexAdapter,
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  { fromVersion: 1_000_000n, blockHeight: 500_000n },
);
if (!report.ok) console.table(report.results.filter((r) => r.status === "fail"));
```

`fromVersion` is where the walk starts (omit it to start where the adapter
does, usually the head), and `blockHeight` lets it check `getBlock`; a rule
whose method or input is missing is `skip`.

Combine sources with `RoutingAdapter` (by descriptor `kind`, with an optional
`default` for the rest) and `FallbackAdapter` (try sources in order on each
call when one fails, while still rejecting invalid input at once):

```ts
import {
  FallbackAdapter,
  FullnodeScanAdapter,
  RoutingAdapter,
  StreamdClient,
} from "streamd";

const fullnode = new FallbackAdapter([
  new FullnodeScanAdapter(primaryAptos),
  new FullnodeScanAdapter(backupAptos),
]);

const streamd = new StreamdClient({
  adapter: new RoutingAdapter({
    module: myModuleIndexAdapter,
    default: fullnode,
  }),
});
```

To log or time requests, wrap any adapter with `withHooks`. `onRequest` runs
before each `fetchEvents` call and `onResponse` after it settles, with the
`page` or the `error` and `durationMs` for the source call alone (time spent
waiting on `maxConcurrentRequests` is not included). Hooks are synchronous; one
that throws fails that fetch. `getHeadVersion`, `getEarliestVersion` and `getBlock` pass
through unhooked:

```ts
import { FullnodeScanAdapter, StreamdClient, withHooks } from "streamd";

const streamd = new StreamdClient({
  adapter: withHooks(new FullnodeScanAdapter(aptos), {
    onRequest: ({ descriptor }) => console.log("fetch", descriptor.kind),
    onResponse: ({ durationMs, page }) =>
      console.log(page ? `${page.events.length} events` : "failed", durationMs),
  }),
});
```

To skip the source when re-reading settled history, wrap an adapter with
`CachingAdapter`. It stores only pages the adapter marks `final: true`,
meaning the same request will always return the same page, and passes
everything else through. `FullnodeScanAdapter` marks a page final when it
started from a cursor, offset or `fromVersion` and read nothing at or past the
chain head; a page that tails the head is never cached. Entries are dropped
least recently used past `maxEntries` (default 1000):

```ts
import { CachingAdapter, FullnodeScanAdapter, StreamdClient } from "streamd";

const streamd = new StreamdClient({
  adapter: new CachingAdapter(new FullnodeScanAdapter(aptos), { maxEntries: 500 }),
});
```

A cached page keeps being served after the fullnode prunes that history, where
the node itself would answer 410. A custom adapter opts in by setting `final`
on pages that cannot change; leave it unset when unsure. `RoutingAdapter` and
`withHooks` pass the flag through, `FallbackAdapter` keeps it only on pages
from its first source, and `MemoryAdapter` never sets it.

For offline unit tests, pass `MemoryAdapter` with a list of events (`aptos` is
optional when a custom `adapter` is supplied):

```ts
import { MemoryAdapter, StreamdClient } from "streamd";

const streamd = new StreamdClient({
  adapter: new MemoryAdapter([
    {
      type: "0x1::fungible_asset::Deposit",
      data: { store: "0x1", amount: "100" },
      version: 1n,
      sequenceNumber: 0n,
      eventIndex: 0,
    },
  ]),
});
```

## Command line

The package installs a `streamd` executable. `streamd tail` follows matching events and
prints each one to stdout as a single JSON line, so the output pipes straight into `jq` or a
file. After `bun run build` it runs from the checkout as `node dist/cli.js`.

```sh
# module events, one or more Move event types
streamd tail 0x1::transaction_fee::FeeStatement

# events from transactions an account sent, optionally narrowed by type
streamd tail --account 0x1 0x1::coin::WithdrawEvent

# a legacy event handle
streamd tail --account 0x1 --handle "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" \
  --field deposit_events
```

| Option | Meaning |
| --- | --- |
| `--network <name>` | `mainnet`, `testnet`, or `devnet` (default `mainnet`) |
| `--node-url <url>` | fullnode URL, overriding the network's default |
| `--from-version <n>` | start at this version instead of the chain head |
| `--poll-interval <ms>` | delay between polls (default `1000`) |
| `--limit <n>` | exit after printing `n` events |
| `-h`, `--help` | show usage |

Each line carries `type`, `data`, `version`, `sequenceNumber`, `eventIndex`, and, when
the source has them, `transactionHash`, `timestamp` and `sender`. Every `bigint`, including any inside
`data`, is printed as a decimal string:

```json
{"type":"0x1::transaction_fee::FeeStatement","data":{"total_charge_gas_units":"7"},"version":"2817350001","sequenceNumber":"0","eventIndex":3,"transactionHash":"0x5c...","timestamp":"1730000000000000","sender":"0x9f..."}
```

To pick up where a run stopped, pass the last printed `version` plus one as
`--from-version`.

Polling errors are reported on stderr and tailing continues until the subscription gives
up. Exit codes: `0` when the limit is reached, on Ctrl-C or `SIGTERM`, or when stdout closes
(as when piped into `head`); `1` when the subscription stopped on an error; `2` for invalid
arguments; `130` on a second Ctrl-C.

## Development

```sh
bun install
bun run test               # unit tests
bun run typecheck
bun run build              # ESM + CJS + types into dist/
bun run demo               # live demo against mainnet
STREAMD_INTEGRATION=1 bun test tests/integration   # hits mainnet
```

## License

[MIT](LICENSE)
