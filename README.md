# streamd

Event subscriptions and historical queries for blockchains, starting with Aptos.

Subscribe to events as they land, or query them historically, with cursors,
deduplication, backoff and client-side filtering handled for you.

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient } from "@streamd/aptos";

const streamd = new AptosClient({ aptos: new Aptos(new AptosConfig({ network: Network.MAINNET })) });

streamd.on(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  (event) => console.log(event.version, event.transactionHash, event.data),
);
```

`AptosClient` is `StreamdClient` from `@streamd/core` plus the Aptos-only methods:
`transfers`, `getTransfers`, `balances`, `balanceAt`, `eventsOf`, `eventsAt`, `expectEvents`,
`previewEvents` and `expectPreview`. It takes every `StreamdClient` option, with `aptos` (an `Aptos` or
`AptosConfig`) required and `adapter` defaulting to a `FullnodeScanAdapter` over it. The
examples below call it `streamd`; on another chain, build a `StreamdClient` with that chain's
adapter.

## Install

Not yet published to npm. To use it now, build from source:

```sh
git clone https://github.com/0xNeit/streamd.git
cd streamd
bun install
bun run build
```

The repository is a Turborepo monorepo on Bun workspaces. Every package ships ESM, CommonJS and
TypeScript types:

- `@streamd/core` (`packages/core`) - the client engine: subscriptions, queries, merges,
  reductions and the `EventAdapter` contract, with the chain-neutral adapters. It depends on no
  chain SDK.
- `@streamd/aptos` (`packages/aptos`) - Aptos: `AptosClient`, `FullnodeScanAdapter`,
  `IndexerAccountAdapter`, the network presets, `checkDescriptor`, `listEventHandles`, `ownedBy`,
  `resolveName`, `primaryName`, and the
  `@streamd/aptos/framework` event catalog. `@aptos-labs/ts-sdk` (>= 1.0.0) is a peer dependency.
- `@streamd/cli` (`packages/cli`) - the `streamd` executable, built on `@streamd/core` and
  `@streamd/aptos`.
- `@streamd/store-sqlite` (`packages/store-sqlite`) - opt-in persistence: events written to a
  SQLite database you supply, and queried back. See [Store events in SQLite](#store-events-in-sqlite).
- `@streamd/store-postgres` (`packages/store-postgres`) - the same over a Postgres client you
  supply. See [Store events in Postgres](#store-events-in-postgres).
- `@streamd/sui` (`packages/sui`) - `SuiClient` and `SuiAdapter`, the same client over Sui events,
  Sui network presets, and `resolveName`, `primaryName` and `ownedBy` over SuiNS and object owners, with
  a `./framework` catalog of `0x2` and `0x3` events. `@mysten/sui` is a peer dependency. See [Sui](#sui).
- `@streamd/react` (`packages/react`) - `useEvents`, `useLogs`, `useTransactionEvents`, `useTransfers` and `useReduction` hooks. `react` (>= 18) and
  `@aptos-labs/ts-sdk` are peer dependencies; `useTransactionEvents` and `useTransfers` take an
  `AptosClient`. See [React hooks](#react-hooks).

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
    maxPollInterval: 30_000, // poll less often while nothing arrives (default: off)
    dedupWindow: 1000,   // seen-event dedup window size, 0 to disable (default 1000)
    id: "deposits",      // sub.id; generated when omitted
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
(default `60000`), and stops after `maxErrors` failures in a row. When the
adapter's error carries a server's `Retry-After` hint (see below), it waits
that long instead, still capped at `maxBackoff` and still counted as a
failure. With `maxPollInterval`, a quiet subscription polls less often: each empty page
doubles the wait from `pollInterval` up to that many ms, and a page with events returns it to
`pollInterval`, so a new event can be seen up to `maxPollInterval` late. An event `where`
filters out still counts as activity. Invalid
input, such as a malformed address or cursor, stops it at once, since retrying
cannot fix it. Pass `onError` to `new StreamdClient({ adapter: new FullnodeScanAdapter(aptos), onError })` to set
a client-wide fallback for any `on()`, `once()`, or `stream()` call that omits
its own `onError`.

`id` names the subscription for your logs and maps; `on()` and `onBatch()` take it. It must
be a non-empty string that no live subscription on the same client holds, or the call throws
`StreamdError` before anything polls. The name is free again once the subscription ends:
`unsubscribe()`, disposal, `off()`, its `signal` aborting, polling giving up, or `destroy()`.
It cannot be a client-wide default.

### Client-wide listener defaults

```ts
const streamd = new StreamdClient({
  adapter: new FullnodeScanAdapter(aptos),
  defaults: { pollInterval: 3000, maxErrors: 3, decode: ["amount"] },
});

streamd.on(deposits, handle);                           // polls every 3000 ms
streamd.on(withdrawals, handle, { pollInterval: 500 }); // its own interval, still maxErrors 3
```

`defaults` sets `pollInterval`, `batchSize`, `maxErrors`, `maxBackoff`, `maxPollInterval`,
`dedupWindow` and `decode` for every `on()`, `onBatch()`, `once()` and `stream()` call on the client.
A call's own value for a field wins, and one it leaves `undefined` takes the default; a
call's `decode` replaces the default list rather than adding to it. `defaults.pollInterval`
wins over `defaultPollInterval`. Start positions, `where`, `signal`, `onCaughtUp` and
`once()`'s `timeout` stay per call, and `onError` has its own client-wide option. The
constructor throws a `StreamdError` for an invalid value or any other key.

### Pause and resume

```ts
sub.pause();   // stop polling; the subscription stays open
sub.paused;    // true
sub.resume();  // poll again at once, from the same cursor
```

Pausing keeps the cursor, the dedup window, and the error count, so `resume()` picks up
with the page it held and skips events already delivered. A handler already running when
you pause finishes, the rest of its page waits for `resume()`, and a fetch in flight runs
to completion with its result discarded: `resume()` waits for it rather than sending a
second request. A `withState` view in flight is abandoned instead, so a hung one never stalls
`resume()` or keeps its request slot once the subscription ends. The abandoned request still
runs on the node, outside `maxConcurrentRequests`, and the resumed run asks again. While
paused, `active` (and `sub.status.active`) is `false`. `unsubscribe()`, `destroy()`, or the
`signal` aborting still ends a paused subscription, and `resume()` never revives one that has
ended, including after `maxErrors`.

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

Every subscription also has `caughtUp`, a promise for the same moment:

```ts
const sub = streamd.on(descriptor, handle, { fromVersion: 3_000_000_000n });
await sub.caughtUp;
console.log("backfill done, now live");
```

It resolves after that page's handlers have finished, and it settles once. It rejects if the
subscription ends first: with the `StreamdPollingError` when polling gives up, the signal's
reason on abort, and a `StreamdError` on `unsubscribe()` or `destroy()`. If nobody awaits it, a
rejection is not reported as unhandled. It stays pending through a pause. On `onTransaction()`
with several descriptors it waits for every walk, and on `watch()` for every address given
to it, where an address that gave up no longer counts and only the end of the whole watch
rejects.

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

### Several descriptors at once

```ts
streamd.on([Swap, Deposit, { kind: "account", accountAddress: treasury }], (event) => {
  // every match from all three, in (version, eventIndex) order, each event once
});
```

`on`, `onBatch`, `once` and `stream` take a list of descriptors of any kinds. Each gets its own
walk, and events are held by version until every walk has passed it, then delivered in order;
an event two descriptors match arrives once. With typed descriptors, `event.data` is the union
of their payloads. `onBatch` gets everything one release frees, so a batch always holds whole
transactions. A throw from an `on` handler goes to `onError` and the rest of the transaction
still arrives.

A list cannot resume: the `cursor` option is refused and `sub.cursor` is `undefined`, so
restart from a version instead. It is not a listener for `off()`, `removeAllListeners()` or
`listenerCount()`; `unsubscribe()` or `destroy()` ends it, and a walk that gives up ends the
rest. `where`, `decode` and `withState` run on each walk. A one-element list is that
descriptor, with everything a single descriptor does.

### Group by transaction

```ts
const sub = streamd.onTransaction([swaps, deposits], async (tx) => {
  // tx: { version, transactionHash?, sender?, timestamp?, entryFunction?, events }
  await db.insertTransaction(tx.version, tx.events);
});
```

`onTransaction` calls your handler once per transaction with every matching event it
emitted, in `eventIndex` order, and transactions in version order. `decode`, `where` and
`withState` apply per event first, so a transaction with no match is not delivered. The
optional fields come from the first event carrying them, and `entryFunction` only when every
event carrying one agrees (on Sui, events of one transaction can come from different
commands); `handle` events carry none.

Adapter pages can end partway through a transaction (handle events and `MemoryAdapter`
page by event), so events are held by version until the page's `throughVersion` passes
it. An adapter that does not report `throughVersion` releases every version below the
highest one a page reached, and the highest once it reports nothing more. That keeps
transactions whole only when a committed transaction's events are all visible at once:
`StoreAdapter` cannot promise this while another writer is still adding rows, and a custom
adapter should set `throughVersion` if it can.

Several descriptors are merged: each is polled as its own walk, a transaction is released
once every walk has passed it, and an event two descriptors both match arrives once. Merged
walks do not share polling with other subscriptions. A walk that has caught up without
`throughVersion` vouches for what the other walks had reached when its fetch started, so a
transaction another walk has just read waits for that walk's next poll. That assumes every
source is as current as the others: a source that lags (an Indexer behind the fullnode, a
node below the head) and reports no `throughVersion` can split a transaction into two calls,
so such a source should report it.

Options and the returned `Subscription` are `onBatch`'s. `batchSize` is each walk's page
size, so one call may hold more events. With one descriptor, `cursor` moves only past a
page that left nothing held: resuming from it never splits a transaction, though it can
repeat ones delivered after it. With several, `cursor` is `undefined` and the `cursor`
option is refused. `pause()` holds the remaining transactions until `resume()`; a walk
that gives up ends the others; `onCaughtUp` fires once every walk has caught up. Like
`reduce()`, it is not counted by `listenerCount()` or stopped by `off()`.

For a past range, `getTransactions` returns the same groups at once:

```ts
const txs = await streamd.getTransactions([swaps, deposits], {
  fromVersion: 3_000_000_000n,
  toVersion: 3_000_010_000n,
  limit: 50, // transactions, not events
});
```

It takes `getEvents`'s options and reads the range as `getEvents` does, a list merged with
each event once, then cuts it wherever the version changes. `decode` and `where` apply per
event first, so a transaction with no match yields no group. `limit` counts groups and never
cuts one: the walk reads on until a later version or the end of the range, so the last
group is whole even when its events span two pages. `orderBy: "desc"` returns the last
`limit` transactions newest first (events inside each stay in `eventIndex` order), and
`withState` reads only the events returned. `onCursor` and `offset` are refused. `maxPages`
caps the work, so a transaction cut off by the cap may be missing its later events.

### Watch lists

```ts
const watch = streamd.watch([alice, bob], (event, address) => {
  console.log(address, event.type, event.version);
});

watch.add(carol); // starts following carol
watch.remove(bob); // stops bob's walk at once
watch.addresses; // [alice, carol], as passed
```

`watch` follows each address with its own `account` walk (`{ kind: "account",
accountAddress }`) and calls your handler with the event and the watched address it came
from, as you spelled it. Each walk goes through the client's shared polling and
`maxConcurrentRequests` like any `on()` subscription, and walks deliver independently, so
there is no order across addresses.

What a walk sees is what the adapter yields for an `account` descriptor. On
`FullnodeScanAdapter` that is every event of the transactions the address sent, orderless
transactions excepted: a deposit someone else sent to the address is not seen. Use
`IndexerAccountAdapter` (alone, or for `account` through a `RoutingAdapter`) to see every
transaction touching the address, received ones included; an event in a transaction
between two watched addresses then arrives once for each. Narrow further with `where`, for
example `ownedBy` or `involvesAddress`.

To continue after a restart, save `watch.positions` and pass it back:

```ts
const watch = streamd.watch(addresses, handler, { fromVersion: 0n, positions: saved });
// later, and again on shutdown:
store(watch.positions);
```

`positions` is a JSON-safe object of long-form address to cursor. An address with a position
continues from it, ahead of `fromVersion` and `fromTime`, and the others start as usual.
Delivery is at least once, since a walk's position moves per page, so a restart can repeat
part of a page. It stays readable after `unsubscribe()`, keeps a walk that gave up, and carries
forward the position of an address you are not watching now, which `add()` then uses once.
`remove()` forgets the address's position. An address with no position yet is left out.

Addresses are checked when given (`StreamdError` for one that is not an Aptos address) and
deduplicated by value, so `0x1` and its long form are one walk. `add(address)` returns
`false` for an address already watched or a watch that has ended; the new walk starts
where `on()` without a start option does (the account's next transaction on the fullnode
and Indexer adapters), since a `fromVersion` or `fromTime` given to `watch` applies only
to the addresses given with it. `remove(address)` stops that walk as `unsubscribe()` does:
a handler in progress finishes and nothing further of its page is delivered.

Options are `on()`'s, applied to each walk, without `cursor` (an account cursor belongs to
one address) or `id`; `cursor` is `undefined`. `pause()` and `resume()` apply to every walk,
and one added while paused starts paused. A walk that gives up after `maxErrors` drops its
address, its errors having gone to `onError`, and the others carry on. `onCaughtUp` fires
once, when every address given to `watch` has caught up. Like `onTransaction()`, it is not
counted by `listenerCount()` or stopped by `off()`; `unsubscribe()`, the `signal` or
`destroy()` ends it.

Every list size uses one walk per address. A single chain scan filtered by the watched set
would be cheaper for long lists, but no source here can scan every transaction without an
event type yet, so that path waits for one that can.

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
per stream: the stream subscribes when the loop first asks for an event. Its `cursor`
resumes like a subscription's (below), passing a page only once the loop has asked past
its last event, so leaving the loop mid-page resumes at that page.

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
idempotent on `(version, eventIndex)`, which [`eventId`](#event-identity) returns as one string.

### Backfill, then follow

Read history first, then go live from exactly where it ended. Pass `onCursor` to a query
and hand the cursor it reports to `on()`:

```ts
let cursor: string | undefined;
const history = await streamd.getEvents(descriptor, {
  fromVersion: -100_000n,
  onCursor: (c) => (cursor = c),
});
render(history);

if (cursor === undefined) throw new Error("no exact resume point");
streamd.on(descriptor, handle, { cursor }); // the first event after the history
```

`onCursor` is called once, when the walk finishes, from `getEvents`, `getEventPages` and
`iterateEvents` alike; leaving a page loop early or a rejected query never calls it. Every
event after the history arrives once, with none skipped, because the cursor is the one the
adapter reported after the last page the query read in full. `orderBy: "desc"`, `where`,
`maxPages` and `fromTime` all keep it exact.

It is `undefined` when the query cannot name an exact point: no page was read (`limit: 0`,
`maxPages: 0`), `limit` cut the last page short, `toVersion` or `toBlock` bounded the walk,
or the adapter gave no cursor. Bound a backfill you mean to follow with `fromVersion` and
`maxPages` rather than `limit`, and after a `toVersion` bound follow with
`fromVersion: toVersion + 1n`. Like a subscription's cursor, it only works with the same
descriptor and the same client's adapter.

### Live derived state

`reduce()` keeps a value current: it folds the history into a starting value, then folds
each live event as it arrives, with every event folded exactly once across the seam.

```ts
const volume = streamd.reduce(
  swaps, // a defineEvent<{ amount: bigint }> descriptor types `e.data`
  (total, e) => total + e.data.amount,
  0n,
  {
    fromVersion: -100_000n,
    decode: ["amount"],
    onChange: (total) => render(total), // after each live event
  },
);
render(await volume.ready); // the backfilled total, once following has started
volume.value; // always the latest total
volume.unsubscribe(); // or `using volume = streamd.reduce(...)`
```

The reducer is synchronous and should return a new state rather than mutate the old one.
History options (`fromVersion`, `fromTime`, `fromBlock`, `maxPages`, `retries`) shape the
backfill, listener options (`pollInterval`, `batchSize`, `maxErrors` and the rest) the
following, and `where` and `decode` (or the client's `defaults.decode`) apply to both.
Options that would leave no exact point to follow from, such as `limit`, `toVersion` or
`orderBy`, are refused.

To survive a restart without refolding the history, save `value` and `cursor` together and pass
them back:

```ts
const volume = streamd.reduce(Deposit, add, saved?.value ?? 0n, {
  fromVersion: 0n,
  cursor: saved?.cursor,
  onChange: (value) => save({ value, cursor: volume.cursor }),
});
```

`cursor` is `undefined` until the backfill has finished, and stays readable after
`unsubscribe()`. It always agrees with `value`, inside `onChange` too, so each event is folded
exactly once. With a cursor no history is read: `ready` resolves with the value you passed, and
`fromVersion`, `fromBlock` and `fromTime` are ignored. It is an opaque, JSON-safe string, and only
works with the descriptor and node that produced it. Keep `value` JSON-safe if you store it as
JSON.

Pass a list of descriptors to fold several streams into one value, each event once, in
`(version, eventIndex)` order across them:

```ts
const net = streamd.reduce([Deposit, Withdraw], (n, e) => e.type === Deposit.eventType ? n + 1 : n - 1, 0);
```

A merged backfill has no adapter cursor to follow from, so the reduction reads the head once,
folds the merged history up to it, and follows the merged live tail from there; the adapter
needs `getHeadVersion`. Its `cursor` resumes it the same way as above. An adapter cursor from
`onCursor` cannot resume a list, and is refused.

A throwing reducer stops the reduction, since folding past a lost event would leave the
value wrong: the error goes to `onError` (or the client's), `ready` rejects if the backfill
was still running, and `value` keeps the last good state. A failed backfill does the same.
Live polling errors behave as they do for `on()`, and `active` turns `false` once polling
gives up. `signal`, `unsubscribe()` and `destroy()` stop it at any stage without calling
`onError`. `ready` never surfaces as an unhandled rejection, so ignoring it is safe. Like
`once()` and `stream()`, a reduction is not counted by `listenerCount()`.

#### Ready-made reducers

`count()`, `sum(path)`, `latest()` and `groupBy(path, inner)` cover the common folds. Each is
a plain reducer with the value it starts from attached as `.initial`, so it goes in the same
place as a hand-written one, and the `initial` argument stays yours for a saved value:

```ts
import { count, groupBy, latest, sum } from "@streamd/core";

const deposits = streamd.reduce(Deposit, count(), 0n, { fromVersion: -10_000n });
const volume = streamd.reduce(Deposit, sum("amount"), 0n, { fromVersion: -10_000n });
const newest = streamd.reduce(Deposit, latest(), undefined);

const perStore = groupBy("store", count());
const byStore = streamd.reduce(Deposit, perStore, perStore.initial, { fromVersion: -10_000n });
byStore.value.get(store); // a bigint, or undefined for a store not seen yet
```

- **`count()`** - the number of events folded, a `bigint` from `0n`.
- **`sum(path)`** - the total of the integer at `path` in `event.data`, a `bigint` from `0n`, at
  full precision whether the field is a decoded `bigint`, a decimal string or a safe-integer
  number. `path` is `whereField`'s: `"amount"`, `"inner.amount"`, `"legs.0.amount"` or a key
  array. A missing or `null` field adds nothing, so a list of descriptors whose types lack it
  still sums, and a mistyped path sums to `0n`. Any other value that is not an integer throws,
  which stops the reduction as any reducer throw does, rather than leave the total wrong.
- **`latest()`** - the most recently folded event, `undefined` before the first.
- **`groupBy(path, inner)`** - a `Map` from the value at `path` to `inner` folded over that
  key's events, each new key starting from `inner.initial`. Keys are strings: a string as the
  source reports it, and a number, `bigint` or boolean through `String`. Addresses are not
  padded, since `0x0a` and `0x000a` may be different byte strings; on one source an address is
  always written the same way. A missing or `null` field skips the event, an object or array
  throws. Every event returns a new `Map`, so the previous value and `initial` are never
  changed. Groups nest: `groupBy("pool", groupBy("store", sum("amount")))`.

Pass `fold.initial`, or a typed literal such as `new Map<string, bigint>()`, rather than a bare
`new Map()`, which TypeScript reads as `Map<any, any>`. To resume, save the value in a
JSON-safe form beside `cursor`: a `bigint` as `value.toString()` and back with `BigInt(...)`, a
`groupBy` map as its entries, and `latest()`'s event with `serializeEvent` and `parseEvent`.

```ts
const save = () => storage.set(JSON.stringify({
  groups: [...byStore.value].map(([key, n]) => [key, n.toString()]),
  cursor: byStore.cursor,
}));
// after a restart
const { groups, cursor } = JSON.parse(storage.get());
const resumed = streamd.reduce(Deposit, perStore, new Map(groups.map(([key, n]) => [key, BigInt(n)])), { cursor });
```

### Replay history

`replay()` feeds a past range through an `on()` callback, so a handler written for live
events can be backtested unchanged. (It is unrelated to `replayAdapter`, which answers
recorded adapter calls offline.)

```ts
const run = streamd.replay(swaps, handleSwap, {
  fromVersion: 3_000_000_000n,
  toVersion: 3_001_000_000n,
  speed: 60, // an hour of chain time per minute; omit for as fast as possible
});
const delivered = await run.done;
```

The callback contract is `on()`'s: each call is awaited before the next event, and a throw
or rejection goes to `onError` (or the client's) while delivery carries on. The range and
narrowing options are those of `getEvents`, applied the same way: `fromVersion`,
`toVersion`, `fromTime`, `fromBlock`, `toBlock`, `limit`, `offset`, `where`, `decode` (or
the client's `defaults.decode`), `withState`, `maxPages`, `retries`, `onCursor` and
`signal`; `batchSize` sets the page size. A list of descriptors is replayed merged, as
`getEvents` merges it. `orderBy` and listener-only options are refused.

`speed` omitted or `Infinity` delivers as fast as the callbacks settle. A positive number
replays chain time scaled by it: before an event with a `timestamp`, the replay waits for
its gap from the previous timestamped event divided by `speed`, less the time already spent
since that delivery, so slow fetches and handlers do not stretch it. An event without a
`timestamp` is delivered at once.

The replay ends where `getEvents` would: at `toVersion`, or with no `toVersion` when the
source reports nothing more (`toVersion: "head"` pins it to the head at the start). Then
`onCaughtUp` is called once and `done` resolves with the number of events delivered. A
failed query (after `retries`) goes to `onError` and `done` rejects with it; `signal`
rejects it with the signal's reason, and `unsubscribe()` or `destroy()` with a
`StreamdError`. `done` never surfaces as an unhandled rejection. `pause()` holds delivery
before the next event and `resume()` carries on. To go live afterwards, take the cursor
from `onCursor` and pass it to `on()` as `cursor`. Like `reduce()`, a replay is not counted
by `listenerCount()` or stopped by `off()`.

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

### End at the head as of the query's start

```ts
// the latest 50 transfers, even on a type that never stops emitting
const latest = await streamd.getEvents(descriptor, {
  fromVersion: -100_000n,
  toVersion: "head",
  orderBy: "desc",
  limit: 50,
});
```

`toVersion: "head"` reads the head once, from the adapter's `getHeadVersion`, before the
first page, and ends the query there (inclusive). Without it a walk over a busy type keeps
finding new pages as transactions land and may never finish. With a negative `fromVersion`
the same single read sets both ends, and `retries` covers it like the other lookups. A query
that fetches nothing (`limit: 0`, `maxPages: 0`) reads no head. An adapter without
`getHeadVersion` is rejected as invalid input before any fetch, as is `"head"` together with
`toBlock`. Like any `toVersion`, it makes `onCursor` report `undefined`. Listeners have no
end bound, so `"head"` is for queries only.

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
polling gives up after `maxErrors`, straight away on invalid input, or on the first
synchronous throw from `where`, which would otherwise skip every event (a rejected
async `where` is retried, as for `on()`). A throwing `onCaughtUp`,
an event that fails `decode`, or a throwing `withState` builder goes to `onError` and
`once()` keeps waiting for a match.

### Events from a submitted transaction

```ts
const { hash } = await aptos.signAndSubmitTransaction({ signer, transaction });
const deposits = await streamd.eventsOf(
  hash,
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" }, // optional
  { decode: ["amount"], timeout: 30_000 }, // optional
);
```

`eventsOf()` looks the transaction up every `pollInterval` (the client-wide default, but at
least 100ms) until
it is committed, then resolves with its events in transaction order, each shaped like any
other event, `version`, `transactionHash`, `sender` and `timestamp` included. A descriptor,
`decode` and `where` narrow them as they narrow a query. A `module` descriptor matches by
type, an `account` one by the transaction's sender, and a `handle` one only by
`creationNumber`, since an event carries its handle's guid rather than the struct and
field holding it (`listEventHandles` reports each guid).

A transaction that failed rejects with `StreamdTransactionError`, whose `vmStatus` says
why: the chain discards a failed transaction's events, so there is nothing to return. One
not committed within `timeout` (default 20 seconds) rejects with `StreamdTimeoutError`, an
abort with `signal.reason`, and a failed lookup at once with its error. The adapter is not
told about a timeout or abort, so a lookup already in flight runs to completion and holds
its `maxConcurrentRequests` slot until then, or until the client's `requestTimeout`. The hash must be
`0x` and 64 hex digits. It needs an adapter with `getTransactionEvents`, which
`FullnodeScanAdapter` and `MemoryAdapter` have and the wrapping adapters pass through.

### A transaction's events by version

```ts
const [event] = await streamd.getEvents(Deposit, { fromVersion: -1000n, limit: 1 });
const all = await streamd.eventsAt(event!.version); // every event of that transaction
const deposits = await streamd.eventsAt(
  event!.version,
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" }, // optional
  { decode: ["amount"] }, // optional: decode, where, signal
);
```

`eventsAt()` reads the transaction committed at one ledger version and resolves with its
events, shaped and narrowed exactly as `eventsOf()` returns them for the same transaction,
`transactionHash`, `sender`, `timestamp` and `entryFunction` included. It is one lookup
through the client's limiter, so `maxConcurrentRequests`, `maxRequestsPerSecond` and
`requestTimeout` apply, and it never waits: there is no `timeout` option.

A failed transaction rejects with `StreamdTransactionError` carrying `vmStatus`, as
`eventsOf()` does, with `transactionHash` taken from the fee statement event it kept. A version
with no transaction yet, above the head, rejects with a `StreamdError` naming it. A version the
fullnode has pruned rejects with its `StreamdNetworkError`, `statusCode` 410; an
`ArchiveFallbackAdapter` over [the archive](#history-the-fullnode-has-pruned) answers it
instead. The version must be a `bigint` from 0 to `u64::MAX`, and a bad version, descriptor or
`decode` is refused before the lookup. It needs an adapter with
`getTransactionEventsByVersion`, which `FullnodeScanAdapter` has and the wrapping adapters pass
through; `MemoryAdapter` and `SuiAdapter` do not, so `SuiClient` has no `eventsAt`, since placing
a Sui version on its transaction takes a checkpoint search.

### Assert a transaction's events in tests

```ts
const tx = streamd.expectEvents(hash, { decode: ["amount"] }); // options optional
const [deposit] = await tx.toEmit(Deposit, whereField("amount", 100n));
await tx.notToEmit({ kind: "module", eventType: "0x1::fungible_asset::Withdraw" });
```

For localnet and devnet integration tests under any runner. `toEmit(descriptor, where?)`
resolves with the matching events, and `notToEmit(descriptor, where?)` with nothing. When an
assertion does not hold it rejects with `StreamdAssertionError`, whose message lists every
event the transaction emitted (index, type and data) and whose `events` holds them, so a
failing test shows what happened instead of what did not. Each assertion waits for the
transaction exactly as `eventsOf()` does and rejects as it does: a failed transaction with
`StreamdTransactionError`, one not committed in `timeout` with `StreamdTimeoutError`.
Nothing is looked up until an assertion runs. A malformed hash or `timeout` throws from
`expectEvents()` itself.

### Preview a transaction's events before signing

```ts
const transaction = await aptos.transaction.build.simple({ sender, data });
const deposits = await streamd.previewEvents(
  transaction,
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" }, // optional
  { signerPublicKey: account.publicKey, decode: ["amount"] }, // optional
);
```

`previewEvents()` simulates a built, unsigned transaction on the fullnode and resolves with
the events it would emit if it ran now, narrowed exactly as `eventsOf()` narrows them.
Nothing is signed or submitted, and the simulation commits nothing. The events are
previews, not history: `version` is the one the node simulated at, and `transactionHash`
is unset, since an unsigned transaction's hash is not the one it will commit under. Pass
`signerPublicKey` to have the node check the sender's auth key; without it that check is
skipped.

Multi-agent transactions (from `aptos.transaction.build.multiAgent`) and sponsored ones (built
`withFeePayer`, with `feePayerAddress` set to the payer) are simulated too. Their keys are
optional in the same way: `secondarySignersPublicKeys`, one per secondary signer in order (an
`undefined` entry skips that check), and `feePayerPublicKey`. Keys that do not fit the
transaction, such as a fee-payer key on a transaction with no fee payer, are refused before
anything is sent.

```ts
const sponsored = await aptos.transaction.build.simple({ sender: newUser, data, withFeePayer: true });
sponsored.feePayerAddress = sponsor.accountAddress;
await streamd.previewEvents(sponsored, undefined, { feePayerPublicKey: sponsor.publicKey });
```

A simulation that fails rejects with `StreamdTransactionError`, whose `vmStatus` says why
and whose `transactionHash` is undefined. On `FullnodeScanAdapter` a failed request
rejects with a `StreamdNetworkError` that carries neither the transaction nor the SDK
error holding it. It needs an adapter with `simulateTransactionEvents`, which
`FullnodeScanAdapter` has. The wrapping adapters pass it through, never cached or recorded,
since a simulation answers for the chain's state at that moment.

To assert on a simulation in a test, `expectPreview()` gives the same `toEmit` and `notToEmit` as
`expectEvents()` (below), with nothing submitted:

```ts
const check = streamd.expectPreview(transaction, { decode: ["amount"] });
await check.toEmit(FungibleAsset_Deposit, (e) => e.data.amount === 1n);
await check.notToEmit({ kind: "module", eventType: "0x1::coin::CoinDeposit" });
```

Each assertion runs its own simulation. A failed one rejects with `StreamdAssertionError`
listing every event the simulation emitted, with `transactionHash` undefined; a simulation that
aborts rejects with `StreamdTransactionError` instead.

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

A source that sends `Retry-After` on a 429 or 503 is listened to: a `StreamdNetworkError`
whose `retryAfter` is set (in ms) replaces the backoff for that wait, capped at 60s here
and at `maxBackoff` for listeners. A custom adapter sets it from the response header:

```ts
import { StreamdNetworkError, parseRetryAfter } from "@streamd/core";

if (!res.ok) {
  throw new StreamdNetworkError(
    `HTTP ${res.status}`,
    res.status,
    undefined,
    parseRetryAfter(res.headers.get("retry-after")),
  );
}
```

`parseRetryAfter` reads both delta-seconds and HTTP-date values, and returns `undefined`
for a missing or malformed one, which falls back to normal backoff. The default
`FullnodeScanAdapter` never sets it: the Aptos SDK drops response headers from its errors,
and the public fullnodes send no `Retry-After` today.

### Query several descriptors at once

Pass an array of descriptors, of any kinds, to read their events as one history:

```ts
const activity = await streamd.getEvents(
  [
    { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
    { kind: "account", accountAddress: "0xcafe" },
  ],
  { fromVersion: -10_000n, toVersion: "head", limit: 100 },
);
```

Each descriptor is walked on its own and the results are merged in ascending
`(version, eventIndex)` order, or newest first with `orderBy: "desc"`. An event two
descriptors both select comes back once. `limit` counts the merged result after `where`;
`maxPages` caps each descriptor's walk. A head, block or time bound is read once and shared,
so every walk covers the same versions, while `"earliest"` is resolved per descriptor. Only
one page per descriptor is held at a time. `getEventPages` and `iterateEvents` take arrays
too. `onCursor` and `offset` are refused, since one cursor cannot resume several walks.

For several Move types, one `module` descriptor with a list of `eventType`s is cheaper: one
scan, one cursor, and it works with `on()` too. Some sources report a handle event's
sequence number as its `eventIndex`, so merging handle events with others is deterministic
but does not reflect position within a transaction. Typed descriptors with different
payloads take an explicit type argument, such as `getEvents<Deposit | Withdraw>([...])`.

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

### Limit the request rate

```ts
const streamd = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos), maxRequestsPerSecond: 2 });
```

Adapter calls start at least `1000 / maxRequestsPerSecond` ms apart, with no burst, across
every subscription and query on the client. A fraction is allowed: `0.5` is one call every two
seconds. It counts what `maxConcurrentRequests` counts, shares its queue, and combines with
it. The unit is one adapter call, so a module-event scan may make several HTTP requests per
call and needs a lower number. `ownedBy` predicates read through the SDK directly and are
not counted.

### Limit concurrent requests

```ts
const streamd = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos), maxConcurrentRequests: 4 });
```

At most that many adapter calls run at once across every subscription and query on the
client; the rest wait their turn in order. `fetchEvents`, the `getHeadVersion` read behind
a negative `fromVersion`, the `getEarliestVersion` read behind `"earliest"`, and the
`getBlock` and `getBlockHeights` lookups behind `fromBlock`/`toBlock` and `fromTime`, and
the lookups behind `eventsOf()` and the simulations behind `previewEvents()` all count. The unit is one adapter call, not one HTTP request:
a module-event scan on `FullnodeScanAdapter` may make several requests per call. A call
still waiting when its subscription stops or pauses, or its query's `signal` aborts, leaves
the queue at once without reaching the adapter, and `destroy()` rejects every call still
waiting. A call the adapter never settles holds its slot unless `requestTimeout` is set.
Omit the option for no limit.

### Time out a hung adapter call

```ts
const streamd = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos), requestTimeout: 15_000 });
```

An adapter call still unfinished after that many milliseconds fails with a
`StreamdNetworkError`. A listener backs off and counts it toward `maxErrors`, and a query's
`retries` retry it, as for any network failure. The clock starts when the call starts, so
time spent waiting for `maxConcurrentRequests` or `maxRequestsPerSecond` does not count. It
covers the calls those two options count, and the view reads behind `transfers()` and
`getTransfers()`. The slot frees at once, but adapters take no
signal, so the request itself runs on, uncounted, until it ends. A module scan is one
adapter call that may make several HTTP requests, so allow for that. Omit the option for no
timeout.

### Shared polling

Subscriptions on one client that would send the adapter the same request share it: the same
descriptor, the same cursor (or absolute start), and the same `batchSize` and `pollInterval`.
They make one fetch per interval between them, while each keeps its own cursor, pause state,
error budget, `where` and `decode`. A shared page can be up to one `pollInterval` old when a
subscription reuses it. A start at the head is never shared, so subscriptions started at
different times share only once their cursors meet: after the next event for `handle` and
`account` descriptors, usually never for `module` descriptors on `FullnodeScanAdapter`.
Shared subscriptions receive the same event objects, so treat events as read-only.

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

Subscriptions and the client are `Disposable`, so `using` cleans up at scope exit:

```ts
{
  using sub = streamd.on(descriptor, (event) => console.log(event));
  // ...
} // sub.unsubscribe() runs here
```

Disposing is `unsubscribe()` (or `destroy()` for the client) and is safe to repeat. `using`
needs a runtime with `Symbol.dispose` (Node 18.18+, 20.4+, Bun) or a polyfill, and
TypeScript 5.2+; the type declarations pull in the `esnext.disposable` lib themselves.

### Shut down cleanly

`destroy()` stops everything at once and returns, so a handler part way through a write keeps
going with nothing waiting for it. `await streamd.close()` also waits for the handlers already
running:

```ts
process.on("SIGTERM", async () => {
  const finished = await streamd.close({ timeout: 10_000 });
  process.exit(finished ? 0 : 1);
});
```

Polling stops at once and no further event is delivered; only handlers already running are
waited for, across everything subscribed when it is called: `on()`, `onBatch()`, `once()`,
`watch()`, `onTransaction()`, `transfers()` and a reduction's live tail. It does not wait for
adapter requests, so a hung source cannot hold it up. It resolves `true` once they have
finished and `false` at `timeout` (default five seconds) without rejecting, and a handler
still running then carries on. A subscription already unsubscribed and `replay()` are not
waited for. A handler that awaits `close()` on its own client is waiting for itself, so it
waits out the timeout.

A predicate still deciding is abandoned, as on any stop. `await using sub = streamd.on(...)`
(or on `onBatch()`, `onTransaction()` or `watch()`) does the same for one subscription, and
`await using streamd = new StreamdClient(...)` for a client, each with the default timeout.

## Choosing events

Every call takes an event descriptor:

| `kind` | Matches | Example |
|---|---|---|
| `module` | Module events of one or more Move types | `{ kind: "module", eventType: "0x1::coin::WithdrawEvent" }` or `{ kind: "module", eventType: ["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::Deposit"] }` |
| `handle` | A legacy `EventHandle` on an account | `{ kind: "handle", accountAddress, eventHandleStruct, fieldName }` or, by its guid, `{ kind: "handle", accountAddress, creationNumber: 3n }` |
| `account` | Events from transactions an account **sent** | `{ kind: "account", accountAddress, eventTypes?: [...] }` |
| `resource` | Every write or delete of one resource, even when no event fires | `{ kind: "resource", accountAddress, resourceType: "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" }` |
| `object` | Every change to one Sui object, even when no event fires ([Sui](#sui) only) | `{ kind: "object", objectId: "0x6" }` |

When `module.eventType` is an array of Move types, `FullnodeScanAdapter` serves
all of them in a single transaction scan per page, delivering matches in
`(version, eventIndex)` order.

A `handle` descriptor takes one form or the other, never both. The creation-number
form names the handle by its guid, so `accountAddress` is the account that created
the handle, and `creationNumber` is a `bigint` up to `u64::MAX`. It reads the same
events as the struct and field form, through the fullnode's
`/accounts/{address}/events/{creation_number}` endpoint.

`account` only sees transactions the account signed. If someone else's
transaction deposits into the account, that event belongs to the sender's
transaction and won't appear.

`resource` reads each transaction's write set rather than its events, so it sees
every transaction that changed the resource, whoever sent it. Each change arrives
as a `StreamdEvent`: `type` is the resource type, `data` is the resource's value
once the transaction ran, or `null` when the transaction deleted it, and
`sequenceNumber` is `0n`. `resourceType` is matched exactly as the fullnode writes
it, with special addresses short (`0x1`) and others in full; `accountAddress` may
be written either way. `eventsOf()` and `previewEvents()` refuse it, since a
transaction lookup carries events, not write sets.

```ts
streamd.on(
  { kind: "resource", accountAddress: "0x1", resourceType: "0x1::block::BlockResource" },
  (change) => console.log(change.version, change.data),
);
```

Each delivered `StreamdEvent` carries `type`, `data`, `version` (`bigint`),
`sequenceNumber` (`bigint`), and `eventIndex` (`number`). Where the source
transaction is in hand (`module` and `account` descriptors on
`FullnodeScanAdapter`), events also carry `transactionHash` (`string`),
`timestamp` (`bigint`, microseconds since the Unix epoch), and `sender`
(`string`, the address that signed the transaction). `handle` events come from
an endpoint that omits the surrounding transaction, so all three are
`undefined` there. `timestamp` is `undefined` on genesis transactions, and
`sender` on system transactions such as block metadata, which no account signs.

Where the transaction is in hand and its payload is an entry function call, events also
carry `entryFunction`: `{ function, typeArguments, arguments }`, with `function` written
`address::module::name` and `arguments` as the fullnode renders them (u64 and wider as
decimal strings). Every event of the transaction shares the same arrays. It is `undefined`
on `handle` events, on system transactions, and on script, multisig and encrypted payloads,
whose inner call is not read. The same holds for `eventsOf`, `previewEvents` and
`IndexerAccountAdapter`, which map transactions the same way. On Sui it is the transaction
block command that emitted the event; see the Sui section.

Events emitted through a legacy `EventHandle` also carry `guid`
(`{ creationNumber: bigint, accountAddress: string }`), on every descriptor kind.
Module events have no handle, so `guid` is `undefined` on them.

### Discover event handles

`listEventHandles(aptos, address)` (an address or an Aptos name) reads an account's resources and returns
each `EventHandle` it finds as a ready-to-use `handle` descriptor, with
where it sits, the number of events emitted on it so far and its guid:

```ts
import { Aptos } from "@aptos-labs/ts-sdk";
import { listEventHandles } from "@streamd/aptos";

const handles = await listEventHandles(new Aptos(), "0x1");
// [{ descriptor: { kind: "handle", accountAddress, eventHandleStruct: "0x1::block::BlockResource", fieldName: "new_block_events" }, resourceType: "0x1::block::BlockResource", path: ["new_block_events"], count: 1165098930n, guid: { creationNumber: 3n, accountAddress: "0x1" } }, ...]

const blocks = handles.find((h) => h.path[0] === "new_block_events");
if (blocks) client.on(blocks.descriptor, (event) => console.log(event.data));
```

It takes an `Aptos` instance rather than living on the client, because a
client may run on any adapter.

Handles nested inside a struct field are listed too. The struct and field form
cannot address one, so its descriptor is by creation number, at the address in
its guid. Each entry says where the handle sits, as `resourceType` and `path`:

```ts
const votes = handles.find((h) => h.path.join(".") === "events.vote_events");
// { descriptor: { kind: "handle", accountAddress, creationNumber: 4n },
//   resourceType: "0x1::voting::VotingForum<...>", path: ["events", "vote_events"], ... }
```

Because `descriptor` can be either form, check for `fieldName` before reading it,
or match on `path`. Handles held in a table do not appear in resource data, so they
are not listed.

### Check a descriptor

A misspelled event type is not an error to the module scan: it just never
matches. `checkDescriptor(aptos, descriptor)` reads the module ABI for each
type in a `module` descriptor and says whether the struct is there:

```ts
import { Aptos } from "@aptos-labs/ts-sdk";
import { checkDescriptor } from "@streamd/aptos";

const check = await checkDescriptor(new Aptos(), {
  kind: "module",
  eventType: ["0x1::fungible_asset::Deposit", "0x1::fungible_asset::Withdrawal"],
});
// { ok: false, types: [
//   { eventType: "0x1::fungible_asset::Deposit", status: "event" },
//   { eventType: "0x1::fungible_asset::Withdrawal", status: "missing-struct" } ] }
if (!check.ok) throw new Error(JSON.stringify(check.types));
```

Each type gets one status:

| Status | Meaning |
| --- | --- |
| `event` | The struct declares `#[event]`. |
| `legacy` | No `#[event]`, but it has `drop` and `store`, so a legacy `EventHandle` can emit it. |
| `not-event` | The struct lacks `drop` or `store`, so no event can carry it. |
| `missing-module` | No such module is published at that address. |
| `missing-struct` | The module exists but declares no such struct. |

`ok` is true when every type is `event` or `legacy`. A generic type is checked
by its outer struct only; its type arguments are not looked up. It makes one
request per distinct module.

`handle` and `resource` descriptors get one entry in `types`, whose `eventType`
is the resource type, or for a handle by creation number that number as a string:

| Status | Meaning | `ok` |
| --- | --- | --- |
| `handle` | The `EventHandle` exists on the account. | yes |
| `missing-resource` | The account holds no resource of the handle's struct. | no |
| `not-handle` | That resource has no such field, or the field is not an `EventHandle`. | no |
| `missing-handle` | Nothing on the account holds that creation number, nested handles included. | no |
| `resource` | The account holds the resource. | yes |
| `not-held` | A valid resource type the account does not hold now; it may be created later. | yes |
| `not-resource` | The struct lacks `key`, so no account can hold it. | no |

A `resource` type can also report `missing-module` or `missing-struct`.
`account` descriptors are refused. Malformed input throws `StreamdError` before
any request, and a node failure is a `StreamdNetworkError`.

### Typed events

`defineEvent<T>(eventType)` returns a `module` descriptor that carries its
payload type, so every client method infers `event.data` from it:

```ts
import { defineEvent } from "@streamd/core";

const Deposit = defineEvent<{ amount: string }>("0x1::coin::DepositEvent");

client.on(Deposit, (event) => console.log(event.data.amount));
const recent = await client.getEvents(Deposit, { fromVersion: -1000n });
```

The type is your claim about the decoded JSON, not a runtime check: `u64`
values still arrive as strings unless you name them in `decode` (below), in
which case describe them as `bigint`. At runtime the descriptor is the same
plain object you would write by hand.

`defineEvents` combines several into one descriptor whose events narrow on `event.type`:

```ts
import { defineEvent, defineEvents } from "@streamd/core";

const Transfers = defineEvents({
  "0x1::fungible_asset::Deposit": defineEvent<{ store: string; amount: string }>(
    "0x1::fungible_asset::Deposit",
  ),
  "0x1::fungible_asset::Withdraw": defineEvent<{ store: string; amount: string }>(
    "0x1::fungible_asset::Withdraw",
  ),
});

client.on(Transfers, (event) => {
  if (event.type === "0x1::fungible_asset::Deposit") console.log("in", event.data.amount);
  else console.log("out", event.data.amount);
});
```

Each key must be the Move type of the descriptor it maps to, or it throws a `StreamdError`:
one that did not match would make the types lie. It works with the catalog's descriptors in
`@streamd/aptos/framework` too. Every method that hands you an event infers the narrowing type,
which you can name as `EventOf<T>`. At runtime it is a `module` descriptor with a list of types.

A function of your own that is generic over the payload should take and return `EventOf<T>`
rather than `StreamdEvent<T>`, since TypeScript cannot resolve which one applies until `T` is known.

### Framework events

`@streamd/aptos/framework` ships a typed descriptor for every `#[event]` struct in the common
framework modules: `0x1`'s `account`, `aptos_account`, `aptos_governance`, `block`, `code`, `coin`,
`delegation_pool`, `fungible_asset`, `multisig_account`, `object`, `object_code_deployment`,
`reconfiguration`, `stake`, `staking_contract`, `transaction_fee`, `vesting` and `voting`,
`0x3::token`, and `0x4`'s `collection` and `token`. They are generated by
[`streamd codegen`](#generate-event-types), so names and field types follow its rules: a name
two modules share is qualified as `<Module>_<Struct>` (`StakingContract_Distribute`,
`Vesting_Distribute`), and generic events take their type arguments. `CreateProposal`, `Vote` and
`SetBeneficiaryForOperator` remain as aliases of the `delegation_pool` events they named before
other modules shared those names. `0x1::dispatchable_fungible_asset` declares no `#[event]`
structs, so it has none here.

```ts
import { FeeStatement, FungibleAsset_Deposit, Coin_Deposit } from "@streamd/aptos/framework";

client.on(FungibleAsset_Deposit, (e) => console.log(e.data.store, e.data.amount));
client.on(Coin_Deposit("0x1::aptos_coin::AptosCoin"), (e) => console.log(e.data.account));
const fees = await client.getEvents(FeeStatement, { fromVersion: -1000n, limit: 10 });
```

The catalog reflects the framework ABIs it was generated from; legacy `EventHandle` events such
as `0x1::coin::DepositEvent` are not `#[event]` structs and are not in it.

### Typed view builders

`streamd codegen` also writes a `ViewRequest` builder for each `#[view]` function in the modules it
reads, so `withState` gets checked arguments:

```sh
streamd codegen --out src/coin.ts 0x1::coin
```

```ts
import { balanceView } from "./coin";

const fees = await streamd.getEvents(FeeStatement, {
  fromVersion: -1000n,
  limit: 10,
  withState: (event) => balanceView("0x1::aptos_coin::AptosCoin", event.sender!),
});
```

A builder is named `<function>View` in camelCase, or `<module>_<function>View` where two modules
share the function. It takes type arguments first, then positional `arg0`, `arg1` since the ABI has
no parameter names: `u64` and wider as `string | bigint`, narrower integers as `number`, addresses
and `Object<T>` as `string`, `vector<u8>` as a hex `string`, and `Option<T>` as `{ vec: [] | [T] }`.
A view that takes a `signer` or another struct cannot be called over JSON, so it gets a comment
instead. A view's return values are not typed; `event.state` stays `unknown[]`. A module with views
and no events now generates too.

### Decode integers

Move `u64`, `u128` and `u256` values arrive as decimal strings. Name the
`data` fields you want as `bigint`:

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

A name below the top level is a dotted path, and `*` steps into every element of an array:
`"pool.reserve"`, `"legs.*.amount"`, `"fees.*"`, or `"limit.vec.*"` for an `Option<u64>`. A key
missing at any depth leaves the event alone, as a missing top-level field does; a value that is
there but does not fit the path, such as `*` on something that is not an array, is the same
error. `parseEvent` takes the same paths.

### State at the event's version

`withState` reads a view function at each event's own `version` and attaches
its return values as `event.state`: state as it stood once the emitting
transaction had run, such as the balance a withdrawal left behind:

```ts
const Withdraw = defineEvent<{ store: string; amount: bigint }>(
  "0x1::fungible_asset::Withdraw",
);

client.on(Withdraw, (event) => console.log(event.data.amount, event.state), {
  decode: ["amount"],
  withState: (event) => ({
    function: "0x1::fungible_asset::balance",
    typeArguments: ["0x1::fungible_asset::FungibleStore"],
    // `withState`, like `where`, sees an untyped event.
    functionArguments: [(event.data as { store: string }).store],
  }),
});
```

Return `undefined` to deliver an event without state. Arguments are JSON Move
values, with `bigint` sent as a decimal string, and `event.state` is the
`unknown[]` the view returned. It works on `on`, `onBatch`, `once`, `stream`,
`getEvents`, `iterateEvents` and `getEventPages`, after `decode` and `where`, so
predicates cannot see `state` and filtered-out events cost nothing. On queries
it runs after `limit` and `orderBy` too, and a merged query reads it through
its first descriptor.

Each delivered event costs one view request, made in order and counted by
`maxConcurrentRequests`. The anonymous fullnode rate limit is a few hundred
requests per five minutes, so a busy event type needs an API key or a
narrow `where`. On `FullnodeScanAdapter`, a view that aborts (400) is refused as
invalid input, since it fails the same way every time. A version the node has
pruned fails as a `StreamdNetworkError` (status 410), so `FallbackAdapter` can
answer it from another node; with no such node, a subscription stops after
`maxErrors` consecutive failed polls.

A query rejects when the builder throws, returns an invalid request, or the
view fails (after its `retries`). A subscription reports a throwing builder or
an invalid request through `onError` and skips that event. A view that fails
with a network error fails the poll instead, so the event is delivered with its
state once a retry succeeds; one refused as invalid input stops the
subscription, as an invalid fetch does. Return `undefined` for events whose
view would abort. The adapter needs `view`, which `FullnodeScanAdapter` implements as
one JSON `/view` request at the event's version.

`withState` may also return an object read, `{ objects: [id, ...] }`, through the adapter's
`readObjects`: `event.state` then holds each object as the event's transaction left it, one entry
per id in order. `SuiAdapter` implements it ([object state on Sui](#sui)); an adapter without it
rejects the read, and a request that is empty, holds an empty id, or names both `function` and
`objects` is an invalid request. Every adapter wrapper passes `readObjects` through as it passes
`view`, and `recordingAdapter` records it by ids, version and transaction hash.

### Serialise events

JSON has no `bigint`, so `JSON.stringify` throws on a `StreamdEvent`. Use
`serializeEvent` and `parseEvent` to store or send events and read them back:

```ts
import { parseEvent, serializeEvent } from "@streamd/core";

const text = serializeEvent(event); // one line, the same format as `streamd tail`
const back = parseEvent(text, { decode: ["amount"] });
back.version; // bigint again
```

`version`, `sequenceNumber` and `timestamp` always come back as `bigint`;
`state` comes back as written.
Inside `data`, every `bigint` is written as a decimal string, and only the
fields named in `decode`, by path, are turned back into `bigint`, by the same
rule as the `decode` option; the rest stay strings. Malformed input throws
a `StreamdError`.

### Event identity

`eventId(event)` returns the key streamd deduplicates on, `version:eventIndex`
(for example `"2817350001:3"`). Use it to make a handler idempotent across restarts, or to key a
store or `Map`. It takes anything with `version` and `eventIndex`, so parsed
events work too:

```ts
import { eventId } from "@streamd/core";

const seen = new Set<string>();
streamd.on(descriptor, (event) => {
  if (seen.has(eventId(event))) return;
  seen.add(eventId(event));
  handle(event);
});
```

`sequenceNumber` is not part of it, since module events commonly report `0`.
`handle` events from `FullnodeScanAdapter` use the handle's sequence number as
`eventIndex`, because that endpoint does not say where an event sits in its
transaction, so their ids stay unique per handle but are not transaction
positions. A `resource` change's `eventIndex` is the transaction's event count
plus the change's position in its write set, so a change never shares an id with
an event of the same transaction; an `object` change's is the event count plus its
position among the transaction's object changes.

## Filtering

Chain APIs can't filter on what's inside an event. `where` filters on your
side, after fetching:

```ts
import { involvesAddress, whereField, whereOneOf, whereCompare, changesOnly, ofType, ofModule, fromSender, fromFunction, every, some, not } from "@streamd/core";

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
- `whereOneOf(path, values)` matches events whose `data` holds any one of
  `values` at `path`, an array, a `Set` or any iterable of strings, numbers,
  bigints and booleans (`whereOneOf("store", myStores)`). Each value compares
  as `whereField` compares it: a string that is an address (`0x` and 1 to 64
  hex digits) matches that address in any spelling, any other string matches
  exactly, and a number, bigint or boolean matches only a field of the same
  type, so `5n` matches a `decode`d field but not `"5"`. The values become a
  set when the filter is made, so a long list costs one lookup per event. An
  `Object<T>` field arrives as `{ inner: "0x..." }`; name the inner field
  (`"metadata.inner"`). Empty `values` matches nothing, and a malformed path
  or value throws when the filter is made. Addresses compare in
  canonical form here, so `0x0a` and `0x000a` match each other, unlike
  `groupBy`'s keys, which keep the value as reported.
- `whereCompare(path, op, value)` compares an integer in `data` using `==`,
  `!=`, `<`, `<=`, `>` or `>=` (`whereCompare("amount", ">", 1_000_000n)`). The
  field can be a decimal string, a number or a bigint, so `decode` is not
  needed, and it compares at full precision. A field that is missing or not an
  integer never matches, under `!=` too.
- `changesOnly(path?)` drops a `resource` or `object` change whose value
  deep-equals the previous change's (`changesOnly()`), or whose value at `path`
  does (`changesOnly("coin.value")`, `whereField`'s path syntax), so a busy
  resource delivers only the writes that changed what you watch. Objects
  compare with keys in any order and bigints by value; a missing value at
  `path` compares as `undefined`; a delete (`data: null`) is a change from any
  write, and a second delete in a row is not. The first change always passes.
  It keeps state, per `event.type`, since a change names no address: one
  descriptor follows one resource, but a list of descriptors following the
  same resource type at several addresses would share it, so give each its own
  subscription. Each call is a fresh state, so call it once per subscription
  or query and never share one (in React, make it once, not inline in a
  render). It cannot tell a change from an event, so on events it drops one
  whose payload repeats the previous one of its type. An event tested again,
  as a listener does after a failed poll, gets its first answer. It compares
  against the previous change it was asked about: put it first in `every`,
  since a predicate before it that rejects a write keeps that write from it,
  and inside `some` a write a predicate before it accepts never reaches it.
  `not(changesOnly())` keeps only the repeats.
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
- `fromFunction(...functions)` matches events produced by a call to one of the
  entry functions, each written `address::module::function`
  (`fromFunction("0x1::aptos_account::transfer")`). It needs `entryFunction` on
  the event; events without one never match. `0x1` and `0x000...01` count as the
  same address.
- `every(...predicates)`, `some(...predicates)`, and `not(predicate)` combine
  and negate filters. They test in order and stop at the first answer that
  decides, so put cheap checks before a lookup.
- `ownedBy(aptos, wallet)` matches events naming a store or object the wallet
  owns; see [Owned stores and objects](#owned-stores-and-objects).
- Any `(event) => boolean` function works too, and so does one returning a
  Promise.

`limit` counts events *after* filtering, so a filter that rarely matches keeps
the query reading pages until it fills. Set `maxPages` to cap that work.

### Async predicates

A predicate may return a Promise, to look something up before deciding:

```ts
const known = async (event: StreamdEvent) => allowlist.has(await ownerOf(event));

streamd.on(deposits, handler, { where: every(ofType(DEPOSIT), known) });
```

- **Order.** Events are tested one at a time, in order, and delivered in order.
  Predicates are your code, so they do not count toward `maxConcurrentRequests`;
  cache or bound your own lookups. Only a returned Promise is awaited, and
  `every`, `some` and `not` over synchronous predicates stay synchronous
  (typed `SyncEventPredicate`), so plain filters cost nothing extra.
- **Queries** reject with the predicate's error, whether it throws or rejects.
  `retries` does not repeat it, and `signal` aborts a pending one.
- **Listeners** treat a throw as a bug in the predicate: it goes to `onError`
  and the event is skipped, without counting toward `maxErrors`. A rejection is
  treated like a failed fetch: events before it on the page are delivered, the
  poll fails with backoff and counts toward `maxErrors`, and the next poll tests
  the event again. Reject with a `StreamdError` (not a `StreamdNetworkError`) to
  stop the subscription at once instead. Pausing or stopping never waits on a
  pending predicate.
- `once()` rejects at once on a throw, and retries a rejection like `on()`.
- `eventsOf`, `previewEvents` and `expectEvents` await it and reject with its
  error.

### Owned stores and objects

Fungible-asset events name a store, not a wallet, and a wallet's stores are
objects it owns. `ownedBy(aptos, wallet, options?)` matches events whose `data`
names a store or object the wallet owns, directly or through objects it owns:

```ts
import { every, ofType } from "@streamd/core";
import { ownedBy } from "@streamd/aptos";

streamd.on(
  { kind: "module", eventType: ["0x1::fungible_asset::Deposit", "0x1::fungible_asset::Withdraw"] },
  (event) => console.log(event),
  { where: ownedBy(aptos, myWallet) },
);
```

- **Fields.** It reads the top-level `store`, `object`, `token` and
  `token_address` fields (`fungible_asset::Deposit`/`Withdraw`/`Frozen`,
  `object::Transfer`, `0x4::collection::Mint`/`Burn`, `0x4::token::Mutation`),
  in order, stopping at the first owned one. `fields` replaces the list. A value
  counts when it is an address or an `Object<T>` rendered as `{ inner }`; an
  event naming none does not match, and costs no read.
- **At the event's version.** Each check is one `0x1::object::owns` view at
  `event.version`, the state once that transaction had run, as `withState`
  reads it. So a `Transfer` matches the wallet it went to, not the one it left.
  An object that does not exist at that version, such as a store the same
  transaction deleted, is not owned.
- **Cost.** Answers are cached per object and version (1000 entries per
  predicate), and `maxConcurrentRequests` (default 4) caps its reads in flight.
  These reads go through `aptos`, not the client's adapter or its
  `maxConcurrentRequests`. Put cheap filters first, as in
  `every(ofType(DEPOSIT), ownedBy(aptos, myWallet))`.
- **Failures.** A node or transport failure rejects with a
  `StreamdNetworkError`, so a listener retries the event after backoff. Any
  other rejected request, or an answer that is not a boolean, rejects with a
  plain `StreamdError`, which stops a listener. A query rejects either way. The version
  must be committed and unpruned on the node `aptos` reaches, so `previewEvents`
  events, whose version is simulated, cannot be checked.

### Aptos names

`resolveName(aptos, nameOrAddress)` turns an Aptos name (`alice.apt`,
`sub.alice.apt`) into the long-form address it points to, read once through
the ANS router at the node's head. An address comes back in long form with no
read. Resolve up front, then pass the address anywhere one is taken:

```ts
import { involvesAddress } from "@streamd/core";
import { resolveName } from "@streamd/aptos";

const alice = await resolveName(aptos, "alice.apt");
streamd.on({ kind: "account", accountAddress: alice }, onEvent);
streamd.on(DEPOSITS, onEvent, { where: involvesAddress(alice) });
```

`primaryName(aptos, address)` goes the other way, returning the name an address has set as
its primary one, such as `alice.apt`, or `undefined` when it has none:

```ts
import { primaryName } from "@streamd/aptos";

const name = await primaryName(aptos, event.sender);
console.log(name ?? event.sender);
```

The name is resolved forward as a check, so one that no longer points back to the address
counts as none. That is two reads when a name exists, and one when it does not, at the node's
head; nothing is cached. A failed read rejects rather than returning `undefined`.

`ownedBy(aptos, "alice.apt")` and `listEventHandles(aptos, "alice.apt")` take a
name directly, since they already hold `aptos`: `ownedBy` resolves it on its
first evaluation, once, and retries a failed resolution on the next.
Descriptors and the synchronous predicates take addresses only, so they stay
synchronous and a descriptor's identity never depends on a lookup.

- **Once, not per event.** A name that later points elsewhere is not followed;
  resolve again to pick up the change. For history this means today's target:
  `getEvents` with `ownedBy(aptos, "alice.apt")` checks past events against the
  wallet the name points to now, not the one it pointed to then.
- **Errors.** An unregistered or expired name, or one with no target address,
  rejects with a `StreamdError`, as does a malformed one before any request. A
  string starting `0x` is always read as an address. A node or transport failure
  rejects with a `StreamdNetworkError`, so a listener retries it.
- **Networks.** Names resolve on Aptos mainnet only. On testnet the SDK's
  router address resolves nothing, and the Movement presets are custom networks
  the SDK knows no ANS contract for, so names reject there with a `StreamdError`.

## Asset transfers

`transfers(callback, options?)` follows coin, fungible-asset and token movements as
`{ from, to, asset, amount }`, and `getTransfers(filter?)` returns them for a
past range. Each arrives as an event whose `data` is a `Transfer`:

```ts
const sub = streamd.transfers(
  ({ data }) => console.log(`${data.amount} of ${data.asset}: ${data.from} -> ${data.to}`),
  { asset: "0xa" }, // APT; omit for every asset
);

const recent = await streamd.getTransfers({ fromVersion: -10_000n, asset: "0xa" });

// what one address sent or received; an Aptos name works too
const mine = await streamd.getTransfers({ fromVersion: -10_000n, account: "alice.apt" });
```

- **Sources.** One scan over `0x1::fungible_asset::Withdraw`/`Deposit`,
  `0x1::coin::CoinWithdraw`/`CoinDeposit`, `0x3::token::TokenWithdraw`/`TokenDeposit`
  and `0x1::object::Transfer`. Coin events name the account and coin
  type; fungible-asset events name only a store, so its asset
  (`fungible_asset::store_metadata`) and owner (`object::owner`) are read through
  the adapter's `view` at the event's version. A store's asset is cached; owners
  are read per event. With `asset` set, other assets cost one metadata read per
  store and no owner read; a coin type costs no store reads at all.
- **Tokens.** `object::Transfer` fires for every object, so each object is
  checked once (cached) with the `0x4::token::creator` view, which aborts unless
  the object holds a `0x4::token::Token`; other objects are skipped, after a
  second read as the object stood before the transaction. A token's record has
  the object address as `asset`, `amount` 1n, and the event's `from` and `to`.
  A legacy `0x3::token` withdraw and deposit pair like coin legs, with `asset`
  `"<creator>::<collection>::<name>::<property_version>"` (creator in long
  form, the rest as stored, so a name containing `::` reads ambiguously) and
  the token amount. `asset` takes a token object address or a legacy id too;
  other objects are then dropped before any read.
- **Asset details.** `withAssetInfo: true` adds `symbol` and `decimals` to fungible-asset and
  coin records, from `fungible_asset::symbol` and `decimals` or the `coin` module's, read
  through `view` at the record's version and cached per asset, so each new asset costs two
  reads. Records the `account` filter drops cost none, tokens have neither field, and an
  asset whose view aborts is left without them. `formatAmount(amount, decimals)` turns base
  units into a decimal string, every digit kept: `formatAmount(1_500_000n, 8)` is `"0.015"`.
- **One account.** `account` keeps the records whose `from` or `to` is that
  address. It filters finished records, because a deposit needs its withdraw to
  report `from`, so it costs the same reads as no filter; add `asset` to save
  them. `from` and `to` are direct owners, so a wallet does not match a store it
  holds through an object. A name needs `aptos` in the client config and
  resolves once per subscription or query, on the first page; an unregistered
  one stops a subscription, and a query rejects.
- **Pairing.** Within a transaction, in event order, a deposit pairs with the
  most recent unpaired withdraw of the same asset and amount. A deposit with
  none (a mint, or a fee-on-transfer token) has no `from`; a withdraw left over
  (a burn) has no `to`. A coin withdraw and the fungible-asset deposit of its
  paired asset report different `asset` strings and stay unpaired.
- **Who.** `from` and `to` are the stores' direct owners, as ownership stood
  once the transaction had run, so a store held by an object (a pool) reports
  the object. A store the transaction deleted is read as it stood just before.
  A store created and deleted within one transaction cannot be read at all, and
  its legs are skipped.
- **Identity.** A record keeps the deposit's `(version, eventIndex)`, or the
  withdraw's when it stands alone, so cursors, dedup and `eventId` work as for
  any event. `where`, `limit`, `cursor`, `onCursor` and the other options apply
  to the records.
- **Failures.** A failed read fails the poll, which retries with backoff as for
  `on()`; a query rejects. Each subscription or query keeps at most 4 reads in
  flight, on top of `maxConcurrentRequests`, while its page fetch holds one slot.
- **Limits.** Pairing sees one page; `FullnodeScanAdapter` pages hold whole
  transactions. Legacy handle events (coin and `0x3::token`
  `DepositEvent`/`WithdrawEvent`, `object::TransferEvent`, from before module
  events) are not read. Token mints and burns emit no `object::Transfer`, so
  they do not appear. A legacy token offer and its claim land in different
  transactions and come out as a lone withdraw and a lone deposit. A transfers subscription is not a
  listener for `off()` or `listenerCount()`; `unsubscribe()` or `destroy()` ends it.

## Live balances

`balances(address, options?)` keeps one wallet's balances current. `address` may be
an Aptos name.

```ts
const wallet = streamd.balances("alice.apt", {
  asset: ["0xa", usdc], // one or a list; omit for APT
  onChange: (all, { data, version }) => console.log(version, data.asset, data.balance),
});

await wallet.ready;
wallet.get("0xa"); // bigint, in base units
wallet.value; // Map of every balance known so far
wallet.unsubscribe();
```

- **Read at the head.** Every `pollInterval`, it reads the head version, then each
  followed balance at that version, and reports what changed. A running total
  built from transfers would drift, since gas moves APT with no withdraw event,
  and a scan of the chain cannot keep up with mainnet.
- **Which assets.** No view lists a wallet's assets, so `asset` names what to follow:
  one, or a list. Without it, APT is followed. Finding what a wallet holds needs the
  Indexer; a chain scan cannot keep up with mainnet and spends the same rate limit.
- **What a balance is.** Fungible assets read the primary store, so secondary stores
  and assets held through an object are not counted. Coin types read `coin::balance`,
  which includes the paired fungible asset, so a coin and its paired asset are two
  views of one holding. APT is one entry, under `0xa` in long form, counting both;
  `get` takes any form.
- **Tokens** have no balance here: a legacy token id is refused at the call, and a
  token object rejects `ready`.
- **Cost.** One head read and one view per followed asset every `pollInterval`
  (default 1 second), so raise it for a long list on the anonymous rate limit.
  `onChange` sees each poll's result, not every transaction in between.
- **Failures.** `ready` rejects if the first read fails. A later failed read retries
  with backoff, and `maxErrors` stops it; a view that aborts stops it at once. A balances handle is not a listener for
  `off()` or `listenerCount()`.

### A balance at a past version

`balanceAt(address, asset, version)` reads one balance as it stood at a ledger version, by
the same view `balances` reads at the head, and resolves the amount in base units:

```ts
const before = await streamd.balanceAt("0x4c...", undefined, 8038000000n); // APT
const usdcThen = await streamd.balanceAt("alice.apt", usdc, 8038000000n);
```

- **Arguments.** `asset` is one asset in any form `balances` takes, or `undefined` for APT.
  `version` is an absolute version, a `bigint`; there is no `"head"` or relative form,
  since `balances` reads the head. A name resolves at the head, to the address it points
  to now, not the one it pointed to at `version`.
- **One request.** One view call through the client's limiter, so `maxConcurrentRequests`,
  `maxRequestsPerSecond` and `requestTimeout` apply, and no retry.
- **Errors.** A malformed address, asset or version, a list of assets, a legacy token id, or
  an adapter without `view` rejects with `StreamdError` before anything is sent. A view that
  aborts, as for a token object, rejects with a `StreamdError` naming the asset, the version
  and the node's reason. A version the fullnode has pruned rejects with its
  `StreamdNetworkError`, `statusCode` 410; an `ArchiveFallbackAdapter` over
  `networks.aptosMainnetArchive` answers it from the archive instead. A version above the head
  rejects with `statusCode` 404.
- **Old history.** Before the fungible-asset migration, `coin::balance` aborts for an account
  with no `CoinStore` for the coin (`ECOIN_STORE_NOT_PUBLISHED`) rather than reading `0`, and in
  the earliest mainnet history it is not yet a view function at all; both reject as an abort.
- **Aptos only.** Sui reads a balance only at its latest state, so `SuiClient` has no
  `balanceAt`.

## Data sources and their limits

By default streamd reads from a fullnode with `FullnodeScanAdapter`:

- **Handle events** use the dedicated fullnode endpoint. Exact and cheap.
- **Account events** walk that account's own transaction history. Cheap even
  far back, but only transactions the account sent; `IndexerAccountAdapter`
  (below) adds the ones it received.
- **Module events** have no by-type endpoint on Aptos, so the adapter scans
  transactions and keeps the matches. That's fine for following the chain
  head, but not for deep history: fullnodes prune old data, and a rare event
  type means scanning every transaction in between.
- **Resource changes** use the same transaction scan, reading write sets
  instead of events, so they share its cost and its limits.

For deep history, implement `EventAdapter` against your own index (a
self-hosted processor or third-party indexer) and pass it in:

```ts
const streamd = new StreamdClient({ adapter: myIndexAdapter });
```

An adapter has one required method, `fetchEvents(descriptor, options)`, returning
`{ events, nextCursor, hasMore }`, an optional `getHeadVersion(descriptor)` that
a negative `fromVersion` needs, an optional `getEarliestVersion(descriptor)` that
`fromVersion: "earliest"` needs, an optional `getBlock(descriptor, height)` that
`fromBlock`/`toBlock` need, an optional `getBlockHeights(descriptor)` that `fromTime`
needs alongside `getBlock`, and an optional `getTransactionEvents(hash, descriptor?)` that
`eventsOf()` needs, returning `{ version, success, vmStatus, events }` or `undefined` while
the transaction is not committed, an optional
`getTransactionEventsByVersion(version, descriptor?)` that `eventsAt()` needs, returning the
same shape or `undefined` when there is no transaction at that version, and an optional
`simulateTransactionEvents({ transaction, signerPublicKey? }, descriptor?)` that
`previewEvents()` needs, returning the same shape for a simulation. See the `EventAdapter` type for the full contract.

A page may also carry `throughVersion`, the highest ledger version the source has
finished examining: resuming from `nextCursor` returns nothing at or below it, so an
empty page still says how far the scan got. `FullnodeScanAdapter` and `MemoryAdapter` set
it whenever they examined something, and the wrapping adapters pass it through. A full
handle page reports one version below its last event, since the next page may hold more
events from that transaction. An account walk only claims the account's own transactions.

To check a custom adapter against that contract, run `checkAdapter` from any
test runner or a plain script. It walks a few pages (`maxPages`, default 3),
then makes two more fetches and one call to each optional method present, and
reports a result per rule (`page-shape`, `order`, `cursor-advance`,
`through-version`, `cursor-precedence`, `foreign-cursor`, `head-version`, ...) instead of
throwing. It only reads, but against a real source those reads are network
calls:

```ts
import { checkAdapter } from "@streamd/core";

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

To check that two sources agree, `compareAdapters(a, b, descriptor, range)` walks both over
the same version range and reports, by `eventId`, what only `a` returned (`missing`), what
only `b` returned (`extra`), and what both returned but disagree on (`different`, with the
fields named). Like `checkAdapter` it only reads, reports instead of throwing, and needs no
test runner:

```ts
import { compareAdapters } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const report = await compareAdapters(
  myIndexAdapter,
  new FullnodeScanAdapter(aptos),
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  { fromVersion: 1_000_000n, toVersion: 1_001_000n, maxPages: 10 },
);
if (!report.ok) console.log(report.window, report.missing, report.extra, report.different);
```

- **Window.** Only versions both walks provably finished are compared: up to each walk's
  `throughVersion`, or, for a source that sets none, the last version it returned (one below
  it when `maxPages` cut the walk). `report.window` says what that was, `report.coverage`
  how far each side got; when they share nothing, `window` is `undefined` and `ok` is false.
  Pick a settled range: a source that lags the head (the Indexer does) covers less.
- **Difference.** `type`, `data` (deep, bigint-safe, key order ignored) and
  `sequenceNumber` always; `guid`, `transactionHash`, `timestamp`, `sender` and
  `entryFunction` only when both sides set them, with addresses compared normalized.
  `state` is never compared.
- **Bounds.** At most `maxPages` (default 10) fetches of `limit` (default 25) per source;
  each list keeps `maxReported` (default 100) entries, and `truncated` says one was cut.
- **Failures.** A source that rejects or returns a malformed page lands in `report.errors`;
  what it returned before that is still compared. Only invalid arguments reject.

Combine sources with `RoutingAdapter` (by descriptor `kind`, with an optional
`default` for the rest) and `FallbackAdapter` (try sources in order on each
call when one fails, while still rejecting invalid input at once):

```ts
import { FallbackAdapter, RoutingAdapter, StreamdClient } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

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

### Sui

`@streamd/sui` serves Sui events through the same client, from Sui's gRPC API (public Sui nodes no
longer answer JSON-RPC):

```ts
import { SuiClient, networks } from "@streamd/sui";

const sui = new SuiClient({ sui: networks.mainnet });
sui.on({ kind: "module", eventType: "0x3::validator::StakingRequestEvent" }, (e) => console.log(e.version, e.data));
```

`SuiClient` is a `StreamdClient` whose `adapter` defaults to a `SuiAdapter`. `sui` is a
`SuiGrpcClient` from `@mysten/sui/grpc`, or the options to build one; a preset from `networks`
is such options, so `new SuiGrpcClient(networks.testnet)` works too. Pass `adapter` to read
through something else, such as a `CachingAdapter` over a `SuiAdapter`, and every other
`StreamdClient` option as usual. Construction without `sui` throws.

| Preset | Network | gRPC endpoint | `graphqlUrl` |
| --- | --- | --- | --- |
| `networks.mainnet` | `mainnet` | `https://fullnode.mainnet.sui.io:443` | `https://graphql.mainnet.sui.io/graphql` |
| `networks.testnet` | `testnet` | `https://fullnode.testnet.sui.io:443` | `https://graphql.testnet.sui.io/graphql` |
| `networks.devnet` | `devnet` | `https://fullnode.devnet.sui.io:443` | `https://graphql.devnet.sui.io/graphql` |
| `networks.mainnetArchive(transport)` | `mainnet` | `archive.mainnet.sui.io:443`, native gRPC only ([below](#sui-history-past-the-fullnode)) | mainnet's |

- **Versions are Sui's global transaction numbers:** the running total before the event's
  checkpoint plus the transaction's index in it, so `fromVersion: -1000n` means 1000 transactions
  back, as on Aptos. Each checkpoint's summary is read once and kept.
- **Events:** `type` with long addresses, `data` the event's JSON (u64 and wider as decimal
  strings, so `decode` works), `eventIndex` Sui's, `sequenceNumber` `0n`, `transactionHash` the
  base58 digest, `sender`, `timestamp` from the checkpoint, and `entryFunction` (below). There is
  no `guid`.
- **Entry functions:** a Sui transaction is a programmable transaction block of several
  commands, and an event names the package and module of the `MoveCall` command whose top-level
  call emitted it, but not the command. So `entryFunction` is the one `MoveCall` on that package
  and module, `function` written `<package>::<module>::<function>` with the package the command
  called in long form; when several commands call that module, or none does (an event from a
  `Publish`'s `init`, a system transaction), the event has no `entryFunction` rather than a guess.
  `fromFunction("0x...::pool::swap")` filters on it, and `onTransaction` groups name a call only
  when all their events agree. `arguments` are the command's references into the block, resolved
  against its inputs, since a pure input is BCS bytes whose type only the function knows:

  | Argument | JSON |
  | --- | --- |
  | the gas coin | `"GasCoin"` |
  | a pure input | `{ "Pure": "<base64 BCS bytes>" }` |
  | an object input (owned, shared, immutable or receiving) | `{ "Object": "0x<64 hex>" }` |
  | another input (a funds withdrawal) | `{ "Input": <index> }` |
  | a command's result | `{ "Result": <command> }` |
  | one of a command's results | `{ "NestedResult": [<command>, <index>] }` |

  `ListEvents` cannot return the transaction, so `fetchEvents` reads each page's transactions
  with one `BatchGetTransactions` (200 digests per request), masked to commands and inputs: on
  mainnet, a 50-event page took about 960 ms against 690 ms without it. A transaction the node
  leaves out fails the page as a `StreamdNetworkError`, so a poll retries. `eventsOf` and
  `previewEvents` read the commands in the request they already make. Transfer records carry
  no `entryFunction`, as on Aptos. A structural client without `batchGetTransactions` gets
  events without it.
- **Descriptors:** `module` (a type or a list, in one request), `account` (events from
  transactions the address sent, optionally narrowed by `eventTypes`, or with `received: true`
  every transaction that affected it, below) and `object` (below). Sui has no event handles or
  account resources, so `handle` and `resource` are refused.
- **Received transactions:** `{ kind: "account", accountAddress, received: true }` delivers the
  events of every transaction the node lists as affecting the address: the ones it signed, and
  the ones it only received, such as coins someone else sent it, which the plain `account`
  descriptor never sees. It walks `ListTransactions` filtered by affected address, reading each
  transaction's events and commands in the same request, so events are exactly what a `module`
  walk delivers for them (`version`, `eventIndex`, digest, `timestamp`, `sender` the signer,
  `entryFunction`), and `eventTypes` narrows them. A transaction that affected the address but
  emitted no events (a plain coin transfer often emits none) delivers nothing; follow those with
  `sui.transfers({ account })`. `limit` counts transactions. `received` must be a boolean. It is a
  core option, so `FullnodeScanAdapter` (Aptos sees received transactions through
  `IndexerAccountAdapter`, which always includes them), `StoreAdapter`, `PostgresStoreAdapter`,
  and `eventsOf`/`previewEvents` narrowing refuse it rather than answer with signed transactions
  only; `received: false` is the same as leaving it out.

  ```ts
  sui.on({ kind: "account", accountAddress: me, received: true }, (e) => console.log(e.sender, e.type));
  ```
- **Object changes:** `{ kind: "object", objectId }` delivers every change to one object, Sui's
  counterpart of `resource`: created, mutated, transferred, wrapped, unwrapped or deleted, whoever
  sent the transaction. It walks `ListTransactions` filtered to the transactions whose effects
  change the object, so the node does the narrowing. Each change is a `StreamdEvent`: `type` the
  object's type in long form, `data` its contents (JSON, as events have it) once the transaction
  ran, or `null` when it deleted or wrapped the object, `sequenceNumber` `0n`, `eventIndex` the
  transaction's event count plus the change's position among its object changes, and the
  transaction's digest, `timestamp` and `sender`; no `entryFunction`. `data` is the contents only:
  an owner change with unchanged contents still arrives, with the same `data`. `objectId` may be
  short (`0x6`). A wrap and an unwrap follow from the effects (`null`, then contents again) but
  were not seen on mainnet when this was built. An object held inside another changes with its
  parent and has no change of its own, and a dynamic field changes on its `dynamic_field::Field`
  object, not the parent. A shared object such as the Clock (`0x6`) changes in nearly every
  checkpoint, and every page reads the JSON of every object in each transaction, so follow busy
  objects with care. `eventsOf` and `previewEvents` refuse it, as they refuse `resource`, and so
  do `MemoryAdapter`, `FullnodeScanAdapter` (follow an Aptos object's resources instead) and
  `checkDescriptor`; `RoutingAdapter` sends it to its `default`.

  ```ts
  sui.on({ kind: "object", objectId: pool }, (change) => console.log(change.version, change.data ?? "deleted"));
  ```
- **Object state at the event:** no public Sui endpoint runs Move at a past state, so `SuiAdapter`
  has no `view` and a view request from `withState` rejects. Return an object read instead,
  `{ objects: [id, ...] }`, and `event.state` holds one entry per id, in order, as the event's own
  transaction left the object: its contents (JSON, as an `object` change's `data`) after the
  transaction for an object it created or mutated, as read for a consensus object it read without
  writing (the Clock, in most DeFi transactions), and `null` for one it deleted or wrapped. The
  versions come from the transaction's effects, so the state is exact, not the end of its
  checkpoint. Only objects in the effects have such a state: any other id, an immutable object or
  a package rejects, and so does an event without a `transactionHash`. Read the field behind a
  versioned wrapper (a DeepBook pool's `inner`) by its own id when the transaction changed it.
  Each event costs one `GetTransaction`, counted by `maxConcurrentRequests`; once the fullnode has
  pruned the transaction (about two weeks) it costs one POST to Sui GraphQL instead, which reaches
  back to genesis. The GraphQL endpoint is the preset's for the client's `network`
  (`https://graphql.<network>.sui.io/graphql`, Mysten Labs' service even when `grpc` reads a private
  node), or `new SuiAdapter(grpc, { graphqlUrl })`; without either, a pruned read rejects. Failures follow the `withState` rules: a GraphQL HTTP failure or
  error is a `StreamdNetworkError` (429 with `retryAfter`), a refused id a `StreamdError`.

  ```ts
  sui.on(SwapEvent, (e) => console.log(e.state), {
    withState: (e) => ({ objects: [(e.data as { pool: string }).pool] }),
  });
  ```
- **Blocks are checkpoints:** `getBlock(descriptor, height)` takes a checkpoint sequence number
  and returns its versions (the previous checkpoint's total up to its own, minus one) and its
  timestamp, and `getBlockHeights()` the node's lowest and latest checkpoints. So `fromTime`
  starts at the first checkpoint stamped at or after it, and `fromBlock`/`toBlock` bound a query
  by checkpoint. A checkpoint the node has pruned fails with `410`, one not yet produced with
  `404`.
- **Transaction lookups:** `sui.eventsOf(digest, descriptor?, options?)` waits for the
  transaction with that base58 digest to be checkpointed and returns its events, and
  `sui.expectEvents(digest).toEmit(descriptor)` / `notToEmit` assert on them, with the same
  options, timeout and errors as on Aptos (the wait loop is core's, shared by both chains). A
  descriptor's types and address may be short (`0x3::validator::...`). A failed transaction
  rejects with `StreamdTransactionError` whose `vmStatus` is the execution error (`MoveAbort(...)
  in command 1`); one never checkpointed within `timeout` with `StreamdTimeoutError`; a `0x` hex
  hash is refused. An event's `version` and `eventIndex` match what `on()` and `getEvents`
  deliver for it. `SuiAdapter.getTransactionEvents` reads the checkpoint's digest list to place
  the transaction, since the node does not report its index.
- **Simulations:** `sui.previewEvents(bytes, descriptor?, { decode?, where? })` simulates built,
  unsigned transaction bytes and returns the events they would emit, and
  `sui.expectPreview(bytes).toEmit(descriptor)` / `notToEmit` assert on them, each assertion
  simulating afresh. `bytes` is what `await tx.build({ client })` returns; nothing is signed or
  submitted, so any sender can be simulated with its own coins as gas. Building a `Transaction`
  with a client already simulates it once to pick gas, and one that aborts fails there with the
  SDK's error, so pass bytes rather than a `Transaction`. A simulated transaction has no
  checkpoint: its events carry the next version the chain would give a transaction (one past the
  head at the time) and no `transactionHash` or `timestamp`. A failed simulation rejects with
  `StreamdTransactionError` whose `vmStatus` is the execution error and whose `transactionHash` is
  undefined; a request the node refuses (such as a gas coin the sender does not own) with
  `StreamdNetworkError`. `SuiAdapter.simulateTransactionEvents` goes through the client's
  `transactionExecutionService`.

  ```ts
  const tx = new Transaction();
  tx.setSender(staker);
  const [stake] = tx.splitCoins(tx.gas, [1_000_000_000n]);
  tx.moveCall({ target: "0x3::sui_system::request_add_stake", arguments: [tx.object("0x5"), stake, tx.pure.address(validator)] });
  const events = await sui.previewEvents(await tx.build({ client: grpc }));
  ```
- **Transfers:** `sui.transfers(callback, { asset?, account?, withAssetInfo? })` and
  `sui.getTransfers(filter)` deliver `Transfer { from, to, asset, amount }` records, as on Aptos.
  Sui has no transfer events, so they are built from each transaction's balance changes (each
  address's net change per coin type), walked with `ListTransactions`. The gas fee is taken out of
  the payer's SUI change first, so a fee is never a transfer and a failed transaction yields
  none. Then, per coin type, each receiver takes from the earliest sender with outflow left, as
  much as both have: inflow no sender covers (a mint, or coins out of a pool) has no `from`,
  outflow no receiver took has no `to`, and each address's records sum to its change. A record
  keeps the transaction's `version`, digest, `timestamp` and `sender`; its `type` is the coin
  type, and its `eventIndex` the transaction's event count plus its position among the records,
  so it never shares an id with an event. `asset` is a coin type in any form; `account` an
  address, narrowed on the node to transactions affecting it, which is what makes a live
  subscription cheap: unnarrowed, a walk reads every transaction on the chain, and following
  mainnet needs a `batchSize` near the node's 500-transaction page. `withAssetInfo` adds `symbol`
  and `decimals` from the coin's metadata, and `formatAmount` applies them. Object (NFT)
  transfers are not balance changes and are not reported.

  ```ts
  sui.transfers((t) => console.log(t.data.from, "->", t.data.to, formatAmount(t.data.amount, 9)), { account: me, asset: "0x2::sui::SUI" });
  ```
- **Balances:** `sui.balances(address, { asset?, onChange? })` keeps an address's coin balances
  current, reading each coin type (SUI when none is named) through the state service every
  `pollInterval`, keyed by coin type in long form, with `value`, `get`, `ready` and `onChange`
  as on Aptos. Sui reads a balance only at its latest state, so a change's `version` is the head
  read just before it. A coin type held nowhere reads `0`.
- **Names:** `resolveName(grpc, nameOrAddress)` turns a SuiNS name into the long-form address it
  points to, through the node's name service at its head, and `primaryName(grpc, address)` returns
  an address's default name (`alice.sui`) or `undefined`, resolved forward as a check so a name
  that no longer points back counts as none, as on Aptos. `grpc` is a `SuiGrpcClient`. Both forms
  work, `alice.sui` and `@alice`, subnames included (`pay.alice.sui`, `pay@alice`); a string
  starting `0x` is always an address. An unregistered or expired name, a registered one with no
  target address (many have none), a malformed one, and any name on a network without SuiNS
  (devnet) reject with a `StreamdError`; a node or transport failure with a `StreamdNetworkError`.
  `sui.transfers` and `sui.getTransfers` take a name as `account`, and `sui.balances` as its
  address, each resolved once on first use, a failed lookup retried. Descriptors take addresses
  only.

  ```ts
  import { primaryName, resolveName } from "@streamd/sui";

  const alice = await resolveName(grpc, "alice.sui");
  console.log((await primaryName(grpc, event.sender!)) ?? event.sender);
  ```
- **Owned objects:** `ownedBy(grpc, wallet, { fields?, maxConcurrentRequests? })` is an async
  `where` predicate matching events whose `data` names an object `wallet` owns, directly or
  through objects it owns. It reads the top-level `id`, `object_id` and `nft_id` fields in order
  (kiosk `ItemListed`/`ItemPurchased`/`ItemDelisted` name the item by `id`), each counting when it
  is an address string; `fields` replaces the list, and an event naming none costs no read. Each
  hop up the owner chain is one object read, at most 8; `wallet` may be an address, an object id
  (objects own objects on Sui) or a name, resolved once. Shared, immutable, deleted and wrapped
  objects are not owned, nor is a dynamic field of a table or bag, whose parent id is not an
  object of its own. Answers are cached per object and event version (1000 entries), and
  `maxConcurrentRequests` (default 4) caps reads in flight, outside the client's limiter.
  **Ownership is read at the head, not at the event's version:** Sui reads an object at one of
  its own versions, not a ledger version, and nothing maps an event's version to the version an
  object had then, so a live listener sees current owners and a history query today's owners. A
  transport failure rejects with `StreamdNetworkError`, so a listener retries the event.

  ```ts
  sui.on({ kind: "module", eventType: "0x2::kiosk::ItemListed" }, onListed, { where: ownedBy(grpc, "alice.sui") });
  ```
- **Descriptor check:** `checkDescriptor(grpc, descriptor)` reads each type's package through the
  node's package service (one read per package) and returns `{ ok, types }` with a status per
  type, as on Aptos. Sui has no `#[event]` attribute: `sui::event::emit` takes any struct or enum
  with `copy` and `drop`, so those abilities are what makes a type an `event`, and there is no
  `legacy`. Types are reported in long form, deduped on it, and a generic type is checked by its
  outer datatype only. An `account` descriptor's address is validated (Sui has no account object
  to look up) and its `eventTypes` checked like a `module` descriptor's; no `eventTypes` is `ok`
  with no entries, an empty list is not. `handle` and `resource` are refused. `streamd check`
  reads Aptos only for now.

  | Status | Meaning |
  | --- | --- |
  | `event` | The struct or enum has `copy` and `drop`. |
  | `not-event` | It lacks `copy` or `drop`, so no event can carry it. |
  | `missing-package` | No package at that address, or the object there is not a package. |
  | `missing-module` | The package has no such module. |
  | `missing-struct` | The module declares no such struct or enum. |
  | `defined-elsewhere` | The address is an upgraded package version, but an earlier version introduced the type, and events name it by that version's id, given as `definingId`. |

  `ok` is true when every type is `event`.

  ```ts
  import { checkDescriptor } from "@streamd/sui";

  const check = await checkDescriptor(grpc, { kind: "module", eventType: "0x3::validator::StakingRequestEvent" });
  // { ok: true, types: [{ eventType: "0x00...03::validator::StakingRequestEvent", status: "event" }] }
  ```
- **Framework events:** `@streamd/sui/framework` ships a typed descriptor for every `copy` and
  `drop` type in `0x2`'s `coin`, `deny_list`, `display`, `kiosk`, `token` and `transfer_policy`
  and `0x3`'s `sui_system_state_inner`, `validator` and `validator_set`, generated by
  [`streamd codegen --chain sui`](#sui-packages) from committed snapshots of the `0x1`, `0x2` and
  `0x3` packages, so it follows that command's rules: long-form type strings, generic events as
  functions of their type arguments, and descriptors for the dynamic-field keys those modules
  declare too (`Item`, `Listing`, `Lock`, `ConfigKey` and the like), which never match an event.

  ```ts
  import { ItemListed, ItemListedDecode, StakingRequestEvent } from "@streamd/sui/framework";

  sui.on(StakingRequestEvent, (e) => console.log(e.data.validator_address, e.data.amount));
  sui.on(ItemListed("0x<package id>::nft::Nft"), (e) => console.log(e.data.kiosk), { decode: ItemListedDecode });
  ```
- **History** reaches back as far as the node keeps checkpoints, about two weeks on public nodes;
  a version before that fails with `410`. Object state at the event reads past it, through GraphQL,
  and the mainnet archive serves the rest ([below](#sui-history-past-the-fullnode)).
- **Command line and React:** every `streamd` command takes [`--chain sui`](#sui-from-the-command-line),
  and `useTransfers` and `useTransactionEvents` take a `SuiClient` (see [React hooks](#react-hooks)).

### Spreading load across fullnodes

`PoolAdapter` shares reads among several fullnodes of one chain, such as your own node and a
provider's, so no single one sets the rate:

```ts
import { PoolAdapter, StreamdClient } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: new PoolAdapter([new FullnodeScanAdapter(ownNode), new FullnodeScanAdapter(providerNode)], {
    cooldown: 30_000, // the default
  }),
});
```

- **Each walk stays on one node.** A read with no cursor takes the next node in turn, and the
  cursors its pages return lead back to that node, since nodes trail one another by a few
  versions. The 1024 most recent cursors are remembered; an older one starts a new walk.
- **A rate-limited node is set aside.** A `429` sets that node aside for its `Retry-After`, or
  `cooldown` without one, and the same call moves to the next node; the walk carries on there.
  With every node set aside, the `429` reaches the subscription's backoff. Other failures are
  rethrown: for failover, wrap the pool in `FallbackAdapter`, or the nodes in it.
- The other adapter methods rotate the same way, and exist only when every node has them.

### History the fullnode has pruned

The public Aptos mainnet fullnode keeps about two weeks of history and answers older reads with
`410`. Its archive node keeps everything from version 0. `ArchiveFallbackAdapter` sends only
those `410`s to it:

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { ArchiveFallbackAdapter, StreamdClient } from "@streamd/core";
import { FullnodeScanAdapter, networks } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: new ArchiveFallbackAdapter(
    new FullnodeScanAdapter(new Aptos(new AptosConfig({ network: Network.MAINNET }))),
    new FullnodeScanAdapter(new Aptos(new AptosConfig(networks.aptosMainnetArchive))),
  ),
});
await streamd.getEvents(NewBlockEvent, { fromVersion: 1000n, limit: 10 }); // served by the archive
```

- **Only pruned reads move.** Event pages, blocks, transaction lookups (by hash and by
  version) and `withState` views
  go to the archive when the fullnode answers `410`; a rate limit or an outage is rethrown, so
  backoff and `maxErrors` work as usual. Each call starts at the fullnode, so a walk returns to
  it once it passes the pruned edge.
- **Bounds:** the head and simulations come from the fullnode, which the archive trails by a
  few dozen versions; `fromVersion: "earliest"` and `fromTime` see the archive's full range.
- **Rate.** Anonymously the archive allowed about 43 requests per five minutes in a 2026-11
  measurement, and its `429` carries no `Retry-After`. That is enough for a handle's early
  events or a narrow range, not for days of module scans: pass an API key in the archive's
  `clientConfig` for more.

For `account` descriptors that see every transaction touching the account, received
ones included, use `IndexerAccountAdapter`. It pages the Aptos Indexer's
`account_transactions` table by version, then reads each transaction's events from the
fullnode, one read per transaction. It serves `account` descriptors only, so route the
other kinds elsewhere:

```ts
import { RoutingAdapter, StreamdClient } from "@streamd/core";
import { FullnodeScanAdapter, IndexerAccountAdapter } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: new RoutingAdapter({
    account: new IndexerAccountAdapter(aptos, { apiKey: process.env.APTOS_API_KEY }),
    default: new FullnodeScanAdapter(aptos),
  }),
});
```

- **Indexer lag.** Reads stop at the version the Indexer has processed, usually about a
  second behind the chain; `getHeadVersion` reports it, and a subscription with no start
  begins there.
- **Pruning.** The Indexer lists history the fullnode has pruned, so `fromVersion` is
  raised to the fullnode's oldest version (`getEarliestVersion`). A cursor older than that
  fails with status 410 rather than skipping history.
- **Rate limits.** Anonymous Indexer access is rate limited per IP; a limit hit rejects as a
  `StreamdNetworkError` with status 429. Pass `apiKey` (sent as a bearer token) for more.
- **Options.** `indexerUrl` (default: the `Aptos` network's Indexer), `apiKey`, `fetch`, and
  `maxScan`, the most transactions read per call (default 100). `offset` is refused, and
  pages are never `final`, so `CachingAdapter` passes them through.

To log or time requests, wrap any adapter with `withHooks`. `onRequest` runs
before each `fetchEvents` call and `onResponse` after it settles, with the
`page` or the `error` and `durationMs` for the source call alone (time spent
waiting on `maxConcurrentRequests` is not included). Hooks are synchronous; one
that throws fails that fetch. `getHeadVersion`, `getEarliestVersion` and `getBlock` pass
through unhooked:

```ts
import { StreamdClient, withHooks } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: withHooks(new FullnodeScanAdapter(aptos), {
    onRequest: ({ descriptor }) => console.log("fetch", descriptor.kind),
    onResponse: ({ durationMs, page }) =>
      console.log(page ? `${page.events.length} events` : "failed", durationMs),
  }),
});
```

For ready-made numbers, wrap the adapter with `adapterMetrics`, which counts every method the
adapter has, where `withHooks` sees only `fetchEvents`:

```ts
import { StreamdClient, adapterMetrics } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const metrics = adapterMetrics(new FullnodeScanAdapter(aptos));
const streamd = new StreamdClient({ adapter: metrics.adapter });

metrics.snapshot();
// { fetchEvents: { requests: 12, errors: 1, rateLimited: 1, latency: { p50, p95, p99, max } }, view: ... }
metrics.reset();
```

`requests` counts calls started, `errors` those that failed, and `rateLimited` the failures with
status 429 or a `retryAfter` hint. Latency is in milliseconds over the most recent 1024 settled
calls per method. A method that has not been called is absent.

To skip the source when re-reading settled history, wrap an adapter with
`CachingAdapter`. It stores only pages the adapter marks `final: true`,
meaning the same request will always return the same page, and passes
everything else through. `FullnodeScanAdapter` marks a page final when it
started from a cursor, offset or `fromVersion` and read nothing at or past the
chain head; a page that tails the head is never cached. Entries are dropped
least recently used past `maxEntries` (default 1000):

```ts
import { CachingAdapter, StreamdClient } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: new CachingAdapter(new FullnodeScanAdapter(aptos), { maxEntries: 500 }),
});
```

Pass `store` to keep final pages past a restart. `@streamd/store-sqlite` ships
one, `@streamd/store-postgres` another (`PostgresPageStore`, see
[Store events in Postgres](#store-events-in-postgres)), and anything with `get(key)` and `set(key, page)` over strings, synchronous
or not, works:

```ts
import { Database } from "bun:sqlite";
import { SqlitePageStore } from "@streamd/store-sqlite";

const store = new SqlitePageStore(new Database("pages.db"), { namespace: "mainnet" });
const adapter = new CachingAdapter(new FullnodeScanAdapter(aptos), { store });
```

Memory is checked first, then the store, then the source. A store that fails or
holds an unreadable entry counts as a miss and reaches `onStoreError`, so a broken
cache never fails a read. A page's key names the request and not the network, so
`namespace` is required: use one per network. `maxEntries` on either store
drops the oldest written; without it the table grows without limit. A `bigint`
inside an event's `data` comes back as a decimal string, as `serializeEvent`
writes it.

A cached page keeps being served after the fullnode prunes that history, where
the node itself would answer 410. A custom adapter opts in by setting `final`
on pages that cannot change; leave it unset when unsure. `RoutingAdapter` and
`withHooks` pass the flag through, `FallbackAdapter` keeps it only on pages
from its first source, and `MemoryAdapter` never sets it.

To capture a real session once and replay it offline, wrap an adapter with
`recordingAdapter`. Every call that succeeds for one of the three descriptor
kinds is kept, `fetchEvents` and the optional `getHeadVersion`,
`getEarliestVersion`, `getBlock`, `getBlockHeights`, `getTransactionEvents` (by
hash) and `getTransactionEventsByVersion` (by version), and `JSON.stringify(recorder)` gives the recording.
`replayAdapter` takes that object or its JSON text and answers the same
requests with no network:

```ts
import { readFile, writeFile } from "node:fs/promises";
import { StreamdClient, recordingAdapter, replayAdapter } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const recorder = recordingAdapter(new FullnodeScanAdapter(aptos));
await new StreamdClient({ adapter: recorder }).getEvents(descriptor, { fromVersion: 3_000_000_000n });
await writeFile("events.recording.json", JSON.stringify(recorder));

// later, offline
const adapter = replayAdapter(await readFile("events.recording.json", "utf8"));
const events = await new StreamdClient({ adapter }).getEvents(descriptor, { fromVersion: 3_000_000_000n });
```

Requests match on the descriptor and every `FetchOptions` field, as
`CachingAdapter` keys them, so replay the same calls with the same options.
Identical requests get their recorded results in the order they were issued,
then the last one again, so a replayed `on()` sees what the recorded one saw.
Any other request rejects with a plain `StreamdError` (not a
`StreamdNetworkError`), and failures are never recorded, so a replayed
subscription stops once it asks for a page the recording never saw. Events
are stored as `serializeEvent` writes them, so a `bigint` inside `data` comes
back as a decimal string; fullnode data has none.
The library writes no files itself. A malformed recording throws a
`StreamdError` when `replayAdapter` reads it.

For offline unit tests, pass `MemoryAdapter` with a list of events (`aptos` is
optional when a custom `adapter` is supplied):

```ts
import { MemoryAdapter, StreamdClient } from "@streamd/core";

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

`MemoryAdapter.fromJsonLines(text, { decode? })` builds one from saved
`streamd tail` output, so a captured stream can drive tests and `replay`:

```ts
const text = await readFile("deposits.jsonl", "utf8");
const streamd = new StreamdClient({ adapter: MemoryAdapter.fromJsonLines(text) });
```

Each event is kept once, since a resumed `tail` can repeat lines. Blank lines
are ignored, and any other unreadable line throws a `StreamdError` naming its
line number, a truncated last line included.

`adapter.push(...events)` appends to a running `MemoryAdapter`, so a test can
drive a live subscription step by step:

```ts
const adapter = new MemoryAdapter();
const streamd = new StreamdClient({ adapter });
const next = streamd.once({ kind: "module", eventType: "0x1::test::Ping" });
adapter.push({
  type: "0x1::test::Ping",
  data: {},
  version: 1n,
  sequenceNumber: 0n,
  eventIndex: 0,
});
await next;
```

Each pushed event must be above the highest version the adapter holds, since
a transaction never appears below one already read. Events in one call may
share a version. Anything else throws a `StreamdError` and adds nothing.

#### Sui history past the fullnode

Mysten Labs' mainnet archive keeps every checkpoint from 0 and answers the same gRPC
`LedgerService`, with cursors that mean the same as the fullnode's, so the same
`ArchiveFallbackAdapter` pairs it with the fullnode. It speaks native gRPC only: the gRPC-web
transport `SuiGrpcClient` uses by default gets `404`, and browsers have nothing else, so this is
for Node and Bun. `networks.mainnetArchive(transport)` takes a native transport you install and
build yourself (streamd adds no dependency for it), aimed at `networks.mainnetArchive.host`, and
returns the options for `new SuiGrpcClient(...)`. It refuses anything that is not a transport, so
a URL cannot be passed by mistake.

```ts
// bun add @protobuf-ts/grpc-transport @grpc/grpc-js
import { ChannelCredentials } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { ArchiveFallbackAdapter } from "@streamd/core";
import { SuiAdapter, SuiClient, networks } from "@streamd/sui";

const fullnode = new SuiGrpcClient(networks.mainnet);
const transport = new GrpcTransport({ host: networks.mainnetArchive.host, channelCredentials: ChannelCredentials.createSsl() });
const archive = new SuiGrpcClient(networks.mainnetArchive(transport));

const sui = new SuiClient({ sui: fullnode, adapter: new ArchiveFallbackAdapter(new SuiAdapter(fullnode), new SuiAdapter(archive)) });
await sui.getEvents(StakingRequestEvent, { fromVersion: 2_948_762_402n, limit: 10 }); // a year back, from the archive
```

- **What moves:** event pages for every descriptor (`received` account walks and `object` changes
  included), blocks, and `withState` object reads, whenever the fullnode answers `410`. The
  earliest version (0) and oldest checkpoint come from the archive, in one read; the head and
  simulations from the fullnode. A start a year back cost about 14 seconds when measured, most of
  it the bisection that places the version among the archive's checkpoints, read once per version.
- **Object state:** with a GraphQL endpoint (the default on a preset network) the fullnode's
  adapter reads a pruned transaction's objects through GraphQL itself; without one it answers
  `410`, and the archive reads them with `GetTransaction`.
- **What stays on the fullnode:** `sui.transfers`, `getTransfers` and `balances` read the `sui`
  client the `SuiClient` was built with, not its `adapter`, so a pruned range fails with `410`;
  for older transfers build a `SuiClient` over the archive client itself. `eventsOf` waits on the
  fullnode, which answers a pruned digest and an unknown one alike, as not yet committed.
- **Not measured:** the archive's rate limits beyond a 40-request burst, and whether Mysten Labs
  keeps it as a supported public service.

## Networks

streamd runs against any network that serves the Aptos fullnode REST API. For
Aptos mainnet, testnet, devnet and localnet, use the SDK's own `Network`, which
already knows their fullnode and Indexer URLs:

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";

const aptos = new Aptos(new AptosConfig({ network: Network.TESTNET }));
```

For other Aptos-compatible chains, `networks` holds presets streamd has been
verified against. Each is shaped as SDK settings, so it passes straight to
`AptosConfig`, and `IndexerAccountAdapter` picks up its Indexer:

```ts
import { Aptos, AptosConfig } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "@streamd/core";
import { IndexerAccountAdapter, networks } from "@streamd/aptos";

const aptos = new Aptos(new AptosConfig({ ...networks.movementMainnet, clientConfig: { http2: false } }));
const streamd = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos) });
const received = new IndexerAccountAdapter(aptos);
```

| Preset | Chain id | Fullnode | Indexer |
| --- | --- | --- | --- |
| `movementMainnet` | 126 | `https://mainnet.movementnetwork.xyz/v1` | `https://indexer.mainnet.movementnetwork.xyz/v1/graphql` |
| `movementTestnet` | 250 | `https://testnet.movementnetwork.xyz/v1` | `https://indexer.testnet.movementnetwork.xyz/v1/graphql` |

The SDK prints `Note: using CUSTOM network will require queries to lookup
ChainId` for every `AptosConfig` built on a preset; it is informational.

### What differs, as measured

Measured on 2026-11-07 with anonymous, read-only requests (about 80 in all)
against each network's public endpoint. Each network was exercised on ledger
info, `getHeadVersion` and `getEarliestVersion`, module `getEvents`, the `account` path, the `0x1` `new_block_events` handle,
`fromBlock`/`toBlock`, `fromTime`, `checkAdapter` on `FullnodeScanAdapter`,
and the Indexer's processed version.

| | Aptos testnet | Aptos devnet | Movement mainnet | Movement testnet |
| --- | --- | --- | --- | --- |
| Chain id | 2 | 226, which changes when devnet resets; the SDK lists none | 126 | 250 |
| Oldest version kept | 213,894,770 of 363,694,761 (pruned) | 0 (full history) | 0 (full history) | 0 (full history) |
| Oldest block kept | 17,651,703 of 25,994,004 | 0 | 0 | 0 |
| Indexer | yes (SDK default) | yes (SDK default) | yes | yes |
| `checkAdapter` | ok | ok | ok | ok |
| Handle from sequence 0 | refused, 410 `version_pruned` (oldest sequence 17,640,121) | ok | ok | ok |
| `fromTime` | failed once: the oldest block was pruned mid-search (410 `block_pruned`) | ok | ok | ok |
| Transfer events | not sampled | `0x1::fungible_asset::Withdraw`/`Deposit` | legacy `0x1::coin::WithdrawEvent`/`DepositEvent` | legacy `0x1::coin::WithdrawEvent`/`DepositEvent` |
| Block close | not sampled | `block_epilogue_transaction` | `state_checkpoint_transaction` | not sampled |
| Traffic | busy | 2 user transactions in the last 100 versions | none in the last 100 versions | low |
| Rate limiting | no 429 seen | no 429, no rate-limit headers | no 429, no rate-limit headers | no 429, no rate-limit headers |

Notes from the same run:

- **Orderless transactions on Aptos testnet.** The busy sender sampled there
  reported `sequence_number` 0 and `/accounts/{address}/transactions` returned
  nothing for it, so the `account` path sees none of its transactions.
  `IndexerAccountAdapter` listed them.
- **Low-traffic chains.** On devnet and both Movement networks a module query
  over the last 100 versions found no `FeeStatement` at all; on both Movement
  networks the same query at a known user transaction found one. Widen `fromVersion` or raise `maxScan` there.
- **Movement fullnodes** report `git_hash` `4be787e` and no `encryption_key` in
  ledger info, and keep every version from genesis, so `fromVersion: "earliest"`
  is version 0.
- **Movement testnet's Indexer** answers at both
  `indexer.testnet.movementnetwork.xyz` and `hasura.testnet.movementnetwork.xyz`
  with the same data; the preset uses the first, which Movement's Indexer docs name.
- **Not verified:** Aptos localnet (`Network.LOCAL`), since none was running.
- **SDK ledger cache.** `@aptos-labs/ts-sdk` caches `aptos.getLedgerInfo()` for
  10 seconds keyed by network name, so every `Network.CUSTOM` instance in one
  process shares it: a Movement testnet instance reported chain id 126.
  streamd's adapters read ledger info directly and are not affected; your own
  `getLedgerInfo()` calls are.

## Store events in SQLite

`@streamd/store-sqlite` keeps events in a SQLite database you open with whichever driver your
runtime has: `bun:sqlite`'s `Database`, `better-sqlite3`, or Node's `node:sqlite`
`DatabaseSync`. It has no SQLite dependency of its own, and core never depends on it.

```ts
import { Database } from "bun:sqlite";
import { StreamdClient } from "@streamd/core";
import { SqliteEventStore, StoreAdapter, storeEvents } from "@streamd/store-sqlite";

const db = new Database("events.db");
const store = new SqliteEventStore(db);

// Follows the chain, writing each batch and the subscription's cursor in one transaction.
// After a restart the same call resumes from the cursor saved under "swaps".
storeEvents(streamd, store, descriptor, { name: "swaps", fromVersion: -100_000n });

// Elsewhere, queries answered from the database instead of the fullnode.
const local = new StreamdClient({ adapter: new StoreAdapter(db) });
const recent = await local.getEvents(descriptor, { fromVersion: -1_000n, toVersion: "head" });
```

- **No gaps, no duplicates.** Rows are keyed on `(version, eventIndex)` and a repeated write
  replaces its row. Each batch is written with the cursor from before it, then the cursor
  after it is saved, so a restart in between reads the batch again and the upsert absorbs it.
  The cursor is also saved when the subscription first catches up, which pins a relative or
  head start; until then a restart re-resolves it, so pass an absolute `fromVersion` to leave
  no window at all.
  A failed write reaches `onError` and ends the subscription before its cursor moves on;
  start it again to retry. Give each descriptor its own `name`, and set `PRAGMA busy_timeout`
  when another connection writes the same file, since a busy database is a failed write.
  `store.write(events, { name, cursor })` (a savepoint, so it works inside your own
  transaction) and `store.readCursor(name)` are there for wiring your own.
- **Exact integers.** Versions, sequence numbers, timestamps and guids round-trip as `bigint`
  up to u64 (u128 for `creationNumber`). Events are stored as `serializeEvent` writes them,
  so a `bigint` inside `data` comes back as a decimal string: query with `decode` to restore it.
  `entryFunction` is kept in that stored JSON, with no column of its own, so it comes back on
  reads but filtering on it happens in `where`, not in SQL.
- **What `StoreAdapter` matches.** `module` by event type; `account` by the stored `sender`,
  so events stored without one (from `handle` descriptors) never match; `handle` by
  `creationNumber` against the stored `guid` (`listEventHandles` returns it), and a struct
  and field handle is refused with a `StreamdError`. It sees only what was stored, from
  whichever subscription stored it. Following the store with `on()` is gap-free only while
  rows arrive in version order, since its cursor is a position in the table; it sets no
  `throughVersion` for the same reason.
- **Head and earliest** are the highest and lowest stored versions across the whole store,
  `0n` for a store that was never pruned and holds nothing, so relative `fromVersion` and
  `toVersion: "head"` count from what is stored. After a prune neither reports below the
  edge. With no `cursor` or `fromVersion` a fetch starts at the earliest stored event.
- **Retention.** `store.prune({ beforeVersion })` deletes every event below that version in
  one transaction and leaves saved cursors alone. A `StoreAdapter` read whose `fromVersion`,
  `cursor` or `toVersion` is below that edge then fails with a `StreamdNetworkError` (status
  410), as a pruned fullnode does, rather than returning a short history. That includes a
  relative `fromVersion` reaching below it, and a follower on a rare event: its cursor stays
  at its last match, so it fails once the edge passes that match even if nothing it wanted
  was deleted. Start such a follower again from `fromVersion: "earliest"`. With no start a
  fetch begins at the oldest version kept. The edge only rises, and is kept under the
  reserved cursor name `streamd:pruned-before`. SQLite reuses the freed pages; run `VACUUM`
  yourself to shrink the file.
- **Saved cursors.** `store.cursors()` lists every saved cursor as `{ name, cursor }`, sorted
  by name in byte order (of UTF-8, SQLite's default encoding), and `store.deleteCursor(name)` removes one, returning `true` when
  there was one and `false` otherwise. Deleting a cursor leaves the events it stored. A
  subscription still running under that name saves it again on its next batch, so stop it
  first. The reserved `streamd:pruned-before` is never listed, and deleting it throws, since
  that would lower the edge and let reads below pruned history through.
- It passes `checkAdapter`. The schema (`streamd_events`, `streamd_cursors`) is created on
  first use.

## Store events in Postgres

`@streamd/store-postgres` does what `@streamd/store-sqlite` does, over a Postgres client you
supply: anything with `query(text, params)` resolving to `{ rows }`, which `pg`'s `Client` and
`Pool` and PGlite already are. It has no driver dependency, and the tables are created on first
use.

```ts
import pg from "pg";
import { StreamdClient } from "@streamd/core";
import { PostgresEventStore, PostgresStoreAdapter, storeEvents } from "@streamd/store-postgres";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const store = new PostgresEventStore(pool);

// Resolves once it has read the cursor saved under "swaps"; a restart resumes from there.
const sub = await storeEvents(streamd, store, descriptor, { name: "swaps", fromVersion: -100_000n });

const local = new StreamdClient({ adapter: new PostgresStoreAdapter(pool) });
```

With `Bun.sql`, wrap it: `{ query: async (text, params = []) => ({ rows: await sql.unsafe(text, params) }) }`.
From the terminal, [`streamd store --postgres <url>`](#store-events) does this for you.

- **One statement per write.** A batch and its cursor are written by a single `INSERT` whose
  parts are data-modifying CTEs, so they land together without a transaction, and a `Pool`
  that runs each query on any connection is safe. Rows go in as one JSON parameter, so no
  driver's array encoding matters and the parameter count is the same for any batch.
- **No gaps, no duplicates,** as with SQLite: rows are keyed on `(version, eventIndex)`, a
  repeated write replaces its row, and the cursor moves only after its batch is stored.
- **Queryable.** `streamd_events` holds `version` (`numeric`, exact for every u64), `event_index`,
  `type`, `sender`, `guid_address`, `guid_creation` and the event as the JSON text `serializeEvent`
  writes, so `SELECT event::jsonb->'data'->>'amount' FROM streamd_events WHERE type = '...'` works.
  It is `text` rather than `jsonb` because `jsonb` cannot hold a NUL character, which on-chain
  strings may carry; the cast fails only on such a row. Saved cursors are in `streamd_cursors`.
- `PostgresStoreAdapter` serves `module`, `account` (by stored `sender`) and `handle` (by
  `creationNumber`) descriptors, with the SQLite adapter's cursors, paging and options. Head and
  earliest are the highest and lowest stored versions, never below a pruned edge.
- **Retention.** `await store.prune({ beforeVersion })` deletes every event below that version
  and leaves saved cursors, with the SQLite store's rules: a `PostgresStoreAdapter` read whose
  `fromVersion`, `cursor` or `toVersion` is below the edge fails with a `StreamdNetworkError`
  (status 410), a follower on a rare event fails once the edge passes its last match, a fetch with
  no start begins at the oldest version kept, and the edge only rises. The delete and the edge
  are one statement, and each read takes the edge and its page in one statement, so a prune on
  another connection of a `Pool` never lands between them. The edge is the row
  `streamd:pruned-before` in `streamd_cursors`, so a database made before `prune` existed needs no
  migration, and that cursor name is reserved. Postgres reuses the space after autovacuum; run
  `VACUUM FULL` yourself to return it to the system.
- **Saved cursors.** `await store.cursors()` and `await store.deleteCursor(name)` work as they
  do on the SQLite store, the delete itself a single statement. Names sort in byte order
  (`COLLATE "C"`) whatever the database's collation, so in a UTF-8 database both stores list
  the same names in the same order.
- **Page cache.** `PostgresPageStore` is the Postgres counterpart of `SqlitePageStore`, over
  the same client, for `CachingAdapter`'s `store`, so a pool of processes shares one cache:

  ```ts
  import { PostgresPageStore } from "@streamd/store-postgres";

  const store = new PostgresPageStore(pool, { namespace: "mainnet", maxEntries: 100_000 });
  const adapter = new CachingAdapter(new FullnodeScanAdapter(aptos), { store });
  ```

  Same `namespace`, keys and `maxEntries` (the oldest written across the table, every namespace
  together, a rewrite counting as new, no default). Pages are kept in `streamd_pages`, created
  on first use without the event tables. A write and its eviction are one statement, so two
  processes writing at once may leave the table a row or two over the bound until the next
  write, never under it. A namespace holding a NUL character is refused, since `text` cannot
  store one. `streamd query --cache` still takes a SQLite file.

  Because the bound is table-wide, one instance's `maxEntries` evicts every namespace's pages,
  so give unrelated services their own database or schema. Stored pages are trusted as they
  are: a final page is served without asking the node again, so grant write access to
  `streamd_pages` only to the processes doing the caching.

## React hooks

`@streamd/react` ties a subscription or a query to a component's lifetime.

```tsx
import { StreamdProvider, useEvents, useLogs } from "@streamd/react";

const deposits = { kind: "module", eventType: "0x1::fungible_asset::Deposit" } as const;

function App() {
  return (
    <StreamdProvider client={streamd}>
      <Feed />
    </StreamdProvider>
  );
}

function Feed() {
  const { events, error, status } = useEvents(deposits, { maxEvents: 20 });
  const { data, loading } = useLogs(deposits, { fromVersion: -1_000n, limit: 10 });
  // ...
}
```

- **`useEvents(descriptor, options?)`** follows the descriptor with `client.onBatch` and
  returns `{ events, error, status }`: the most recent `maxEvents` events (default 100), oldest
  first; the last error, cleared when events next arrive; and `status`, `"idle"` (no
  descriptor, or not mounted yet), `"live"`, or `"stopped"` once `maxErrors` is reached.
  Options are `on()`'s, without `signal`. With `history: n` and a `fromVersion` or `fromTime`,
  it first reads the `n` most recent matches between that start and the head, shows them in one
  update (`status` is `"loading"` until then), and follows live from exactly where the read
  ended, so a feed opens full rather than filling page by page:
  `useEvents(Swap, { fromVersion: -100_000n, history: 20 })`. A failed read stops the hook with
  its error.
- **`useTransactions(descriptor | descriptors, options?)`** follows one descriptor, or a list
  merged into one stream, with `client.onTransaction` and returns `{ data, error, status }`:
  `data` is the most recent `maxTransactions` transactions (default 100), oldest first, each a
  `TransactionGroup` holding all of that transaction's matching events; `error` and `status` are
  `useEvents`'s. A typed list infers the union of its payloads, as `on()` does. Options are
  `onTransaction`'s, without `signal`; an empty list, and `cursor` with several descriptors, throw
  while rendering. With `history: n` and a `fromVersion` or `fromTime`, it first reads the `n`
  most recent matching transactions through `getTransactions` and shows them in one update, then
  follows from the version after the newest of them. That hand-off misses and repeats nothing,
  since the read walks the whole window, but the live walk re-examines the versions between that
  transaction and the head once, so keep the window to what a rare type needs. With no match in
  the window it follows from the window's start, and `history` refuses `cursor`.
- **`useWatch(addresses, options?)`** follows a list of addresses with `client.watch` and
  returns `{ data, error, status, addresses }`: `data` is the most recent `maxEvents` deliveries
  (default 100), oldest first, each `{ event, address }` with the watched address as the list
  first spelled it; `addresses` is what is being watched now, in the order added. A different
  list does not restart anything: addresses that left are removed from the running watch and
  new ones added, and the list is compared by canonical address, so reordering, `0x1` against
  its long form, case and duplicates change nothing. An added address starts where
  `watch.add()` starts it (the account's next transaction on the fullnode and Indexer
  adapters), not at `fromVersion` or `fromTime`, which apply only to the addresses listed when
  the watch started. A walk that gives up after `maxErrors` sets `error` and leaves
  `addresses` while the others carry on, and it is not re-added while the list still names it:
  take the address out of the list and put it back, or change an option, to try again. A
  changed option or client restarts the watch over the current list with `data` empty. `[]`
  runs a watch with nothing in it, so the first address added later starts no new watch; `null`
  waits. `status` is `"idle"` or `"live"`, since a watch does not stop on its own. Options are
  `watch()`'s, without `signal`, `positions` included to resume a saved run; a malformed address
  throws while rendering, so check addresses a user types before listing them.
- **`useLogs(descriptor, options?)`** runs `client.getEvents` and returns
  `{ data, error, loading }`. `data` is the last successful result and stays through refetches
  and errors. Options are `getEvents`'s, without `signal`; it takes a list of descriptors too.
- **`useTransactionEvents(hash, options?)`** waits for a submitted transaction through
  `client.eventsOf` and returns `{ data, error, loading, vmStatus }`. A transaction that failed
  on chain sets `error` and `vmStatus`, the chain's status; one not found in time sets `error`
  alone. It takes `eventsOf`'s options plus `descriptor`. Nothing from the previous hash is kept,
  and a `null` hash waits for nothing. It needs a client with `eventsOf`
  (`TransactionEventsClient`): an `AptosClient`, or a `SuiClient`, which takes a base58 digest.
- **`useTransfers(options?)`** follows `client.transfers` and returns the same
  `{ events, error, status }` as `useEvents`, with each event's `data` a `Transfer`. It takes
  `asset`, `account` and `maxEvents`; pass `null` to wait. It needs a client with `transfers`
  (`TransfersClient`): an `AptosClient`, or a `SuiClient`, whose `asset` is a coin type.
  Any of these hooks given another client throws a `StreamdError` naming the method.
- **`useReduction(descriptor, reducer, initial, options?)`** runs `client.reduce` and returns
  `{ value, error, status }`. `value` is `initial` until the backfill finishes; `status` is
  `"idle"`, `"loading"`, `"live"`, or `"stopped"` once the backfill fails, the reducer throws or
  polling gives up. It restarts when the descriptor, options or `initial` change by value, and
  calls the latest `reducer` without restarting. `initial` is compared by value, a `Map` by its
  entries, so a fresh `new Map()` each render does not restart it; a class instance compares as
  empty. The ready-made reducers work here as they do with `client.reduce`:
  `useReduction(Deposit, perStore, perStore.initial)` returns a `Map<string, bigint>`.
- **`useBalances(address, options?)`** runs `client.balances` and returns
  `{ balances, error, status }`. `balances` is a `ReadonlyMap` of every balance read so far, keyed
  by asset in long form as the client keys it (APT as `0x000...00a`), empty until the first read
  finishes; `status` is `"idle"` (no address), `"loading"`, `"live"` once the first read is in,
  or `"stopped"` once that read fails, polling gives up, or the address or `asset` is malformed,
  which shows as `error` rather than throwing. It re-renders only when a balance changes, not on
  every poll, and restarts, emptying `balances`, when the address or `asset` change by value. It
  takes `balances`' options, without `signal`; `onChange` is called in its latest form. A
  `null`, `undefined` or empty address waits. It needs a client with `balances`
  (`BalancesClient`): an `AptosClient`, or a `SuiClient`, keyed by coin type.
- **Keyed by value.** Inline descriptors and options are compared by value (bigints
  included), so a re-render with an equal one keeps the subscription or result. A different
  one resubscribes, starting `events` empty, or refetches, aborting the request it replaces.
  Function options such as `where` are always called in their latest form without
  resubscribing.
- **Lifetime.** Unmounting unsubscribes or aborts, StrictMode's double mount included. Nothing
  polls or fetches during server rendering. Pass `null` as the descriptor to wait, for
  example until a wallet is connected.
- **Errors.** No hook retries on its own: the subscription's backoff and `maxErrors`
  apply. `options.onError` still runs; the client's `onError` does not, since the error is in
  the hook's result. In a browser every visitor shares one anonymous rate limit per IP, and a
  rate-limited fullnode response can surface as a `StreamdNetworkError` without `retryAfter`,
  so give the `Aptos` client an API key.
- `client` in any hook's options overrides the provider's.

## Command line

`@streamd/cli` installs a `streamd` executable. `streamd tail` follows matching events and
prints each one to stdout as a single JSON line, so the output pipes straight into `jq` or a
file. After `bun run build` it runs from the checkout as `node packages/cli/dist/cli.js`.

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
| `--node-url <url>` | fullnode URL, overriding the network's default; repeat it to spread reads |
| `--archive <url>` | archive node for pruned reads, or `default` for Aptos mainnet's |
| `--from-version <n>` | start at this version instead of the chain head |
| `--poll-interval <ms>` | delay between polls (default `1000`) |
| `--limit <n>` | exit after printing `n` events |
| `-h`, `--help` | show usage |

Each line carries `type`, `data`, `version`, `sequenceNumber`, `eventIndex`, and, when
the source has them, `transactionHash`, `timestamp`, `sender` and, last, `entryFunction`. Every `bigint`, including any inside
`data`, is printed as a decimal string:

```json
{"type":"0x1::transaction_fee::FeeStatement","data":{"total_charge_gas_units":"7"},"version":"2817350001","sequenceNumber":"0","eventIndex":3,"transactionHash":"0x5c...","timestamp":"1730000000000000","sender":"0x9f..."}
```

On Aptos, every command that takes a descriptor takes all four kinds. With `--account`, add
`--handle` and `--field`, or `--creation-number <n>` for a handle by its number, or
`--resource <type>` for every change to one resource:

```sh
streamd tail --account 0x1 --resource 0x1::block::BlockResource
streamd tail --account 0x1 --creation-number 11
```

A handle is followed from its first event, so on a node that has pruned those, as Aptos mainnet
has for long-lived handles, it fails at the start, with `--handle` as well.

`--network` takes `mainnet`, `testnet`, `devnet`, `movement-mainnet` or `movement-testnet`, and
`--node-url` overrides its fullnode.

Commands that read events (`tail`, `forward`, `store`, `query`, `record`, `tx`, `transfers`,
`balances` and `watch`) take `--node-url` more than once, spreading reads across the nodes through
[`PoolAdapter`](#spreading-load-across-fullnodes): each walk stays on one node, and a node that
answers `429` is set aside for 30 seconds while the others carry on. `--api-key` goes to the first
node only. They also take `--archive <url>`, which sends only what the fullnodes refuse as pruned
(`410`) to that archive node through [`ArchiveFallbackAdapter`](#history-the-fullnode-has-pruned),
behind the one node or the pool; `--archive default` is Aptos mainnet's archive, refused on other
networks:

```sh
streamd query --node-url https://my-node.example/v1 --node-url https://api.mainnet.aptoslabs.com/v1 \
  --from-time 1h 0x1::transaction_fee::FeeStatement
streamd query --archive default --from-version 1000 --limit 5 0x1::block::NewBlockEvent
```

Aptos Labs' archive gets the API key only when there is no `--node-url`, so a key reaches it only
when it was already going to Aptos Labs' fullnode; an archive URL gets none. The archive allows
few anonymous requests, so keep archive reads to narrow ranges. `check`, `codegen`, `handles`,
`name` and `compare` read one node: a second `--node-url` there is refused, as is `--archive`.
With `--chain sui`, gRPC URLs pool the same way (transfers and balances read the first node), and
`--archive` is refused, since Sui's archive needs a native gRPC transport the CLI does not include.

Every command takes `--api-key <key>` for a keyed fullnode, sent as `Authorization: Bearer`, the
SDK's API key. Without the flag, `APTOS_API_KEY` from the environment is used, but only on
`mainnet`, `testnet` or `devnet` with no `--node-url`, so a key meant for Aptos Labs' endpoints
never reaches a custom node or Movement; pass `--api-key` to send one there, as `--api-key "$NODE_KEY"` so the key stays out of shell
history. The key is never printed.

`--format pretty` prints one aligned line per event instead of JSON, for reading in a terminal:
UTC time, type without its address, shortened sender, and the payload clipped to 80 columns.
Control characters in on-chain text print as `?`. Handle events have no time or sender, so those
columns show `-`.

To pick up where a run stopped, pass `--cursor-file <path>`: `tail` saves its position there and
resumes from it, and a saved position beats `--from-version` and `--from-time`. It is at least
once, since stdout has no acknowledgement: a restart can reprint part of the last page. By hand,
pass the last printed `version` plus one as `--from-version`.

Polling errors are reported on stderr and tailing continues until the subscription gives
up. Exit codes: `0` when the limit is reached, on Ctrl-C or `SIGTERM`, or when stdout closes
(as when piped into `head`); `1` when the subscription stopped on an error, or output or the
cursor file could not be written; `2` for invalid arguments; `130` on a second Ctrl-C.

### Request stats

`--stats` on the commands that read events (`tail`, `forward`, `store`, `query`, `record`, `tx`,
`transfers`, `balances` and `watch`) prints what the command has asked of each node, through
`adapterMetrics`, as one JSON line on stderr: once when the command ends, by
`--limit`, the end of its range, Ctrl-C, `SIGTERM`, stdout closing or a failure, and again each
time the process gets `SIGUSR1` (not on Windows, which has no such signal). stdout keeps only
events:

```sh
streamd tail --stats 0x1::transaction_fee::FeeStatement > fees.jsonl &
kill -USR1 $!
# {"stats":{"nodes":[{"fetchEvents":{"requests":42,"errors":0,"rateLimited":0,"latency":{"p50":180,"p95":410,"p99":650,"max":650}}}]}}
```

`nodes` holds one entry per `--node-url`, in the order given (one for the network's default
node), and `archive` appears with `--archive`; each maps the adapter methods called so far to
`requests`, `errors`, `rateLimited` and latency percentiles in milliseconds over the most recent
1024 calls. The numbers are totals since the start, never reset. Nodes are named by position,
never by URL, since a URL may carry a key. Each node is counted inside the pool, the archive
fallback and `--cache`, so a page the cache answered is not a request, a `429` the pool moved to
another node counts on both, and a pruned read counts as an error on the fullnode and a request
on the archive. Reads made outside the adapter are not counted: name lookups (an `--account` or
address given as a name), and with `--chain sui`, `transfers` and `balances`, which read the
first node directly. A second Ctrl-C exits at once, without the line.

### Sui from the command line

Every command but `handles` takes `--chain sui` to read Sui through [`@streamd/sui`](#sui) instead of Aptos,
with the same output and exit codes:

```sh
streamd tail --chain sui 0x3::validator::StakingRequestEvent
streamd query --chain sui --from-time 1h --limit 10 0x<package id>::pool::SwapEvent
streamd tx --chain sui <digest>
streamd check --chain sui 0x3::validator::StakingRequestEvent
```

- `--network` is `mainnet`, `testnet` or `devnet` (default `mainnet`), and `--node-url` and
  `compare --against` are Sui gRPC URLs, such as `https://fullnode.testnet.sui.io:443`.
- Descriptors are event types and `--account` (with or without types); `--handle`, `--field` and
  `--creation-number` are refused, since Sui has no event handles, and so is `--resource`, which
  no Sui source serves yet.
- `--api-key` is refused and `APTOS_API_KEY` is never read: public Sui nodes take no key.
- `tx` takes a base58 transaction digest. `--from-time` finds its start by checkpoint time.
- `check` prints `@streamd/sui`'s statuses (`event`, `not-event`, `missing-package`,
  `missing-module`, `missing-struct`, `defined-elsewhere`), widening the status column to fit;
  a `defined-elsewhere` line adds the package id events name the type by.
- `store`'s default cursor name starts `sui:`, as `sui:mainnet:<descriptor>`, so one database
  can hold both chains without either resuming from the other's cursor.
- `@streamd/sui` and `@mysten/sui` are imported only for `--chain sui`; without `@mysten/sui`
  installed, it exits 1 with `--chain sui needs @mysten/sui installed`.

### Filter events

`tail`, `query`, `forward`, `store` and `watch` narrow what they print, send or store with three
repeatable flags, which run after fetching, through the [filters](#filtering) of the same names:

```sh
# deposits of at least 1 APT from one wallet's own transactions
streamd tail --sender 0x9f... --where "amount>=100000000" 0x1::coin::CoinDeposit

# events from either entry function, with a nested field equal to an address
streamd query --from-time 1h --function 0x1::aptos_account::transfer \
  --function 0x1::coin::transfer --where "store.owner==0x9f..." 0x1::fungible_asset::Deposit
```

| Flag | Matches |
| --- | --- |
| `--sender <address>` | the transaction was signed by one of the addresses (`fromSender`) |
| `--function <address::module::function>` | the event came from a call to one of the functions (`fromFunction`) |
| `--where <field><op><value>` | every clause holds on the event's `data` |

The kinds combine: an event must pass every `--where` and, when given, one of the `--sender`
addresses and one of the `--function` names. Addresses compare in long form, so `0x1` matches
`0x000...01`. Events with no `sender` or `entryFunction`, such as handle events, never match those
flags.

In `--where`, the field is a dotted path into `data`, with numbers for array positions
(`legs.0.amount`), and the operator is `==` (or `=`), `!=`, `<`, `<=`, `>` or `>=`; spaces around
the field and value are ignored, so quote the expression. An integer value compares as an integer
at full precision (`whereCompare`), whether the field is a decimal string or a number; a field
that is missing or not an integer never matches, under `!=` too. Any other value is matched as
text under `==` or `!=` (`whereField`), addresses in long form, with `true` and `false` also
matching a JSON boolean; `!=` there also matches events without the field. A malformed
expression, an unknown operator, or a text value under `<`, `<=`, `>` or `>=` exits `2`, naming
the flag.

`--decode <paths>` names `data` fields to read as unsigned integers before `--where` runs,
through `fieldDecoder` (the [`decode`](#decode-integers) option): a comma-separated list, repeatable,
with `*` for every element of an array.

```sh
streamd query --from-time 1h --decode amount --where "amount>=100000000" 0x1::fungible_asset::Deposit
streamd tail --decode "legs.*.amount,fee" 0x9f...::pool::Swap
```

It does not change what is printed or what matches: JSON lines and `--format pretty` print the
decoded bigints as the same decimal strings, `store` writes the same rows, and `--where` already
compares integer strings at full precision. What it adds is a check. A named field that is
missing is skipped, but one that is present and not an unsigned decimal string (a text value, a
JSON number such as a `u8`, an object, or a non-array under `*`) is an error naming the field and
the event's `version:eventIndex`: `query` exits `1`, and `tail`, `forward` and `store` print it to
stderr, skip the event and carry on. Paths take `*`, not array positions, so `legs.0.amount` fails
on an array; an empty entry or segment, or a leading `*`, exits `2`. The paths are not part of
`store`'s default cursor name. The flag works the same under `--chain sui`, which also writes
`u64` and wider as strings.

`--limit` counts matches, so a rare match keeps `query` and `tail` reading until it fills.
`store`'s default cursor name carries the filter, so a filtered and an unfiltered run into one
database keep separate positions. A `--cursor-file` does not: give each filter its own. The flags
work the same under `--chain sui`, where `sender` and `entryFunction` come from the transaction.

### Forward events to a webhook

`streamd forward` follows the same descriptors as `tail` and POSTs each batch of matching
events (up to 25) to a webhook. The body is a JSON array of the objects `tail` prints, with
`content-type: application/json`.

```sh
streamd forward --to https://hooks.example.com/aptos \
  --header "Authorization: Bearer $HOOK_TOKEN" \
  --cursor-file deposits.cursor 0x1::coin::DepositEvent
```

| Option | Meaning |
| --- | --- |
| `--to <url>` | webhook URL, `http` or `https` (required) |
| `--cursor-file <path>` | where the position is saved (default `streamd-forward.cursor`) |
| `--retries <n>` | retries per batch after the first attempt (default `5`) |
| `--timeout <ms>` | per-request timeout (default `10000`) |
| `--header 'Name: value'` | extra request header, repeatable |
| `--secret <key>` | sign each body (or set `STREAMD_WEBHOOK_SECRET`, which keeps it out of `ps` and shell history) |
| `--batch-size <n>` | most events per POST (default `25`); a page larger than it, since sources return whole transactions, goes in several |
| `--to-version <n>` | deliver the range up to this version, then exit `0` |

`--network`, `--node-url`, `--from-version` and `--poll-interval` work as for `tail`.

Delivery is at least once. The next page is not fetched until the webhook answers, and the
cursor file is written (to a temporary file, then renamed over it) only after a `2xx`, so a
restart resumes after the last acknowledged batch and may resend the one in flight. An
existing cursor file wins over `--from-version`, and only works with the descriptor and
node that wrote it.

With a secret, each POST carries `x-streamd-signature: sha256=<hex>`, an HMAC-SHA256 of the
exact request body. Check it with `verifyWebhook` from `@streamd/core`, on the raw bytes before
parsing them, since re-serialised JSON will not match:

```ts
import { WEBHOOK_SIGNATURE_HEADER, verifyWebhook } from "@streamd/core";

export default {
  async fetch(request: Request): Promise<Response> {
    const body = new Uint8Array(await request.arrayBuffer());
    const signature = request.headers.get(WEBHOOK_SIGNATURE_HEADER);
    if (!(await verifyWebhook(body, signature, process.env.STREAMD_WEBHOOK_SECRET!))) {
      return new Response("bad signature", { status: 401 });
    }
    const events = JSON.parse(new TextDecoder().decode(body));
    // ...
    return new Response("ok");
  },
};
```

It runs wherever Web Crypto does (browsers, edge workers, Bun, Deno, Node 19+) and compares in
constant time through `crypto.subtle.verify`. A string body is read as UTF-8. A missing or
malformed header, a changed body or another secret gives `false`; the hex may be either case.
An empty secret, a body that is neither a string nor a `Uint8Array`, or a runtime without
`crypto.subtle` throws a `StreamdError`.

The signature has no timestamp, so it proves origin and integrity but not freshness: a captured
request can be replayed. Deduplicate on each event's `version` and `eventIndex`, which delivery
being at least once already calls for.

Network errors, timeouts, `408`, `429` and `5xx` are retried with exponential backoff from
1s, capped at 60s; a `Retry-After` on `429` or `503` replaces the backoff for that wait.
Any other status, redirects included, fails at once. The webhook receives only the body,
the content type, your `--header` values and any signature, and neither the URL, header
values nor the secret are printed.

`--to-version` turns `forward` into a backfill job. It needs `--from-version` or `--from-time`
(a saved cursor still wins), and the bound must already be history: the head is read once, and a
bound above it fails. Nothing above the bound is sent. The run ends with exit `0` when the walk
catches up, saving the cursor, or at the first page that crosses the bound, once that page's
events up to the bound are acknowledged. The cursor then stays before that page, so a later run
past the bound resends those events rather than skipping the rest.

```sh
streamd forward --to https://hooks.example.com/backfill --from-version 3000000000 \
  --to-version 3001000000 --batch-size 100 0x1::coin::CoinDeposit
```

Exit codes: `0` on Ctrl-C or `SIGTERM`, which abandons an in-flight POST without moving the
cursor; `1` when a batch fails past its retries, the subscription stops on an error, the
cursor file cannot be read or written, or `--to-version` is above the head; `2` for invalid
arguments. Run it under a supervisor
that restarts on `1`.

### Start from a time

`tail`, `forward` and `query` take `--from-time` instead of `--from-version`: a duration back from
now (`90s`, `30m`, `2h`, `7d`) or an ISO date or date-time, which is UTC unless it names an offset:

```sh
streamd tail --from-time 2h 0x1::fungible_asset::Deposit
streamd query --from-time 2026-11-10T09:00 --to-version 7985000000 0x1::coin::CoinDeposit
```

It starts at the first block stamped at or after that time. A time before the oldest block the
node serves starts at that block. On `forward` a saved cursor beats it, so a restart does not
rewind.

### Query a range

`streamd query` reads a closed range as JSON lines and exits:

```sh
streamd query --from-version 7984000000 --to-version 7984001000 0x1::fungible_asset::Deposit
# the latest 10 up to the head, newest first
streamd query --from-version 7984000000 --limit 10 --order desc 0x1::fungible_asset::Deposit
# the last hour
streamd query --from-time 1h 0x1::fungible_asset::Deposit
```

`--to-version` defaults to the head, read once when the query starts, so a busy type still ends.
`--order desc` has to walk the whole range before it prints, since sources only page forward. It
takes the same descriptors as `tail`. Failed pages are retried. It exits 0 when finished or when
interrupted or piped into something that stops reading, such as `head`, 1 on failure, and 2 for
bad usage.

`--cache <file>` on `query` and `record` keeps every page the fullnode marks settled in a SQLite
file, through `CachingAdapter` and `SqlitePageStore`, and the next run
over the same range reads those pages from the file without asking the node, ahead of any pool or
archive:

```sh
streamd query --cache pages.db --from-version 7984000000 --to-version 7984001000 0x1::fungible_asset::Deposit
```

- **What repeats.** A page is reused only for the same request, so run the same descriptor and
  range. Pages reaching the head are never settled, so a query to the default head end reads its
  last page every time, and the head itself, the `--from-time` block search and anything a page
  does not hold are always read from the node.
- **Per source.** Pages are kept under the network, plus a SHA-256 of the `--node-url`s and an
  `--archive` URL when given, since a custom node can serve any chain whatever `--network` says;
  the URLs themselves, which may carry a provider's key, are not written. The network's own
  endpoint and the same URL given with `--node-url` therefore keep separate pages. A chain that is
  reset, such as devnet or a localnet, needs a fresh file.
- **Failures.** A file that cannot be opened as SQLite exits 1 before any read. A broken entry, or
  a read or write that fails later, counts as a miss: the page comes from the node, and the first
  such failure is reported on stderr.
- **Sui.** `SuiAdapter` never marks a page settled, so `--cache` with `--chain sui` is refused.

### Follow or query transfers

`streamd transfers` prints [asset transfers](#asset-transfers) as JSON lines, through the chain
client's `transfers()` or `getTransfers()`:

```sh
# APT transfers as they land
streamd transfers --asset 0xa
# what one account sent or received in a closed range, with symbols and decimals
streamd transfers --account alice.apt --with-asset-info --from-version 8033637000 --to-version 8033638000
# the latest 20 USDC transfers in a range, newest first
streamd transfers --asset 0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b \
  --from-version 8033637000 --to-version 8033638000 --order desc --limit 20
```

Each line is a record event as `tail` prints events, its `data` a `Transfer`, with `amount` in
base units as a decimal string and `from` or `to` left out when that side is unknown:

```json
{"type":"0x1::fungible_asset::Deposit","data":{"asset":"0x...a","amount":"150000000","from":"0x9f...","to":"0x4c..."},"version":"3000000123","sequenceNumber":"0","eventIndex":2,"transactionHash":"0x5c...","timestamp":"1730000000000000","sender":"0x9f..."}
```

| Option | Meaning |
| --- | --- |
| `--asset <asset>` | only this asset: on Aptos a fungible-asset metadata or token object address (`0xa` for APT), a coin type or a legacy token id; under `--chain sui` a coin type |
| `--account <address or name>` | only what this account sent or received: an address, an ANS name on Aptos mainnet, or a SuiNS name under `--chain sui`, resolved once |
| `--with-asset-info` | add `symbol` and `decimals` to each record, read once per asset |
| `--to-version <n \| head>` | read the range up to this version, or the head read once, then exit; needs `--from-version` or `--from-time` |
| `--order <asc \| desc>` | with `--to-version`: oldest or newest first (default `asc`) |
| `--limit <n>` | exit after printing `n` records |
| `--cursor-file <path>` | following: save and resume from here, as for `tail` |
| `--batch-size <n>` | following: transactions per page (default `25`) |

Without `--to-version` it follows from the head, `--from-version` or `--from-time`, as `tail`
does, with `--poll-interval` and the same at-least-once `--cursor-file`. With it, the whole range
is read before anything is printed, since `getTransfers` returns it at once, and failed pages are
retried; `--cursor-file`, `--poll-interval` and `--batch-size` are refused there, and `--order`
is refused without it. On Aptos each fungible-asset store in the range costs view reads, so
a range without `--asset` is slow, about a minute per thousand mainnet versions with `--asset`
and longer without, and `--order desc` walks all of it before printing. Aptos Labs' public
endpoints rate-limit each IP; pass an `--api-key` for long ranges, since a refused read exits
`1`. Every Aptos asset filter, the `account` filter's cost and the pairing
rules are as described under [Asset transfers](#asset-transfers). Under `--chain sui`, records
come from balance changes, `--account` narrows the walk on the node, and an unnarrowed follow
reads every transaction on the chain, so give mainnet a `--batch-size` near `500`.

`--sender`, `--function`, `--where` and `--decode` are not taken: `--asset` and `--account` are
the narrowing, and `amount` is already an integer. A malformed `--asset` or `--account`, or a
name that does not resolve, is the chain client's to judge, so it exits `1` with the client's
message rather than `2`.

Exit codes: `0` when the limit is reached, the range is printed, on Ctrl-C or `SIGTERM`, or when
stdout closes; `1` when reading fails (following: once the subscription gives up), or output or
the cursor file could not be read or written; `2` for invalid arguments; `130` on a second
Ctrl-C while following.

### Follow balances

`streamd balances <address>` follows one wallet's [balances](#live-balances) through the chain
client's `balances()`: each asset's balance once the first read finishes, then a line each time
one changes.

```sh
# APT, the default
streamd balances alice.apt
# APT and USDC, read every 5 seconds, exiting after the first change
streamd balances 0x4c... --asset 0xa \
  --asset 0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b --poll-interval 5000 --limit 1
# SUI, the default under --chain sui
streamd balances --chain sui alice.sui
```

Every line is JSON with base units as decimal strings and `asset` as the client keys it, in long
form. The opening line of each asset has no `previous` or `version`; a change line has both:

```json
{"asset":"0x...a","balance":"150000000"}
{"asset":"0x...a","balance":"149990000","previous":"150000000","version":"3000000123"}
```

`version` is the head version the change was read at. On Aptos the balance is read at that
version, so the change happened after the previous read and at or before it; under `--chain sui`
balances are read at the latest state, which is no older than `version`. Several moves between
two reads print as one line with the net balance, and a move that comes back to the same balance
prints nothing.

| Option | Meaning |
| --- | --- |
| `<address>` | an address, an ANS name on Aptos mainnet, or a SuiNS name under `--chain sui`, resolved once |
| `--asset <asset>` | an asset to follow, repeatable: on Aptos a fungible-asset metadata address (`0xa` for APT) or a coin type, with `0xa` and `0x1::aptos_coin::AptosCoin` one entry; under `--chain sui` a coin type (default: APT, or SUI) |
| `--poll-interval <ms>` | delay between reads (default `1000`) |
| `--limit <n>` | exit after printing `n` changes; the opening lines are not counted |

There is no start position or cursor file, since balances are read at the head, and no
`--with-asset-info`: format a balance with `formatAmount`, or read an asset's symbol and decimals
with `streamd transfers --with-asset-info`. A malformed `--asset`, a legacy token, or a name that
does not resolve is the chain client's to judge, so it exits `1` with the client's message rather
than `2`. A failed read is retried with backoff and reported on stderr; following stops after the
client's error budget.

Exit codes: `0` when the limit is reached, on Ctrl-C or `SIGTERM`, or when stdout closes; `1` when
the first read fails, reading gives up, or output could not be written; `2` for invalid
arguments; `130` on a second Ctrl-C.

### Watch several accounts

`streamd watch <address>...` follows the transactions each address sent, with one
[`watch`](#watch-lists) walk per address, and prints each event as a JSON line led by the watched
address it came from:

```sh
# two accounts from the head, until Ctrl-C
streamd watch 0x4c... 0x9f...
# from a version, resuming each address from its own saved position on the next run
streamd watch 0x4c... 0x9f... --from-version 3000000000 --cursor-file watch.json
# only events with an amount of at least 1 APT, under --decode
streamd watch 0x4c... 0x9f... --decode amount --where "amount>=100000000"
streamd watch --chain sui 0x7d... 0x2b...
```

```json
{"address":"0x00...4c","type":"0x1::coin::CoinWithdraw","data":{"amount":"150000000"},"version":"3000000123","sequenceNumber":"0","eventIndex":0,"sender":"0x4c..."}
```

A line is the `tail` line with a leading `address`, in long form. `parseEvent` reads events from
named fields and ignores `address`, so a saved watch still loads through
`MemoryAdapter.fromJsonLines`.

| Option | Meaning |
| --- | --- |
| `<address>...` | one or more addresses (no names), each followed once |
| `--from-version <n>`, `--from-time <t>` | where an address with no saved position starts (default: the head) |
| `--cursor-file <path>` | read each address's position from here, and save them as they move |
| `--sender`, `--function`, `--where`, `--decode` | narrow every walk, as on `tail` |
| `--poll-interval <ms>` | delay between each walk's polls (default `1000`) |
| `--limit <n>` | exit after printing `n` lines across every address |

The cursor file holds `watch.positions`, one JSON object of long-form address to cursor, such as
`{"0x00...4c":"3000000124"}`, replaced whole by a rename after a line, and at exit, whenever a
position has moved. A saved position wins over `--from-version` and `--from-time` for that address.
Positions for addresses not on the command line are carried through, their keys in long form, so
leaving an address out for one run does not lose its place. It is at least once: a restart can
reprint part of a page.

An error on one walk is reported on stderr and retried. A walk that gives up is reported as
`streamd: stopped following <address>` and the others carry on; its position stays in the cursor
file, so the next run resumes it. When every walk has given up, the command exits `1`.

There is no event-type narrowing and no `received` under `--chain sui`: `watch` builds each walk
as `{ kind: "account", accountAddress }` itself. Narrow by data with `--where`, or follow one
account with `streamd tail --account <address> <eventType>...`.

Exit codes: `0` when the limit is reached, on Ctrl-C or `SIGTERM`, or when stdout closes; `1` when
the cursor file cannot be read or written, every walk gave up, or output could not be written; `2`
for invalid arguments; `130` on a second Ctrl-C.

### List event handles

`streamd handles <address | name>` lists every `EventHandle` on an Aptos account through
[`listEventHandles`](#discover-event-handles), one JSON line per handle, ready to follow:

```sh
streamd handles 0x1
streamd handles alice.apt
```

```json
{"resourceType":"0x1::block::BlockResource","path":["new_block_events"],"count":"1165098930","guid":{"creationNumber":"3","accountAddress":"0x1"},"descriptor":{"kind":"handle","accountAddress":"0x00...01","eventHandleStruct":"0x1::block::BlockResource","fieldName":"new_block_events"},"flags":"--account 0x00...01 --handle 0x1::block::BlockResource --field new_block_events"}
```

`descriptor` is the library's `handle` descriptor with `creationNumber` as a decimal string: by
struct and field for a top-level handle, by creation number for one nested in a struct field.
`flags` is the same descriptor as `tail` flags, with any struct type holding generics or spaces
single-quoted, so it pastes into `streamd tail`, `query`, `forward` or `store`; a handle is
followed from its first event, which mainnet may have pruned (see above). `count` is the events
emitted on the handle so far, and `guid.accountAddress` is as the node reports it.

An address or an Aptos name is taken, resolved once as `balances` does; a malformed value or a
name that does not resolve exits `1` with the library's message. An account with no handles
prints nothing and exits `0`. Handles held in a table are not listed. Under `--chain sui` it is
refused, since Sui has no event handles.

Exit codes: `0` when listed, or when stdout closes; `1` when the read fails or output could not
be written; `2` for invalid arguments.

### Resolve names

`streamd name <name | address>` resolves either way through [`resolveName`](#aptos-names) and
`primaryName`: a value starting `0x` is an address, looked up for its primary name, and anything
else is a name, resolved to its address.

```sh
streamd name alice.apt
# {"name":"alice.apt","address":"0x00...4c"}
streamd name 0x4c...
# {"name":"alice.apt","address":"0x00...4c"}
streamd name --chain sui @alice
```

The line always has `name`, as given for a name, and `address`, in long form. An address with no
primary name, or whose name no longer points back to it, prints `{"name":null,"address":"0x..."}`
and exits `0`, so a script can test it with `jq -e .name`. A name that is malformed,
unregistered, expired or has no target address exits `1` with the library's message, since there
is no address to print.

On Aptos, names are `alice.apt`, `alice` or `sub.alice.apt`, and resolve on mainnet only: on
testnet the SDK's router resolves nothing, and the Movement networks have no ANS, so names exit
`1` there. Under `--chain sui`, names are SuiNS names such as `alice.sui` or `@alice`, served on
Sui mainnet and testnet; devnet has no SuiNS.

Exit codes: `0` when printed, or when stdout closes; `1` when the lookup is refused or fails, or
output could not be written; `2` for invalid arguments.

### Inspect a transaction

`streamd tx <hash>` waits for a transaction to commit and prints its events as JSON lines:

```sh
streamd tx 0x5d16bc7f...
```

`--timeout <ms>` bounds the wait (default 20000). It exits 0 for a successful transaction, 1 if the
transaction failed, with its VM status on stderr and nothing on stdout, or if it was not found in
time, 2 for a malformed hash, and 130 if interrupted while waiting.

### Check event types

`streamd check` says whether each event type can be emitted, before a `tail` or `forward` that
would otherwise wait in silence on a typo:

```sh
streamd check 0x1::fungible_asset::Deposit 0x1::coin::Depositt
# event          0x1::fungible_asset::Deposit
# missing-struct 0x1::coin::Depositt
```

The statuses are `event`, `legacy` (no `#[event]`, but a legacy `EventHandle` can emit it),
`not-event`, `missing-module` and `missing-struct`. It exits 0 when every type is `event` or
`legacy`, 1 when one is not or the fullnode fails, and 2 for bad usage. It takes event types
only, not `--account` or `--handle`.

### Record a session for tests

`streamd record` runs a bounded query and writes a [recording](#data-sources-and-their-limits) that
`replayAdapter` answers with no network:

```sh
streamd record --from-version 7984000000 --to-version 7984000500 --out deposits.json \
  0x1::fungible_asset::Deposit
```

```ts
const adapter = replayAdapter(readFileSync("deposits.json", "utf8"));
const client = new StreamdClient({ adapter });
const events = await client.getEvents(descriptor, { fromVersion: 7984000000n, toVersion: 7984000500n });
```

A recording answers only the requests it saw, so replay with the same descriptor and options; the
command prints them to stderr. `--from-version` and `--to-version` are both required, so the query
has an end. Failed pages are retried and not recorded, and the file appears only once the whole
query has succeeded. Without `--out` the recording goes to stdout. It exits 0 on success, 1 on
failure, 130 if interrupted and 2 for bad usage.

With `--cache <file>`, as on [`query`](#query-a-range), settled pages come from the file when it
has them. The recorder sits above the cache, so the recording holds every call either way and
replays the same.

### Compare two nodes

`streamd compare` reads one range from two fullnodes through
[`compareAdapters`](#data-sources-and-their-limits) and prints what they disagree on, to check a
node against a reference:

```sh
streamd compare --node-url http://my-node:8080/v1 --against https://api.mainnet.aptoslabs.com/v1 \
  --from-version 8002810605 --to-version 8002811605 0x1::fungible_asset::Deposit
```

The command's node (`--network`, or `--node-url`, with its `--api-key`) is compared with
`--against`, which gets no API key. Each finding is a JSON line, events written as `tail` writes
them: `{"kind":"missing","event":…}` for one only the first node has, `"extra"` for one only the
second has, and `{"kind":"different","id","fields","a","b"}`. A last line, `"kind":"summary"`,
gives `ok`, the window both nodes covered, `matched`, the pages read and any errors. Events are
identified by version and index, so an event dropped from a transaction also shows the next one
there as missing and extra.

The range is required and at most 100,000 versions; `--max-pages` (default 10) caps each walk and
`--limit` sets the page size. It exits 0 when the nodes agree, 1 when they disagree, a node fails
or no window was compared, and 2 for bad usage.

### Store events

`streamd store` follows the same descriptors as `tail` into a SQLite file, through
[`storeEvents`](#store-events-in-sqlite). Run the same command again and it resumes where it
stopped:

```sh
streamd store --db events.db 0x1::transaction_fee::FeeStatement --from-version 7984000000
sqlite3 events.db 'SELECT count(*) FROM streamd_events'
```

- It uses the runtime's own SQLite: `bun:sqlite`, or `node:sqlite` on Node 22.5 or newer, which
  prints one experimental warning on stderr.
- The cursor is saved under `--name`, or by default under the network and descriptor, so one
  file can follow several descriptors. A saved cursor beats `--from-version`, which only
  seeds the first run.
- Use one file per network, since events are keyed on `(version, eventIndex)`.
- The file is in WAL mode with a 5 second busy timeout, so it can be queried while written.
- Exit code 0 on Ctrl-C or SIGTERM, 1 when polling gives up, a write fails, or the file cannot
  be opened.

`--postgres <url>` writes to a Postgres database instead, through
[`@streamd/store-postgres`](#store-events-in-postgres), with the same cursor names, resume,
filter and decode flags, and `--chain sui`. Give exactly one of `--db` and `--postgres`.

```sh
PGPASSWORD="$DB_PASSWORD" streamd store --postgres postgres://streamd@db.example:5432/events \
  0x1::transaction_fee::FeeStatement
psql -h db.example -U streamd events -c 'SELECT count(*) FROM streamd_events'
```

- It uses the runtime's own Postgres client under Bun, `Bun.SQL`. Node has none, so there it
  loads [`pg`](https://www.npmjs.com/package/pg) if you installed it, and otherwise exits 1 with
  `--postgres under Node needs pg installed: npm i pg (Bun has its own client)`. `pg` is an
  optional peer dependency, never installed for you.
- The URL is passed to the driver as given, so `sslmode` and the driver's other parameters work.
  The tables are created on first use.
- The URL is never printed: error lines name it with the password as `***` and no query string,
  and the password is scrubbed from the driver's messages. A password in the URL is still visible
  to `ps` and shell history, so leave it out and set `PGPASSWORD`, which both drivers read.
- Exit code 0 on Ctrl-C or SIGTERM, also while still connecting; 1 when the server cannot be
  reached or refuses the login (`cannot connect to <url>`), a write fails, or polling gives up.
  A stop waits up to 5 seconds for a batch already being written, then up to a second for the
  connection to close.

### Generate event types

`streamd codegen` reads module ABIs from the fullnode and writes a TypeScript file with a type
and a `defineEvent` descriptor for every `#[event]` struct, plus types for the structs those
events reference, fetched from whichever modules and accounts declare them.

```sh
streamd codegen --out src/events.ts 0x1::fungible_asset 0x1::coin
```

It also writes a typed `handle` descriptor builder for each legacy `0x1::event::EventHandle<T>`
field of a resource (a struct with `key`) in the modules it reads, named `<Struct>_<Field>` and
taking the account address, then a generic resource's type arguments:

```ts
import { CoinStore_DepositEvents } from "./events.js";

client.on(CoinStore_DepositEvents(wallet, "0x1::aptos_coin::AptosCoin"), (e) => console.log(e.data.amount));
```

Handles nested below a resource's top level have no builder; reach them by `creationNumber`.

`--validators` also writes an `is<Type>(value): value is <Type>` check for every generated type,
with no dependencies, for payloads from a source you do not control:

```ts
import { FungibleAsset_Deposit, isFungibleAsset_Deposit } from "./events.js";

client.on(FungibleAsset_Deposit, (e) => {
  if (!isFungibleAsset_Deposit(e.data)) return report(e); // not the shape the ABI describes
});
```

It follows the table below, accepting `bigint` where a decimal string is expected, so a payload
already through `decode` passes. Extra fields pass, so an upgraded module's new fields do not
break old checks, and values of a type parameter are not checked. It checks shape, not
identity: two event types with the same fields pass each other's check.

A bare address covers every module published there, skipping the ones with no `#[event]` struct,
event handle or `#[view]` function, and mixes with named modules:

```sh
streamd codegen --out src/protocol.ts 0x<protocol address>
```

```ts
import { FungibleAsset_Deposit, Coin_Deposit } from "./events.js";

client.on(FungibleAsset_Deposit, (e) => console.log(e.data.store, e.data.amount));
// generic events take their type arguments in the fullnode's own form
client.on(Coin_Deposit("0x1::aptos_coin::AptosCoin"), (e) => console.log(e.data.account));
```

Each type describes the JSON the fullnode returns:

| Move | TypeScript |
| --- | --- |
| `u8`, `u16`, `u32`, `i8`, `i16`, `i32` | `number` |
| `u64`, `u128`, `u256`, `i64`, `i128`, `i256` | `string` (decimal; name the field in `decode` for a `bigint` at runtime) |
| `bool` | `boolean` |
| `address`, `vector<u8>` (hex), `0x1::string::String` | `string` |
| `vector<T>` | `T[]` |
| `0x1::option::Option<T>` | `{ vec: [] \| [T] }` |
| `0x1::object::Object<T>` | `{ inner: string }` |
| other structs | a generated interface |
| enums | a union discriminated on `__variant__`, with that variant's fields beside it |
| type parameters `T0`, `T1` | type parameters, defaulting to `unknown` |

Each event with `u64`, `u128` or `u256` fields at its top level also gets a list of their names,
ready for `decode`:

```ts
import { FungibleAsset_Deposit, FungibleAsset_DepositDecode } from "./events.js";

client.on(FungibleAsset_Deposit, (e) => console.log(e.data.amount), { decode: FungibleAsset_DepositDecode });
```

The list is named `<Event>Decode`, with an underscore added if a struct already has that name.
`event.data` keeps its generated type, which says `string` for those fields; `decode` makes
them `bigint` at runtime. Signed integers are not listed, since `decode` reads unsigned digits
only, and neither are nested fields.

Signed integers follow the unsigned rule; none had been seen as event fields when this was
written. A struct keeps its Move name unless two modules share it, in which case both become
`<Module>_<Struct>` (`Coin_Deposit`, `FungibleAsset_Deposit`), with the address appended if
that still collides. A descriptor shares its type's name; a generic event's descriptor is a
function of its type arguments, since module descriptors match the full type string. The
output reflects the ABI at generation time, so regenerate after a module upgrade.

`--check` catches a file that has fallen behind, in CI for instance. It generates in memory with
every other flag as given, compares the result byte for byte with the `--out` file, and writes
nothing:

```sh
streamd codegen --check --out src/events.ts 0x1::fungible_asset 0x1::coin
```

It exits `0` when they match, printing nothing, and `1` when the file is missing or differs,
naming the file and the first line that differs, each side as a JSON string cut to 120
characters (a line ending or byte order mark counts as a difference, since regenerating would
change it):

```text
streamd: src/events.ts differs from the generated output at line 42
  on disk:   "  store: string;"
  generated: "  store: { inner: string };"
```

Run the same command without `--check` to bring the file up to date. `--check` needs `--out`,
and works the same with `--chain sui`.

| Option | Meaning |
| --- | --- |
| `--out <file>` | write to this file instead of stdout |
| `--check` | compare with the `--out` file instead of writing it |
| `--chain <aptos\|sui>` | the chain to read (default `aptos`); see below |

`--network` and `--node-url` work as for `tail`. Exit codes: `0` once the file is written, or
under `--check` when it matches; `1` when a module or a struct it references is missing, a
module declares no events, an ABI is malformed, a field type is unsupported (such as a function
type), the node cannot be read, or under `--check` when the file differs or is missing (told
apart by the message); `2` for invalid arguments, `--check` without `--out` among them.

#### Sui packages

`--chain sui` reads Sui packages through the node's gRPC package service instead, and writes
the same kind of file for [`SuiClient`](#sui):

```sh
streamd codegen --chain sui --out src/events.ts 0x<package id>::<module>
streamd codegen --chain sui --out src/protocol.ts 0x<package id>
```

- **Every struct or enum with `copy` and `drop` in a named module gets a descriptor.** Sui has no
  `#[event]` attribute, `sui::event::emit` accepts any such type, and the node serves no bytecode
  to tell which ones a module emits, so dynamic-field keys and plain values with those abilities
  (`0x2::kiosk::Item`) get descriptors that never match anything. Types the events reference
  from other modules get an interface and no descriptor. A bare package id names all its
  modules.
- **Types are named by the package version that introduced them**, in long form, because that is
  how events carry them. Give the latest package id: an upgraded module is split by defining id,
  so its first version's events and a later version's appear side by side, each under its own
  id. References inside a package use its original id, and are resolved to the defining id,
  reading later versions where needed.
- No view or handle builders: Sui has neither. `--validators` works, against the JSON below.
- `--network` takes `mainnet`, `testnet` or `devnet`, and `--node-url` a Sui gRPC URL.
  `--api-key` is refused and `APTOS_API_KEY` is never read: public Sui nodes take no key.
- `@streamd/sui` and `@mysten/sui` are imported only for `--chain sui`. `@mysten/sui` is an optional peer of `@streamd/cli`, but `@streamd/sui`, a dependency, still declares it as a peer, so package managers may install or ask for it.

Each type describes the JSON a Sui node returns, as `SuiAdapter` delivers it:

| Move | TypeScript |
| --- | --- |
| `u8`, `u16`, `u32` | `number` |
| `u64`, `u128`, `u256` | `string` (decimal), with `<Event>Decode` lists as on Aptos |
| `bool` | `boolean` |
| `address`, `0x2::object::ID`, `0x2::object::UID` | `string` (`0x` and 64 hex digits) |
| `vector<u8>` | `string` (base64) |
| `0x1::string::String`, `0x1::ascii::String`, `0x2::url::Url`, `0x1::type_name::TypeName` | `string` (a `TypeName` has no `0x`) |
| `0x1::option::Option<T>` | `T \| null` |
| `0x2::balance::Balance<T>` | `string` (decimal) |
| other structs | a generated interface |
| enums | a union discriminated on `"@variant"` |
| `vector<T>`, type parameters | as on Aptos |

A generic event's descriptor passes its type arguments through as given; the node matches either
form, but `event.type` carries long-form addresses, so pass long form to compare against it.

## Development

```sh
bun install
bun run test               # unit tests in every package, through turbo
bun run typecheck
bun run build              # each package into its own dist/
bun run demo               # live demo against mainnet
bun run test:integration   # @streamd/aptos and @streamd/sui integration tests, hit mainnet
bun packages/cli/scripts/framework.ts  # regenerate @streamd/aptos/framework from its ABI snapshots
bun packages/cli/scripts/sui-framework.ts  # regenerate @streamd/sui/framework (--refresh rereads mainnet)
```

## License

[MIT](LICENSE)
