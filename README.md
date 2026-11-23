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
`transfers`, `getTransfers`, `activity`, `getActivity`, `swaps`, `getSwaps`, `sales`, `getSales`, `proposals`, `stakePools`, `multisig`, `positions`, `balances`, `balanceAt`,
`eventsOf`, `eventsAt`, `explain`, `expectEvents`, `previewEvents` and `expectPreview`. It takes every `StreamdClient` option, with `aptos` (an `Aptos` or
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

The repository is a Turborepo monorepo on Bun workspaces. Workspace packages declare internal links as `workspace:*`, so publishing uses `bun publish` to rewrite those ranges (`@streamd/core` and `@streamd/cli` guard this with `prepublishOnly`). Every package ships ESM, CommonJS and
TypeScript types:

- `@streamd/core` (`packages/core`) - the client engine: subscriptions, queries, merges,
  reductions and the `EventAdapter` contract, with the chain-neutral adapters. It depends on no
  chain SDK.
- `@streamd/aptos` (`packages/aptos`) - Aptos: `AptosClient`, `FullnodeScanAdapter`,
  `IndexerAccountAdapter`, the network presets, `checkDescriptor`, `listEventHandles`, `ownedBy`,
  `resolveName`, `primaryName`, `sendOnce`, and the
  `@streamd/aptos/framework` event catalog. `@aptos-labs/ts-sdk` (>= 1.0.0) is a peer dependency.
- `@streamd/cli` (`packages/cli`) - the `streamd` executable, built on `@streamd/core` and
  `@streamd/aptos`.
- `@streamd/codegen` (`packages/codegen`) - the generator behind
  [`streamd codegen`](#generate-event-types), with no Node or database import, so it also runs
  in a browser.
- `@streamd/store-sqlite` (`packages/store-sqlite`) - opt-in persistence: events written to a
  SQLite database you supply, and queried back. See [Store events in SQLite](#store-events-in-sqlite).
- `@streamd/store-postgres` (`packages/store-postgres`) - the same over a Postgres client you
  supply. See [Store events in Postgres](#store-events-in-postgres).
- `@streamd/sui` (`packages/sui`) - `SuiClient` and `SuiAdapter`, the same client over Sui events,
  Sui network presets, and `resolveName`, `primaryName` and `ownedBy` over SuiNS and object owners, with
  a `./framework` catalog of `0x2` and `0x3` events. `@mysten/sui` is a peer dependency. See [Sui](#sui).
- `@streamd/testing` (`packages/testing`) - start or attach to an Aptos localnet, publish Move
  packages to it and assert their events. Needs the `aptos` CLI. See
  [Test against a localnet](#test-against-a-localnet).
- `@streamd/store-indexeddb` (`packages/store-indexeddb`) - `IndexedDbPageStore`, which keeps
  `CachingAdapter`'s settled pages in the browser's IndexedDB. See
  [History the fullnode has pruned](#history-the-fullnode-has-pruned).
- `@streamd/receiver` (`packages/receiver`) - receive `streamd forward` webhooks: the signature
  checked, the events parsed and repeats dropped. See
  [Forward events to a webhook](#forward-events-to-a-webhook).
- `@streamd/gateway` (`apps/gateway`) - the gateway behind `streamd serve`, and
  `GatewayAdapter`, which reads through one, from the browser-safe `@streamd/gateway/adapter`.
  See [Serve a gateway](#serve-a-gateway).
- `@streamd/playground` (`apps/playground`) - a browser page to check an event type, watch it
  live and copy its generated types and a snippet. Not published. See [Playground](#playground).
- `@streamd/react` (`packages/react`) - `useEvents`, `useLogs`, `useEventPages`, `useTransactionEvents`, `useSubmitTransaction`, `useTransfers` and `useReduction` hooks. `react` (>= 18) and
  `@aptos-labs/ts-sdk` are peer dependencies; `useTransactionEvents`, `useSubmitTransaction` and
  `useTransfers` take an `AptosClient`. See [React hooks](#react-hooks).

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

#### Points and quests

`points(options)` folds events into a score per address, for leaderboards, quests and loyalty
programs. Each rule awards the address it names whenever an event matches; thresholds record
when a score first reaches them, and decay shrinks idle scores over chain time:

```ts
import { points } from "@streamd/core";

const quest = points({
  rules: [
    { id: "swap", points: 10n, firstTime: 50n },
    { id: "big", where: (e) => BigInt((e.data as { amount: string }).amount) >= 1_000_000n, points: 100n, limit: 3 },
    { id: "referral", address: "referrer", points: 25n },
  ],
  thresholds: [{ id: "bronze", points: 100n }, { id: "gold", points: 1_000n }],
  decay: { interval: 7n * 86_400_000_000n, keep: { numerator: 9n, denominator: 10n } }, // keep 90% a week
});
const board = streamd.reduce(Deposit, quest, quest.initial, { fromVersion: -100_000n });
await board.ready;
quest.standings(board.value, { limit: 10 }); // the top ten, highest first
```

- **Rules.** Every rule that matches awards on its own. `id` names the rule (unique, non-empty).
  `where` is a synchronous predicate, since a reducer is; a returned promise throws. `address`
  is `"sender"` (the default, the transaction's signer) or a `whereField` path in `event.data`;
  a missing or `null` address skips the rule, anything else that is not an address string
  throws. `points` is a `bigint` or a function of the event returning one, `1n` by default, and
  may be negative. `firstTime` is added once, on an address's first award by that rule, and
  `limit` caps the awards one address gets from it.
- **Scores.** The state is a `Map` from long-form address to `Score { address, points, awards,
  reached, version, updatedAt? }`: `awards` counts each rule's awards, `reached` holds the
  version at which each threshold was first reached (kept even if the score later falls),
  `version` is the last event that changed it. A fold returns a new `Map`, copying only the
  scores it changed, so earlier values and `initial` stay as they were.
- **Decay.** `interval` is in microseconds, the unit of `event.timestamp`, and `keep` is a
  fraction from 0 to 1. Before an award, the whole intervals since the score's `updatedAt` are
  applied at once as `points * numerator^k / denominator^k`, rounded toward zero, and the clock
  moves on by those intervals. With decay set, an awarding event without a `timestamp` throws.
- **Standings.** `quest.standings(state, { at, limit })` sorts by points, highest first, ties
  by address in byte order. `at` applies decay up to that timestamp on copies, leaving the state
  unchanged, and is refused when earlier than a score's `updatedAt`.
- **Saving.** `scoreToJson(score)` writes a score as JSON text with bigints as decimal strings,
  and `scoreFromJson(text)` reads it back, refusing a malformed field by name. `storePoints` in
  both store packages keeps the whole leaderboard and its cursor for you.

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
not committed within `timeout` (default 20 seconds) rejects with `StreamdTimeoutError`
(carrying the last `429` or `5xx` error in its message and `cause` when retries
exhausted the wait), an abort with `signal.reason`, and any other failed lookup
at once with its error (`429` and `5xx` responses retry until `timeout`,
honouring `Retry-After` hints). The adapter is not
told about a timeout or abort, so a lookup already in flight runs to completion and holds
its `maxConcurrentRequests` slot until then, or until the client's `requestTimeout`. The hash must be
`0x` and 64 hex digits. It needs an adapter with `getTransactionEvents`, which
`FullnodeScanAdapter` and `MemoryAdapter` have and the wrapping adapters pass through.
`StoreAdapter` and `PostgresStoreAdapter` have it too, answering from a saved store (see
[Transaction lookups](#store-events-in-sqlite)).

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
`getTransactionEventsByVersion`, which `FullnodeScanAdapter`, `StoreAdapter` and
`PostgresStoreAdapter` have and the wrapping adapters pass through; `MemoryAdapter` and `SuiAdapter` do not, so `SuiClient` has no `eventsAt`, since placing
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

### Test against a localnet

`@streamd/testing` starts an Aptos localnet with the installed `aptos` CLI, publishes a Move
package to it, and gives you an `AptosClient` on it for `expectEvents`:

```ts
import { startLocalnet } from "@streamd/testing";

await using localnet = await startLocalnet();
const account = await localnet.fundedAccount();
const me = account.accountAddress.toString();
await localnet.publish("./move/counter", { account, namedAddresses: { counter: me } });

const transaction = await localnet.aptos.transaction.build.simple({
  sender: account.accountAddress,
  data: { function: `${me}::counter::bump`, functionArguments: [42] },
});
const { hash } = await localnet.aptos.signAndSubmitTransaction({ signer: account, transaction });
await localnet.client.expectEvents(hash).toEmit({ kind: "module", eventType: `${me}::counter::Bumped` });
```

- **Start.** `startLocalnet(options?)` runs `aptos node run-localnet` with no Docker, no
  transaction stream and no Indexer, in a fresh temporary directory, and resolves once the node
  and faucet are ready (about 16 s on an M-series Mac; `readyTimeout`, default 60000 ms). `cli`
  names the binary when it is not `aptos` on `PATH`; a missing one rejects with a `StreamdError`
  saying so. A node that exits first rejects with its last output lines, and one not ready in
  time is killed and rejects with `StreamdTimeoutError`.
- **Ports.** Each instance takes eight loopback ports, so several run at once: the ready server,
  REST API, faucet, inspection, admin, backup service, and the validator and public network
  listeners. They are free ports the OS picks, or eight consecutive from `basePort`, each checked
  free first, and `localnet.ports` lists them. A start refuses a ready port something already
  answers on, so it never hands back another process's node. The node also opens two random ports of its own, one on every
  interface.
- **Attach.** `attachLocalnet({ nodeUrl, faucetUrl? })` uses a node already running, such as
  `http://127.0.0.1:8080/v1`; it reads the ledger once so an unreachable node fails at once, and
  `stop()` leaves that node running.
- **The client.** `localnet.client` is an `AptosClient` on `localnet.aptos` (an `Aptos` on
  `Network.LOCAL` with `http2: false`). It polls every 100 ms, so `eventsOf` and `expectEvents`
  see a commit within about 100 ms; the `client` option takes any other `AptosClient` config,
  `defaultPollInterval` or `defaults.pollInterval` included.
  A localnet has no Aptos names, so `withNames` is refused there.
- **Accounts.** `fundedAccount(amount?)` generates an account and funds it through the faucet,
  10 APT by default, enough for the SDK's default gas limit.
- **Publish.** `publish(packageDir, { account, namedAddresses? })` compiles with
  `aptos move build-publish-payload` and submits `0x1::code::publish_package_txn` from
  `account` through the SDK, resolving `{ transactionHash, version }` once committed. A compile
  failure rejects with the compiler's output, a failed transaction with
  `StreamdTransactionError`. Named addresses must be addresses, not names. The CLI writes
  `build/` inside the package; compiles of one package from one process run one at a time.
- **Staying offline.** The compile passes `--skip-fetch-latest-git-deps`, so a framework
  dependency resolves from the CLI's own cache when its `rev` is the CLI's release tag, such as
  `rev = "aptos-cli-v9.4.0"` for CLI 9.4.0. Another `rev` is fetched from GitHub on first use.
- **Stop.** `stop()` (or `await using`) destroys the client, sends the node SIGINT, kills it after
  20 s, and removes its directory. Call it in a `finally` or `afterAll`, or use `await using`:
  the node is also killed if the process exits normally while it runs, but not when the process
  is killed by a signal, and `bun test` does not run that hook for a failed test either.
- **Its own test.** `bun run test:localnet` in `packages/testing` starts two localnets side by
  side and asserts a published package's events on each; the normal test run skips it.

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

### Explain a transaction

`explain(txHash | transaction, options?)` on `AptosClient` sums up what one transaction did,
and `formatExplanation(explanation)` renders that summary as text. A hash is waited for as
`eventsOf` waits; a built, unsigned transaction is simulated as `previewEvents` simulates it.

```ts
import { formatExplanation } from "@streamd/aptos";

const explanation = await streamd.explain("0xaaa0871a...", { withNames: true });
console.log(explanation.gas?.fee, explanation.balanceChanges);
process.stdout.write(formatExplanation(explanation));
// Transaction 0xaaa0871a... at version 8062800017, 2026-11-15T12:05:02.630Z
// Status: success (Executed successfully)
// Sender: 0x172d...
// Function: 0x1::aptos_account::transfer
// Gas: 5414 units at 100 octas, fee 0.005414 APT (storage 0.005324 APT, refund 0 APT)
// Transfers:
//   0.494586 APT from 0x172d... to 0xfe56...
// Balance changes:
//   0x172d...: -0.494586 APT
//   0xfe56...: +0.494586 APT
// Events: 3

const preview = await streamd.explain(await aptos.transaction.build.simple({ sender, data }));
```

- **The summary.** A `TransactionExplanation` holds `simulated`, `version`,
  `transactionHash` (not for a simulation), `success`, `vmStatus`, `sender`, `feePayer` (a
  sponsored transaction's), `timestamp`, `entryFunction`, `gas`, `transfers`,
  `balanceChanges`, `swaps`, `sales` and every event in `events`.
- **Failures.** A failed transaction or simulation resolves with `success: false` and its
  `vmStatus`, rather than rejecting as `eventsOf` does, since its gas was still charged. The
  chain keeps only its fee statement, so it has no transfers.
- **Gas.** From the transaction's `0x1::transaction_fee::FeeStatement`: `units` (the
  transaction's `gas_used`), `executionUnits`, `ioUnits`, `storageFee` and `storageRefund`
  in octas, and, when the adapter reports the gas unit price (`FullnodeScanAdapter` does),
  `unitPrice` and `fee`, `units * unitPrice`. The payer is the sender, or `feePayer`, and
  gets `storageRefund` back on top. The fee is burned without a withdraw event, so it is not
  in `transfers` or `balanceChanges`.
- **Transfers and balance changes.** `transfers` are built and paired exactly as
  `transfers()` builds them, over the transaction's events. `balanceChanges` is the net
  `{ account, asset, delta }` per account and asset over them, grouped by account in order
  of first appearance and each account's assets in theirs, zero nets left out. `symbol` and `decimals` are read by view unless
  `withAssetInfo: false`.
  `withValue: true` adds `value` in USDC to transfers and balance changes, a change taking the
  price of its asset's transfers (see [Values](#values)); `formatExplanation` prints it in
  brackets after the amount. A simulation is priced at the state its views read.
- **Swaps and sales.** Decoded from the same events as `swaps()` and `sales()` decode them.
- **Names.** `withNames` (mainnet only) adds `names`: the primary name or `null`
  of the sender, the fee payer, every transfer's `from` and `to` and every sale's buyer and
  seller, through the client's name cache.
- **Simulations.** The node simulates on top of its newest state at a version not yet
  committed, so the store and asset reads run at the newest version below it that the
  client's adapter reports. A store the simulation would create does not exist there, so
  that leg is left out: a transfer to an account with no store of the asset shows as a
  withdraw with no `to`.
- **Reads.** The lookup or simulation, the head read for a simulation, and every store,
  asset and name read go through the client's limiter. A failed read rejects. It needs an
  adapter with `view`, plus `getTransactionEvents` for a hash, or
  `simulateTransactionEvents` and `getHeadVersion` for a transaction; bad input, signer
  keys passed with a hash and `timeout` passed with a transaction are refused before any
  read. Aborting `signal` rejects at once and drops the reads still queued.
- **The text.** `formatExplanation` writes one fact per line, amounts in the asset's
  decimals and symbol when known (a swap takes them from its transaction's transfers) and
  otherwise in base units with the asset, addresses in long form with their names, and
  control and bidirectional override characters in on-chain text replaced with `?`.

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
call and needs a lower number. `ownedBy` predicates read through the SDK directly, and the
internal `view` reads inside `transfers()` / `getTransfers()` (up to 4 concurrent per call) and
`balances()` run inside their page fetch, so none of those are spaced by `maxRequestsPerSecond`.

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

### Share polling across browser tabs

```ts
import { TabSharedAdapter } from "@streamd/core";
import { AptosClient, FullnodeScanAdapter } from "@streamd/aptos";

const adapter = new TabSharedAdapter(new FullnodeScanAdapter(aptos), {
  name: "streamd-aptos-mainnet",
  requestTimeout: 15_000,
  joinHeadStarts: true,
});
const streamd = new AptosClient({ aptos, adapter, requestTimeout: 15_000 });
```

With the same app open in several tabs, one tab (the leader, chosen with `navigator.locks`)
sends every `fetchEvents` request to the source. The other tabs post their requests to it over
a `BroadcastChannel` named `name` and get its page back, so they stay under one tab's request
budget. Identical requests from several tabs within `shareFor` ms (default 1000) are one source
request. Each tab keeps its own cursors, so when the leader's tab closes or navigates away the
next tab in the lock's queue takes over and every subscription resumes where it was, with no
gap and no repeat. Requests the old leader had not answered are sent again to the new one.

- **`joinHeadStarts` lets a start at the head join the leader's walk.** Two tabs started at
  different times hold different cursors, and module scan cursors rarely meet, so without it
  each tab's walk is its own source request. With it, a `module` or `resource` request with no
  cursor, `fromVersion`, `toVersion` or `offset` gets the newest caught-up page fetched for the
  same descriptor and options within `shareFor`, so a tab opened later shares every page with
  the rest, and may begin with events from up to `shareFor` ms before it started. Turn it on
  only for a source that reads such a request from the head, as `FullnodeScanAdapter` does
  (`MemoryAdapter` reads it from the start, and a query with no `fromVersion` would get one
  page). `handle` and `account` walks are never joined. A walk with its own start
  (`fromVersion`, `fromTime`, a saved cursor) is fetched on its own until it matches another.
- **`name` must differ per network and source.** Any tab on the name may answer the others.
- **Only `fetchEvents` is shared.** Views, head reads, block reads and transaction lookups run
  in the calling tab.
- **Without Web Locks or `BroadcastChannel`** (server rendering, older browsers) every call goes
  straight to the source, so each tab polls on its own. `isLeader` says whether this tab leads.
- **A leader that is alive but stuck** keeps the lock: the others wait for it until it answers,
  or, with the adapter's `requestTimeout` (pass the client's), fail with a
  `StreamdNetworkError` and retry. The same timeout stops the leader joining a source request
  that never settles.
- `close()` leaves the election and fails this tab's waiting requests. A source failure reaches
  the asking tab as a `StreamdNetworkError` (keeping `statusCode` and `retryAfter`), so it backs
  off as in the leading tab. Any other `StreamdError` arrives as a plain `StreamdError`, which
  stops a listener as invalid input, as it would in the leading tab.
- Each request and reply is broadcast to every tab and dropped by all but the one it is for, so
  each page is copied into every open tab.

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

### List running subscriptions

```ts
for (const s of streamd.listSubscriptions()) {
  console.log(s.id, s.descriptor.kind, s.paused, s.cursor, s.lastVersion, s.lastError?.message);
}
```

`listSubscriptions()` returns one frozen snapshot per subscription the client is running, in
start order: `on()`, `onBatch()`, `once()` and `stream()`, each walk of a merged list, and the
walks behind `reduce()`, `onTransaction()`, `watch()` and the chain helpers (`balances`,
`transfers` and the rest). Each holds `id`, a frozen copy of the `descriptor`, `listener` (whether
`listenerCount()` counts it), `active`, `paused`, `cursor`, `lastVersion`, `consecutiveErrors`,
`lastSuccessAt` and `lastError`, the most recent error it reported as `{ name, message, cause?,
at }`. `lastError` stays after the subscription recovers; `consecutiveErrors` says whether it is
still failing. URLs in the error text become `<url>`, and credentials become `<redacted>`: an
`Authorization` header's `Bearer` or `Basic` value in any case, a `Bearer` or `Basic` token of 8
or more characters elsewhere, and the value after a whole key-like name (`api_key=`,
`x-api-key:`, `"token":`, `password=`; compound names such as `refresh_token` are not
matched), since node URLs and headers can carry API keys; `cause` is the direct cause's
message only. Ended subscriptions are left out, unless you pass `{ ended: true }`: then the final
snapshots of the last 20 that ended follow the running ones, newest first, with `ended: true`, so
a walk that gave up still shows its last error. Every ending counts toward the 20, a resolved
`once()`, an unsubscribe and each subscription `destroy()` stops included, so on a busy client a
give-up can be pushed out within seconds. Error text is cut to 2,000 characters before
redaction. It is read-only: nothing in an entry pauses,
resumes or moves a subscription.

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

A type is matched as the source reports it. One written without type arguments also
matches its struct with any, so a generic event can be followed across every
instantiation: `0x190d...e12::liquidity_pool::SwapEvent` matches
`0x190d...e12::liquidity_pool::SwapEvent<0x1::aptos_coin::AptosCoin, ...>` for every pool, but
not `SwapEventV2`. A struct is either generic or not, so a non-generic type matches exactly what it
did before, and a type with arguments matches only that instantiation. This holds on
`FullnodeScanAdapter`, `MemoryAdapter`, both store adapters, `SuiAdapter` (whose node filter
works this way) and in `eventsOf`, `eventsAt`, `previewEvents` and the test assertions. An
`account` descriptor's `eventTypes`, the `ofType` predicate and a `RoutingAdapter` route are still
matched as before, and `defineEvents` types `event.type` as the key you wrote, so a generic
struct's events carry their type arguments past a `switch` on it. A persisted page cache
(`CachingAdapter` with a `PageStore`) written by an earlier release may hold an empty settled
page for such a type; clear it, or use a new namespace, before following generic events.

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

`ok` is true when every type is `event` or `legacy`. It makes one request per
distinct module, at the normalized address. A generic type is checked by its
outer struct only; neither its `<...>` type-argument count (`generic_type_params`)
nor the type arguments themselves are checked. Module scans (`on`, `getEvents`,
`streamd tail`, `streamd forward`) match an unparameterized `address::module::Struct`
against any `<...>`, but require an exact string match whenever `<...>` is written
and do not normalize addresses, so wrong type-argument arity or an address not in
the fullnode's reported form still passes here while matching nothing in a scan.

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

A function of your own that is generic over the payload can take an `EventDescriptor<T>` and
use `StreamdEvent<T>` as before. To accept `defineEvents` descriptors generically as well,
parameterize over the event type with `EventDescriptor<T, TEvent>` and `TEvent = StreamdEvent<T>`.

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

### Sender names

`withNames` on a query or listener attaches `senderName`, the primary name of each event's
`sender`, as `primaryName` reads it:

```ts
const deposits = await streamd.getEvents(DEPOSITS, { fromVersion: -1000n, withNames: true });
for (const e of deposits) console.log(e.senderName ?? e.sender);
```

- **The field.** `senderName` is the name (`alice.apt`), `null` when the sender was looked up
  and has none, and absent when nothing was looked up: no `withNames`, no `sender`, or a
  `sender` that is not a hex address. It round-trips through `serializeEvent`/`parseEvent`,
  and an `onTransaction` group carries its events' name.
- **Where.** Every call that takes `withState` takes it: `getEvents`, `getEventPages`,
  `iterateEvents`, `getTransactions`, `replay`, `on`, `onBatch`, `once`, `stream`,
  `onTransaction`, `watch`, `transfers`, `getTransfers`, `activity`, `getActivity`, `swaps`, `getSwaps`, `sales` and
  `getSales` (where `activity` also names every counterparty, below). Not `reduce`, `balances`, or the
  one-shot transaction calls (`eventsOf`, `eventsAt`, `previewEvents` and the assertions).
- **After `where`.** It runs after `decode`, `where` and `withState`, and for queries after
  `limit` and `orderBy`, so only the events you get back cost a lookup.
- **Cached per client.** One cache per client, shared by every call, keyed by long-form
  address. A name, or a miss, is reused for 5 minutes; `withNames: { ttl }` sets another
  expiry in ms (`0` reuses nothing, `Infinity` never expires), each call reading the shared
  entry by its own. Concurrent lookups for one address share one request. At most 10,000
  addresses are kept, the one fetched longest ago leaving first. Failures are not cached.
- **Through the limiter.** Each lookup takes one slot of `maxConcurrentRequests` and
  `maxRequestsPerSecond`, and `requestTimeout` applies to it (on Aptos, `primaryName` is two
  reads in that slot when a name exists).
- **Failures, as `withState`.** A failed lookup rejects a query, with its `retries`, and fails
  a listener's poll: events before it are delivered and the next poll retries from it,
  counting toward `maxErrors`. A name is cosmetic, but quietly leaving it off would make
  "absent" mean two things, and every later event from that sender would try again at once
  rather than back off from a node that is rate-limiting.
- **Networks.** `AptosClient` serves names on mainnet. On testnet, devnet, a localnet and
  custom networks such as the Movement presets, `withNames` is refused at the call with a
  `StreamdError`. A localnet has no ANS published, so every lookup would fail. A plain
  `StreamdClient` refuses it too: it has no name service.
- **Sui.** `SuiClient` serves SuiNS primary names the same way, through `@streamd/sui`'s
  `primaryName`.

## Asset transfers

`transfers(callback, options?)` follows coin, fungible-asset and token movements as
`{ from, to, asset, amount }`, `getTransfers(filter?)` returns them for a past range, and
`iterateTransfers(filter?)` yields them one at a time. Each arrives as an event whose `data` is a `Transfer`:

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
- **Values.** `withValue: true` adds `value`, the amount in USDC at the record's version, to
  APT, USDt and USDC records; see [Values](#values).
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

## Account activity

`activity(address, callback, options?)` follows every transaction an address sent or
received as one readable entry, and `getActivity(address, filter?)` returns them for a past
range. Each arrives as an event whose `data` is an `Activity`, and whose envelope is the
transaction's: `version`, `transactionHash`, `timestamp`, `sender` and `entryFunction`.

```ts
import { RoutingAdapter } from "@streamd/core";
import { AptosClient, FullnodeScanAdapter, IndexerAccountAdapter } from "@streamd/aptos";

const streamd = new AptosClient({
  aptos,
  adapter: new RoutingAdapter({
    account: new IndexerAccountAdapter(aptos, { apiKey: process.env.APTOS_API_KEY }),
    default: new FullnodeScanAdapter(aptos),
  }),
});

const history = await streamd.getActivity("alice.apt", { fromVersion: -100_000n, withNames: true });
for (const { version, entryFunction, data } of history) {
  console.log(version, data.direction, entryFunction?.function, data.balanceChanges);
}

const sub = streamd.activity("0x1234...", ({ data }) => console.log(data.transfers), { withAssetInfo: true });
```

- **The entry.** `direction` is `"sent"` when the address is the transaction's sender and
  `"received"` for anything else that affected it: a payment in, a sponsored transaction it
  only paid gas for, a system transaction. `transfers` are the transaction's `Transfer`
  records whose `from` or `to` is the address, built and paired exactly as `transfers()`
  builds them (`withAssetInfo` adds `symbol` and `decimals`). `balanceChanges` is the net
  `{ asset, delta }` per asset over those transfers, positive for a gain, in order of first
  appearance, with zero nets (a transfer to itself) left out. `events` is every event the
  transaction emitted, in `eventIndex` order.
- **Gas.** The fee is burned without a withdraw event, so `balanceChanges` leaves it out:
  for a sent transaction, the APT balance also fell by `gas_used * gas_unit_price` less any
  storage refund. The transaction's `0x1::transaction_fee::FeeStatement` is in `events`.
- **Direct owners.** As with `transfers()`, `from` and `to` are the stores' direct owners,
  so a store the address holds through an object does not count toward its deltas, and APT
  moved as a coin (`0x1::aptos_coin::AptosCoin`) and as the fungible asset `0xa` are
  separate assets.
- **Names.** `address` may be an Aptos name, resolved once on the first page. Under
  `withNames`, `senderName` is set as on any event, and `names` maps the long-form address,
  the sender and every `from` and `to` in `transfers` to a primary name or `null`, through
  the client's name cache. These lookups run while the page is built, before `where`.
- **Source.** It reads an `account` descriptor with `received: true`, which the client's
  adapter must serve: `IndexerAccountAdapter`, routed beside a fullnode for the `view`
  reads that resolve fungible stores, as above. An adapter without `view` is refused at
  the call; `FullnodeScanAdapter` alone refuses `received` and says so, a query rejecting
  and a subscription reporting it. Reads stop at the Indexer's processed version and
  start no earlier than the fullnode's oldest version, as for `IndexerAccountAdapter`.
- **Identity.** An entry keeps its transaction's first event's `(version, eventIndex)`, so
  cursors, dedup, `eventId`, `where`, `limit` and `onCursor` work as for any event. A
  transaction that emitted no events gives no entry; every user transaction emits its
  fee statement.
- **Not here.** Whether the transaction succeeded is not on the entry. An activity subscription is not a listener for `off()` or
  `listenerCount()`; `unsubscribe()` or `destroy()` ends it.

## Fund-flow tracing

`trace(address, options?)` follows the funds around an address for a few hops and resolves a
counterparty graph: every address reached, and every transfer between them summed per
sender, receiver and asset. `tracePath(graph, from, to)` reads the shortest chain of
transfers from one address to another out of that graph.

```ts
import { RoutingAdapter } from "@streamd/core";
import { AptosClient, FullnodeScanAdapter, IndexerAccountAdapter, tracePath } from "@streamd/aptos";

const streamd = new AptosClient({
  aptos,
  adapter: new RoutingAdapter({
    account: new IndexerAccountAdapter(aptos, { apiKey: process.env.APTOS_API_KEY }),
    default: new FullnodeScanAdapter(aptos),
  }),
});

const graph = await streamd.trace("0x1234...", {
  hops: 2,
  fromVersion: -1_000_000n,
  minAmount: 1_000_000n,
  stop: ["0x5678..."], // an exchange: kept as a node, never expanded
});
for (const { from, to, asset, amount, transfers } of graph.edges) console.log(from, to, asset, amount, transfers);

const path = tracePath(graph, "0x1234...", "0x9abc...");
```

- **The walk.** Breadth first from `address` (an address or Aptos name, resolved once). Each
  node within `hops` (default 1, at most 6) is read once, one node at a time, as
  `getActivity` reads it: every transaction it sent or received, with the `Transfer` records
  it took part in, built and paired as `transfers()` builds them. Each counterparty joins at
  the next hop. `direction` follows what a node sent (`"out"`), received (`"in"`) or both, the
  default.
- **The graph.** `nodes` are `TraceNode { address, hop, expanded }` in the order they joined;
  `expanded` is false for a node at the hop cap, on the `stop` list, or not reached before the
  walk stopped. `edges` are `TraceEdge { from, to, asset, amount, transfers, firstVersion,
  lastVersion }`, one per sender, receiver and asset, `amount` the exact `bigint` sum in base
  units. A transfer read from both of its ends counts once. A transfer with no `from` or no
  `to` (a lone withdraw or deposit), or to the same address, makes no edge. Addresses are long
  form, and assets are in `Transfer.asset`'s form, so APT as a coin and as the fungible asset
  `0xa` are separate edges.
- **Narrowing.** `minAmount` ignores transfers below that many base units, whatever their
  asset, so pair it with `asset` (any form `transfers()` takes) to make it mean one unit.
  `stop` and `target` take addresses, not names.
- **The range.** `fromVersion` and `toVersion` bound every node's read. `toVersion` defaults to
  `"head"`, and the head is read once when the trace starts, as is the base of a negative
  `fromVersion`, so every node is read over the same range. Reads start no earlier than the
  fullnode's oldest version and stop at the Indexer's processed version, as for
  `getActivity`.
- **Bounds.** `maxNodes` (default 100) caps the graph, root included: the transfer that would
  add one more node is left out and the walk ends. `maxRequests` (default 1000) caps adapter
  calls, the head read, page reads and the views that resolve fungible stores alike, though
  one page read on `IndexerAccountAdapter` is several HTTP requests (a listing, then one
  fullnode read per transaction). Each node's pages fold into the graph as they arrive: when
  `maxRequests` runs out on a page or one of its views, that unfinished page is dropped,
  earlier finished pages stay in the graph, and the node keeps `expanded: false`.
  `target` ends the walk as soon as that address joins. Whichever ended it is `stoppedBy`
  (`"nodes"`, `"requests"` or `"target"`), absent when the walk ran to its end (stop-listed
  nodes are never read either way), and
  `requests` counts the calls made. `retries` and `signal` apply to the head read and to each node's
  read as for `getEvents`, and any other failure rejects the trace.
- **Paths.** `tracePath(graph, from, to)` follows edges in their direction and returns the
  fewest edges from `from` to `to`, `[]` for the same address, or `undefined` when the graph
  holds no such chain. It only knows the graph: a trace that stopped early, or a `minAmount`,
  can hide a path that exists on chain.
- **Source.** The client's adapter must serve an `account` descriptor with `received: true`:
  `IndexerAccountAdapter` routed beside a fullnode, as above, since the fullnode alone sees
  only transactions an account signed. The head and page reads take the client's
  `maxConcurrentRequests` slots and `maxRequestsPerSecond` pacing; the store views run beside
  them, up to 4 at a time and not paced, as for `transfers()`, and store metadata is read once
  per trace. Each node's read costs calls in proportion to the transactions it made, so put
  exchanges and routers in `stop` or a busy node can still spend the rest of the budget
  across its pages.
- **Aptos only.** Over `getTransfers({ account })` every node would scan every transaction
  in the range, so the walk stays on the account path. There is no `SuiClient.trace`.

## DEX swaps

`swaps(callback, options?)` follows swaps on the main Aptos DEXes as they land, and
`getSwaps(filter?)` returns them for a past range. Each arrives as an event whose `data` is a
`Swap { protocol, poolId, trader, assetIn, amountIn, assetOut, amountOut }`, one per pool hop:

```ts
const sub = streamd.swaps(
  ({ data }) => console.log(`${data.protocol}: ${data.amountIn} ${data.assetIn} -> ${data.amountOut} ${data.assetOut}`),
  { asset: "0xa" }, // swaps with APT on either side; omit for every swap
);

const thala = await streamd.getSwaps({ fromVersion: -10_000n, protocol: ["thala-clamm", "thala-v2"] });
```

- **Sources.** One scan over these swap events, each decoded from its on-chain struct, with
  no view read:

  | `protocol` | Package | Event |
  | --- | --- | --- |
  | `"hyperion"` | `0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c` | `pool_v3::SwapEventV3` |
  | `"thala-clamm"` | `0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920` (ThalaSwap CLAMM) | `pool::SwapEvent` |
  | `"thala-v2"` | `0x007730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5` (ThalaSwap v2) | `pool::SwapEvent` |
  | `"cellana"` | `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1` | `liquidity_pool::SwapEvent` |
  | `"tapp"` | `0x487e905f899ccb6d46fdaec56ba1e0c4cf119862a16c409904b8c78fab1f5e8a` (Tapp router) | `router::Swapped` |
  | `"liquidswap"` | `0x190d44266241744264b964a37b8f09863167a12d3e70cda39376cfb4e3561e12` (v0), `0x0163df34fccbf003ce219d3f1d9e70d140b60622cb9dd47599c25fb2f797ba6e` (v0.5) | `liquidity_pool::SwapEvent<X, Y, Curve>` |
  | `"pancakeswap"` | `0xc7efb4076dbe143cbcd98cfaaa929ecfc8f299203dfff63b95ccb6bfe19850fa` | `swap::SwapEvent<X, Y>` |
  | `"thala-v1"` | `0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af` (ThalaSwap v1) | `stable_pool::SwapEvent<..>`, `weighted_pool::SwapEvent<..>` |

  `protocol`, one or a list, narrows the scanned types; an unknown name throws. The last three
  emit legacy handle events generic over the pool's coin types, followed for every pool by
  naming the struct without type arguments (see [Choosing events](#choosing-events)).
- **Trader.** None of these events names the swapper, and routers and aggregators call the
  pools on a user's behalf, so `trader` is the transaction's sender in long form (for a
  sponsored transaction, the user, not the fee payer). It is absent when the source reports no
  sender.
- **Direction.** Hyperion and Cellana name the token in and out. Thala CLAMM reports its two
  assets and `zero_for_one`; ThalaSwap v2 and Tapp an asset list and the in and out indexes into
  it; ThalaSwap v1 indexes its first four type arguments, the pool's coins. LiquidSwap and
  PancakeSwap report an in and an out amount for each of the pair's two coins: one side in and
  the other out, or the event is skipped.
- **Pools.** `poolId` is the pool object's address in long form. LiquidSwap, PancakeSwap and
  ThalaSwap v1 pools are not objects (all of one package's pools sit under one account), so
  their `poolId` is the pool's resource type with the event's type arguments, addresses in long
  form: `<package>::liquidity_pool::LiquidityPool<X, Y, Curve>`,
  `<package>::swap::TokenPairMetadata<X, Y>`, `<package>::stable_pool::StablePool<..>` or
  `<package>::weighted_pool::WeightedPool<..>`.
- **Assets.** A fungible asset is its metadata address in long form, as `Transfer.asset`
  reports it. Cellana names a coin by its type (`0x1::aptos_coin::AptosCoin`, addresses inside
  in long form), and LiquidSwap, PancakeSwap and ThalaSwap v1 pools hold coins, named the same
  way. So APT is `0x...0a` on Hyperion, Thala CLAMM, ThalaSwap v2 and Tapp, and the coin type on
  the coin pools: no coin is mapped to its paired fungible asset. `asset` keeps swaps with that asset on
  either side and takes what `transfers`'s `asset` takes.
- **Amounts.** `amountIn` and `amountOut` are the event's base units: what the pool took in and
  paid out, its own fee inside `amountIn`, not split out. A fee taken before the pool is not in
  them, so the trader can have paid more: Hyperion's router keeps about 1 basis point
  (`fee_sharer::DistributeEvent`), and an aggregator may take its own. On a Thala CLAMM
  exact-out swap, `amountIn` is net of the refund.
- **Routes.** A multi-hop route gives one record per pool hop, in event order, not joined:
  an aggregator may take a fee between hops, so one hop's `amountOut` need not equal the
  next one's `amountIn`.
- **Identity.** A record is the swap event with `data` replaced, so it keeps the event's
  `(version, eventIndex)`, `transactionHash`, `timestamp`, `sender` and `entryFunction`;
  cursors, dedup, `where` (`fromSender`, `whereField("poolId", ...)`), `limit` and the other
  options work as for any event.
- **Limits.** PancakeSwap's `user` and Tapp's `creator` are not read. Hyperion's earlier `SwapEvent` and `SwapEventV2` are not read. An event whose fields do not
  have its struct's shape is skipped. A swaps subscription is not a listener for `off()` or
  `listenerCount()`; `unsubscribe()` or `destroy()` ends it.

## NFT sales

`sales(callback, options?)` follows NFT marketplace sales as they land, and
`getSales(filter?)` returns them for a past range. Each arrives as an event whose `data` is a
`Sale { marketplace, kind, token, collection?, seller, buyer, price, royalty?, fee? }`, one per
token sold:

```ts
const sub = streamd.sales(({ data }) => console.log(`${data.marketplace} ${data.kind}: ${data.token} for ${data.price}`));

const wapal = await streamd.getSales({ fromVersion: 7028273582n, toVersion: 7028273582n, marketplace: "wapal" });
```

- **Sources.** One scan over these fill events, all legacy handle events, each decoded from its
  on-chain struct, with no view read:

  | `marketplace` | Package | Events | `kind` |
  | --- | --- | --- | --- |
  | `"wapal"` | `0x584b50b999c78ade62f8359c91b5165ff390338d45f8e55969a04e65d76258c9` | `events::ListingFilledEvent`, `events::TokenOfferFilledEvent`, `events::CollectionOfferFilledEvent` | `listing`, `token-offer`, `collection-offer` |
  | `"tradeport"` | `0xe11c12ec495f3989c35e1c6a0af414451223305b579291fc8f3d9d0575a23c26` | `listings_v2::BuyEvent`, `biddings_v2::AcceptTokenBidEvent`, `biddings_v2::AcceptCollectionBidEvent`, and the same three in `listings` and `biddings` for legacy tokens | `listing`, `token-offer`, `collection-offer` |

  `marketplace`, one or a list, narrows the scanned types; an unknown name throws.
- **Activity.** Aptos NFT trading has nearly stopped: the last fills on both marketplaces are
  around version 7,030,000,000 (September 2026), and Tradeport cancelled its open bids at
  7125917832. Those versions are older than the public fullnode keeps, so read them through
  [the archive](#history-the-fullnode-has-pruned); `sales()` delivers only if trading resumes.
- **The order book names the marketplace.** Tradeport's aggregator buys Wapal listings, and such
  a fill emits only Wapal's event, so it is a `wapal` sale; one transaction can hold sales on
  both. A fee the aggregator adds on top is in no event and not in `price`.
- **Buyer and seller** come from the event, in long form, never from the sender: an offer fill
  is sent by the seller, an aggregated buy by the buyer. A Tradeport legacy-token collection bid
  leaves the token in Tradeport's store for the buyer to claim later, so that sale's transaction
  withdraws the token from the seller without depositing it to the buyer.
- **Price** is what the buyer paid, in base units, royalty and fee inside: on Wapal `price`
  equals `royalty` plus `fee` (the marketplace's commission) plus the seller's proceeds. Tradeport's
  events report only the price, so its records have no `royalty` or `fee`. Neither
  marketplace's events name the coin: Tradeport takes only APT, and every Wapal fill seen paid
  APT, but Wapal's orders are generic over the coin type, so no currency field is reported.
- **Tokens.** `token` is a token object's address in long form, or a legacy token's id as
  `<creator>::<collection>::<name>::<property_version>`, the forms `Transfer.asset` reports.
  `collection` is the collection object's address, present only when the event names one (Wapal,
  token objects). The `token` option keeps sales of one token, in either form; a collection is
  matched with `where: whereField("collection", ...)`, which only Wapal records can satisfy.
- **Identity.** A record is the fill event with `data` replaced, so it keeps the event's
  `(version, eventIndex)`, `transactionHash`, `timestamp`, `sender` and `guid`, and a bulk buy gives
  one record per token, in event order.
- **Limits.** Rarible's marketplace (`0x465a0051e8535859d4794f0af24dbf35c5349bedadab26404b20b825035ee790`)
  is not read: no fill of it could be found. Neither are Topaz, BlueMove, Souffl3 (no activity found)
  or Strike (token-for-token offers that are no single sale). Wapal's events come from its
  marketplace package, so a storefront that created its own fee schedule on that package is
  reported as `wapal` too. An event whose fields do not have its struct's shape is skipped. A
  sales subscription is not a listener for `off()` or `listenerCount()`.

## Governance proposals

`proposals(options?)` keeps every Aptos governance proposal it sees current: what was proposed,
the stake for and against, each vote, and the resolution. `value` is a map by proposal id of
`Proposal { id, proposer?, stakePool?, executionHash?, metadata?, expirationSecs?,
minVoteThreshold?, earlyResolutionVoteThreshold?, createdAt?, yesVotes, noVotes, votes, resolution? }`,
and `proposalState(proposal, nowSecs?)` gives what `0x1::voting::get_proposal_state` would report:

```ts
import { proposalState } from "@streamd/aptos";

const governance = streamd.proposals({
  onChange: (p, event) => console.log(`proposal ${p.id}: ${p.yesVotes} yes, ${p.noVotes} no at ${event.version}`),
});
await governance.ready;
for (const p of governance.value.values()) console.log(p.id, proposalState(p), p.votes.length);
```

- **Sources.** Five walks, each its own `reduce()`, folding into one map:

  | Walk | Gives |
  | --- | --- |
  | `0x1::voting::CreateProposalEvent` (handle, creation number 5 at `0x1`) | `executionHash`, `metadata`, `expirationSecs`, `minVoteThreshold`, `earlyResolutionVoteThreshold`, `createdAt` |
  | `0x1::aptos_governance::CreateProposalEvent` (`GovernanceEvents.create_proposal_events`) | `proposer`, `stakePool` |
  | `0x1::aptos_governance::VoteEvent` (`GovernanceEvents.vote_events`) | one `ProposalVote { voter, stakePool, votes, shouldPass, version }` each, summed into `yesVotes` or `noVotes` |
  | `0x1::voting::ResolveProposal` (handle, creation number 7) | `resolution { version, yesVotes, noVotes, early, steps }` |
  | one scan over `voting::CreateProposal`, `aptos_governance::CreateProposal`, `aptos_governance::Vote` and `voting::ResolveProposal` | the same, for proposals after the module-event migration |

  The legacy handles carry proposals 0 to 134 (the migration itself, resolved at version
  2812968150); every later proposal emits only module events. `voting::Vote` is not read, since
  `aptos_governance::Vote` is emitted beside it with the voter, pool and direction. A legacy
  resolution, whose type the module event shares, is folded from its handle only. Votes are
  kept in version order and `resolution` is the lowest-version one, whichever walk folds first.
- **Forged forums are dropped.** `0x1::voting` is a library any account can host its own forum
  on, and its module events name no forum, so anyone can emit a `voting::CreateProposal` or
  `ResolveProposal` with a real proposal's id. The module walk keeps a `voting::CreateProposal`
  only when `aptos_governance::CreateProposal`, which only governance emits, follows it in the
  same transaction, and a `ResolveProposal` only when `0x1`'s forum shows it at that version: the
  id is below `next_proposal_id` at the version before (no proposal resolves in the transaction
  that creates it), and the proposal became resolved there (a last step) or its execution hash
  changed (an earlier step), by up to four `0x1::voting` views at the version and the one before,
  at most two at a time, outside `maxConcurrentRequests` (the page asking for them holds a slot)
  and each timed by `requestTimeout`. A failed view fails the page, which retries. So
  `proposals()` needs an adapter with `view`, and throws at the call without one. The legacy handles are `0x1`'s own and `aptos_governance` events cannot be forged, so
  neither is checked. Pairing reads one page at a time, which `FullnodeScanAdapter` keeps to whole
  transactions; a source that splits a transaction across pages would drop that creation.
- **Counts.** `yesVotes`, `noVotes` and `votes` count the votes seen; a tracker that started
  after a vote has not seen it. A resolution's figures are final, since no vote lands once
  voting closes, and `proposalState` uses them when there is one. A multi-step proposal resolves
  once per step, so `resolution.version` is the first step's and `steps` counts them; mainnet's
  proposal 205 took seven. No event marks the last step, so `resolution` is present once any
  step resolved: mainnet's multi-step proposals 68 and 88 resolved one step each and never ran
  the rest, so `0x1::voting::is_resolved` is false for them.
- **State.** `proposalState` is `"pending"` until `expirationSecs` passes or either side reaches
  `earlyResolutionVoteThreshold`, then `"succeeded"` when yes beats no and the two reach
  `minVoteThreshold`, else `"failed"`; `undefined` when the creation was not seen. `nowSecs`
  (Unix seconds) defaults to the wall clock; pass the ledger timestamp to match the chain exactly.
- **As created.** `executionHash` and `metadata` are the creation's: a multi-step proposal's
  `get_execution_hash` view moves on to later steps. `metadata` values are text, or hex as the
  fullnode writes them when the bytes are not printable UTF-8, such as the `0x01` of
  `IS_MULTI_STEP_PROPOSAL_KEY`. Addresses are in long form. A vote through a delegation pool
  names the pool's own voter, not the delegator.
- **History needs the right sources.** Each walk reads from `fromVersion`, as `getEvents` does
  when it is omitted: a handle from its first event, and a `FullnodeScanAdapter` module scan from
  the head. Mainnet's legacy events are all older than the public fullnode keeps, and its handle
  endpoint answers that pruned history with an empty page rather than `410`, so
  `ArchiveFallbackAdapter` never falls back for it: route `handle` to the archive instead. No
  public source serves module events by type, so the module walk is a transaction scan; history
  before the head needs `fromVersion` and a source that can scan or index that far (a store from
  `streamd store`, an archive with an API key for a narrow range):

  ```ts
  const streamd = new AptosClient({
    aptos,
    adapter: new RoutingAdapter({
      handle: new FullnodeScanAdapter(new Aptos(new AptosConfig(networks.aptosMainnetArchive))),
      default: new FullnodeScanAdapter(aptos),
    }),
  });
  ```

  The legacy handles hold 199,058 votes, about 2,000 archive pages, well past the archive's
  anonymous rate, so give it an API key.
- **Following.** Each walk polls on its own cursor after its backfill, so five requests go out
  per `pollInterval`, four of them to legacy handles that no longer grow on mainnet;
  `maxPollInterval` lets the quiet ones back off. `ready` resolves with the map once every walk
  is following. The map and its records are updated in place; `onChange` fires for each live
  event that changed a proposal. Options: `fromVersion`, `onChange`, `onError`, `signal`,
  `pollInterval`, `batchSize`, `maxErrors`, `maxBackoff`, `maxPollInterval` and `retries`;
  anything else throws.
- **Stops together.** A walk that fails its backfill, gives up after `maxErrors`, or meets an
  event of a known type without its fields stops every walk, reported once through `onError`,
  since the map would otherwise go on missing that walk's events. There is no cursor to resume
  from: a restart reads the history again. Like `reduce()`, it is not a listener for `off()` or
  `listenerCount()`; `unsubscribe()` or `destroy()` ends it.

## Stake pools

`stakePools(options?)` keeps every Aptos stake pool it sees current: what each pool was paid at
every epoch change, its delegation pool commission changes, and each delegator's stake moves.
`value` is a map by long-form pool address of
`StakePool { address, rewards, commissionPercentage?, commissionChanges, delegators }`:

```ts
import { formatAmount } from "@streamd/aptos";

const pools = streamd.stakePools({
  // Three hours back, so at least one epoch change: a scan of every transaction since, which needs an API key.
  fromTime: BigInt(Date.now() - 3 * 3600 * 1000) * 1000n,
  onChange: (pool, event) => console.log(pool.address, event.type, pool.rewards.at(-1)),
});
await pools.ready;
for (const pool of pools.value.values()) {
  const last = pool.rewards.at(-1);
  if (last !== undefined) console.log(pool.address, `epoch ${last.epoch}`, formatAmount(last.rewards + last.fees, 8), "APT");
}
```

- **Sources.** One scan over eight module types, folded by one `reduce()`:

  | Event | Gives |
  | --- | --- |
  | `0x1::stake::DistributeRewards` | `EpochRewards.rewards`, zero included |
  | `0x1::stake::DistributeTransactionFee` | `EpochRewards.fees` (`0n` when the pool was paid none) |
  | `0x1::reconfiguration::NewEpoch` | `EpochRewards.epoch`, the epoch that ended (`NewEpoch.epoch - 1`) |
  | `0x1::delegation_pool::CommissionPercentageChange` | one `CommissionChange { percentage, owner, version }`, the latest as `commissionPercentage` |
  | `0x1::delegation_pool::AddStake`, `UnlockStake`, `ReactivateStake`, `WithdrawStake` | one `Delegator { address, added, addStakeFees, unlocked, reactivated, withdrawn, firstAddedAt?, lastVersion }` per delegator, summed |

  Only the module defining an event type can emit it, so every event read is the framework's
  own. Amounts are octas, commission hundredths of a percent (`1000n` is 10%), and the
  commission is the one for the pool's next lockup cycle, as the change sets it.
- **Epoch changes.** `stake::on_new_epoch` emits a fee (when non-zero) and a reward for every
  active pool, then `NewEpoch` last, in one transaction (a `validator_transaction` on mainnet,
  which runs randomness). Payments are held until that `NewEpoch` names the epoch, then appended
  to each pool's `rewards` as one `EpochRewards { epoch, rewards, fees, version }`; a pool's
  total stake (`0x1::stake::get_stake`, all four parts) rises by exactly `rewards + fees` there.
  A source that delivers payments with no `NewEpoch` in their transaction stops the monitor.
- **Joins.** `firstAddedAt` is the first `AddStake` seen: a join when the monitor read the pool
  from before the delegator's first stake. `added` counts the amount added, `addStakeFees` the
  fee taken from it, so the delegator's active stake rose by the difference.
- **Narrowing.** `pool` (an address or a non-empty list) keeps only those pools; the scan is the
  same, since every pool's events share these types. Other pools' events are still checked, so
  a malformed one stops the monitor.
- **Who appears.** Anyone can create a delegation pool and stake to it from as many addresses
  as they like, so `value` holds a record for every pool and delegator seen, validators or not;
  only pools in the active validator set get `rewards` entries.
- **History needs the right sources.** No public source serves module events by type, so the
  scan reads every transaction from `fromVersion` or `fromTime` (as `getEvents` does when both
  are omitted: a `FullnodeScanAdapter` from the head). Epochs change about every two hours on
  mainnet, so a monitor started at the head sees its first rewards within two hours; a start
  further back reads every transaction since, which takes an API key past a few minutes of
  mainnet history. Legacy handle events (`DistributeRewardsEvent`, `AddStakeEvent` and the rest,
  before the module-event migration) are not read.
- **Not covered.** Commission set through `staking_contract` (non-delegation pools), operator
  commission paid out (`DistributeCommission`), `stake::AddStake` and `UnlockStake` of pools
  without delegators, delegated voting, validator set joins and leaves, and lockup changes.
- **Following.** `ready` resolves with the map once the history is folded and the scan is
  following. The map and its records are updated in place, and nothing is evicted: `rewards`
  grows by one entry per pool per epoch (about twelve a day) and `delegators` by one per
  delegator seen, so narrow a long-running monitor with `pool`. `onChange(pool, event)` fires for
  each live event that changed a pool: once per pool an epoch change paid, with its `NewEpoch`.
  Options: `pool`, `fromVersion`, `fromTime`, `onChange`, `onError`, `signal`, `pollInterval`,
  `batchSize`, `maxErrors`, `maxBackoff`, `maxPollInterval` and `retries`; anything else throws.
- **Stops.** A failed backfill, `maxErrors` reached, or an event of a known type without its
  fields stops it, reported through `onError`. There is no cursor to resume from: a restart
  reads the history again. Like `reduce()`, it is not a listener for `off()` or
  `listenerCount()`; `unsubscribe()` or `destroy()` ends it.

## Multisig accounts

`multisig(address, options?)` keeps one `0x1::multisig_account` current and previews what its
pending transactions would do. `value` is a `MultisigState { address, version, owners,
threshold, timelock?, lastResolved, nextSequenceNumber, pending }`, `pending` a map by sequence
number of `PendingMultisigTransaction { sequenceNumber, creator, createdAtSecs, payload?,
payloadHash?, call?, approvals, rejections, executableAtSecs? }`:

```ts
const treasury = streamd.multisig("0x...", {
  onChange: (state, event) => console.log(event.type, [...state.pending.keys()]),
});
await treasury.ready;
for (const tx of treasury.value.pending.values()) {
  console.log(tx.sequenceNumber, tx.call?.function, `${tx.approvals.length}/${treasury.value.threshold}`);
  const preview = await treasury.preview(tx.sequenceNumber);
  console.log(preview.partial ? "partial" : `as ${preview.sender}`, preview.events.map((e) => e.type));
}
```

- **State from views.** Seven `0x1::multisig_account` views at one version: `owners`,
  `num_signatures_required`, `last_resolved_sequence_number`, `next_sequence_number`,
  `get_pending_transactions`, `timelock_period` and `timelock_override_threshold`, through
  `EventAdapter.view`, so the adapter must implement it and `getHeadVersion`. Pending items are
  numbered from `lastResolved + 1`, since the view returns them in order without their numbers.
  `call` is the entry function a stored payload calls, decoded with the SDK's
  `MultiSigTransactionPayload`, its arguments each a BCS value as `0x` hex (their types need the
  function's ABI); an item stored by hash has `payloadHash` and no `call`. `executableAtSecs` is
  the creation time plus the timelock period while the approvals are below the override
  threshold.
- **Live changes.** The account never signs (owners create and vote, an owner executes), so an
  `account` walk sees nothing. Instead one scan over every `0x1::multisig_account` event type but
  `MetadataUpdated` (metadata is not kept) is
  narrowed by the events' `multisig_account` field, and the views are reread at the version of
  each of the account's events, once per version and at most four at a time, outside
  `maxConcurrentRequests`; the state is replaced field by field in place. The legacy
  `EventHandle`s on `MultisigAccount` are not read: the framework no longer writes them.
  `onChange(state, event)` fires after each live event of the account. `fromVersion` (an absolute
  `bigint`) reads the state at the version before and folds every change after it before `ready`;
  without it the state is read at the head. A `fromVersion` older than the timelock views fails.
- **Previews.** `preview(sequenceNumber, { payload? })` simulates with no key through
  `previewEvents`. The queue position is read again at the head, and a transaction resolved
  there is refused. For the next transaction in the queue, the first owner (in `owners` order)
  for whom `can_execute(owner, multisig, sequenceNumber)` is true at the head is the sender of a
  multisig-wrapped transaction, as an execution would run; `can_execute` counts that owner's own
  approval when they have not voted, so a transaction one approval short previews too. Otherwise
  (further short, held by the timelock, or behind another pending transaction) the entry
  function is simulated directly as the multisig, sponsored by the first owner so the multisig
  needs no APT, and the preview is `partial`: it skips the multisig's checks and runs from the
  current state, not after the transactions before it. The multisig's own `Vote`,
  `TransactionExecutionSucceeded` and `TransactionExecutionFailed` go in `executorEvents`, since
  they name the simulated executor rather than any vote cast; a payload that would abort shows
  as `TransactionExecutionFailed` there, with only the fee statement in `events`.
- **Payloads stored by hash.** Pass the payload (BCS `MultisigTransactionPayload` bytes or `0x`
  hex); it is checked against `payloadHash` by SHA3-256 before anything is simulated, and a
  payload passed for a transaction that stores its own is refused.
- **Stops.** A failed start read rejects `ready` (an account that is not a multisig aborts the
  views), as does `unsubscribe()`, `destroy()` or `signal` before the start read finishes; a
  failed view on a live page fails the poll, which retries as for `on()`. Options:
  `fromVersion`, `onChange`, `onError`, `signal`, `pollInterval`, `batchSize`, `maxErrors`,
  `maxBackoff`, `maxPollInterval` and `retries`; anything else throws. `address` may be an
  Aptos name. Like `reduce()`, it is not a listener for `off()` or `listenerCount()`;
  `unsubscribe()` or `destroy()` ends it. Metadata is not kept, and nothing is submitted.

## Lending positions

`positions(protocol, options?)` keeps lending positions current, each as the protocol computes
it at a version through its own views and its own oracle. `"aave"` reads Aave V3 on Aptos
mainnet (package `AavePool` at `0x39ddcd9e...016fb`, priced by `AaveOracle` over Chainlink), and
`"echelon"` reads Echelon's vaults (`0xc6bc659f...21ba`, below).
`value` maps each long-form account to a `LendingPosition { protocol, account, version, assets,
collateralValue?, debtValue?, liquidationThreshold?, health?, below?, unknown? }`, `assets` the
reserves it supplies or borrows as `PositionAsset { asset, decimals, supplied, borrowed,
collateral }` in base units:

```ts
const book = streamd.positions("aave", {
  account: ["0x...", "0x..."],
  threshold: { numerator: 11n, denominator: 10n },
  onCross: (p) => console.log(p.account, p.below ? "below 1.1" : "back above 1.1"),
});
await book.ready;
for (const p of book.value.values()) {
  if (p.health) console.log(p.account, Number(p.health.numerator) / Number(p.health.denominator));
}
```

- **Health is Aave's own number.** Every figure is `user_logic::get_user_account_data`'s, which
  `generic_logic::calculate_user_account_data` computes (the package publishes its source):
  collateral sums `ray_mul_down(scaled aToken balance, normalized income) * price / 10^decimals`
  over reserves used as collateral, debt sums `ceil_div(price * ray_mul_up(scaled debt,
  normalized debt), 10^decimals)`, the liquidation threshold is the collateral-weighted average
  (e-mode's for e-mode assets), and the health factor is `wad_div(percent_mul(collateral,
  threshold), debt)`. Prices are `oracle::get_asset_price`, USD with 18 decimals. `health` is
  that WAD as an exact `Fraction { numerator, denominator }` over 10^18, reduced, so `health` is
  below 1 exactly when `validation_logic` would allow a liquidation at that version
  (`health_factor < 1e18`). `collateralValue` and `debtValue` are USD over 10^18 and
  `liquidationThreshold` basis points over 10^4, all reduced. No debt means no `health` and
  `below` false.
- **Unknown, never guessed.** When the account data view aborts (an oracle refusing a price aborts
  the whole calculation), the position keeps its balances, which are read without prices, and has
  no values, no `health` and no `below`; `unknown` holds the Move abort the node reported,
  without its URL or the version. A network failure fails
  the read instead, and the poll retries.
- **Reads.** Three views per position at one version: `get_user_account_data`,
  `ui_pool_data_provider_v3::get_user_reserves_data` (which reserves hold a balance), and
  `pool_data_provider::get_user_reserve_data` for each of those (current balances, interest
  included). The adapter must implement `view` and `getHeadVersion`.
- **Live changes.** One scan over Aave's `Supply`, `Withdraw`, `Borrow`, `Repay`,
  `LiquidationCall`, `DeficitCreated`, `ReserveUsedAsCollateralEnabled`/`Disabled`,
  `UserEModeSet` and `BalanceTransfer` (both sides, so a liquidator taking aTokens is read too)
  rereads each account the event names at its version, once per account and version and at most
  four at a time, outside `maxConcurrentRequests` and `maxRequestsPerSecond`. `account` (an address or a list) reads those
  accounts at the head first, or at the version before `fromVersion`, and ignores the rest;
  without it every account an event names is tracked from the start version on. A
  `FullnodeScanAdapter` scan walks every transaction, so from the head through an anonymous IP it
  can fall behind; Aave's own events are rare.
- **Price moves.** Health moves with prices and a price moves with no Aave event, so after `ready`
  every tracked position is also read at the head every `refreshInterval` ms: 60000 by default
  with `account`, and none by default without it, since that map holds every account any event
  named (0 turns it off). Each head read is one poll through the client's limiter whose views
  run at most four at a time outside `maxConcurrentRequests` and `maxRequestsPerSecond`, as the
  scan's do: about two views plus one per reserve held, per account, per interval. An account
  whose read fails goes to `onError` and is read again at the next head; when every read fails
  the poll retries with backoff, and once it gives up after `maxErrors` the whole monitor stops
  rather than show stale health as live. One account whose reads keep failing is reported each
  interval and keeps its last read; its `version` says when that was. A read replaces a position only when it is not older
  than the one held, so a scan that lags the head never overwrites a newer read.
- **Callbacks.** `onChange(position, event?)` fires after `ready` when a read changes any figure
  (the event is absent for a head read); `onCross(position, event?)` when `below` changes between
  two known values against `threshold` (a `Fraction`, 1 when omitted, compared exactly). An
  account's first read is not a crossing: after `ready`, `value` shows who is already below.
  An unknown read between two known ones keeps the last known side. Each callback runs on its
  own, so one that throws (reported to `onError`) does not hide another.
- **Options.** `account`, `threshold`, `refreshInterval`, `fromVersion`, `onChange`, `onCross`,
  `onError`, `signal`, `pollInterval`, `batchSize`, `maxErrors`, `maxBackoff`,
  `maxPollInterval` and `retries`; anything else throws, as does a protocol other than `"aave"` or `"echelon"`.
  Accounts are addresses, not names. Like `reduce()`, it is not a listener for `off()` or
  `listenerCount()`; `unsubscribe()`, `signal` or `destroy()` ends the scan and the refresh. A
  failed start read rejects `ready`. Mainnet only: the package address is mainnet's.

### Echelon

`positions("echelon", options)` takes the same options and keeps the same `LendingPosition`, one
per vault. Echelon publishes no source; what follows was read from its `lending` module's
bytecode, decompiled locally, and confirmed against its views on mainnet.

- **Health is Echelon's own comparison.** `can_liquidate(account)` is
  `account_liquidation_threshold < account_liability_value`, both `FixedPoint64` USD sums: the
  threshold sums `price * coins / mantissa * threshold_bps / 10000` over collateral markets
  (e-mode's threshold for e-mode markets), the liability `price * debt / mantissa` over
  liability markets. A liquidation accrues the vault's interest before that check, and the plain
  `account_liability_value` view does not, so it reads low by the interest since the vault last
  accrued. With a debt, each read calls `preview_health_factor_given_repay(account, market, 0)`,
  which accrues as the liquidation does and returns the same pair; `health` is their exact ratio,
  reduced, so it is below 1 exactly when a liquidation at that version would pass.
  `collateralValue` is `account_lend_value` and `debtValue` the accrued liability, both USD over
  2^64; `liquidationThreshold` is the threshold over the collateral value, the collateral-weighted
  ratio. An account with no vault, or no debt, has no `health` and `below` false.
- **Prices** are Echelon's own: `0x92e95ed7...::oracle` (its `tiered_oracle`) for coin markets and
  most fungible asset markets, an LP oracle or a linear discount oracle for some others. Several
  markets' prices were refused on mainnet in November 2026 (Thala Token, StakeStone Ether and
  Bitcoin, two Thala LP markets); a vault holding one has `unknown` with that abort, and its
  balances.
- **Assets.** One `PositionAsset` per market holding a supply or a debt: `asset` the coin type for
  a coin market, as Echelon names it, or the fungible asset's metadata address; `decimals` from the
  market's mantissa; `supplied` from `account_coins`, `borrowed` from `account_liability`
  (accrued); `collateral` true for every supply, since Echelon keeps supplies in the vault's
  collateral. Each market's asset is read once per monitor.
- **Reads.** About five views per position, plus one per market supplied and one per market
  borrowed, and three more the first time a market is seen. A head read of 100 tracked accounts
  is several hundred views per interval from one IP.
- **Live changes.** One scan over `lending::SupplyEvent`, `WithdrawEvent`, `BorrowEvent`,
  `RepayEvent` (the borrower; a repayer pays from its wallet) and `LiquidateEvent` (the borrower,
  and the liquidator, whose vault receives the seized shares). Entering or leaving an efficiency
  mode, and the manager's `clear_bad_debt`, change a vault with no event, so only a head read
  (with `account`) sees them.

## Package upgrades

`upgrades(address, callback, options?)` follows package publishes at one address as they land,
and `getUpgrades(address, filter?)` returns them for a past range. Each arrives as an event whose
`data` is a `PackageUpgrade { address, isUpgrade, events, entryFunctions, views }`, saying which
event structs, entry functions and view functions the publish added, changed or removed:

```ts
const sub = streamd.upgrades("0x1", ({ version, data }) => {
  for (const { name } of data.entryFunctions.added) console.log(version, "new entry function", name);
  for (const { before, after } of data.views.changed) console.log(version, before.name, before.returns, "->", after.returns);
});

const past = await streamd.getUpgrades("0x1", { fromVersion: 7454757100n, toVersion: 7454757960n });
```

- **Source.** One scan over `0x1::code::PublishPackage`, kept where its `code_address` is
  `address` (long form; an Aptos name is resolved once). Only `0x1::code` can emit that event,
  so no other module can forge a record. `isUpgrade` is the event's `is_upgrade`: false for a
  package's first publish.
- **The diff.** Every module at the address is read at the version before the publish and at
  its own, through the client's `aptos` (every page of the fullnode's `/accounts/{address}/modules` at
  `ledger_version`; the SDK's own module listings drop the version and read the head), and their
  ABIs compared: structs marked `#[event]` (`is_event`), functions with `is_entry`, and functions
  with `is_view`, each named `address::module::name` with the address in long form. A name only
  after the publish is `added`, only before it `removed`, and in both with a different
  signature `changed` as `{ before, after }`; each list is sorted by name. A struct that gains
  `#[event]` is an added event, and a public function that becomes a view an added view. The
  diff spans the whole address, so another package there would show too, though only one is
  published per transaction in practice; a second publish there in one transaction adds no
  record when both are on one page, as a `FullnodeScanAdapter` page always holds whole
  transactions.
- **Signatures.** An `EventStructSignature` is `{ name, abilities, typeParameters, fields,
  variants? }` (`variants` for an enum) and a `FunctionSignature` `{ name, visibility,
  typeParameters, params, returns }`, types as the fullnode writes them. Public functions that
  are neither entry nor view, plain structs and bytecode-only changes are not reported.
- **Reads.** Two module listings per publish (one at version 0), each of one or more pages,
  read two at a time outside `maxConcurrentRequests`, as `transfers()` reads stores.
  `requestTimeout` bounds the whole page, its scan and every listing in it together. A page given
  up on stops reading once the next page arrives, and otherwise finishes its reads; a listing
  whose cursor repeats fails. A failed read fails the poll, which retries as for `on()`, or rejects `getUpgrades`; a module
  without a readable ABI, or a listing that is not a list, is refused rather than reported as
  removed, and stops a subscription as invalid input does. The reads do not go
  through the adapter, so `ArchiveFallbackAdapter` does not answer them: a publish older than
  the node's retained state fails with its 410 unless the client's `aptos` is an archive node.
- **History.** No public source serves module events by type, so the scan reads every
  transaction from `fromVersion` (as `getEvents` does when it is omitted: a
  `FullnodeScanAdapter` from the head), which takes an API key past a few minutes of mainnet
  history.
- **Identity.** A record is the `PublishPackage` event with `data` replaced, keeping its
  `(version, eventIndex)`, `transactionHash`, `sender` and `entryFunction`; `where` and `limit`
  apply to records. Like `swaps()`, it is not a listener for `off()` or `listenerCount()`.

## Object provenance

`getProvenance(object, filter?)` returns the history of one Aptos object, a `0x4` token or any
`0x1::object` object, as one record per transaction that touched it, and
`provenance(object, callback, options?)` follows new ones. Each record's `data` is a
`Provenance { object, success, actions, changes, properties?, names? }`:

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { ArchiveFallbackAdapter } from "@streamd/core";
import { AptosClient, FullnodeScanAdapter, networks } from "@streamd/aptos";

const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET }));
const archive = new Aptos(new AptosConfig(networks.aptosMainnetArchive));
const streamd = new AptosClient({
  aptos,
  // Tokens minted long ago need the archive: the fullnode keeps only recent transactions.
  adapter: new ArchiveFallbackAdapter(new FullnodeScanAdapter(aptos), new FullnodeScanAdapter(archive)),
});

for (const { version, data } of await streamd.getProvenance(token, { withNames: true })) {
  for (const action of data.actions) {
    if (action.kind === "transfer") console.log(version, data.names?.[action.from], "->", data.names?.[action.to]);
    else console.log(version, action);
  }
}
```

- **Source.** The transactions come from the Aptos Indexer's `account_transactions` queried
  with the object's address, the only public source that lists every transaction touching an
  object, write-set-only changes included (the fullnode has no route by object). `indexer` takes
  any `{ getHeadVersion, listTransactionVersions }`; by default it is an anonymous
  `IndexerAccountAdapter` over the client's `aptos`. Each listed transaction is then read by
  version through the adapter's `getTransactionEventsByVersion`, so `ArchiveFallbackAdapter`
  answers the ones the fullnode pruned; without one, a pruned transaction rejects with its 410.
- **Actions.** In event order: `mint` (`0x4::collection::Mint`, `MintEvent`,
  `ConcurrentMintEvent`), `transfer` (`0x1::object::Transfer` and the legacy `TransferEvent`,
  with `from` and `to`), `mutation` (`0x4::token::Mutation`, `0x4::collection::Mutation` and
  their legacy handle events, with the field and its old and new values when the event has
  them) and `burn` (`0x4::collection::Burn`, `BurnEvent`, `ConcurrentBurnEvent`), each kept only
  when it names this object, then a bare `burn` when the transaction deleted the object (an
  `ObjectCore` delete, or the `0x1::object::ObjectGroup` delete the fullnode reports when the
  whole group goes) with no burn event.
- **Changes and properties.** `changes` holds every resource the transaction wrote or deleted at
  the object (`data` `null` on delete), so a record with no actions still says what it touched,
  such as a fungible store's balance. An object's resources are one resource group, written
  whole, so any write to the object lists all of them, unchanged ones included. `properties` is
  the object's `PropertyMap` as the transaction left it, each value decoded from its BCS bytes
  (integers as `bigint`); `0x4::property_map` emits no event, so a property changed where a
  record's `properties` differs from the previous record's.
- **Range.** `getProvenance` starts at version 0 unless `fromVersion`, `fromTime` or
  `fromBlock` says otherwise, and ends at the Indexer's processed version, which trails the
  fullnode by a few hundred versions; `provenance()` starts there and follows it. `limit`,
  `where`, `toVersion` and the cursor work as for `getEvents`. `fromVersion: "earliest"` is
  refused, since an archive behind the adapter serves every version. `orderBy: "desc"` lists
  newest-first from the Indexer and reads only the pages needed (and only up to `limit`, unless
  `where` skips matches).
- **Reads.** Each page is one Indexer listing of as many versions as the page size (the
  query's or the subscription's `batchSize`, at most 100), then one fullnode read per version,
  one at a time and outside `maxConcurrentRequests`. `requestTimeout` bounds each Indexer,
  fullnode or name read individually rather than the whole page; a page given up on stops reading
  once the next page starts. The anonymous Indexer and archive budgets are small, so an object
  with a long
  history wants an API key on both. A version the node does not have yet fails the page as a
  network error, which retries; a listing out of order or out of range fails it as invalid.
- **Names.** Under `withNames`, `names` holds the primary name (`null` for none) of the sender
  and every address in `actions`, through the client's name cache. Lookups the cache misses run
  inside the page's slot, so `maxRequestsPerSecond` does not space them.
- **Identity.** A record keeps the transaction's first event's `(version, eventIndex)`,
  `transactionHash`, `sender`, `timestamp` and `entryFunction`, with `type`
  `0x1::object::ObjectCore` whatever that event was. Like `activity()`, it is not a listener for
  `off()` or `listenerCount()`.
- **Not covered.** Legacy `0x3::token` tokens, which have no object address; transactions that
  touch only an object this object owns, unless the Indexer lists them.

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
- **Cost.** One head read plus one view per followed asset every `pollInterval`
  (default 1 second): even a single asset makes 2 requests per second and exceeds the
  anonymous Aptos fullnode rate limit over a few minutes, logging HTTP 429s and
  updating with gaps. Pass an API key or raise `pollInterval` (for example to `5000`)
  on the anonymous endpoint, and raise it further for longer asset lists.
  `onChange` sees each poll's combined balances atomically rather than every transaction
  in between. The inner asset views run inside the poll fetch (when fetching through a
  `streamd` client with `maxRequestsPerSecond`, they are not spaced separately).
- **Failures.** `ready` rejects if the first read fails. A later failed read retries
  with backoff, and `maxErrors` stops it; a view that aborts stops it at once. A balances handle is not a listener for
  `off()` or `listenerCount()`.
- **Values.** `withValue: true` values each priced asset (see [Values](#values)) at the head
  version its balance was read at, on the first read and whenever the balance changes:
  `wallet.values` maps each to its `AssetValue`, and each change carries `value`. A price move
  alone does not refresh a value or call `onChange`. It costs one more view per priced asset
  on those reads.

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

### Token holders at a version

`holdersAt(asset, version, options?)` lists every fungible store holding one asset at a past
version, with its owner and exact balance, and checks the result against the chain there:

```ts
const sol = "0x0d829a23c3a3760e2fed60ca24d83cee8943f7c61859608ae0726fdb2e4f2784";
const snapshot = await streamd.holdersAt(sol, 8120000000n);
// { asset, version, indexedVersion, holders: [{ store, owner, balance }, ...], total, supply,
//   verified: "supply", dispatchable: false, requests }
```

- **Source.** No view lists an asset's holders, and `Deposit` and `Withdraw` name only a store,
  so the fold runs backward over the Aptos Indexer: every row of
  `current_fungible_asset_balances` for the asset, then each `fungible_asset_activities` row
  after `version` undone (a deposit subtracted, a withdraw added back), up to the newest
  version the fungible asset processor has finished. A store's row is undone only through its
  own `last_transaction_version`, so a store that changes while the walk runs stays exact. A
  store seen only in activities is looked up: one deleted before the walk folds from zero, and
  one created during it is left out.
- **Checks.** `verify` is `"supply"` by default: the total must equal
  `0x1::fungible_asset::supply` at `version`, and an asset with no tracked supply rejects.
  `"stores"` also reads every holder's `0x1::fungible_asset::balance` at `version`, one view
  each, and rejects up front when `maxRequests` cannot cover them. A store the fold puts at 0
  is covered only by the supply check, so not at all for an asset with no tracked supply.
  `"none"` checks nothing and trusts the Indexer. A failed check, a balance that folds below zero, and an
  activity type that moves an amount the fold does not know reject with a `StreamdError`
  rather than resolving a wrong list.
- **Refused.** APT, since gas fees and storage refunds move it in ways the fold was never
  checked against; an asset paired with a coin (`0x1::coin::paired_coin`), since balances still
  in a `CoinStore` are not in fungible stores; coin types and legacy tokens. A dispatchable
  asset (`is_asset_type_dispatchable`) needs `verify: "stores"`, since its hooks are not proven
  to move balance only through the events the Indexer records, and the snapshot carries
  `dispatchable: true`.
- **Owner.** `owner` is the Indexer's: the current owner of a store that still exists, or the
  owner on its first activity row after `version` for one since deleted. It is not read at
  `version`, so a secondary store whose object changed hands without moving balance reports
  its later owner.
- **Cost.** About one Indexer request per 100 stores and one per 100 activity rows after
  `version`, plus two views at the processed version, the supply view and, under `"stores"`,
  one view per holder. Every request goes through the client's limiter (`maxRequestsPerSecond`,
  `requestTimeout`), retries under `retries` with any `Retry-After`, and counts toward
  `maxRequests` (default 1000), past which the call rejects. The anonymous Indexer and
  fullnode share a budget of roughly 40 requests per 5 minutes per IP, so a busy asset over a
  wide window needs an `indexer` built with an API key,
  `new IndexerAccountAdapter(aptos, { apiKey })`, and a fullnode key.
- **Window.** The views at `version` need it inside the fullnode's window, or an
  `ArchiveFallbackAdapter`; `verify: "none"` reads no view at `version`. A version the
  Indexer has not processed yet is refused.
- **Measured.** For SOL (above) 200,000 versions back, the snapshot made 43 requests over
  about 3 minutes anonymously: 55 current stores, 517 activity rows, 31 holders, the total
  equal to the supply at `version`, and every holder equal to its balance view there.
- **Not checked on mainnet:** dispatchable assets, stores deleted after `version`, frozen
  transfers, and mints or burns inside the window. The supply check covers the total in each
  case, and `"stores"` every holder.
- **Large amounts.** The Indexer sends amounts as JSON numbers. They are read exactly where
  `JSON.parse` gives a reviver the source text (Bun, Node 21 and later, current browsers);
  elsewhere an amount above 2^53 rejects with a `StreamdError` rather than being rounded.

## Oracle prices

`prices(feedId, callback, options?)` follows Pyth price updates for one feed or a list, and
`priceAt(feedId, version)` reads the price a feed held at a ledger version. Both give a `Price`:

Pyth's Aptos deployment is deprecated and has published no update since 2026-09-21, so these
read history only: give `prices()` a past `fromVersion` or `fromTime` to replay updates, and
use `priceAt` for a version before then. A follow from the head delivers nothing new.

```ts
const APT_USD = "0x03ae4db29ed4ae33d323568895aa00337e658e348b37509f5372ae51f0af00d5";
const BTC_USD = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43";

const then = await streamd.priceAt(APT_USD, 7302080573n);
// { oracle: "pyth", feedId: APT_USD, price: 77353341n, confidence: 152447n, exponent: -8,
//   publishTime: 1790009897000000n, observedAt: 1790009907927720n, staleness: 10927720n }

const sub = streamd.prices([APT_USD, BTC_USD], ({ data }) => {
  if (data.staleness > 60_000_000n) return; // older than a minute when it landed
  console.log(data.feedId, data.price, data.exponent);
}, { fromVersion: 7302000000n });
```

- **Exact numbers.** The price is `price × 10^exponent`, both integers, so APT/USD above is
  0.77353341. `confidence` is at the same exponent. Nothing is a float.
- **Times are microseconds** since the Unix epoch, as `StreamdEvent.timestamp` is. Pyth
  publishes seconds, scaled here. `observedAt` is the chain time the price was seen at, and
  `staleness` is `observedAt - publishTime`, negative when Pyth's clock ran ahead of the chain's.
- **Feed ids** are Pyth's 32-byte price ids, with or without `0x`, in any case; the record
  carries them as `0x` and lowercase hex. Anything else is refused at the call.
- **`prices()`** reads Pyth's one legacy event handle, which carries every feed's updates, and
  keeps the feeds named. `observedAt` is the update's block time. With no `cursor`, `fromVersion`
  or `fromTime` it starts one version below the head; `cursor` is the handle's sequence number. Like `swaps()`,
  it is not a listener for `off()` or `listenerCount()`.
- **`priceAt()`** reads Pyth's latest-price table at `version`, so it is the price a contract
  calling Pyth would have read then, and `observedAt` is the time of the block holding
  `version`. A feed with no entry then resolves `undefined`. It costs one table read and one
  block read through the client's limiter and `requestTimeout`, with no retry, plus one read
  of the table's handle per client.
- **Errors.** A malformed feed id or version rejects before anything is sent. Pyth has no view
  functions, so `priceAt` reads through the client's `aptos`, not the adapter, and an
  `ArchiveFallbackAdapter` does not answer it: a version the fullnode has pruned rejects with its
  `StreamdNetworkError`, `statusCode` 410, unless `aptos` points at
  `https://archive.mainnet.aptoslabs.com/v1`. A version above the head rejects with 404.
- **Pyth on Aptos is no longer updated.** Its last update on mainnet was at version 7302080573
  (2026-09-21), and Pyth no longer lists Aptos among the chains it supports. History reads
  as above, `priceAt` on any later version returns that last price with its staleness growing,
  and a live `prices()` delivers nothing until updates resume.
- **Switchboard is not read.** Neither of its Aptos packages has updated a price since
  2026-08-29 (on-demand) or before the fullnode's retained window (v2).

## Values

`withValue: true` on `transfers`, `getTransfers`, `iterateTransfers`, `balances` and `explain`
prices amounts in native USDC at the version they were read at:

```ts
const transfers = await streamd.getTransfers({ fromVersion: -1000n, asset: "0xa", withValue: true });
// A 10 APT transfer at version 8123509211:
transfers[0]?.data.value;
// { quote: "0xbae2...6f3b", decimals: 6, amount: 4334712n,
//   price: { numerator: ..., denominator: ... }, source: "pool", reference: "0x9256...00d8" }
formatAmount(transfers[0]!.data.value!.amount, 6); // "4.334712" USDC
```

- **What is priced.** A curated table; any other asset, and every token, has no `value`:

  | Asset | Pool read (Hyperion, `0x8b4a2c4b...e05c`) | Pyth feed before the pool |
  | --- | --- | --- |
  | APT (`0xa` and `0x1::aptos_coin::AptosCoin`) | APT/USDC, 0.05%, `0x925660b8618394809f89f8002e2926600c775221f43bf1919782b297a79400d8` | APT/USD |
  | USDt (`0x357b0b74...dc2b`) | USDt/USDC, 0.01%, `0xd3894aca06d5f42b27c89e6f448114b3ed6a1ba07f992a58b2126c71dd83c127` | USDT/USD |
  | USDC (`0xbae20765...6f3b`) | none: at par, `source: "par"` | |

  Each pool was the deepest for its pair when chosen. No BTC is priced: every BTC pool
  against USDC held under 2,000 USDC.
- **A pool's price.** `pool_v3::current_tick_and_price(pool)` at the version returns the
  pool's `sqrt_price`, a Q64.64 square root of USDC base units per base unit of the asset, so
  the price is exactly `sqrt_price² / 2^128`, reduced to lowest terms. A pool at
  `sqrt_price` 0 gives no value.
- **Pyth, before the pools.** The pools were created at versions 2321906214 (USDt) and
  2321920813 (APT), in February 2025. Below its pool's creation version an asset is priced
  by Pyth (`priceAt`), taken as USDC at par, `price × 10^(exponent + 6 - decimals)` per base
  unit, `source: "oracle"`, only when positive and at most 60 seconds from the block's time
  either way; otherwise there is no value. From the creation version on, only the pool is
  read, and any failed read fails, so a pool problem never turns into an oracle price. Pyth
  reads go through the client's `aptos`, so a pruned version needs `aptos` on an archive node.
  In practice few records reach it: on mainnet, transfers before the pools mostly cannot be
  built at all, since `0x1::object::owner` was not yet a view (checked at versions 2000000000
  and 2321009010) and coin moves were legacy handle events, which `transfers()` does not read.
- **Exact.** `price` is the exact fraction of quote base units per base unit of the asset;
  `amount` is the asset amount times `price`, rounded toward zero, so a negative balance
  change has a negative value. Nothing passes through a float.
- **Spot prices can be moved.** A value is the pool's price after the whole transaction at
  that version. A swap through the pool in the same transaction (a sandwich, or the
  transfer's own swap) moves it, and there is no time-weighting and no liquidity check.
  Treat values as indicative, not as a settlement price.
- **Cost.** One pool view per price source and version, shared by the records at that
  version (the APT coin and its fungible asset share one), each read inside the transfer
  reads' limit of 4 and its `requestTimeout`, outside `maxConcurrentRequests`. Before the
  pools a value costs a Pyth table read and a block read instead. A failed read fails the
  page or query, as the transfer reads do. Pool views go through the adapter, so
  `ArchiveFallbackAdapter` answers pruned versions.
- **Mainnet only.** The table names mainnet pools and feeds; on another network the pool
  views fail and so does the call.
- **Other chains.** `SuiClient.balances` refuses `withValue`.

## Data sources and their limits

By default streamd reads from a fullnode with `FullnodeScanAdapter`:

- **Handle events** use the dedicated fullnode endpoint. Exact and cheap. The endpoint pages
  by sequence number and cannot seek by version, so a walk given `fromVersion` and no cursor
  first reads the handle's resource at the version before it and starts at its `counter`, the
  first event at or after `fromVersion`. A walk with no `fromVersion`, `fromTime`, `cursor`, or
  `offset` reads from sequence `0` (so pass one on a pruning fullnode), as does a creation-number
  handle (which has no resource to read). When `fromVersion` is the node's oldest version (as
  with `"earliest"` or an old `fromTime`), whose predecessor is pruned, the seek falls back from
  that `410` to read the handle's counter at `oldest_ledger_version` itself (starting after any
  events in that version, or at sequence `0` if absent or also pruned). A `fromVersion` below the
  oldest version rejects with the node's 410. An
  `ArchiveFallbackAdapter` answers that first page from the archive, but not the pages after
  it, which the fullnode answers empty: route `handle` to the archive with `RoutingAdapter` to
  walk pruned handle history, and pass `cursor` (a sequence number) rather than `fromVersion`
  to start a busy handle's walk at a known point.
- **Account events** walk that account's own transaction history. Cheap even
  far back, but only sequence-numbered transactions the account sent (omitting
  orderless replay-protection nonce transactions); `IndexerAccountAdapter`
  (below) sees orderless transactions and the ones it received.
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
`eventsOf()` needs, returning `{ version, success, vmStatus, events }` (with optional
`gasUnitPrice` and `feePayer`, which `explain()` reads) or `undefined` while
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
then makes two more fetches and one call to each optional method it checks that
is present (two to `getBlock`), so at most `maxPages + 10` calls, and reports a
result per rule (`page-shape`, `order`, `cursor-advance`, `through-version`,
`cursor-precedence`, `foreign-cursor`, `head-version`, ...) instead of throwing.
It only reads, but against a real source those reads are network calls:

```ts
import { checkAdapter } from "@streamd/core";

const report = await checkAdapter(
  myIndexAdapter,
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  {
    fromVersion: 1_000_000n,
    view: { request: { function: "0x1::timestamp::now_microseconds" } },
  },
);
if (!report.ok) console.table(report.results.filter((r) => r.status === "fail"));
```

`fromVersion` is where the walk starts (omit it to start where the adapter
does, usually the head). A rule whose method or input is missing is `skip`, with
the reason in `message`. The optional methods are judged against what the walk saw:

- `block-heights` - `getBlockHeights` resolves `{ oldest, newest }` with `oldest <= newest`,
  as `fromTime` reads it.
- `block` - `getBlock` at `blockHeight`, or at `newest` when that is not given, returns that
  height with `firstVersion <= lastVersion`. `oldest` is never read, since a node may report
  an oldest block it has already pruned.
- `block-sequence` - the block before ends at the version just below where that block starts,
  and block time does not fall: what `fromBlock`/`toBlock` and the `fromTime` search assume.
- `transaction-events` - `getTransactionEvents` on the first walked event carrying a
  `transactionHash` finds that transaction at the event's version, well formed, in
  `eventIndex` order, and holding the event (a `handle` event matches on `sequenceNumber`,
  since `FullnodeScanAdapter` indexes handle events by it).
- `transaction-events-by-version` - the same for `getTransactionEventsByVersion` on that
  event's version (or the first walked event's), and both lookups list the same events.
  Both lookup rules skip `resource` and `object` descriptors, whose changes are not
  transaction events.
- `view` - with a `view: { request, version? }` option, `view` resolves an array at
  `version`, else the first walked event's version, else the head. No view function exists
  on every chain, so without the option the rule skips. The values themselves are not judged,
  but the function must be a view at that version: pass `version` when the walk reaches back
  before it was published, since a node then refuses the call and the rule fails.

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

`module` descriptors can also be routed by the event types they name, ahead of the kind
routes:

```ts
const router = new RoutingAdapter({
  types: { "0x1::fungible_asset::Deposit": depositIndex },
  modules: { "0xabc::pool": poolIndex },
  default: fullnode,
});
```

- **Keys.** `types` keys are `address::module::Struct`, with or without type arguments;
  `modules` keys are `address::module`. Addresses compare in long form and whitespace is
  ignored, so `0x1` and its padded form are one key, inside type arguments too. A malformed
  key, two keys that are the same after that, or a value that is not an adapter throws a
  `StreamdError` at construction.
- **Precedence.** For each type: the exact `types` key, then for a generic type its struct
  without type arguments (`0x1::coin::CoinStore` serves `0x1::coin::CoinStore<0xf::usdc::USDC>`
  unless that exact key is also listed), then the `modules` key, then the `module` route, then
  `default`. A type that is not a Move type string skips the first three, so the adapter it
  lands on refuses it as before.
- **Lists.** A descriptor with a list of types goes to the one adapter they all resolve to
  (two routes holding the same adapter count as one). Types resolving to different adapters
  reject with a `StreamdError` naming each type and its route, before any fetch. To read across
  routes, pass a list of descriptors instead: `getEvents([depositDescriptor, swapDescriptor])`
  merges walks from each source.
- **Kinds.** Only `module` descriptors consult `types` and `modules`. `handle`, `account`
  (`eventTypes` included), `resource` and `object` route by kind as before.
- **Optional methods.** `getHeadVersion`, `getBlock`, `view`, `readObjects`, the transaction
  lookups and the rest resolve their adapter from the descriptor the same way, and use
  `default` when called without one.
- **Cursors.** Routing depends only on the descriptor and the routes fixed at construction,
  so a descriptor always reaches the same adapter and its cursor never goes to another.

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
- **Transfers:** `sui.transfers(callback, { asset?, account?, withAssetInfo? })`,
  `sui.getTransfers(filter)` and `sui.iterateTransfers(filter)` deliver `Transfer { from, to, asset, amount }` records, as on Aptos.
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
  only. `withNames` attaches each sender's primary name as `senderName`, as on Aptos (see
  Sender names); on a network without SuiNS each lookup rejects, as `primaryName` does.

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
  it once it passes the pruned edge. Because Aptos fullnodes answer pruned `handle` sequence
  numbers with empty `200 []` rather than `410`, only the initial pruned resource or page
  reaches the archive; route `handle` directly to the archive via `RoutingAdapter` for
  multi-page historical handle walks.
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
  begins there. A bounded `toVersion` above the processed version pages through what is
  indexed with `hasMore: true`, then rejects with `StreamdNetworkError` once the cursor
  passes it until the Indexer catches up.
- **Pruning.** The Indexer lists history the fullnode has pruned, so `fromVersion` is
  raised to the fullnode's oldest version (`getEarliestVersion`). A cursor older than that
  fails with status 410 rather than skipping history.
- **Rate limits.** Anonymous Indexer access is rate limited per IP; a limit hit rejects as a
  `StreamdNetworkError` with status 429. Pass `apiKey` (sent as a bearer token) for more.
- **Storing.** `StoreAdapter` and `PostgresStoreAdapter` index `account` descriptors by
  stored `sender` only and refuse `received: true`. Received transactions stored from
  `IndexerAccountAdapter` through `storeEvents` are kept in the store, so read them back
  with `module`, `handle`, `eventsOf`, or `eventsAt` rather than an `account` descriptor.
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

`toPrometheus` renders a snapshot as Prometheus text exposition format, so a process can serve
it on a `/metrics` route with no client library. Serve it as
`text/plain; version=0.0.4; charset=utf-8`:

```ts
import { toPrometheus } from "@streamd/core";

Bun.serve({
  port: 9464,
  fetch: () =>
    new Response(toPrometheus(metrics.snapshot()), {
      headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
    }),
});
```

```text
# HELP streamd_adapter_requests_total Adapter calls started, in-flight ones included.
# TYPE streamd_adapter_requests_total counter
streamd_adapter_requests_total{method="fetchEvents"} 12
...
# TYPE streamd_adapter_latency_seconds gauge
streamd_adapter_latency_seconds{method="fetchEvents",quantile="0.5"} 0.12
```

| Metric | Type | Labels | From |
| --- | --- | --- | --- |
| `streamd_adapter_requests_total` | counter | `method` | `requests` |
| `streamd_adapter_errors_total` | counter | `method` | `errors` |
| `streamd_adapter_rate_limited_total` | counter | `method` | `rateLimited` |
| `streamd_adapter_latency_seconds` | gauge | `method`, `quantile` (`0.5`, `0.95`, `0.99`, `1`) | `p50`, `p95`, `p99`, `max`, in seconds |

Latency is a gauge rather than a summary: the percentiles cover only the most recent 1024
settled calls per method, not every call since start, and there is no sum or settled count to
write a summary's `_sum` and `_count` from. `quantile="1"` is the maximum. A method with no
settled call has no latency series, a family with no series is left out, and an empty snapshot
renders `""`. After `reset()` the counters start again from zero, which Prometheus reads as a
counter reset.

To render several sources as one exposition, such as each fullnode of a pool and the archive,
pass a list of `{ snapshot, labels }`. Each family's `# HELP` and `# TYPE` are written once,
series follow the list's order, and `labels` in the options go on every series, a source's own
label of the same name winning:

```ts
const nodes = urls.map((url) => adapterMetrics(new FullnodeScanAdapter(aptosFor(url))));

toPrometheus(
  nodes.map((node, i) => ({ snapshot: node.snapshot(), labels: { node: String(i) } })),
  { prefix: "myapp_aptos", labels: { network: "mainnet" } },
);
// myapp_aptos_requests_total{network="mainnet",node="0",method="fetchEvents"} 40
```

`prefix` (default `streamd_adapter`) must be a valid metric name. Label names must be valid
Prometheus label names, not starting with `__` and not `method` or `quantile`, which the
function sets itself; label values must be strings, and backslashes, double quotes and newlines
in them are escaped. Two sources with the same labels would write the same series twice, so
they are refused. Each refusal is a `StreamdError`, thrown before anything is rendered.

To see adapter calls in your traces, wrap the adapter with `withTracing` and an OpenTelemetry
tracer. It opens one span per call of every method the adapter has, named `streamd.<method>`
(`streamd.fetchEvents`, `streamd.view`), and ends it once the call settles. streamd does not
depend on `@opentelemetry/api`: `withTracing` takes a structural `TracerLike`, the
`startSpan` and the four span methods it calls, which a real tracer satisfies:

```ts
import { trace } from "@opentelemetry/api";
import { StreamdClient, withTracing } from "@streamd/core";
import { FullnodeScanAdapter } from "@streamd/aptos";

const streamd = new StreamdClient({
  adapter: withTracing(new FullnodeScanAdapter(aptos), trace.getTracer("my-app")),
});
```

| Attribute | Set from |
| --- | --- |
| `streamd.descriptor.kind` | the call's descriptor, when it has one |
| `streamd.event_type` | `eventType` of a `module` descriptor or `eventTypes` of an `account` one, a string or a string array |
| `streamd.account` | `accountAddress` |
| `streamd.handle.struct`, `streamd.handle.field`, `streamd.handle.creation_number` | a `handle` descriptor's fields |
| `streamd.resource_type`, `streamd.object_id` | a `resource` or `object` descriptor |
| `streamd.limit`, `streamd.cursor_present` | `fetchEvents` options; whether a cursor was passed, never its value |
| `streamd.block_height` | `getBlock` |
| `streamd.transaction_hash` | `getTransactionEvents` |
| `streamd.version` | `getTransactionEventsByVersion`, `view`'s ledger version, `readObjects`'s `at.version` |
| `streamd.view.function` | `view`'s function name, never its arguments |
| `streamd.event_count`, `streamd.has_more`, `streamd.final` | a page from `fetchEvents` |
| `streamd.found`, `streamd.event_count`, `streamd.success` | the transaction lookups; `simulateTransactionEvents` sets the last two |
| `error.type`, `streamd.rate_limited`, `http.response.status_code` | a failure: the error's `name` (`_OTHER` for a non-Error), status 429 or a `retryAfter` hint, and a `StreamdNetworkError`'s status |

Versions and heights are decimal strings, since attributes cannot be `bigint`. Payloads, event
data, view arguments and simulated transactions are never written. A call that succeeds leaves
the span's status unset, as OpenTelemetry asks of instrumentation; one that fails records the
exception, sets status `ERROR` (code 2) with its message, and rejects with the same error.
The message is the error's own, and a fullnode failure's includes the request URL, so a node
URL carrying a key in its path or query reaches the trace backend.
Adapter calls take no abort signal, so any rejection is a failure, and a client
`requestTimeout`, which gives up on the call outside the adapter, leaves the span open until the
call itself settles.

Spans start with `startSpan`, so they are children of whatever span is active where the call
is made. They are not made active themselves, which would need `context.with` from
`@opentelemetry/api`: an HTTP auto-instrumentation span for the fetch underneath appears beside
streamd's span rather than inside it.

A tracer fault never fails a call: when `startSpan` throws or returns nothing the call runs
untraced, a span method that throws is skipped, and a rejected promise returned by any of them
is caught rather than left unhandled. A tracer without a `startSpan` method is
refused with a `StreamdError` when wrapping. `withTracing`, `adapterMetrics` and `withHooks`
stack in any order. Outside `CachingAdapter` a cache hit is still a span, with no call beneath
it; inside it a hit makes none.

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

In the browser, `@streamd/store-indexeddb` keeps them in IndexedDB, so a reload reads settled
history without the source:

```ts
import { IndexedDbPageStore } from "@streamd/store-indexeddb";

const store = new IndexedDbPageStore({ namespace: "aptos-mainnet", maxEntries: 5_000 });
const adapter = new CachingAdapter(new FullnodeScanAdapter(aptos), { store });
```

It writes one database, `streamd-pages`, shared by every tab and every app of the origin, and
a write lands only when its transaction commits. `maxEntries` counts the whole database, so
give every store on the origin the same limit, and when the source is configurable put it in
the namespace (as `streamd query --cache` puts a hash of the node URLs in its own), so one
node's pages never answer for another's. To forget everything stored, for example on sign-out,
call `indexedDB.deleteDatabase("streamd-pages")`. A write the origin's storage quota refuses rejects with a
`StreamdError` naming the quota, which reaches `onStoreError` while the page is still served,
so set `maxEntries` to keep the store inside the quota. Where there is no IndexedDB (server
rendering, a worker without it) every read is a miss. Over `TabSharedAdapter`, wrap it
outside (`new CachingAdapter(new TabSharedAdapter(...), { store })`) so each tab answers
settled pages from the shared database without asking the leader; every tab sharing the
`TabSharedAdapter` name must then use the same source and namespace.

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
  The cursor is also saved when the subscription first catches up (which pins a relative or
  head start), every 5 seconds while scanning empty pages, and when unsubscribing cleanly or
  stopping `streamd store` (even if no events matched yet); until then a restart re-resolves
  a relative start, so pass an absolute `fromVersion` to leave no window at all.
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
- **What `StoreAdapter` matches.** `module` by event type; `account` by the stored `sender`
  only (refusing `received: true` with a `StreamdError`, so received transactions stored from
  `IndexerAccountAdapter` only match `module`, `handle`, or transaction lookups), so events
  stored without a `sender` (from `handle` descriptors) never match `account`; `handle` by
  `creationNumber` against the stored `guid` (`listEventHandles` returns it), and a struct
  and field handle is refused with a `StreamdError`; `resource` by the stored address and the
  resource type, exactly as the fullnode writes it (see Resource changes below). It sees only
  what was stored, from whichever subscription stored it. Following the store with `on()` is
  gap-free only while rows arrive in version order, since its cursor is a position in the
  table; it sets no `throughVersion` for the same reason.
- **Head and earliest** are the highest and lowest stored versions across the whole store,
  `0n` for a store that was never pruned and holds nothing, so relative `fromVersion` and
  `toVersion: "head"` count from what is stored. After a prune neither reports below the
  edge. With no `cursor` or `fromVersion` a fetch starts at the earliest stored event.
- **Retention.** `store.prune({ beforeVersion })` deletes every event and stored transfer
  below that version in one transaction and leaves saved cursors alone. A `StoreAdapter` read whose `fromVersion`,
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
- **Stats.** `store.stats()` says what the file holds, all of it read in one savepoint, so a
  writer on another connection never lands between the counts:

  ```ts
  const { events, resourceChanges, transfers, lowestVersion, highestVersion, prunedBefore, byType } =
    store.stats({ byType: true });
  // byType: [{ type: "0x1::fungible_asset::Deposit", count: 81234n }, ...]
  ```

  Every count is a `bigint`. `events` counts the rows of `streamd_events` that are events, and
  `resourceChanges` the rows stored as `resource` changes (with an address), so the two never
  overlap; changes an earlier release stored without an address count as events, as the
  transaction lookups treat them. `transfers` counts `streamd_transfers`, `0n` before it exists.
  `lowestVersion` and `highestVersion` are the lowest and highest version of any stored event or
  change, absent when there are none, and are what is stored rather than what is readable: a row
  written below the edge after a prune shows there, while `StoreAdapter` refuses to read it.
  `prunedBefore` is the edge, `0n` until the first prune. `byType` appears only with
  `{ byType: true }`: events per type (not resource changes, so the counts add up to `events`),
  most first, ties in byte order of type. Each count scans its table or an index, so it takes
  time in proportion to the rows; read it on demand rather than polling a large file. It never
  migrates or creates a table, so it works on a read-only connection to an older database.
- **Stored transfers.** `storeTransfers(client, store, options)` does for a chain client's
  `transfers()` what `storeEvents` does for events, with the same resume, catch-up and failure
  rules. `client` is anything with that method, an `AptosClient` or a `SuiClient`, and `options`
  are its own (`asset`, `account`, `withAssetInfo` and the listener options) plus `name`:

  ```ts
  import { SqliteEventStore, storeTransfers } from "@streamd/store-sqlite";

  // Every APT movement from here on, resuming after a restart.
  storeTransfers(aptosClient, store, { name: "apt", asset: "0xa", fromVersion: -10_000n });
  ```

  Records land in `streamd_transfers`, keyed on their `(version, eventIndex)` (the deposit's,
  or the lone withdraw's) so a redelivered record replaces its row. Columns: `version` (the
  zero-padded text the event table uses), `event_index`, `asset`, `amount` (exact decimal text),
  `from_address`, `to_address`, `symbol` and `decimals` (null when the record has none) and
  `transfer`, the whole record as `serializeEvent` writes it, with indexes on `asset`,
  `from_address` and `to_address`. Read them back with SQL; there is no adapter over them. Each
  record is one transaction, with the cursor saved under `transfers:<name>`, so it never shares
  a cursor with an event subscription of the same `name` (avoid event names starting
  `transfers:`); `cursors()` lists it with the prefix. A cursor is a position in the transfer
  scan as its filter reads it, so give each `asset` or `account` its own name. A malformed
  record (no asset, an amount that is not a u256 `bigint`) is refused before anything is
  written, which ends the subscription as a failed write does. `store.writeTransfers(records,
  { name, cursor })` is there for wiring your own, and `prune` deletes transfers below its edge
  with the events.
- **Stored points.** `storePoints(client, store, descriptor, fold, options)` runs
  `client.reduce()` over a `points()` fold and keeps the leaderboard in `streamd_points`, one
  row per `name` and address: `points` (exact decimal text), `version` (zero-padded) and
  `score`, the whole score as `scoreToJson` writes it. `options` are `reduce()`'s, but `cursor`,
  plus `name`. The first run folds the history from the given start, then writes every row and
  the cursor (under `points:<name>`) in one transaction; each live change then writes the rows
  it changed and the cursor in one transaction. A restart with a saved cursor reads the rows back
  as the initial value and follows on from the cursor, without folding the history again (the
  history options are ignored). It returns the `Reduction`. A failed write stops it and reaches
  `onError`, and a failed first write also rejects `ready`.

  ```ts
  import { points } from "@streamd/core";
  import { SqliteEventStore, storePoints } from "@streamd/store-sqlite";

  const quest = points({ rules: [{ id: "swap", points: 10n }] });
  const board = storePoints(streamd, store, Deposit, quest, { name: "quest", fromVersion: -100_000n });
  await board.ready;
  quest.standings(store.readPoints("quest"), { limit: 10 });
  ```

  `store.writePoints(name, scores, { name, cursor })` and `store.readPoints(name)` are there for
  wiring your own. The table is created by the first points write; `prune` leaves it, as it
  leaves cursors.
- **Resource changes.** A change carries its resource type but not the address holding it, so
  `storeEvents` over a `resource` descriptor stores the descriptor's address with each change,
  in the `account_address` column (long form, `0x` and 64 lowercase hex digits; null on every
  other row), and `StoreAdapter` serves that descriptor back with the same cursors, paging and
  pruned edge as every kind. `store.write(events, checkpoint, { accountAddress })` does the same
  for your own wiring; a rewrite without `accountAddress` keeps the address a row has. A change
  stored with an address is served only as `resource`, never by `module`, `account` or `handle`,
  so a store following a wallet's resources and its events keeps the two apart, as `stats()` counts
  them. Changes an earlier release stored have no address and cannot be told from events: they are
  never served as `resource`, and still match `module` by type and `account` by `sender`. Store
  that range again to serve it as changes.
- **Transaction lookups.** Every `write` also keeps the event's `transactionHash` in the
  `transaction_hash` column (an Aptos `0x` hash lowercased, anything else, such as a Sui base58
  digest, exactly as given, since base58 is case-sensitive), and `StoreAdapter` implements
  `getTransactionEvents` and `getTransactionEventsByVersion` over it, so `eventsOf`,
  `expectEvents` and `eventsAt` answer from the file with no network:

  ```ts
  const offline = new AptosClient({ aptos, adapter: new StoreAdapter(db) });
  const events = await offline.eventsOf(hash, undefined, { timeout: 1_000 });
  await offline.expectEvents(hash).toEmit(Swap);
  ```

  Both lookups return the same thing for one transaction: every event stored at its version, in
  `eventIndex` order. By hash, the hash finds the version first, so events stored without a hash
  (`handle` events, rows an earlier release wrote) come back too, as they do by version. Both leave
  out `resource` changes, which are write-set changes rather than events (`object` changes and
  changes an earlier release stored without an address cannot be told apart and come back with
  them). It sees **only what was stored**: a store following one descriptor holds that
  descriptor's events of a transaction, not every event it emitted, so `toEmit` on a type the
  store never followed fails and `notToEmit` passes whatever the chain did. With several
  subscriptions writing one store, a lookup answers as soon as the first has written its rows,
  so another's events of the same transaction may not be there yet. A store keeps **no
  transaction status**, so a found transaction is `success: true` with `vmStatus` `""`; the chain
  keeps an aborted transaction's fee statement, so a store following
  `0x1::transaction_fee::FeeStatement` holds events of failed transactions, and `eventsOf`
  resolves with them instead of rejecting with `StreamdTransactionError`. A hash the store does
  not hold is not found, so `eventsOf` keeps asking until its `timeout` (20 seconds by default)
  and rejects with `StreamdTimeoutError`; that wait is what lets it pick up a transaction a
  running `storeEvents` has yet to write, and for a store nobody is writing, pass a short
  `timeout`. A version below the pruned edge fails with the 410 `StreamdNetworkError`; a pruned
  hash is simply not found.
- **Older databases.** The first `write` adds `account_address` and `transaction_hash`, each with
  its index (`streamd_events_resource`, `streamd_events_transaction`), to a database an earlier
  release made, each in one savepoint, keeping every row; when another connection added one first
  it carries on. Building the indexes reads the whole table once, inside that first write. Nothing else migrates, so a read-only connection still opens the store and the
  adapter, a `resource` read of a database no write has migrated is empty, and a hash lookup
  there finds nothing. Rows stored before the column existed are not backfilled, since that
  rewrites the whole table on the first write after an upgrade; their hashes are still in the
  stored JSON, and this statement fills them when it suits you (or store the range again):

  ```sql
  UPDATE streamd_events
  SET transaction_hash = CASE
    WHEN json_extract(event, '$.transactionHash') GLOB '0x[0-9a-fA-F]*' THEN lower(json_extract(event, '$.transactionHash'))
    ELSE json_extract(event, '$.transactionHash') END
  WHERE transaction_hash IS NULL AND account_address IS NULL AND event LIKE '%"transactionHash"%';
  ```
- It passes `checkAdapter`. The schema (`streamd_events`, `streamd_cursors`) is created on
  first use, and `streamd_transfers` on the first transfer write or prune.

### Typed event tables

`storeTables(client, store, tables, { name })` follows every event type in `tables`, a generic
one in every instantiation, and upserts each event into its own table, one column per field,
with the subscription's cursor in the same transaction. `streamd codegen --sql sqlite` writes
`tables` from the module ABIs ([Generate event types](#generate-event-types) has the mapping):

```ts
import { SqliteEventStore, storeTables } from "@streamd/store-sqlite";
import { streamdTables } from "./events.js"; // streamd codegen --sql sqlite

const sub = storeTables(streamd, new SqliteEventStore(db), streamdTables, { name: "fa", fromVersion: -10_000n });
```

It creates the tables before subscribing and keeps `storeEvents`'s rules: resume from the cursor
saved under `tables:` and `name` (listed by `cursors()`, apart from any `storeEvents` cursor of
the same name), save it again on first catching up, and end before the cursor moves when a write
fails. An event whose value its column refuses (a `u16` out of range, a missing required field)
is such a failure, named by table, column and event. `store.writeTables(tables, events,
checkpoint)` writes one batch in a savepoint. `prune` and `stats` leave these tables alone. An
upgraded module keeps its event structs' fields, so a table stays valid; an enum's new variant
lands in `data`.

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
- **[Typed event tables](#typed-event-tables)** work the same, from `streamd codegen --sql
  postgres`: `await storeTables(client, store, streamdTables, { name })`, each batch, every
  table it touches and the cursor in one statement.
- **Queryable.** `streamd_events` holds `version` (`numeric`, exact for every u64), `event_index`,
  `type`, `sender`, `guid_address`, `guid_creation`, `account_address`, `transaction_hash` and the event as the JSON text `serializeEvent`
  writes, so `SELECT event::jsonb->'data'->>'amount' FROM streamd_events WHERE type = '...'` works.
  It is `text` rather than `jsonb` because `jsonb` cannot hold a NUL character, which on-chain
  strings may carry; the cast fails only on such a row. Saved cursors are in `streamd_cursors`.
- `PostgresStoreAdapter` serves `module`, `account` (by stored `sender`), `handle` (by
  `creationNumber`) and `resource` (by stored address and type) descriptors, with the SQLite
  adapter's cursors, paging and options; a change stored with an address is served only as
  `resource`, as there. Head and earliest are the highest and lowest stored
  versions, never below a pruned edge.
- **Transaction lookups** work as on the SQLite store: `getTransactionEvents` and
  `getTransactionEventsByVersion` over `transaction_hash` and `version`, with the same hash
  rule, the same events at the hash's version either way, the same rows left out, only what was stored, `success: true` and `vmStatus` `""`, the
  same wait on a hash not held, and the 410 below the pruned edge, which a version lookup reads
  in the same statement as its rows. Older rows are not backfilled; this fills them, leaving
  out rows whose JSON holds an escaped NUL character (`\u0000`), which Postgres cannot turn into
  `text` and which would fail the whole statement. It rewrites every row it fills in one
  transaction, so on a large table run it in version ranges (`AND version BETWEEN ...`):

  ```sql
  UPDATE streamd_events
  SET transaction_hash = CASE
    WHEN event::json->>'transactionHash' ~ '^0x[0-9a-fA-F]+$' THEN lower(event::json->>'transactionHash')
    ELSE event::json->>'transactionHash' END
  WHERE transaction_hash IS NULL AND account_address IS NULL AND event LIKE '%"transactionHash"%'
    AND strpos(event, '\u0000') = 0;
  ```
- **Resource changes** work as on the SQLite store: `storeEvents` over a `resource` descriptor
  and `await store.write(events, checkpoint, { accountAddress })` store the address in
  `account_address`, and changes stored by an earlier release, with none, are never served as
  `resource`. A database an earlier release made gains `account_address` and `transaction_hash`
  on first use by either the store or the adapter, in one `ALTER` that checks the catalog first,
  so once they are there it never runs again; two connections migrating at once wait on each
  other and both succeed. The `ALTER` waits at most 5 seconds for its lock, so a long read on the
  table fails that first use (it is tried again on the next call) rather than queueing every other
  statement behind it. `streamd_events_resource` and `streamd_events_transaction` are then
  each built in a statement of their own, which on a large table holds back writes, not reads,
  for one scan of it. Like the other `CREATE INDEX IF NOT
  EXISTS` statements each client runs on first use, it takes a brief SHARE lock even when the
  index exists, and the role connecting must own the tables.
- **Retention.** `await store.prune({ beforeVersion })` deletes every event and stored transfer
  below that version and leaves saved cursors, with the SQLite store's rules: a `PostgresStoreAdapter` read whose
  `fromVersion`, `cursor` or `toVersion` is below the edge fails with a `StreamdNetworkError`
  (status 410), a follower on a rare event fails once the edge passes its last match, a fetch with
  no start begins at the oldest version kept, and the edge only rises. The delete and the edge
  are one statement, and each read takes the edge and its page in one statement, so a prune on
  another connection of a `Pool` never lands between them. The edge is the row
  `streamd:pruned-before` in `streamd_cursors`, so a database made before `prune` existed needs no
  migration, and that cursor name is reserved. Postgres reuses the space after autovacuum; run
  `VACUUM FULL` yourself to return it to the system.
- **Stored transfers.** `await storeTransfers(client, store, options)` and
  `await store.writeTransfers(records, checkpoint)` work as on the SQLite store, each record and
  its cursor one statement. `streamd_transfers` holds `version` (`numeric`), `event_index`,
  `asset`, `amount` (`numeric(78, 0)`, exact for every u256, so `SUM(amount)` is too),
  `from_address`, `to_address`, `symbol`, `decimals` and the record as JSON text in `transfer`
  (`asset`, `from_address` and `to_address` are capped at 2,048 UTF-8 bytes so oversized values
  stay within Postgres's btree index limit, while `transfer` keeps the full strings).
  It is created on the first transfer write or prune, apart from the event tables, and `prune`
  deletes transfers below the edge in its one statement.
- **Stored points.** `await storePoints(client, store, descriptor, fold, options)`,
  `await store.writePoints(name, scores, checkpoint)` and `await store.readPoints(name)` work as
  on the SQLite store, resolving once the saved cursor and rows are read. `streamd_points` holds
  `points` as `numeric` (exact, so `ORDER BY points DESC` ranks it), `version` as
  `numeric(20, 0)` and `score` as JSON text. Each write, the first one with every row included,
  is one statement with the cursor, and live writes run one at a time in event order.
- **Saved cursors.** `await store.cursors()` and `await store.deleteCursor(name)` work as they
  do on the SQLite store, the delete itself a single statement. Names sort in byte order
  (`COLLATE "C"`) whatever the database's collation, so in a UTF-8 database both stores list
  the same names in the same order.
- **Stats.** `await store.stats({ byType: true })` returns what the SQLite store's `stats()`
  returns, field for field: in a UTF-8 database the two stores holding the same rows give the
  same answer, `byType` in the same order (`COLLATE "C"` for ties). It is one statement, so every
  count reads one snapshot. Like the other reads it creates and migrates the event tables on
  first use, and it creates `streamd_transfers` when it is missing, as `prune` does. `count(*)`
  in Postgres reads the whole table (a sequential or index-only scan), and these counts are
  exact, never the planner's estimate, so on a large table read them on demand rather than on
  every request.
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
  `{ data, error, loading, refetch }`. `data` is the last successful result and stays through refetches
  and errors. Options are `getEvents`'s, without `signal`; it takes a list of descriptors too.
  `refetch()` runs the same query again on demand: `loading` is `true` while `data` and `error`
  keep their previous values, then a success replaces `data` and clears `error` and a failure
  sets `error` and keeps `data`. A negative `fromVersion` is measured from the head at each run,
  so `useLogs(Swap, { fromVersion: -1_000n })` with a refresh button shows the latest window.
  A refetch, like a changed input, aborts any request still in flight, and only the latest one
  sets state. `refetch` is the same function on every render, so it can sit in effect
  dependencies; its promise resolves once that request settles and never rejects. With no
  descriptor, after unmount, or during server rendering it does nothing.
- **`useEventPages(descriptor, options?)`** walks `client.getEventPages` a page at a time
  and returns `{ pages, hasMore, loading, error, loadMore }`. The first page loads on mount and
  each `loadMore()` appends the next, so `pages` holds every page loaded so far as
  `getEventPages` yielded it, oldest first, or newest first with `orderBy: "desc"`. Options are
  `getEventPages`'s, without `signal`, plus `pageSize` (default 25), the most events each
  adapter request asks for (`FullnodeScanAdapter` keeps every event of the transactions a
  module scan read, so its pages can be longer); `limit` counts across all pages, and a list of descriptors is
  merged. `hasMore` turns `false` once a fetch finds the walk finished: the last page cannot tell
  it is the last, so the `loadMore()` after it adds nothing and turns it off, and an empty range
  shows `false` after the first fetch. A failure sets `error` and keeps the pages, and the next
  `loadMore()` retries, carrying on from the event after the last one held with no repeat and
  no gap: it starts a new walk at that event's version (ends there with `"desc"`) and skips the
  events it reads again, so the first page after a retry can be shorter, `maxPages` counts from
  there, and an ascending `toVersion: "head"` or a `"desc"` negative `fromVersion` is measured
  from the head again. A changed descriptor, option or client starts over, showing no pages
  from that render on and aborting the page in flight. `loadMore` is the same function on every
  render; its promise resolves once its page settles and never rejects, and it does nothing
  while a page is in flight, once `hasMore` is `false`, with no descriptor, after unmount, or
  during server rendering.
- **`useTransactionEvents(hash, options?)`** waits for a submitted transaction through
  `client.eventsOf` and returns `{ data, error, loading, vmStatus }`. A transaction that failed
  on chain sets `error` and `vmStatus`, the chain's status; one not found in time sets `error`
  alone. It takes `eventsOf`'s options plus `descriptor`. Nothing from the previous hash is kept,
  and a `null` hash waits for nothing. It needs a client with `eventsOf`
  (`TransactionEventsClient`): an `AptosClient`, or a `SuiClient`, which takes a base58 digest.
- **`useSubmitTransaction(options)`** sends a transaction and watches it land. `submit(transaction)`
  simulates it through `client.previewEvents`, passes it to your `signAndSubmit(transaction,
  { signal })`, then waits through `client.eventsOf`; the hook returns `{ status, preview,
  previewError, hash, events, error, vmStatus, submit, reset }`. `status` goes `"idle"`,
  `"previewing"`, `"signing"`, `"confirming"`, then `"confirmed"` or `"failed"`; a run never
  throws or rejects.
  A failed run sets `error` (the simulation, the wallet, the transaction or the wait) and, when
  the chain reported one, `vmStatus`. `signAndSubmit` may resolve with a hash or an object with
  `hash` (the Aptos wallet adapter, `PendingTransactionResponse`) or `digest` (Sui); anything
  else fails the run.
  - **A failed preview stops the run** before anything is signed, since the transaction would
    most likely fail and still cost gas. `submitOnPreviewFailure: true` signs anyway and keeps the
    reason as `previewError`; `preview: false` skips the simulation.
  - **One run at a time.** `submit` and `reset` are the same functions on every render, and
    `submit` resolves with the run's final state and never rejects. Calling it while a run is
    going starts nothing and returns that run's promise, so a double click signs once (and a
    `signAndSubmit` must not await `submit`, which would wait on itself).
  - **Unmount, `reset()` or an `<Activity>` hiding the component** abandons the run: the signal given to `signAndSubmit` and
    `eventsOf` aborts, and a run still previewing never reaches `signAndSubmit`. A transaction
    already submitted stays submitted, and a wallet prompt already open can still be approved
    unless your `signAndSubmit` honours the signal. `reset()` returns to `"idle"` and the
    abandoned run sets nothing more. A run cut short by an `<Activity>` hiding the component
    returns to `"idle"`, or to `"failed"` keeping its `hash` once the wallet has returned one,
    even after the component is shown again, unless a newer run or `reset()` came since. The abandoned
    run's promise resolves the same way: `"idle"`, or `"failed"` with the hash.
  - It takes `descriptor`, `decode`, `where` and `timeout`, read when `submit` is called and
    applied to both the preview and the events. The Aptos preview runs without signer keys, so
    the auth key check is skipped. It needs a client with `eventsOf` and, unless `preview: false`,
    `previewEvents` (`SubmitTransactionClient`), and throws a `StreamdError` during render
    otherwise: an `AptosClient`, passed a built transaction, or a `SuiClient`, passed built
    transaction bytes.

  ```tsx
  const { submit, status, preview, events, vmStatus } = useSubmitTransaction({
    // A local signer here; a wallet's sign-then-submit goes in the same place.
    signAndSubmit: (transaction) => aptos.signAndSubmitTransaction({ signer, transaction }),
  });
  ```
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

### Devtools

```tsx
import { adapterMetrics } from "@streamd/core";
import { StreamdDevtools } from "@streamd/react";

const metrics = adapterMetrics(new FullnodeScanAdapter(aptos));
const streamd = new AptosClient({ aptos, adapter: metrics.adapter });

<StreamdProvider client={streamd}>
  <App />
  {import.meta.env.DEV && <StreamdDevtools metrics={metrics} />}
</StreamdProvider>
```

`<StreamdDevtools />` is a small panel fixed to the bottom-right corner, closed until its
`streamd (n running)` button is clicked. It lists every subscription from
`client.listSubscriptions({ ended: true })`: id, descriptor, state (`live`, `retrying` while polls are failing,
`paused`, `ended`), cursor (shortened, the full value on hover), last version, the error count
with the last error, and how long ago the last good poll was. The last 20 subscriptions that
ended stay listed as `ended`, with their final snapshot, so a walk that gave up still shows why. With `metrics`, one `adapterMetrics` wrapper or a record of them by a label
you choose (`{ fullnode: a, archive: b }`), it adds requests, errors, rate-limited calls and
p50, p95 and max latency per method.

- **Props.** `client` (else the provider's), `metrics`, `interval` (how often it re-reads, in
  ms, default 1000; anything but a positive number throws) and `defaultOpen` (default `false`).
- **Read-only.** It offers no pause, resume, stop or cursor control.
- **Secrets.** It shows no node URL or adapter setting: sources are named by your labels, and
  error text is redacted as `listSubscriptions()` redacts it.
- **Footprint.** Inline styles, no styling library and no dependency beyond React. It renders
  nothing during server rendering or before mount. It is meant for development builds; gate it
  as above.

### Fullnode errors in the browser

The SDK's browser client parses every response body as JSON, so a public fullnode's plain-text
429 reaches you as a JSON syntax error, with no status and no backoff hint. Give the SDK
`sendOnce` from `@streamd/aptos`, which sends each request once through `fetch` and keeps a
non-JSON body as text, so the SDK's error keeps the 429 and streamd's adapters and checks
report it as a `StreamdNetworkError` with `statusCode` 429:

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { AptosClient, sendOnce } from "@streamd/aptos";

const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, client: sendOnce(30_000) }));
const streamd = new AptosClient({ aptos, maxRequestsPerSecond: 2 });
```

The argument is a per-request timeout in ms. It does not retry; the poller's backoff does.

## Playground

`apps/playground` is a small web page over the same packages. Enter a Move event type and press
Check, and it shows:

- **The check:** `checkDescriptor`'s verdict, such as "a module event" or "not declared
  `#[event]`".
- **Live events:** after a passing check, the most recent 25 through `useEvents`, newest
  first, with Stop and Start.
- **Generated types:** what [`streamd codegen <address>::<module>`](#generate-event-types)
  writes for the type's module, generated in the browser by `@streamd/codegen`.
- **A snippet:** a Bun script that follows the type with `AptosClient`, importing the
  generated descriptor from `./events.js` (or naming the type inline for a generic event).

Both have Copy buttons. Build and serve it locally:

```sh
cd apps/playground
bun run build   # dist/, a browser bundle
bun run serve   # http://127.0.0.1:5207 (PORT to change it)
```

- **Networks.** Aptos mainnet (the default) and testnet.
- **API key.** Optional. It is sent only to the Aptos node, as the SDK's `API_KEY`, and kept
  in memory unless "Remember the key on this device" is ticked, which stores it in
  `localStorage`; unticking removes it. The snippet never contains it: it reads
  `APTOS_API_KEY` from the environment.
- **Rate limits.** The public fullnodes limit anonymous requests per IP, and following a module
  event scans every transaction. Without a key the client makes at most two adapter calls a
  second; each module-scan call reads up to ten pages of transactions, so a busy type such as
  `0x1::fungible_asset::Deposit` runs at about four HTTP requests a second. Requests go through
  `sendOnce`, a 429 is shown as a rate limit with the advice to add a key, and the
  poller backs off, giving up after five failures in a row. Polling starts only after a passing check, and Stop ends it.
  Expect an anonymous tab to be limited after some minutes on a busy chain.
- **Scope.** Module events only, on Aptos. No handle, account or resource descriptors, type
  arguments, or codegen options.

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
`activity`, `trace`, `balances`, `watch` and `inspect`) take `--node-url` more than once, spreading reads across the nodes through
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

To pick up where a run stopped, pass `--cursor-file <path>`: `tail` checkpoints its position
after each printed line and resumes past it on restart (a saved position beats
`--from-version` and `--from-time`, so even `--limit` or a mid-page stop makes progress). It is
at least once, since stdout has no acknowledgement: a crash between writing a line and saving
the cursor file can repeat that line. By hand, pass the last printed `version` plus one as
`--from-version`.

Polling errors are reported on stderr and tailing continues until the subscription gives
up. Exit codes: `0` when the limit is reached, on Ctrl-C or `SIGTERM`, or when stdout closes
(as when piped into `head`); `1` when the subscription stopped on an error, or output or the
cursor file could not be written; `2` for invalid arguments; `130` on a second Ctrl-C.

### Request stats

`--stats` on the commands that read events (`tail`, `forward`, `store`, `query`, `record`, `tx`,
`transfers`, `activity`, `trace`, `balances`, `watch` and `inspect`) prints what the command has asked of each node, through
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
node), `archive` appears with `--archive`, and `indexer` on `activity` and `trace`, counting
`IndexerAccountAdapter`'s calls (its own fullnode reads of each listed transaction are not
counted); each maps the adapter methods called so far to
`requests`, `errors`, `rateLimited` and latency percentiles in milliseconds over the most recent
1024 calls. The numbers are totals since the start, never reset. Nodes are named by position,
never by URL, since a URL may carry a key. Each node is counted inside the pool, the archive
fallback and `--cache`, so a page the cache answered is not a request, a `429` the pool moved to
another node counts on both, and a pruned read counts as an error on the fullnode and a request
on the archive. Reads made outside the adapter are not counted: name lookups (an `--account` or
address given as a name), `inspect`'s resource reads, and with `--chain sui`, `transfers` and `balances`, which read the
first node directly. A second Ctrl-C exits at once, without the line.

### Sui from the command line

Every command but `handles`, `inspect`, `explain`, `swaps` and `sales` takes `--chain sui` to read Sui through [`@streamd/sui`](#sui) instead of Aptos,
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
- `@streamd/sui` and `@mysten/sui` are optional peer dependencies imported only for `--chain sui`;
  without them installed, `--chain sui` exits 1 with `--chain sui needs @mysten/sui installed`.

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
matching a JSON boolean; a missing field never matches, under `!=` too. A malformed
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
| `--to <url>` | webhook URL, `http` or `https` (required), or a Redis, NATS or Kafka URL (below) |
| `--stream <key>` | the Redis stream to add each batch to (required with a Redis `--to`) |
| `--subject <name>` | the NATS subject to publish each batch to (required with a NATS `--to`) |
| `--topic <name>` | the Kafka topic to produce each batch to (required with a Kafka `--to`) |
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

The sending side is in core too, for code that posts its own webhooks the way `forward` and the
gateway do: `signWebhook(body, secret)` resolves the header value, `webhookSink({ to, secret?,
headers?, timeout })` makes one signed POST that never follows a redirect, and
`deliver(body, sink, retries, { error, signal, retryDelay? })` retries it with the same backoff
and `Retry-After` rule, resolving `true` once acknowledged.

The signature has no timestamp, so it proves origin and integrity but not freshness: a captured
request can be replayed. Deduplicate on each event's `version` and `eventIndex`, which delivery
being at least once already calls for.

Network errors, timeouts, `408`, `429` and `5xx` are retried with exponential backoff from
1s, capped at 60s; a `Retry-After` on `429` or `503` replaces the backoff for that wait.
Any other status, redirects included, fails at once. The webhook receives only the body,
the content type, your `--header` values and any signature, and neither the URL, header
values nor the secret are printed.

**Receive them with `@streamd/receiver`.** `createReceiver` returns a `Request` to `Response`
handler that does the checks above, parses the events with `parseEvent` and drops any already
handled, by `eventId`, so a resent batch does not reach `onEvents` again while its store remembers it.
It depends only on `@streamd/core`, and runs anywhere with `fetch`'s `Request` and Web Crypto:
Bun, Deno, edge workers and Node 19+ through any Fetch-style server.

```ts
import { createReceiver } from "@streamd/receiver";

const receive = createReceiver({
  secret: process.env.STREAMD_WEBHOOK_SECRET!,
  decode: ["amount"],
  async onEvents(events) {
    for (const event of events) await save(event);
  },
});

Bun.serve({ port: 8787, fetch: receive });
```

```sh
streamd forward --to http://127.0.0.1:8787 --secret "$STREAMD_WEBHOOK_SECRET" 0x1::coin::DepositEvent
```

Each request is checked in order, and nothing is parsed before the signature holds:

| Answer | When | What `forward` does |
| --- | --- | --- |
| `204` | handled, or every event was a repeat | moves on |
| `405` | not `POST` | stops, exit 1 |
| `415` | not `application/json` | stops, exit 1 |
| `413` | body over `maxBodyBytes` (default 4 MiB), by `content-length` or as read | stops, exit 1 |
| `401` | signature missing, malformed or wrong | stops, exit 1 |
| `400` | not UTF-8, not a JSON array, or an event `parseEvent` refuses | stops, exit 1 |
| `500` | the store or `onEvents` threw or rejected, or `seen` answered the wrong number of results | retries |
| `503` | another delivery of one of these events is still being handled | retries with its backoff |

`onEvents` gets only the events not seen before, in body order, and is not called when there
are none. They are recorded as handled only after it resolves, so a failure leaves them for the
retry. That makes handling at least once, not exactly once: when `onEvents` succeeds and
recording then fails, or the process stops between the two, the retry runs it again. Make
`onEvents` idempotent where that matters. `onError(error, request)` is told what caused a `400`
or `500`; the response body never says.

The default store, `MemoryDedupStore`, keeps the last `maxEntries` ids (default 100,000) in
memory and forgets them on restart. Behind more than one instance, or across restarts, pass a
`store`: any object with `seen(ids)`, answering a boolean per id in order, and `add(ids)`,
either of which may be async. Ids are `version:eventIndex`, which repeat across networks and
chains, so when forwarders for more than one network share a receiver or store, give each its
own receiver with a `namespace` (`"aptos:mainnet"`), which prefixes every id it stores. Over
`bun:sqlite`:

```ts
import { Database } from "bun:sqlite";
import type { DedupStore } from "@streamd/receiver";

const db = new Database("seen.db");
db.run("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)");
const has = db.query("SELECT 1 FROM seen WHERE id = ?");
const insert = db.query("INSERT OR IGNORE INTO seen (id) VALUES (?)");

const store: DedupStore = {
  seen: (ids) => ids.map((id) => has.get(id) !== null),
  add: db.transaction((ids: readonly string[]) => {
    for (const id of ids) insert.run(id);
  }),
};
```

The `503` guard covers one process; two instances sharing a store can both handle a resent batch
that arrives while the first is still running. A handler slower than `--timeout` spends
`forward`'s `--retries` on `503`s, so keep `onEvents` quick or raise them. The signature has no
timestamp, so a captured body replays once its ids are forgotten (after a restart with the memory
store, or past `maxEntries`): use `https` and a durable store where that matters. A body over `maxBodyBytes` stops `forward`, so
lower `--batch-size` rather than raising the limit far. Alert payloads from `streamd alert` are a
different body; check those with `verifyWebhook` and deduplicate on their `id`.

**To a Redis stream.** Give `--to` a Redis URL, `redis://[user:password@]host[:port][/db]` or
`rediss://` for TLS, and `--stream` the stream key. Each batch becomes one entry, added with
`XADD <stream> * body <json>`, where `body` is exactly the webhook body. With a secret the entry
also carries `signature`, the same `sha256=<hex>` the webhook header would, so a consumer checks
it with `verifyWebhook`:

```sh
streamd forward --to redis://localhost:6379 --stream aptos:deposits \
  --cursor-file deposits.cursor 0x1::coin::DepositEvent
```

```ts
import { verifyWebhook } from "@streamd/core";

// One entry's fields as a record, from XREAD, XREADGROUP or XRANGE.
async function handle(fields: Record<string, string>): Promise<void> {
  if (!(await verifyWebhook(fields.body, fields.signature, process.env.STREAMD_WEBHOOK_SECRET!))) return;
  const events = JSON.parse(fields.body);
  // ...
}
```

The reply to `XADD`, the new entry's id, is the acknowledgement, and the cursor file moves only
after it, exactly as after a webhook's `2xx`. It means the primary accepted the entry; streamd does
not wait for replicas or the append-only file. Connection failures, timeouts and transient
replies such as `LOADING`, `READONLY` or `OOM` are retried with the same backoff and `--retries`
budget; `ERR`, `WRONGTYPE`, `NOPERM`, `NOAUTH` and `WRONGPASS` fail at once. Bun's client
reports a refused handshake, such as a wrong password, as a closed connection, so under Bun that
is retried through the budget before exiting `1`. `--timeout` bounds
each `XADD`, and a timed-out one drops the connection so the retry opens a fresh one; it may
still have landed, which at least once allows. A Redis restart mid-run is retried the same way,
reconnecting on the next attempt. `--header` is refused, since an entry has no headers, and the
URL and its password are never printed. Nothing trims the stream: cap it with `XTRIM` once your
consumers have read past an entry.

Under Bun this uses Bun's own Redis client. Under Node it needs `ioredis` installed
(`npm i ioredis`), loaded only when `--to` is a Redis URL. Redis Cluster and Sentinel URLs are
not supported.

**To NATS JetStream.** Give `--to` a NATS URL, `nats://host[:port]` (port `4222` by default)
with `user:password@` or a `token@` before the host when the server asks for them, and
`--subject` the subject to publish to, which a JetStream stream must capture. Each batch becomes
one message whose payload is exactly the webhook body, with two headers: `Nats-Msg-Id`, the
SHA-256 of the body in hex, and with a secret `x-streamd-signature`, the same `sha256=<hex>` the
webhook header carries:

```sh
nats stream add DEPOSITS --subjects 'aptos.deposits' --defaults
streamd forward --to nats://localhost:4222 --subject aptos.deposits \
  --cursor-file deposits.cursor 0x1::coin::DepositEvent
```

```ts
import { WEBHOOK_SIGNATURE_HEADER, verifyWebhook } from "@streamd/core";
import type { JsMsg } from "@nats-io/jetstream";

async function handle(msg: JsMsg): Promise<void> {
  const signature = msg.headers?.get(WEBHOOK_SIGNATURE_HEADER);
  if (!(await verifyWebhook(msg.data, signature, process.env.STREAMD_WEBHOOK_SECRET!))) return msg.term();
  const events = JSON.parse(msg.string());
  // ...
  msg.ack();
}
```

The stream's PubAck, the reply naming the stream and the message's sequence, is the
acknowledgement, and the cursor file moves only after it, exactly as after a webhook's `2xx`. A
PubAck marked `duplicate` counts too: the message id is the body's hash, so when a batch is sent
again, after a retry of a publish that landed or a restart that resends the batch in flight,
JetStream drops the copy inside the stream's duplicate window (two minutes by default) and
consumers see it once. Outside the window a resend is delivered again, which at least once
allows.

What fails at once, exiting `1`: no stream capturing the subject (the server's `503` no
responders reply), a JetStream error with a `4xx` code (such as a message over the stream's size
limit), a reply that is not a PubAck (something other than a stream answered), and the server's
`Authorization Violation`, `Permissions Violation` and `Maximum Payload Violation` errors (lower
`--batch-size` for the last). Everything else is retried with the same backoff and `--retries`
budget: refused or dropped connections, other server errors, JetStream errors with a `5xx` code,
and no reply within `--timeout`, which bounds connecting and publishing together and drops the
connection so the retry opens a fresh one. A NATS restart mid-run is retried the same way,
reconnecting on the next attempt.

The CLI speaks the NATS protocol itself, so nothing needs installing under Bun or Node. It
connects in plaintext only: `tls://` is refused, and so is a server that requires TLS, as are
NKEY, JWT and `.creds` authentication and lists of several servers. `--header` is refused, the
server needs headers (NATS 2.2 or later), and the URL, its password and its token are never
printed.

**To a Kafka topic.** Give `--to` a Kafka URL, `kafka://host[:port]` (port `9092` by default),
several brokers separated by commas (`kafka://b1:9092,b2:9092`), or `kafkas://` for TLS, and
`--topic` a topic that already exists. For SASL put `user:password@` before the brokers and name
the mechanism: `?sasl=plain`, `?sasl=scram-sha-256` or `?sasl=scram-sha-512` (there is no
default, and PLAIN sends the password as it is, so pair it with `kafkas://`). Each batch becomes
one record whose value is exactly the webhook body. Its key is the descriptor, such as
`["module",["0x1::coin::DepositEvent"]]`, the same for every batch of one forwarder, so the
default partitioner keeps them on one partition, in order. With a secret the record carries the
header `x-streamd-signature`, the same `sha256=<hex>` the webhook header carries:

```sh
kafka-topics.sh --create --topic aptos.deposits --bootstrap-server localhost:9092
streamd forward --to kafka://localhost:9092 --topic aptos.deposits \
  --cursor-file deposits.cursor 0x1::coin::DepositEvent
```

```ts
import { WEBHOOK_SIGNATURE_HEADER, verifyWebhook } from "@streamd/core";
import type { KafkaMessage } from "kafkajs";

async function handle(message: KafkaMessage): Promise<void> {
  const signature = message.headers?.[WEBHOOK_SIGNATURE_HEADER]?.toString();
  if (!(await verifyWebhook(message.value!, signature, process.env.STREAMD_WEBHOOK_SECRET!))) return;
  const events = JSON.parse(message.value!.toString());
  // ...
}
```

The producer sends with `acks=all`, so a send resolves only once every in-sync replica has the
record, and the cursor file moves only after that, exactly as after a webhook's `2xx`. The
producer is idempotent: a resend of a batch that was not acknowledged, whether the client's own
retry of a stalled request (each request gets a third of `--timeout`, so one retry fits) or a
later attempt after a dropped connection or a broker restart, carries the same producer id and
sequence, and the broker drops the copy. That holds under `kafkajs`; under Confluent's client
only librdkafka's own retries are covered, and a later attempt is a new record. At least once
still allows a duplicate when a send outlives `--timeout`, which closes the producer so the retry opens a fresh one with a new
producer id, and when `forward` restarts and resends the batch in flight. A resent record's value is byte-identical, so a consumer drops
copies by the value's SHA-256 or by each event's `version` and `eventIndex`.

What fails at once, exiting `1`: a topic that does not exist (the producer never creates one,
and a broker answering "unknown topic or partition" for longer than one metadata refresh, as it
can briefly during a partition reassignment, counts too, so the supervisor restarts it),
authorization or SASL authentication failures, a record over the broker's or topic's size limit
(lower `--batch-size`), an invalid topic or record, a topic policy violation, and a broker too
old for the request. Everything else is retried with the same backoff and `--retries` budget:
refused or dropped connections, leader elections, too few in-sync replicas, request timeouts,
and a send still unfinished at `--timeout`, which bounds connecting and sending together. A
broker restart mid-run is retried the same way, reconnecting on the next attempt. `--header`
is refused, and the URL and its password are never printed.

The CLI ships no Kafka client: install one, and it is loaded only when `--to` is a Kafka URL.
`@confluentinc/kafka-javascript` is the one to choose, maintained by Confluent over
librdkafka, and streamd drives it through its KafkaJS-compatible API; `kafkajs`, pure
JavaScript, works too but has had no release since February 2023, and under Node 23 or later
and Bun it prints a harmless `TimeoutNegativeWarning` on stderr. When both are installed,
Confluent's is used. Under Confluent's client, librdkafka waits up to 30 seconds for a topic
missing from the metadata to appear, longer than the default `--timeout`, so a missing topic
is retried as a timeout until the budget runs out rather than failing at once. Transactions
(exactly once), OAUTHBEARER, client certificates, a custom CA and IPv6 broker addresses (kafkajs
cannot reach them; use a hostname) are not supported.

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

Exit codes: `0` on Ctrl-C or `SIGTERM`, which abandons an in-flight POST, `XADD`, publish or send
without moving the cursor; `1` when a batch fails past its retries or fails at once, the Redis
or Kafka client cannot be loaded, the subscription stops on an error, the cursor file cannot be read or
written, or `--to-version` is above the head; `2` for invalid arguments. Run it under a supervisor that restarts on `1`.

### Alert on thresholds

`streamd alert` follows the rules in a JSON file and POSTs one alert to a webhook the first time
a rule's count or sum of matching events in a window reaches its threshold, or leaves the band its
recent windows set. Delivery, signing, retries and the cursor file work as for `forward`.

```sh
# signed with STREAMD_WEBHOOK_SECRET when it is set
streamd alert --rules rules.json --to https://hooks.example.com/alerts --cursor-file alerts.cursor
```

```json
{
  "rules": [
    {
      "name": "fee-burst",
      "descriptor": { "eventType": "0x1::transaction_fee::FeeStatement" },
      "where": ["total_charge_gas_units>=1000"],
      "window": "1m",
      "threshold": { "count": 50 }
    },
    {
      "name": "big-deposits",
      "descriptor": { "eventType": ["0x1::fungible_asset::Deposit"] },
      "window": "5m",
      "threshold": { "sum": "amount", "gte": "1000000000000" }
    }
  ]
}
```

| Rule field | Meaning |
| --- | --- |
| `name` | unique, non-empty; names the rule in alerts and in the cursor file (required) |
| `descriptor` | `eventType` (a type or a list), `account` and `resource`, combined as the `tail` flags of those names are (required) |
| `where`, `sender`, `function`, `decode` | arrays of strings, exactly as the repeatable `--where`, `--sender`, `--function` and `--decode` flags |
| `window` | `<n>s`, `<n>m`, `<n>h` or `<n>d` (required) |
| `threshold` | `{ "count": n }`, at least `n` matching events, or `{ "sum": "<path>", "gte": "<integer>" }`, the field at that path summed at full precision (`gte` a decimal string or a safe integer), or `{ "anomaly": { ... } }`, below (required) |

The file is read once, before anything reaches the chain. Any other key, a missing field, or a
value the flags would refuse exits `1` with a message naming the file, the rule and the field, such
as `rules.json: rule "fee-burst" (#1): window: must be a duration such as 30s, 5m, 1h or 1d`.
Handle descriptors are refused: their events carry no timestamp.

| Option | Meaning |
| --- | --- |
| `--rules <file>` | the rules file (required) |
| `--to <url>` | webhook URL for every rule, `http` or `https` (required) |
| `--cursor-file <path>` | each rule's position and open window (default `streamd-alert.cursor`) |
| `--retries`, `--timeout`, `--header`, `--secret` | as for `forward`, per alert |

`--network`, `--node-url`, `--archive`, `--api-key`, `--stats`, `--chain sui`, `--from-version`,
`--from-time` and `--poll-interval` work as for `tail`; a start applies to rules with no saved
position.

**Windows** are fixed and back to back, in chain time: an event belongs to the window of
`window` length, aligned to the Unix epoch, that holds its `timestamp`. The same history always
gives the same windows and the same alerts, whenever it is read. An event with no timestamp stops
the command with exit `1`.

**Firing** happens once per window and rule, on the event that first takes the count or sum to the
threshold. Later events in that window count but send nothing, and nothing waits for a window to
close. A summed field that is present and not an integer is reported and that event skipped.

**Anomaly thresholds** fire when a window's count or sum leaves the band its rule's previous
closed windows set, rather than at a fixed number:

```json
{
  "name": "fee-rate",
  "descriptor": { "eventType": "0x1::transaction_fee::FeeStatement" },
  "window": "1m",
  "threshold": { "anomaly": { "metric": "count", "k": 3, "baseline": 30, "minBaseline": 10, "direction": "both" } }
}
```

| `anomaly` field | Meaning |
| --- | --- |
| `metric` | `count`, matching events per window, or `sum` with `sum` the field path summed as above (required) |
| `k` | band multiplier, a positive decimal with at most 4 fractional digits, number or string (default `3`) |
| `baseline` | how many closed windows the band is taken from, 3 to 1000 (default `30`) |
| `minBaseline` | closed windows needed before the rule can fire, 1 to `baseline` (default 10, or `baseline` when smaller) |
| `direction` | `above`, `below` or `both` (default `both`) |

The band is `median ± k × 1.4826 × MAD` over the baseline, where MAD is the median of each
window's distance from the median; the 1.4826 makes `k` read as standard deviations for normally
distributed values. A value fires only strictly outside the band. Everything is exact: medians are
taken over integers (the mean of the two middle values for an even baseline) and compared without
floating point, so sums past 2^64 work. When more than half the baseline's windows equal its
median, as when they are all equal or a sparse type is mostly empty, the MAD is 0, so the band is
the median alone and any other value fires; choose a longer window for such a type.

A window with no matching events counts as zero. A window closes when an event lands in a later
one, and every window skipped in between closes as a zero too and joins the baseline, the oldest
dropping out past `baseline`. A run of empty windows is judged once, at its first window with a long enough baseline, so one
outage sends at most one alert. The first window a rule opens after its start (the head,
`--from-version` or `--from-time`), before any window was saved, is left out of the baseline and
never fires, since the start may fall mid-window and undercount it; empty windows between the
start and that first event are not counted either.

`above` fires on the event that first takes the value past the band, as a count threshold does.
`below` waits for the window to close, since a count still open may grow, and is carried by the
event that closed it, which is also the event in its payload. As with every threshold, nothing
uses the wall clock: a type that stops entirely closes no window, so it is never reported as below.

Each rule has its own subscription; rules over the same descriptor, start and poll interval share
one fetch per interval, except when starting at the head.

**The alert** is one POST per crossing, `content-type: application/json`, signed as `forward`
signs:

```json
{"id":"fee-burst:1763130000000000","rule":"fee-burst","windowStart":"1763130000000000","windowEnd":"1763130060000000","threshold":{"count":"50"},"value":"50","event":{"type":"0x1::transaction_fee::FeeStatement","version":"3480218811",...}}
```

Times are microseconds since the epoch and integers are decimal strings; `event` is the crossing
event as `tail` prints it, and `threshold` is `{"count":"n"}` or `{"sum":"<path>","gte":"n"}`.

An anomaly alert's `threshold` echoes the rule with its defaults filled in, `value` is the
window's count or sum, and one more field says how it left the band, as exact decimals with
trailing zeros trimmed (`mad` unscaled, `low` and `high` the band's bounds):

```json
{"id":"fee-rate:1763130060000000","rule":"fee-rate","windowStart":"1763130060000000","windowEnd":"1763130120000000","threshold":{"anomaly":{"metric":"count","k":"3","baseline":"30","minBaseline":"10","direction":"both"}},"value":"412","anomaly":{"side":"above","median":"201.5","mad":"18","low":"121.4396","high":"281.5604","baselineWindows":"30"},"event":{...}}
```
`id` is the rule and the window start, the same on every redelivery, so a receiver can
deduplicate on it.

**The cursor file** holds one JSON object: each rule's cursor beside its open window (start,
count or sum, and whether it alerted; for an anomaly rule also `history`, the baseline's values
oldest first, and `partial` on the first window), written atomically after each delivered page and on exit.
The window saved is always the one as of the end of the page the cursor follows, and it is saved
only after that page's alerts were acknowledged, so a restart resumes the window without losing an
alert. Delivery is at least once: a crash, a failed delivery, or a stop while a page's alerts are
being sent replays that page, resending its alerts with the same `id`. A stop between pages
repeats nothing. An entry records what its rule counts; changing a rule's descriptor, filter,
decode, window or threshold (any `anomaly` field included) makes the next run exit `1` naming it, until its entry is removed or
another cursor file is used. Entries for rules no longer in the file are kept.

Exit codes: `0` on Ctrl-C or `SIGTERM`; `1` when the rules file or the cursor file cannot be read
or is invalid, an alert fails past its retries, a rule's subscription stops on an error, an event
has no timestamp, or the cursor file cannot be written; `2` for invalid arguments.

### Start from a time

`tail`, `forward`, `alert` and `query` take `--from-time` instead of `--from-version`: a duration back from
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

### Inspect a resource over time

`streamd inspect` prints what each transaction in a range did to one resource, one JSON line per
write or delete, listing every field it changed:

```sh
streamd inspect 0x1 --resource 0x1::stake::ValidatorPerformance --from-version 8071200052 --to-version 8071200082
# {"version":"8071200082","transactionHash":"0xd768...","timestamp":"1794796910359480","change":"updated","changes":[{"path":"validators.6.successful_proposals","before":"302","after":"303"}]}
```

- **Changes.** `change` is `updated`, with `changes` listing each leaf that differs as
  `{ path, before, after }` (possibly none, for a write that left the value as it was); `created`,
  with the whole new `value`; or `deleted`. A path is dotted, array elements by index, the form
  `--where` reads; a field that appears or disappears has no `before` or no `after`, and a field
  that changes shape, such as an object becoming a string, is one change with both values whole.
  Bigints in the line are decimal strings.
- **The first change** is diffed against the value at `--from-version - 1`, read once at the start.
  If the node has pruned that version and there is no `--archive`, the first change is printed as
  `written`, with its whole `value`, and a note on stderr says so.
- **Range.** `--from-version` is required and `--to-version` is a version or `head` (the default,
  read once at the start). `--limit <n>` stops after `n` changes. Changes come from transaction write
  sets through a scan of every transaction in the range, so keep ranges short: a thousand mainnet
  versions took about 25 seconds, in 5 requests, against the public fullnode. A range the node has pruned goes
  to `--archive`, as `query`'s does.
- **`--at <n>`** prints the resource's value at one version, `{"version":"<n>","value":...}`, with
  `value` `null` when the account did not hold it then:

  ```sh
  streamd inspect 0x1 --resource "0x1::coin::CoinInfo<0x1::aptos_coin::AptosCoin>" --at 8071200057
  streamd inspect 0x1 --resource 0x1::block::BlockResource --at 100 --archive default
  ```

- **`--format pretty`** prints a line of version, UTC time and change, then one indented
  `path: before -> after` line per field, `(absent)` for a missing side; `--at` prints the value
  as indented JSON. Control characters in on-chain text print as `?`.
- **Reads.** The value before the range and `--at` are one
  `/accounts/<address>/resource/<type>?ledger_version=` read on the first `--node-url`, sent to
  `--archive` only when that node answers `410`; any other failure, such as a version above the
  head, exits 1. These reads are not counted by `--stats`.
- **Aptos only.** `--chain sui` is refused: Sui reads object state only at the head, so there is
  no value before a range or at a past version to read.

Exit codes are `query`'s: 0 when finished, interrupted or piped into something that stops reading,
1 on a failed read or write, 2 for bad usage.

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
does, with `--poll-interval` and the same at-least-once `--cursor-file`. With it, ascending reads
stream each page as it arrives through `iterateTransfers` (while `--order desc` without `--limit`
buffers the range to reverse it), and failed pages are
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

### Follow or query swaps and sales

`streamd swaps` prints [DEX swaps](#dex-swaps) and `streamd sales` [NFT sales](#nft-sales) as
JSON lines, through `AptosClient`'s `swaps()`/`getSwaps()` and `sales()`/`getSales()`, followed
or over a closed range exactly as `streamd transfers` is:

```sh
# every swap with APT on either side, as it lands
streamd swaps --asset 0xa
# Hyperion and Tapp swaps over a closed range, newest first
streamd swaps --protocol hyperion --protocol tapp --from-version 8177444976 --to-version 8177447976 --order desc
# the Wapal sales in one transaction of the archived history
streamd sales --marketplace wapal --from-version 7028273582 --to-version 7028273582 --archive default
```

Each line is the record event as `tail` prints events, its `data` a `Swap` (one per pool hop) or
a `Sale` (one per token sold), with amounts and prices in base units as decimal strings:

```json
{"type":"0x8b4a...e05c::pool_v3::SwapEventV3","data":{"protocol":"hyperion","poolId":"0x9256...00d8","assetIn":"0x...0a","amountIn":"2127446234","assetOut":"0xbae2...6f3b","amountOut":"8076694","trader":"0x989b...9fec"},"version":"8177445224","sequenceNumber":"0","eventIndex":13,"transactionHash":"0x4819...0af8","timestamp":"1795395453252889","sender":"0x989b...9fec","entryFunction":{...}}
```

| Option | Meaning |
| --- | --- |
| `--protocol <name>` | swaps: only this DEX, repeatable (any one matches): `hyperion`, `thala-clamm`, `thala-v2`, `cellana`, `tapp`, `liquidswap`, `pancakeswap` or `thala-v1` |
| `--asset <asset>` | swaps: only swaps with this asset on either side, as `transfers --asset` takes it |
| `--marketplace <name>` | sales: only this marketplace, repeatable: `wapal` or `tradeport` |
| `--token <token>` | sales: only sales of this token, a token object address or a legacy token id |

`--from-version`, `--from-time`, `--to-version <n \| head>`, `--order`, `--limit`,
`--cursor-file`, `--poll-interval` and `--batch-size` behave as they do for `transfers`, with the
same combinations refused. A range is read whole through `getSwaps` or `getSales` and then
printed, since neither has a page-by-page iterator. Every scanned transaction costs a read, so
keep ranges small on public endpoints or pass `--api-key`. Both marketplaces stopped trading
near version 7,030,000,000, so sales history needs `--archive`. A name, asset or token the client
does not accept exits `1` with its message; exit codes are otherwise `transfers`'s. Aptos only:
`--chain sui` exits `2`, since `SuiClient` has neither method.

### Account activity and fund-flow traces

`streamd activity <address>` prints an account's [activity](#account-activity), one JSON line per
transaction it sent or received, through `AptosClient`'s `activity()` or `getActivity()`, and
`streamd trace <address>` resolves the [fund-flow graph](#fund-flow-tracing) around it through
`trace()`, as one JSON line or as Graphviz DOT:

```sh
# what alice.apt sent and received in a closed range, with symbols and decimals
streamd activity alice.apt --from-version 8177444976 --to-version 8177447976 --with-asset-info
# follow it live
streamd activity 0x4c...
# the counterparties one hop out, drawn with Graphviz
streamd trace 0x4c... --from-version 8177000000 --format dot | dot -Tsvg > trace.svg
# two hops of APT, an exchange kept but not expanded, and the path to one address
streamd trace 0x4c... --hops 2 --asset 0xa --min-amount 100000000 --stop 0x5678... --target 0x9abc... \
  --from-version 8177000000 --max-requests 200
```

Each `activity` line is the activity event as `tail` prints events, its `data` an `Activity`
(`direction`, `transfers`, `balanceChanges` and `events`) with bigints as decimal strings. The
flags are `transfers`': `--from-version`, `--from-time`, `--to-version <n | head>`, `--order`,
`--limit`, `--cursor-file`, `--poll-interval` and `--batch-size`, with the same combinations
refused, plus `--with-asset-info`. A range is read whole through `getActivity` and then printed.

`trace` prints the `FundFlowGraph` as one line, `{"root":...,"nodes":[...],"edges":[...],"requests":n}`
with `stoppedBy` when a bound ended the walk and `amount`, `firstVersion` and `lastVersion` as
decimal strings. With `--target` it adds `path`, the fewest edges `tracePath` finds from the root to
the target (from the target to the root under `--direction in`, either way for `both`), or `null`.
`--format dot` prints `digraph trace { ... }` instead: one node per address labelled with its hop,
the root drawn heavier and unexpanded nodes dashed, one edge per sender, receiver and asset
labelled with the asset, the summed amount and the transfer count, the path drawn bold, and
`stoppedBy` as the graph's `comment`.

| Option | Meaning |
| --- | --- |
| `--indexer <url>` | the Indexer's GraphQL URL (default: the network's own) |
| `--hops <n>` | trace: hops to follow, 1 to 6 (default `1`) |
| `--direction <out \| in \| both>` | trace: follow what each address sent, received or both (default `both`) |
| `--from-version <n>`, `--to-version <n \| head>` | trace: the range every address is read over (default: from the oldest version the fullnode holds to the head, read once) |
| `--asset <asset>` | trace: only transfers of this asset, as `transfers --asset` takes it |
| `--min-amount <n>` | trace: ignore transfers below this many base units |
| `--stop <address>` | trace: keep this address as a node but never expand it, repeatable |
| `--target <address>` | trace: end the walk once this address joins, and add `path` |
| `--max-nodes <n>`, `--max-requests <n>` | trace: most addresses in the graph (default `100`) and most adapter calls (default `1000`) |
| `--format <json \| dot>` | trace: one JSON line (default) or Graphviz DOT |

Both read over an Indexer route: the account's transactions come from the Aptos Indexer through
`IndexerAccountAdapter`, since the fullnode sees only the ones an account signed, and every other
read, the fungible-store views included, from the fullnode stack the other commands use, with
several `--node-url`s and `--archive` as usual. The API key goes to the network's Indexer only when
it goes to the network's fullnode, that is with no `--node-url`; an `--indexer` URL gets none.
A network with no Indexer exits `1`. Reads stop at the Indexer's processed version and start no
earlier than the fullnode's oldest. Every listed transaction costs a fullnode read and its stores a
view each, so keep ranges small on public endpoints, and give `trace` a `--max-requests` and the
busy addresses (exchanges, routers) as `--stop`. Addresses, names, `--stop`, `--target` and
`--asset` the client does not accept exit `1` with its message.

Exit codes: `activity`'s are `transfers`'s. `trace` exits `0` once the graph is printed, even when
a bound stopped it early (`stoppedBy` says which), `1` when the trace or the write fails, `2` for
invalid arguments, and `130` on Ctrl-C before it printed. Aptos only: `--chain sui` exits `2`,
since `SuiClient` has neither method.

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

`streamd explain <hash>` waits the same way, then prints what the transaction did as
[`formatExplanation`](#explain-a-transaction) renders `AptosClient.explain`'s summary: status,
sender, function, gas and fee, transfers with asset symbols and decimals, net balance changes,
swaps, sales and the event count:

```sh
streamd explain 0x5d16bc7f...
streamd explain --json 0x5d16bc7f... | jq '.balanceChanges'
```

`--json` prints the whole `TransactionExplanation` as one JSON line instead, every bigint as a
decimal string. It takes `--timeout`, `--network`, `--node-url` (repeatable), `--archive`,
`--api-key` and `--stats` as `tx` does. A failed transaction is still printed, with its gas and no
transfers, and then exits 1 with its VM status on stderr; otherwise the exit codes are `tx`'s.
Aptos only: `--chain sui` exits 2, since Sui has no explainer yet.

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
only, not `--account` or `--handle`. Like `checkDescriptor`, it checks only the outer struct
at the normalized address (not `<...>` type-argument arity or the type arguments themselves),
whereas `tail` and `forward` compare `event.type` without normalizing addresses and require an
exact match whenever `<...>` is given.

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

### Monitor several nodes

`streamd monitor` reads every node once per round and prints, for each, how far its head is
behind the best one, how many of its calls failed or were refused with `429` over a sliding
window, and whether its recent events match the first node's, as a JSON line or as Prometheus
text:

```sh
streamd monitor --node-url https://api.mainnet.aptoslabs.com/v1 \
  --node-url https://fullnode.mainnet.aptoslabs.com/v1 --node-url https://my-node.example/v1
streamd monitor --node-url "$A" --node-url "$B" --format prometheus --listen 9464
```

| Option | Meaning |
| --- | --- |
| `--node-url <url>` | a node to monitor, at least twice; node `0` is the first and the reference |
| `--interval <ms>` | time from one round's start to the next, and each call's timeout (default `10000`) |
| `--window <duration>` | how far back the window sums reach, `30s`, `5m`, `1h` or `1d` (default `5m`) |
| `--range <n>` | versions compared with node `0` per round (default `100`, at most `10000`) |
| `--format <name>` | `json` or `prometheus` (default `json`) |
| `--listen <[host:]port>` | also serve the latest round's Prometheus text at `/metrics`, on `127.0.0.1` unless a host is given |
| `--limit <n>` | exit after `n` rounds |

Each round, every node reads its head version and its head block's time, then each node that
answered is compared with node `0` through [`compareAdapters`](#data-sources-and-their-limits)
over the newest `--range` versions both have, ending at the lower of the two heads, so a slow node
narrows only its own comparison. The events compared are those of the event types given (or
`--account`, or a handle or resource, as `tail` takes them), by default
`0x1::transaction_fee::FeeStatement`, which every user transaction emits; `--chain sui` has no
default and needs one. A round never overlaps the next, and a call that takes longer than
`--interval` fails as a timeout, so one silent node cannot stall the monitor.

- **Lag** is behind the best node of the round: `lagVersions` from head versions, `lagMs` in chain
  time from head blocks' timestamps (`null` when a node's block cannot be read). The leader reads
  `0`. The two come from separate reads, so they agree closely, not exactly.
- **Requests, errors and `429`s** count every call the monitor made to a node, head, block and
  comparison reads alike. Each node's requests go out once, without the SDK's own retries, and a timed-out one
  is abandoned on Aptos, so a `429` reaches the monitor rather than being waited out. A node that answers `429` to any call, comparison reads included, is set aside
  for its `Retry-After`, or for one interval when it names none, as Aptos fullnode errors never
  do, so at least the next round skips it and reports `error: "429"`.
- **The window** holds the rounds that started less than `--window` ago, at most 10,000.

A JSON line per round, versions as decimal strings:

```json
{"round":3,"time":"2026-11-14T22:00:00.000Z","nodes":[
  {"node":0,"up":true,"error":null,"head":"8053398912","lagVersions":"0","lagMs":0,"compare":null,
   "window":{"rounds":3,"up":3,"requests":12,"errors":0,"rateLimited":0,"errorRate":0,"maxLagVersions":"0","maxLagMs":0}},
  {"node":1,"up":true,"error":null,"head":"8053398907","lagVersions":"5","lagMs":130,
   "compare":{"fromVersion":"8053398808","toVersion":"8053398907","window":{"fromVersion":"8053398808","toVersion":"8053398907"},"matched":61,"missing":0,"extra":0,"different":0},
   "window":{"rounds":3,"up":3,"requests":12,"errors":0,"rateLimited":0,"errorRate":0,"maxLagVersions":"12","maxLagMs":400,"matched":180,"disagreements":0}}]}
```

(one line in practice). `error` is `null` for a node that answered its head read, else
`"timeout"`, the HTTP status (`"429"`, `"503"`) or `"failed"`: never the error's message, which
can carry the node's URL, so nodes are named by position, as `--stats` names them. `compare` is
this round's comparison with node `0`, `null` for node `0` and for a node not compared; its
`window` is the versions both walks covered, `null` when one failed. `window.disagreements` sums
`missing`, `extra` and `different` over the window. To see the events themselves, run
[`streamd compare`](#compare-two-nodes) over the range reported. A node so far behind that node `0`
has pruned its range makes node `0`'s walk fail with `410`, counted as an error on node `0`.

`--format prometheus` prints each round as text exposition, rounds separated by a blank line, and
`--listen` serves the latest at `GET /metrics` whatever `--format` prints. The endpoint has no
authentication; it serves only counts, versions and node positions, but a host other than
`127.0.0.1` makes it reachable by anyone on that network. Gauges labelled
`node`: `streamd_monitor_up`, `streamd_monitor_head_version`, `streamd_monitor_lag_versions`,
`streamd_monitor_lag_seconds`, `streamd_monitor_window_requests`, `streamd_monitor_window_errors`,
`streamd_monitor_window_rate_limited`, and for nodes from `1` up `streamd_monitor_window_matched`
and `streamd_monitor_window_disagreements`, a series left out when its value is unknown; then
`toPrometheus` over each node's lifetime counts (`streamd_adapter_requests_total`
and the rest, by `node` and `method`), from which Prometheus computes its own rates.

`--api-key` goes to node `0` only; `APTOS_API_KEY` is not used, since every node is a URL given.
`--archive`, `--cache` and `--stats` are refused: each would hide or repeat what the monitor
measures. The defaults cost each node about four reads per 10 seconds, and node `0` one more walk
per other node, within the anonymous Aptos fullnode limit for a few nodes. Exit codes: `0` on Ctrl-C, `SIGTERM`, or after `--limit` rounds; `1` when
`--listen` cannot bind or stdout fails; `2` for invalid arguments. A node that is down never ends
the command.

### Serve a gateway

`streamd serve` runs a gateway that many clients follow at once. It polls each descriptor its
clients ask for once, from the head, and streams every event to all of them as Server-Sent
Events or WebSocket messages. Browsers get live events without an API key or a node of their
own, and a hundred tabs on one event type cost the node one walk.

```sh
# loopback only, port 8787
bun x streamd serve --network mainnet

# reachable from elsewhere: a token is required
STREAMD_GATEWAY_TOKEN=... bun x streamd serve --listen 0.0.0.0:8787 --allow-origin https://app.example
```

It needs Bun, for its WebSocket server; under Node it exits 1. It takes the endpoint flags of
the commands that read events (`--network`, `--node-url`, `--api-key`, `--archive`, `--stats`,
`--chain sui`), `--poll-interval` for every topic, and these:

| Flag | Default | |
| --- | --- | --- |
| `--listen [host:]port` | `127.0.0.1:8787` | any host but loopback needs a token |
| `--token` or `STREAMD_GATEWAY_TOKEN` | none | required on every request as `Authorization: Bearer <t>`, or `?access_token=<t>` where a browser cannot set headers (`EventSource`, `WebSocket`); query strings can end up in logs |
| `--allow-origin <origin>` | none | browser origins sent CORS headers and allowed to open WebSockets, repeatable, or `*` |
| `--max-topics <n>` | 32 | descriptors followed at once; a new one past it answers 503 |
| `--max-connections <n>` | 1000 | open SSE and WebSocket streams; past it, 503 |
| `--webhooks <file>` | none | enables the webhook API, keeping webhooks and their cursors in the file |
| `--webhook-allow-private` | off | lets webhook targets be non-public addresses |

Clients name the descriptor as JSON in a `descriptor` query parameter, with `creationNumber`
as a decimal string:

```sh
curl -N "http://127.0.0.1:8787/v1/events?descriptor=%7B%22kind%22%3A%22module%22%2C%22eventType%22%3A%220x1%3A%3Atransaction_fee%3A%3AFeeStatement%22%7D"
# id: 3f2a91c0:0
#
# id: 3f2a91c0:1
# data: {"type":"0x1::transaction_fee::FeeStatement","data":{...},"version":"4205513021",...}
```

```ts
import { parseEvent } from "@streamd/core";

const descriptor = encodeURIComponent(JSON.stringify({ kind: "module", eventType: "0x1::coin::CoinDeposit" }));

const source = new EventSource(`http://127.0.0.1:8787/v1/events?descriptor=${descriptor}`);
source.onmessage = (message) => console.log(parseEvent(message.data));
source.addEventListener("end", (message) => source.close()); // the gateway gave up on this descriptor

const socket = new WebSocket(`ws://127.0.0.1:8787/v1/ws?descriptor=${descriptor}`);
socket.onmessage = (message) => {
  const body = JSON.parse(message.data);
  if (body.end !== undefined) return; // the gateway gave up; a 1011 close follows
  if (body.event === undefined) return; // the opening { id }: where the socket starts
  console.log(body.id, parseEvent(JSON.stringify(body.event)));
};
```

**Resuming.** Each event's id is `<epoch>:<seq>`, numbered by the gateway as its topic receives
events. A stream opens with the id it starts after (an SSE `id:` with no data, or a WebSocket
`{ "id" }` frame), so a client that has seen no event yet can still resume. It starts at the
newest event, or, given `Last-Event-ID` (which `EventSource`
sends by itself on reconnect) or `?lastEventId=` (for WebSockets), right after that id, with no
gap and no repeat. Each topic keeps its last 1000 events, and lingers 60 seconds after its last
client leaves. A resume point the topic no longer holds, or one from before the gateway
restarted, answers 410: start the stream again, and read what was missed by version with a
query.

**Slow clients.** A stream more than 1 MiB behind is dropped (WebSocket code 1013), its queue
discarded, rather than buffered without bound. Its client reconnects and resumes from its last id while the topic still
holds it.

**`GatewayAdapter`.** In `@streamd/gateway/adapter`, an `EventAdapter` over the gateway's
`POST /v1/fetch`, `/v1/head` and `/v1/earliest`, so a `StreamdClient` in the browser reads
through it. That subpath imports only `@streamd/core`; the package root also exports it, beside
`createGateway`, whose Node built-ins a browser bundle cannot load:

```ts
import { StreamdClient } from "@streamd/core";
import { GatewayAdapter } from "@streamd/gateway/adapter";

const streamd = new StreamdClient({ adapter: new GatewayAdapter({ url: "http://127.0.0.1:8787", token }) });
streamd.on(descriptor, (event) => console.log(event));                    // the shared topic
const recent = await streamd.getEvents(descriptor, { fromVersion: -1000n }); // the gateway's node
```

A walk with no start follows the gateway's topic from its newest event, as its streams do,
whatever the source would do with no start. A walk with a start, an end, an offset or
newest-first order reads the gateway's node, identical requests within a poll interval sharing
one read, at most 4 in flight and each abandoned after 30 seconds, and keeps following it on
that node's cursors: backfill-then-follow walks are not shared. Requests must be
`application/json` (415 otherwise), so a page on another origin cannot send one without the
CORS preflight `--allow-origin` governs. With no token, the gateway answers only requests whose
`Host` is loopback, which refuses a page that rebinds its own name to 127.0.0.1. Block lookups, `fromTime`, transaction lookups, simulations and views are not served.
`where` and `decode` run in the client as usual.

`GET /v1/status` lists each topic (descriptor, clients, last id, events held), the open
stream count and the number of registered webhooks.

**Webhooks.** With `--webhooks <file>`, clients register webhooks through the gateway's API, and
the gateway POSTs every new event on each one's descriptor to its URL, as `streamd forward`
does: a JSON array of events per POST, signed in `x-streamd-signature`, retried with backoff
and `Retry-After`, at least once. `@streamd/receiver` or `verifyWebhook` reads them.

```sh
bun x streamd serve --webhooks ./webhooks.json

curl -X POST http://127.0.0.1:8787/v1/webhooks -H "content-type: application/json" \
  -d '{"url":"https://hooks.example/streamd","descriptor":{"kind":"module","eventType":"0x1::coin::CoinDeposit"}}'
# {"id":"9c1e0b4f7a2d3e65","url":"https://hooks.example/streamd","descriptor":{...},"status":"active",
#  "createdAt":"2026-11-19T12:00:00.000Z","delivered":0,"fromVersion":"4205513021","secret":"<64 hex characters>"}
```

| Route | |
| --- | --- |
| `POST /v1/webhooks` | `{ url, descriptor }`; 201 with the webhook and its `secret`, the only response that carries it |
| `GET /v1/webhooks` | `{ webhooks: [...] }`, each with `status` (`active` or `failed`), `cursor`, `delivered` (events acknowledged, redeliveries included) and `lastError` |
| `GET /v1/webhooks/<id>` | one webhook |
| `DELETE /v1/webhooks/<id>` | 204; it stops at once, abandoning a POST in flight |
| `POST /v1/webhooks/<id>/resume` | restarts a failed webhook from its cursor |

The routes take the gateway's token like every other, and exist only with `--webhooks`.
Without a token they refuse any request carrying an `Origin` header (403): a loopback gateway
is reachable from every page its operator opens, so browsers are kept off the webhook API.

- **Start and cursor.** A webhook starts at the head the gateway's node reports when it is
  registered (`fromVersion`), and follows with a cursor of its own. The file holds every webhook, its secret and its cursor (mode 0600, replaced
  whole on each write), and its cursor moves only after the target acknowledged the page
  with a 2xx, so a restarted gateway carries on from there and may repeat a page, never skip
  one. A file that cannot be read stops `serve` with exit code 1.
- **Delivery.** `--retries` (default 5) per batch, `--timeout` (default 10000 ms) per POST,
  `--batch-size` events per POST (default: each page). A 408, 429 or 5xx is retried, any other
  status is not, and redirects are never followed.
- **A target that keeps failing.** Once a batch fails past its retries, or polling gives up,
  the webhook is marked `failed` with the reason in `lastError`, stops, and keeps its cursor.
  `resume` delivers from that page on. A failed webhook stays failed across restarts.
- **Targets.** `http` or `https`, no credentials in the URL. The gateway refuses a host that
  is, or resolves to, a loopback, private, link-local, shared, multicast or reserved address,
  checked at registration and again before every POST, unless `--webhook-allow-private` is
  given (for a receiver on the same network). The address is not pinned for the request, so a
  name that resolves to a public address at the check and a private one at the connect is not
  stopped: give a gateway that registers untrusted URLs no route to what it must not reach.
- **Limits.** At most 100 webhooks. Each polls on its own: webhooks on one descriptor share a
  node read only while their cursors are equal, so many on a busy type cost the node one walk
  each.

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
- `--account <address> --resource <type>` stores each change with that address, so
  `StoreAdapter` and `PostgresStoreAdapter` serve the `resource` descriptor back.
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

The generator is `generateEventTypes` in `@streamd/codegen`, which takes a function reading a
module's ABI, so it runs anywhere, the [Playground](#playground) included.

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

`--arbitraries` also writes an `arbitrary<Type>(rng)` generator for every generated type,
returning a random value in the JSON form the node reports, with no dependencies. Use them to
fill a `MemoryAdapter` with thousands of valid events for a property test:

```ts
import { MemoryAdapter, StreamdClient } from "@streamd/core";
import { arbitraryFungibleAsset_Deposit, FungibleAsset_Deposit, streamdEvent, streamdRng } from "./events.js";

const rng = streamdRng(42); // the same seed gives the same events on every run
const events = Array.from({ length: 1000 }, (_, i) =>
  streamdEvent(FungibleAsset_Deposit, arbitraryFungibleAsset_Deposit(rng), BigInt(i + 1)),
);
const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
const deposits = await client.getEvents(FungibleAsset_Deposit, { fromVersion: 0n });
```

- **`streamdRng(seed)`** is a seeded mulberry32, a `StreamdRng`: a function returning a uint32
  per call. Any function with that contract works in its place.
- **A generic type's generator takes one generator per type parameter** after `rng`, each
  `(rng) => T`, so `arbitraryCoin_Deposit(rng, myGenerator)` or another `arbitrary<Type>`
  fills its `T0` fields. A phantom parameter still takes one, so the arity matches the type.
- **`streamdEvent(descriptor, data, version, eventIndex = 0)`** builds a `StreamdEvent` of the
  descriptor's one event type, `sequenceNumber` `0n`; a descriptor over several types throws.
  Give each event a distinct `(version, eventIndex)`, since `MemoryAdapter` keys them on it.
- **Values follow the table below:** integers in range as numbers or decimal strings,
  addresses in lowercase hex with leading zeros dropped (`0x1`, and 63 digits one time in
  sixteen, as the fullnode writes them), `vector<u8>` as `0x` hex, `Option` as `{ vec: [] }` or
  `{ vec: [x] }`, enums one variant chosen uniformly. With `--chain sui`, addresses and object
  ids are long form, bytes base64 and options `null` or the value.
- **Edge values come up often:** a quarter of integers are their minimum, maximum, `0` or `1`,
  and the rest vary in magnitude; a quarter of vectors, byte vectors and strings are empty, and
  half of options are none. Strings include multi-byte characters.
- **Sizes are bounded:** vectors hold 0 to 4 elements and byte vectors 0 to 32 bytes. Move
  allows no recursive types, so every value is finite.
- Every generated value passes the `--validators` check of its type. A struct named
  `StreamdRng`, `StreamdEvent` or `ModuleEventDescriptor` is refused, since the generated file
  uses those names.

`--sql sqlite` or `--sql postgres` also writes `streamdTables`, one table per event type with a
column per field, and `streamdSchema`, that dialect's `CREATE TABLE IF NOT EXISTS` statements,
one per string, for a migration tool. `storeTables` in [`@streamd/store-sqlite`](#typed-event-tables)
or [`@streamd/store-postgres`](#store-events-in-postgres) creates the tables itself and fills
them from the chain:

```sh
streamd codegen --sql sqlite --out src/events.ts 0x1::fungible_asset
```

```ts
import { Database } from "bun:sqlite";
import { SqliteEventStore, storeTables } from "@streamd/store-sqlite";
import { streamdTables } from "./events.js";

storeTables(streamd, new SqliteEventStore(new Database("events.db")), streamdTables, { name: "fa", fromVersion: -10_000n });
// SELECT store, amount FROM fungible_asset_deposit WHERE sender = '0x...' ORDER BY version DESC
```

- **A table is named `<module>_<struct>` in snake case** (`fungible_asset_deposit`,
  `pool_v3_swap_event_v3`). A name two generated structs share (one module and struct at two
  addresses, such as two DEXes' `pool::SwapEvent`) or one over Postgres's 63 bytes is cut and
  suffixed with 8 hex digits of a SHA-256 of the long-form type, so adding a struct that shares
  a table's name renames both: generate such modules together from the start, or rename the
  table yourself. A name starting `streamd_`, `sqlite_` or `pg_` gets `move_` in front.
- **Columns are the field names, lowercased,** after the standard ones: `version`,
  `event_index`, `transaction_hash` (hex lowercased), `timestamp` (microseconds), `sender` (long
  form), and for a generic event `type_arguments`, the text inside its type's outer `<...>`. A
  field that would collide with one of them, with an earlier field or with a Postgres system
  column (`xmin`, `ctid` and the like) gets `_` appended (`timestamp_`), or, at 63 bytes, a hash. The key is `(version, event_index)`, and a redelivered event replaces its row.
- **Nothing is lossy.** Integers wider than 32 bits are exact decimal text on SQLite, so compare
  them as numbers with care (`CAST` reads past 2^63 as a float), and `numeric` on Postgres.
  Postgres `text` and `jsonb` cannot hold NUL or a lone surrogate, which become U+FFFD there.
- **Nested values are JSON,** one column per field rather than flattened, so names stay stable.
  An enum event has `variant` and `data`, the whole payload.
- Every name is checked (lowercase letters, digits and `_`, at most 63 bytes) and quoted, and
  every value is a bound parameter. A struct named `StreamdEventTable` is refused, since the
  file imports that name.

| Move | Column | SQLite | Postgres |
| --- | --- | --- | --- |
| `u8`, `u16`, `i8`, `i16`, `i32` | the integer | `INTEGER` | `integer` |
| `u32` | the integer | `INTEGER` | `bigint` |
| `u64`, `u128`, `u256`, `i64`, `i128`, `i256` | the integer | `TEXT` (decimal) | `numeric(20\|39\|78, 0)`, `bigint` for `i64` |
| `bool` | | `INTEGER` (0, 1) | `boolean` |
| `address`, `Object<T>`, Sui `ID` and `UID` | long-form address | `TEXT` | `text` |
| `String`, `vector<u8>` as the node writes it, Sui `ascii::String`, `Url`, `TypeName` | the text | `TEXT` | `text` |
| Sui `Balance<T>` | as `u64` | | |
| `Option<T>` | `T`'s column, `NULL` when none | | |
| other structs, `vector<T>`, type parameters | JSON, bigints as strings | `TEXT` | `jsonb` |

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
| `--validators`, `--arbitraries` | also write runtime checks, random payload generators |
| `--sql <sqlite\|postgres>` | also write a table per event type and its DDL |
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
- `@streamd/sui` and `@mysten/sui` are optional peers of `@streamd/cli` and are imported only for `--chain sui`.

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

### Event coverage for Move tests

`streamd coverage` tells you which `#[event]` structs your tests never emit. It starts an Aptos
localnet with the installed `aptos` CLI (through [`@streamd/testing`](#test-against-a-localnet)),
publishes the Move package from a funded account, runs your test command against that localnet,
then prints one JSON line per event struct the package declares:

```sh
streamd coverage ./move/counter -- bun test tests/move
# {"event":"0x396c...::counter::Bumped","emitted":true,"count":3,"first":{"version":"33","eventIndex":0,"transactionHash":"0x77e2...","function":"0x396c...::counter::bump","sender":"0x396c..."}}
# {"event":"0x396c...::counter::Reset","emitted":false,"count":0}
```

- **The test command** is everything after `--`, run without a shell, with its stdout sent to
  stderr so it cannot mix with the report. It finds the localnet in its environment:
  `STREAMD_NODE_URL`, `STREAMD_FAUCET_URL`, `STREAMD_PACKAGE_ADDRESS` (long form) and
  `STREAMD_PUBLISHER_KEY`, the publisher's Ed25519 private key in AIP-80 form
  (`ed25519-priv-0x...`, which `new Ed25519PrivateKey(...)` reads), so it can call entry
  functions only the publisher may. A TypeScript test can pass the first two to `attachLocalnet`. Move unit tests
  (`aptos move test`) run with no node, so their events cannot be counted; drive the package
  through transactions instead.
- **Publishing.** Every named address the package's `Move.toml` leaves as `"_"` in `[addresses]`
  is set to the publisher's address. Events `init_module` emits count, with
  `0x1::code::publish_package_txn` as their call.
- **What counts.** The `#[event]` structs of every module at the package address, read from the
  node's ABIs after the command exits; events of a generic struct count under the struct, whatever
  their type arguments. Events are read with one `getEvents` from the publish to the head.
- **Each line** has `event`, `emitted`, `count` and, when emitted, `first`: the version, event
  index, transaction hash, entry function (`null` for a script) and sender of the earliest event.
  Lines are sorted by type. A summary such as `streamd: 3 of 4 event structs emitted` goes to
  stderr. `--format pretty` prints one plain line per struct instead.
- **Exit codes.** 0 when the command passed, 1 when it failed (the report still prints) or when the
  localnet, compile or publish failed, 2 for bad usage. The summary reports a command killed by a
  signal as exiting `128 +` its number, as a shell does.
- **Interrupts.** On the first SIGINT, SIGTERM or SIGHUP the run waits for the command (Ctrl-C
  reaches it through the terminal; SIGTERM and SIGHUP are passed on), stops the localnet and exits
  130 with no report, since Ctrl-C reaches the node too. A signal before the command starts skips
  it. A second signal kills the command, or exits at once when none is running. Only SIGKILL
  leaves the node running.
- **Aptos only**, and one package per run. `bun run test:localnet` in `packages/cli` runs it for real
  on a fixture package; the normal test run skips it.

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
