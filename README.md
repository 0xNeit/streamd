# streamd

Event subscriptions and historical queries for blockchains, starting with Aptos.

Subscribe to events as they land, or query them historically, with cursors,
deduplication, backoff and client-side filtering handled for you.

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "@streamd/core";

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

The repository is a Turborepo monorepo on Bun workspaces with two packages:

- `@streamd/core` (`packages/core`) - the library. `@aptos-labs/ts-sdk` (>= 1.0.0) is a peer
  dependency, and it ships ESM, CommonJS and TypeScript types.
- `@streamd/cli` (`packages/cli`) - the `streamd` executable, built on `@streamd/core`.

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
failure. Invalid
input, such as a malformed address or cursor, stops it at once, since retrying
cannot fix it. Pass `onError` to `new StreamdClient({ aptos, onError })` to set
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
  aptos,
  defaults: { pollInterval: 3000, maxErrors: 3, decode: ["amount"] },
});

streamd.on(deposits, handle);                           // polls every 3000 ms
streamd.on(withdrawals, handle, { pollInterval: 500 }); // its own interval, still maxErrors 3
```

`defaults` sets `pollInterval`, `batchSize`, `maxErrors`, `maxBackoff`, `dedupWindow`
and `decode` for every `on()`, `onBatch()`, `once()` and `stream()` call on the client.
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

A throwing reducer stops the reduction, since folding past a lost event would leave the
value wrong: the error goes to `onError` (or the client's), `ready` rejects if the backfill
was still running, and `value` keeps the last good state. A failed backfill does the same.
Live polling errors behave as they do for `on()`, and `active` turns `false` once polling
gives up. `signal`, `unsubscribe()` and `destroy()` stop it at any stage without calling
`onError`. `ready` never surfaces as an unhandled rejection, so ignoring it is safe. Like
`once()` and `stream()`, a reduction is not counted by `listenerCount()`.

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
throw from `where`, which would otherwise skip every event. A throwing `onCaughtUp`,
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
its `maxConcurrentRequests` slot until then. The hash must be
`0x` and 64 hex digits. It needs an adapter with `getTransactionEvents`, which
`FullnodeScanAdapter` and `MemoryAdapter` have and the wrapping adapters pass through.

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
skipped. Only single-signer transactions are accepted, a fee-payer one included.

A simulation that fails rejects with `StreamdTransactionError`, whose `vmStatus` says why
and whose `transactionHash` is undefined. On `FullnodeScanAdapter` a failed request
rejects with a `StreamdNetworkError` that carries neither the transaction nor the SDK
error holding it. It needs an adapter with `simulateTransactionEvents`, which
`FullnodeScanAdapter` has. The wrapping adapters pass it through, never cached or recorded,
since a simulation answers for the chain's state at that moment.

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

### Limit concurrent requests

```ts
const streamd = new StreamdClient({ aptos, maxConcurrentRequests: 4 });
```

At most that many adapter calls run at once across every subscription and query on the
client; the rest wait their turn in order. `fetchEvents`, the `getHeadVersion` read behind
a negative `fromVersion`, the `getEarliestVersion` read behind `"earliest"`, and the
`getBlock` and `getBlockHeights` lookups behind `fromBlock`/`toBlock` and `fromTime`, and
the lookups behind `eventsOf()` and the simulations behind `previewEvents()` all count. The unit is one adapter call, not one HTTP request:
a module-event scan on `FullnodeScanAdapter` may make several requests per call. A call
still waiting when its subscription stops or pauses, or its query's `signal` aborts, leaves
the queue at once without reaching the adapter, and `destroy()` rejects every call still
waiting. A call the adapter never settles holds its slot, so give a custom adapter its own
timeouts. Omit the option for no limit.

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

## Choosing events

Every call takes an event descriptor:

| `kind` | Matches | Example |
|---|---|---|
| `module` | Module events of one or more Move types | `{ kind: "module", eventType: "0x1::coin::WithdrawEvent" }` or `{ kind: "module", eventType: ["0x1::fungible_asset::Withdraw", "0x1::fungible_asset::Deposit"] }` |
| `handle` | A legacy `EventHandle` on an account | `{ kind: "handle", accountAddress, eventHandleStruct, fieldName }` or, by its guid, `{ kind: "handle", accountAddress, creationNumber: 3n }` |
| `account` | Events from transactions an account **sent** | `{ kind: "account", accountAddress, eventTypes?: [...] }` |

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

Each delivered `StreamdEvent` carries `type`, `data`, `version` (`bigint`),
`sequenceNumber` (`bigint`), and `eventIndex` (`number`). Where the source
transaction is in hand (`module` and `account` descriptors on
`FullnodeScanAdapter`), events also carry `transactionHash` (`string`),
`timestamp` (`bigint`, microseconds since the Unix epoch), and `sender`
(`string`, the address that signed the transaction). `handle` events come from
an endpoint that omits the surrounding transaction, so all three are
`undefined` there. `timestamp` is `undefined` on genesis transactions, and
`sender` on system transactions such as block metadata, which no account signs.

Events emitted through a legacy `EventHandle` also carry `guid`
(`{ creationNumber: bigint, accountAddress: string }`), on every descriptor kind.
Module events have no handle, so `guid` is `undefined` on them.

### Discover event handles

`listEventHandles(aptos, address)` reads an account's resources and returns
each `EventHandle` it finds as a ready-to-use `handle` descriptor, with the
number of events emitted on it so far and its guid:

```ts
import { Aptos } from "@aptos-labs/ts-sdk";
import { listEventHandles } from "@streamd/core";

const handles = await listEventHandles(new Aptos(), "0x1");
// [{ descriptor: { kind: "handle", accountAddress, eventHandleStruct: "0x1::block::BlockResource", fieldName: "new_block_events" }, count: 1165098930n, guid: { creationNumber: 3n, accountAddress: "0x1" } }, ...]

const blocks = handles.find((h) => h.descriptor.fieldName === "new_block_events");
if (blocks) client.on(blocks.descriptor, (event) => console.log(event.data));
```

It takes an `Aptos` instance rather than living on the client, because a
client may run on any adapter. Only handles stored directly on a resource field
are listed, since the struct and field form cannot address one nested inside a
struct field. A nested handle is still readable by creation number, taken from
the `guid` in the resource data.

### Check a module descriptor

A misspelled event type is not an error to the module scan: it just never
matches. `checkDescriptor(aptos, descriptor)` reads the module ABI for each
type in a `module` descriptor and says whether the struct is there:

```ts
import { Aptos } from "@aptos-labs/ts-sdk";
import { checkDescriptor } from "@streamd/core";

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
request per distinct module, accepts `module` descriptors only, and throws
`StreamdError` for malformed input before any request, or
`StreamdNetworkError` when the node fails.

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
top-level fields named in `decode` are turned back into `bigint`, by the same
rule as the `decode` option; deeper ones stay strings. Malformed input throws
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
positions.

## Filtering

Chain APIs can't filter on what's inside an event. `where` filters on your
side, after fetching:

```ts
import { involvesAddress, whereField, ofType, ofModule, fromSender, every, some, not } from "@streamd/core";

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
`fromBlock`/`toBlock` need, an optional `getBlockHeights(descriptor)` that `fromTime`
needs alongside `getBlock`, and an optional `getTransactionEvents(hash, descriptor?)` that
`eventsOf()` needs, returning `{ version, success, vmStatus, events }` or `undefined` while
the transaction is not committed, and an optional
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

Combine sources with `RoutingAdapter` (by descriptor `kind`, with an optional
`default` for the rest) and `FallbackAdapter` (try sources in order on each
call when one fails, while still rejecting invalid input at once):

```ts
import {
  FallbackAdapter,
  FullnodeScanAdapter,
  RoutingAdapter,
  StreamdClient,
} from "@streamd/core";

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
import { FullnodeScanAdapter, StreamdClient, withHooks } from "@streamd/core";

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
import { CachingAdapter, FullnodeScanAdapter, StreamdClient } from "@streamd/core";

const streamd = new StreamdClient({
  adapter: new CachingAdapter(new FullnodeScanAdapter(aptos), { maxEntries: 500 }),
});
```

A cached page keeps being served after the fullnode prunes that history, where
the node itself would answer 410. A custom adapter opts in by setting `final`
on pages that cannot change; leave it unset when unsure. `RoutingAdapter` and
`withHooks` pass the flag through, `FallbackAdapter` keeps it only on pages
from its first source, and `MemoryAdapter` never sets it.

To capture a real session once and replay it offline, wrap an adapter with
`recordingAdapter`. Every call that succeeds for one of the three descriptor
kinds is kept, `fetchEvents` and the optional `getHeadVersion`,
`getEarliestVersion`, `getBlock`, `getBlockHeights` and `getTransactionEvents` (by
hash), and `JSON.stringify(recorder)` gives the recording.
`replayAdapter` takes that object or its JSON text and answers the same
requests with no network:

```ts
import { readFile, writeFile } from "node:fs/promises";
import { FullnodeScanAdapter, StreamdClient, recordingAdapter, replayAdapter } from "@streamd/core";

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
(as when piped into `head`); `1` when the subscription stopped on an error, or output could not
be written; `2` for invalid arguments; `130` on a second Ctrl-C.

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

`--network`, `--node-url`, `--from-version` and `--poll-interval` work as for `tail`.

Delivery is at least once. The next page is not fetched until the webhook answers, and the
cursor file is written (to a temporary file, then renamed over it) only after a `2xx`, so a
restart resumes after the last acknowledged batch and may resend the one in flight. An
existing cursor file wins over `--from-version`, and only works with the descriptor and
node that wrote it.

Network errors, timeouts, `408`, `429` and `5xx` are retried with exponential backoff from
1s, capped at 60s; a `Retry-After` on `429` or `503` replaces the backoff for that wait.
Any other status, redirects included, fails at once. The webhook receives only the body,
the content type and your `--header` values, and neither the URL nor header values are
printed.

Exit codes: `0` on Ctrl-C or `SIGTERM`, which abandons an in-flight POST without moving the
cursor; `1` when a batch fails past its retries, the subscription stops on an error, or the
cursor file cannot be read or written; `2` for invalid arguments. Run it under a supervisor
that restarts on `1`.

## Development

```sh
bun install
bun run test               # unit tests in every package, through turbo
bun run typecheck
bun run build              # each package into its own dist/
bun run demo               # live demo against mainnet
bun run test:integration   # @streamd/core integration tests, hits mainnet
```

## License

[MIT](LICENSE)
