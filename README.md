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
```

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
`once()`, `stream()`, `getEvents()` or `getEventPages()` stops polling or
paging and rejects with `signal.reason`. An already-aborted signal never
fetches.

### Limit concurrent requests

```ts
const streamd = new StreamdClient({ aptos, maxConcurrentRequests: 4 });
```

At most that many adapter calls run at once across every subscription and query on the
client; the rest wait their turn in order. Both `fetchEvents` and the `getHeadVersion` read
behind a negative `fromVersion` count. The unit is one adapter call, not one HTTP request:
a module-event scan on `FullnodeScanAdapter` may make several requests per call. A call
still waiting when its subscription stops or pauses, or its query's `signal` aborts, is
dropped without reaching the adapter. Omit the option for no limit.

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
`FullnodeScanAdapter`), events also carry `transactionHash` (`string`) and
`timestamp` (`bigint`, microseconds since the Unix epoch). `handle` events
come from an endpoint that omits the surrounding transaction, so both fields
are `undefined` there (and `timestamp` is `undefined` on genesis transactions).

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
values still arrive as strings. At runtime the descriptor is the same plain
object you would write by hand.

## Filtering

Chain APIs can't filter on what's inside an event. `where` filters on your
side, after fetching:

```ts
import { involvesAddress, whereField, ofType, every, some, not } from "streamd";

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
`{ events, nextCursor, hasMore }`, and an optional `getHeadVersion(descriptor)` that
a negative `fromVersion` needs. See the `EventAdapter` type for the full contract.

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
the source has them, `transactionHash` and `timestamp`. Every `bigint`, including any inside
`data`, is printed as a decimal string:

```json
{"type":"0x1::transaction_fee::FeeStatement","data":{"total_charge_gas_units":"7"},"version":"2817350001","sequenceNumber":"0","eventIndex":3,"transactionHash":"0x5c...","timestamp":"1730000000000000"}
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
