# streamd

EVM-style event listening and querying for Aptos MoveVM chains.

If you know `contract.on(...)` and `eth_getLogs` from ethers, you already know
streamd: subscribe to events as they land, or query them historically, with
cursors, deduplication, backoff and client-side filtering handled for you.

```ts
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "streamd";

const streamd = new StreamdClient({
  aptos: new Aptos(new AptosConfig({ network: Network.MAINNET })),
});

streamd.on(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  (event) => console.log(event.version, event.data),
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
    pollInterval: 3000, // ms between polls (default 1000)
    batchSize: 25,      // events fetched per poll (default 25)
    maxErrors: 10,      // consecutive failures before stopping (default 10)
    onError: (err) => console.error(err),
  },
);

sub.unsubscribe();
```

Handlers may be `async`: each one is awaited before the next event, and polling waits
for the page to finish, so a slow handler slows the subscription down instead of piling
up work. A rejected handler is reported through `onError` and the next event is still
delivered. A handler that never settles stops delivery with nothing reported, so give
long-running work its own timeout; `unsubscribe()` still works.

Listening polls under the hood. When the source reports more data is waiting,
the poller fetches again immediately instead of waiting for the next interval.
On errors it backs off exponentially, and stops after `maxErrors` failures in
a row. Invalid input, such as a malformed address or cursor, stops it at once,
since retrying cannot fix it.

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
invalid input); other errors go to `onError`. One loop per stream.

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

### Wait for one event

```ts
const event = await streamd.once({
  kind: "module",
  eventType: "0x1::transaction_fee::FeeStatement",
});
```

The promise rejects if polling gives up after `maxErrors`, or straight away on
invalid input.

### Query history

```ts
const logs = await streamd.getLogs(
  {
    kind: "account",
    accountAddress: "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d",
  },
  { fromVersion: 1_000_000n, limit: 50 },
);

// Or page through large result sets
for await (const page of streamd.getLogsIterator(descriptor, { limit: 1000 }, 100)) {
  handle(page);
}
```

### Clean up

```ts
streamd.destroy(); // stops every active subscription
```

## Choosing events

Every call takes an event descriptor:

| `kind` | Matches | Example |
|---|---|---|
| `module` | Module events of one Move type | `{ kind: "module", eventType: "0x1::coin::WithdrawEvent" }` |
| `handle` | A legacy `EventHandle` on an account | `{ kind: "handle", accountAddress, eventHandleStruct, fieldName }` |
| `account` | Events from transactions an account **sent** | `{ kind: "account", accountAddress, eventTypes?: [...] }` |

`account` only sees transactions the account signed. If someone else's
transaction deposits into the account, that event belongs to the sender's
transaction and won't appear.

## Filtering

Chain APIs can't filter on what's inside an event. `where` filters on your
side, after fetching:

```ts
import { involvesAddress, ofType, every, some } from "streamd";

streamd.on(
  { kind: "module", eventType: "0x1::fungible_asset::Deposit" },
  (event) => console.log(event),
  { where: involvesAddress(myWallet) },
);
```

- `involvesAddress(address, { fields?, maxDepth? })` matches events whose
  payload mentions the address anywhere. `0x1` and `0x000…01` count as the
  same address.
- `ofType(...types)` matches exact event types.
- `every(...predicates)` and `some(...predicates)` combine filters.
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

An adapter has one method, `fetchEvents(descriptor, options)`, returning
`{ events, nextCursor, hasMore }`. See the `EventAdapter` type for the full
contract.

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
