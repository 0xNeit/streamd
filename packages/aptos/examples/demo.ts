import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient, involvesAddress } from "@streamd/core";
import { FullnodeScanAdapter } from "../src/index.js";

const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));

// Defaults to FullnodeScanAdapter, which finds module events by scanning
// transactions off a fullnode. That suits tailing the head, as below. It does
// not suit deep historical queries: fullnodes prune, and a rare event type
// costs a scan of every transaction in between. Pass `adapter` to read from
// your own index instead.
const streamd = new StreamdClient({ adapter: new FullnodeScanAdapter(aptos) });

// --- Historical query (getEvents) ---
console.log("Fetching recent FeeStatement events...\n");

interface FeeStatement {
  total_charge_gas_units: string;
  execution_gas_units: string;
  io_gas_units: string;
  storage_fee_octas: string;
  storage_fee_refund_octas: string;
}

const fees = await streamd.getEvents<FeeStatement>(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  { limit: 5 },
);

for (const event of fees) {
  console.log(
    `  v${event.version} — gas: ${event.data.total_charge_gas_units} units, storage fee: ${event.data.storage_fee_octas} octas`,
  );
}

console.log(`\nFetched ${fees.length} events.\n`);

// --- Events for one account ---
// Walks that account's own transaction history instead of scanning the chain,
// so it reaches deep history cheaply. Covers transactions the account *sent*;
// events where it is only a recipient live in the sender's transaction.
const account =
  "0xd503b95164384a5ebbccbb5c4bdc8b4a5893d9651e9953abda8e1c22fcc1181d";

console.log(`Fetching earliest events for ${account.slice(0, 10)}...\n`);

const accountEvents = await streamd.getEvents(
  { kind: "account", accountAddress: account },
  { offset: 0, limit: 5 },
);

for (const event of accountEvents) {
  console.log(`  v${event.version} — ${event.type}`);
}

// `where` filters on payload contents, which no chain API can do for you.
// Note it is the wrong tool for the account itself: an account's address
// lives in the transaction's `sender`, not inside the events it emits — so
// involvesAddress(account) here would match nothing. It earns its keep on
// addresses that *are* in the payload, and on module queries where the type
// alone is too broad ("all Deposit events, but only mine").
const market = "0x5e0e16f34adfb4b316f8d532d68acbfa206826feaaa418d3938046bdc2044861";

const onMarket = await streamd.getEvents(
  { kind: "account", accountAddress: account },
  { offset: 0, limit: 5, maxPages: 2, where: involvesAddress(market) },
);

console.log(
  `\n${onMarket.length} of those reference market ${market.slice(0, 10)}… in their payload.\n`,
);

// --- Real-time listening (on) ---
console.log("Listening for new FeeStatement events (polling every 3s)...\n");

const sub = streamd.on<FeeStatement>(
  { kind: "module", eventType: "0x1::transaction_fee::FeeStatement" },
  (event) => {
    console.log(
      `  [live] v${event.version} — gas: ${event.data.total_charge_gas_units} units`,
    );
  },
  {
    pollInterval: 3000,
    batchSize: 5,
    onError: (err) => console.error("  [error]", err.message),
  },
);

console.log(`Subscription ${sub.id} active. Press Ctrl+C to stop.\n`);

process.on("SIGINT", () => {
  console.log("\nStopping...");
  streamd.destroy();
  process.exit(0);
});
