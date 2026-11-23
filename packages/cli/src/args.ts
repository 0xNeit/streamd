import { parseArgs } from "node:util";
import type { ModuleRef } from "@streamd/codegen";
import { WEBHOOK_SIGNATURE_HEADER, normalizeAddress, requireAddress, requireEventType, requireResourceType } from "@streamd/core";
import type { EventDescriptor, ModuleEventDescriptor, MoveEventType } from "@streamd/core";
import { FILTER_OPTIONS, parseDecode, parseFilter } from "./filter.js";
import { parseKafkaUrl } from "./kafka.js";
import type { CliFilter, FilterFlags } from "./filter.js";

export type Network = "mainnet" | "testnet" | "devnet" | "movement-mainnet" | "movement-testnet";

/** Which chain and node a command reads. */
export interface Endpoint {
  /** Unset for Aptos. */
  chain?: "sui";
  /** With `chain: "sui"`, only mainnet, testnet or devnet. */
  network: Network;
  /** A fullnode URL, or a gRPC URL for Sui; the first `--node-url`. */
  nodeUrl?: string;
  /** The other `--node-url`s, pooled with `nodeUrl`; never empty when set. */
  extraNodeUrls?: string[];
  /** `--archive`: an archive fullnode URL, or `"default"` for Aptos mainnet's; never set for Sui. */
  archive?: string;
  /** Sent to the fullnode as the SDK's API key; never printed, and never set for Sui. */
  apiKey?: string;
  /** `--stats`: print request counts per source to stderr; only on commands that read through the adapter. */
  stats?: true;
}

/** Where events come from: shared by `tail` and `forward`. */
export interface SourceOptions extends Endpoint {
  descriptor: EventDescriptor;
  fromVersion?: bigint;
  /** Microseconds since the epoch; never set with `fromVersion`. */
  fromTime?: bigint;
  pollInterval?: number;
  /** Client-side narrowing by `--sender`, `--function` and `--where`. */
  filter?: CliFilter;
  /** `--decode` paths, applied before `filter`; not part of `store`'s cursor name. */
  decode?: string[];
}

export interface TailOptions extends SourceOptions {
  limit?: number;
  cursorFile?: string;
  format?: "json" | "pretty";
}

export const SIGNATURE_HEADER = WEBHOOK_SIGNATURE_HEADER;
export const SECRET_ENV = "STREAMD_WEBHOOK_SECRET";
export const TOKEN_ENV = "STREAMD_GATEWAY_TOKEN";
export const DEFAULT_SERVE_PORT = 8787;
export const API_KEY_ENV = "APTOS_API_KEY";

export interface ForwardOptions extends SourceOptions {
  /** Webhook URL, or a `redis://`, `rediss://`, `nats://`, `kafka://` or `kafkas://` URL; may carry a token or password, so it is never printed. */
  to: string;
  /** The Redis stream each batch is added to; set exactly when `to` is a Redis URL. */
  stream?: string;
  /** The NATS subject each batch is published to; set exactly when `to` is a NATS URL. */
  subject?: string;
  /** The Kafka topic each batch is produced to; set exactly when `to` is a Kafka URL. */
  topic?: string;
  cursorFile: string;
  /** Retries per batch after the first attempt. */
  retries: number;
  /** Per-request timeout in ms. */
  timeout: number;
  /** Static request headers; values are never printed. */
  headers: Record<string, string>;
  /** Signs each body with HMAC-SHA256; never printed. */
  secret?: string;
  /** Last version to deliver; the run exits 0 once past it. */
  toVersion?: bigint;
  /** Most events per POST. */
  batchSize?: number;
}

export interface AlertOptions extends Endpoint {
  /** The rules file; read and validated before any read from the chain. */
  rules: string;
  /** Webhook URL for every rule; may carry a token, so it is never printed. */
  to: string;
  /** Each rule's cursor and open window, as one JSON object. */
  cursorFile: string;
  retries: number;
  timeout: number;
  headers: Record<string, string>;
  secret?: string;
  /** Where a rule with no saved entry starts; never both. */
  fromVersion?: bigint;
  fromTime?: bigint;
  pollInterval?: number;
}

export interface StoreOptions extends SourceOptions {
  /** SQLite file to write; the parser sets exactly one of this and `postgres`. */
  db?: string;
  /** Postgres URL to write to; may carry a password, so never printed as given. */
  postgres?: string;
  /** Name its cursor is saved under; derived from the network and descriptor when unset. */
  name?: string;
}

export interface TxOptions extends Endpoint {
  /** A transaction hash, or a base58 digest for Sui. */
  hash: string;
  /** How long to wait for the transaction to commit, in ms. */
  timeout?: number;
}

/** Aptos only; named apart from the library's `ExplainOptions`. */
export interface ExplainCommandOptions extends Endpoint {
  hash: string;
  /** Print the explanation as one JSON line instead of text. */
  json?: true;
  /** How long to wait for the transaction to commit, in ms. */
  timeout?: number;
}

export interface QueryOptions extends Endpoint {
  descriptor: EventDescriptor;
  /** Exactly one of `fromVersion` and `fromTime` is set. */
  fromVersion?: bigint;
  fromTime?: bigint;
  toVersion: bigint | "head";
  /** Counted after `filter`. */
  limit?: number;
  order: "asc" | "desc";
  filter?: CliFilter;
  decode?: string[];
  /** `--cache`: a SQLite file of final pages; never set with `chain: "sui"`. */
  cache?: string;
}

/** A record command's walk: bounded when `toVersion` is set, otherwise followed. */
export interface RecordWalkOptions extends Endpoint {
  fromVersion?: bigint;
  fromTime?: bigint;
  toVersion?: bigint | "head";
  limit?: number;
  /** Bounded only. */
  order?: "asc" | "desc";
  /** Follow only, as are `batchSize` and `cursorFile`. */
  pollInterval?: number;
  batchSize?: number;
  cursorFile?: string;
}

/** Bounded through `iterateTransfers` or `getTransfers`; followed through `transfers`. */
export interface TransfersOptions extends RecordWalkOptions {
  /** An asset as the chain's client takes it; validated there. */
  asset?: string;
  /** An address or name; validated and resolved by the chain's client. */
  account?: string;
  withAssetInfo?: true;
}

/** Aptos only; names and the asset are validated by the client. */
export interface SwapsCommandOptions extends RecordWalkOptions {
  /** Non-empty when set. */
  protocol?: string[];
  asset?: string;
}

/** Aptos only; names and the token are validated by the client. */
export interface SalesCommandOptions extends RecordWalkOptions {
  /** Non-empty when set. */
  marketplace?: string[];
  token?: string;
}

/** Aptos only, read over the Indexer route; the address or name is validated by the client. */
export interface ActivityCommandOptions extends RecordWalkOptions {
  address: string;
  withAssetInfo?: true;
  /** `--indexer`: the Indexer's GraphQL URL (default: the network's). */
  indexer?: string;
}

/** Aptos only, read over the Indexer route; addresses, names and the asset are validated by the client. */
export interface TraceCommandOptions extends Endpoint {
  address: string;
  /** `--indexer`: the Indexer's GraphQL URL (default: the network's). */
  indexer?: string;
  format: "json" | "dot";
  hops?: number;
  direction?: "out" | "in" | "both";
  fromVersion?: bigint;
  toVersion?: bigint | "head";
  minAmount?: bigint;
  asset?: string;
  maxNodes?: number;
  maxRequests?: number;
  /** Non-empty when set. */
  stop?: string[];
  target?: string;
}

/** Aptos only; the asset is validated by the client. */
export interface HoldersCommandOptions extends Endpoint {
  asset: string;
  at: bigint;
  /** `--indexer`: the Indexer's GraphQL URL (default: the network's). */
  indexer?: string;
  format: "json" | "csv";
  verify?: "none" | "supply" | "stores";
  maxRequests?: number;
}

/** Named apart from core's `BalancesOptions`, which the runner builds from it. */
export interface BalancesCommandOptions extends Endpoint {
  /** An address or name; validated and resolved by the chain's client. */
  address: string;
  /** Non-empty when set; validated by the chain's client. */
  asset?: string[];
  pollInterval?: number;
  /** Change lines to print before exiting; the opening lines are not counted. */
  limit?: number;
}

/** Several `account` walks through `watch`; each address is in long form. */
export interface WatchCommandOptions extends Endpoint {
  /** Non-empty. */
  addresses: string[];
  fromVersion?: bigint;
  fromTime?: bigint;
  pollInterval?: number;
  /** Lines to print across every address before exiting. */
  limit?: number;
  /** Holds `Watch.positions` as one JSON object. */
  cursorFile?: string;
  filter?: CliFilter;
  decode?: string[];
}

/** Aptos only. Exactly one of `at` and `fromVersion` is set. */
export interface InspectOptions extends Endpoint {
  /** Long form. */
  address: string;
  resourceType: string;
  /** `--at`: print the value at this version instead of a range's changes. */
  at?: bigint;
  fromVersion?: bigint;
  /** Set with `fromVersion`. */
  toVersion?: bigint | "head";
  /** Records printed, with `fromVersion`. */
  limit?: number;
  format: "json" | "pretty";
}

/** Runs on a localnet it starts itself, so it takes no endpoint. */
export interface CoverageOptions {
  /** The Move package directory, holding `Move.toml`. */
  packageDir: string;
  /** The test command and its arguments, run after the publish; never empty. */
  command: string[];
  format: "json" | "pretty";
}

/** Aptos only: Sui has no event handles. */
export interface HandlesOptions extends Endpoint {
  /** An address or name; validated and resolved by `listEventHandles`. */
  address: string;
}

export interface NameOptions extends Endpoint {
  /** An address when it starts `0x`, otherwise a name; validated by the chain's library. */
  value: string;
}

export interface RecordOptions extends Endpoint {
  descriptor: EventDescriptor;
  fromVersion: bigint;
  toVersion: bigint;
  limit?: number;
  /** File to write; stdout when unset. */
  out?: string;
  /** As on `QueryOptions`. */
  cache?: string;
}

/** `apiKey` goes to the first node only. */
export interface CompareOptions extends Endpoint {
  descriptor: EventDescriptor;
  /** The second node's fullnode URL, or gRPC URL for Sui. */
  against: string;
  fromVersion: bigint;
  toVersion: bigint;
  /** Most pages read from each node. */
  maxPages?: number;
  /** Page size. */
  limit?: number;
}

/** Node 0 is `nodeUrl` and takes the API key; the others are `extraNodeUrls`, compared with it. */
export interface MonitorOptions extends Endpoint {
  /** What each round compares; FeeStatement on Aptos when no descriptor is given. */
  descriptor: EventDescriptor;
  nodeUrl: string;
  extraNodeUrls: string[];
  /** Milliseconds from one round's start to the next, and the timeout of each node call. */
  interval: number;
  /** Milliseconds of rounds the window sums cover. */
  window: number;
  /** Versions compared per round. */
  range: number;
  format: "json" | "prometheus";
  /** Where the latest round's Prometheus text is served. */
  listen?: { host: string; port: number };
  /** Rounds to run before exiting. */
  limit?: number;
}

/** `streamd serve`: the gateway's bind address, access and limits. */
export interface ServeOptions extends Endpoint {
  listen: { host: string; port: number };
  /** Required unless `listen.host` is loopback. */
  token?: string;
  allowOrigins?: string[];
  pollInterval?: number;
  maxTopics?: number;
  maxConnections?: number;
  /** Registered webhooks, kept in `file`; absent means the gateway has no webhook API. */
  webhooks?: {
    file: string;
    allowPrivateTargets?: boolean;
    retries?: number;
    timeout?: number;
    batchSize?: number;
  };
}

export interface CheckOptions extends Endpoint {
  descriptor: ModuleEventDescriptor;
}

export interface CodegenOptions extends Endpoint {
  modules: ModuleRef[];
  /** File to write; stdout when unset. */
  out?: string;
  /** Also write an `is<Type>` runtime check per generated type. */
  validators?: boolean;
  /** Also write an `arbitrary<Type>` random payload generator per generated type. */
  arbitraries?: boolean;
  /** Also write a table spec per event type and its DDL in this SQL dialect. */
  sql?: "sqlite" | "postgres";
  /** Compare with `out` instead of writing it; set only with `out`. */
  check?: boolean;
}

export type ParsedCommand =
  | { command: "help" }
  | { command: "tail"; options: TailOptions }
  | { command: "forward"; options: ForwardOptions }
  | { command: "alert"; options: AlertOptions }
  | { command: "store"; options: StoreOptions }
  | { command: "check"; options: CheckOptions }
  | { command: "record"; options: RecordOptions }
  | { command: "query"; options: QueryOptions }
  | { command: "tx"; options: TxOptions }
  | { command: "explain"; options: ExplainCommandOptions }
  | { command: "transfers"; options: TransfersOptions }
  | { command: "swaps"; options: SwapsCommandOptions }
  | { command: "sales"; options: SalesCommandOptions }
  | { command: "activity"; options: ActivityCommandOptions }
  | { command: "trace"; options: TraceCommandOptions }
  | { command: "holders"; options: HoldersCommandOptions }
  | { command: "balances"; options: BalancesCommandOptions }
  | { command: "watch"; options: WatchCommandOptions }
  | { command: "inspect"; options: InspectOptions }
  | { command: "handles"; options: HandlesOptions }
  | { command: "name"; options: NameOptions }
  | { command: "codegen"; options: CodegenOptions }
  | { command: "coverage"; options: CoverageOptions }
  | { command: "compare"; options: CompareOptions }
  | { command: "monitor"; options: MonitorOptions }
  | { command: "serve"; options: ServeOptions };

/** Bad command-line input: reported with usage, exit code 2. */
export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsageError";
  }
}

const NETWORKS: readonly Network[] = ["mainnet", "testnet", "devnet", "movement-mainnet", "movement-testnet"];
const U64_MAX = 2n ** 64n - 1n;
const MAX_TIMEOUT = 2_147_483_647;

const DESCRIPTOR_OPTIONS = {
  account: { type: "string" },
  handle: { type: "string" },
  field: { type: "string" },
  resource: { type: "string" },
  "creation-number": { type: "string" },
} as const;

const ENDPOINT_OPTIONS = {
  chain: { type: "string" },
  network: { type: "string" },
  "node-url": { type: "string", multiple: true },
  "api-key": { type: "string" },
} as const;

/** The endpoint of a command that reads events through the adapter. */
const READ_OPTIONS = { ...ENDPOINT_OPTIONS, archive: { type: "string" }, stats: { type: "boolean" } } as const;

const SOURCE_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  ...FILTER_OPTIONS,
  ...READ_OPTIONS,
  "from-version": { type: "string" },
  "poll-interval": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const FROM_TIME = { "from-time": { type: "string" } } as const;

const TAIL_OPTIONS = {
  ...SOURCE_OPTIONS,
  ...FROM_TIME,
  limit: { type: "string" },
  "cursor-file": { type: "string" },
  format: { type: "string" },
} as const;

const DELIVERY_OPTIONS = {
  to: { type: "string" },
  "cursor-file": { type: "string" },
  retries: { type: "string" },
  timeout: { type: "string" },
  header: { type: "string", multiple: true },
  secret: { type: "string" },
} as const;

const FORWARD_OPTIONS = {
  ...SOURCE_OPTIONS,
  ...FROM_TIME,
  ...DELIVERY_OPTIONS,
  "to-version": { type: "string" },
  "batch-size": { type: "string" },
  stream: { type: "string" },
  subject: { type: "string" },
  topic: { type: "string" },
} as const;

const ALERT_OPTIONS = {
  ...READ_OPTIONS,
  ...FROM_TIME,
  ...DELIVERY_OPTIONS,
  rules: { type: "string" },
  "from-version": { type: "string" },
  "poll-interval": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const STORE_OPTIONS = {
  ...SOURCE_OPTIONS,
  db: { type: "string" },
  postgres: { type: "string" },
  name: { type: "string" },
} as const;

const COMPARE_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  ...ENDPOINT_OPTIONS,
  against: { type: "string" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  "max-pages": { type: "string" },
  limit: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

/** The widest range `compare` walks, so one command stays a bounded job. */
export const MAX_COMPARE_RANGE = 100_000n;

const SERVE_OPTIONS = {
  ...READ_OPTIONS,
  listen: { type: "string" },
  token: { type: "string" },
  "allow-origin": { type: "string", multiple: true },
  "poll-interval": { type: "string" },
  "max-topics": { type: "string" },
  "max-connections": { type: "string" },
  webhooks: { type: "string" },
  "webhook-allow-private": { type: "boolean" },
  retries: { type: "string" },
  timeout: { type: "string" },
  "batch-size": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const MONITOR_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  ...ENDPOINT_OPTIONS,
  interval: { type: "string" },
  window: { type: "string" },
  range: { type: "string" },
  format: { type: "string" },
  listen: { type: "string" },
  limit: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

/** Emitted by every user transaction, so a range of recent versions always has some to compare. */
export const MONITOR_DEFAULT_TYPE = "0x1::transaction_fee::FeeStatement";

const RECORD_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  ...READ_OPTIONS,
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  out: { type: "string" },
  cache: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const QUERY_OPTIONS = {
  ...FROM_TIME,
  ...FILTER_OPTIONS,
  ...DESCRIPTOR_OPTIONS,
  ...READ_OPTIONS,
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  order: { type: "string" },
  cache: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

/** What `transfers`, `swaps` and `sales` share. */
const WALK_OPTIONS = {
  ...FROM_TIME,
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  order: { type: "string" },
  "poll-interval": { type: "string" },
  "batch-size": { type: "string" },
  "cursor-file": { type: "string" },
} as const;

const TRANSFERS_OPTIONS = {
  ...READ_OPTIONS,
  ...WALK_OPTIONS,
  asset: { type: "string" },
  account: { type: "string" },
  "with-asset-info": { type: "boolean" },
  help: { type: "boolean", short: "h" },
} as const;

const SWAPS_OPTIONS = {
  ...READ_OPTIONS,
  ...WALK_OPTIONS,
  protocol: { type: "string", multiple: true },
  asset: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const SALES_OPTIONS = {
  ...READ_OPTIONS,
  ...WALK_OPTIONS,
  marketplace: { type: "string", multiple: true },
  token: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const ACTIVITY_OPTIONS = {
  ...READ_OPTIONS,
  ...WALK_OPTIONS,
  "with-asset-info": { type: "boolean" },
  indexer: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const TRACE_OPTIONS = {
  ...READ_OPTIONS,
  indexer: { type: "string" },
  format: { type: "string" },
  hops: { type: "string" },
  direction: { type: "string" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  "min-amount": { type: "string" },
  asset: { type: "string" },
  "max-nodes": { type: "string" },
  "max-requests": { type: "string" },
  stop: { type: "string", multiple: true },
  target: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const HOLDERS_OPTIONS = {
  ...READ_OPTIONS,
  indexer: { type: "string" },
  at: { type: "string" },
  verify: { type: "string" },
  "max-requests": { type: "string" },
  format: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const BALANCES_OPTIONS = {
  ...READ_OPTIONS,
  asset: { type: "string", multiple: true },
  limit: { type: "string" },
  "poll-interval": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const WATCH_OPTIONS = {
  ...READ_OPTIONS,
  ...FILTER_OPTIONS,
  ...FROM_TIME,
  "from-version": { type: "string" },
  "poll-interval": { type: "string" },
  limit: { type: "string" },
  "cursor-file": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const INSPECT_OPTIONS = {
  ...READ_OPTIONS,
  resource: { type: "string" },
  at: { type: "string" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  format: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

/** `handles` and `name`: one positional and the endpoint. */
const HANDLES_OPTIONS = {
  ...ENDPOINT_OPTIONS,
  help: { type: "boolean", short: "h" },
} as const;

const TX_OPTIONS = {
  ...READ_OPTIONS,
  timeout: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const EXPLAIN_OPTIONS = {
  ...TX_OPTIONS,
  json: { type: "boolean" },
} as const;

const CHECK_OPTIONS = {
  ...ENDPOINT_OPTIONS,
  help: { type: "boolean", short: "h" },
} as const;

const COVERAGE_OPTIONS = {
  format: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const CODEGEN_OPTIONS = {
  ...ENDPOINT_OPTIONS,
  out: { type: "string" },
  validators: { type: "boolean" },
  arbitraries: { type: "boolean" },
  sql: { type: "string" },
  check: { type: "boolean" },
  help: { type: "boolean", short: "h" },
} as const;

const MODULE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

const DEFAULT_CURSOR_FILE = "streamd-forward.cursor";
const DEFAULT_ALERT_CURSOR_FILE = "streamd-alert.cursor";
const DEFAULT_RETRIES = 5;
const MAX_RETRIES = 100;
const DEFAULT_TIMEOUT = 10_000;
// RFC 9110 token characters.
const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
// Printable latin1 plus tab, a little stricter than fetch: a value fetch refuses
// would fail every attempt with an error naming the offending character.
const HEADER_VALUE = /^[\t\x20-\x7e\x80-\xff]*$/;

export const USAGE = `Usage:
  streamd tail [options] <eventType>...
  streamd tail --account <address> [options] [<eventType>...]
  streamd tail --account <address> --handle <struct> --field <name> [options]
  streamd tail --account <address> --creation-number <n> [options]
  streamd tail --account <address> --resource <type> [options]
  streamd forward --to <url> [options] <eventType>...
  streamd forward --to <url> --account <address> [...]
  streamd forward --to redis://<host> --stream <key> [options] <eventType>...
  streamd forward --to nats://<host> --subject <name> [options] <eventType>...
  streamd forward --to kafka://<host> --topic <name> [options] <eventType>...
  streamd alert --rules <file> --to <url> [options]
  streamd store --db <file> [options] <eventType>...
  streamd store --db <file> --account <address> [...]
  streamd store --postgres <url> [options] <eventType>...
  streamd query (--from-version <n> | --from-time <t>) [--to-version <n>] [options] <eventType>...
  streamd tx <hash | digest>
  streamd explain <hash> [--json] [options]
  streamd transfers [--asset <asset>] [--account <address | name>] [--to-version <n | head>] [options]
  streamd swaps [--protocol <name>]... [--asset <asset>] [--to-version <n | head>] [options]
  streamd sales [--marketplace <name>]... [--token <token>] [--to-version <n | head>] [options]
  streamd activity <address | name> [--to-version <n | head>] [--indexer <url>] [options]
  streamd trace <address | name> [--hops <n>] [--format <json|dot>] [--indexer <url>] [options]
  streamd holders <asset> --at <version> [--verify <none|supply|stores>] [--format <json|csv>] [options]
  streamd balances <address | name> [--asset <asset>]... [options]
  streamd watch <address>... [--cursor-file <path>] [options]
  streamd inspect <address> --resource <type> --from-version <n> [--to-version <n | head>] [options]
  streamd inspect <address> --resource <type> --at <n> [options]
  streamd handles <address | name>
  streamd name <name | address>
  streamd check <eventType>...
  streamd record --from-version <n> --to-version <n> [--out <file>] [--cache <file>] <eventType>...
  streamd codegen [--out <file> [--check]] [--validators] [--arbitraries] [--sql <sqlite|postgres>] (<address> | <address>::<module>)...
  streamd compare --against <url> --from-version <n> --to-version <n> [options] <eventType>...
  streamd monitor --node-url <url> --node-url <url>... [options] [<eventType>...]
  streamd serve [--listen <[host:]port>] [--token <t>] [--allow-origin <origin>]... [options]
  streamd coverage <package> [--format pretty] -- <command> [<arg>...]

tail follows matching events and prints each as one JSON line on stdout.
forward POSTs each batch of matching events to a webhook as a JSON array, or
adds it to a Redis stream, publishes it to a NATS JetStream subject or produces
it to a Kafka topic, saving
its position to a cursor file once the destination acknowledges it.
alert follows the rules in a JSON file, each a descriptor, filter, window and
count, sum or anomaly threshold, and POSTs a signed alert to a webhook the first
time a window of chain time crosses its threshold or leaves its baseline's
band, saving each rule's position and open window to a cursor file.
store follows matching events into a SQLite file, or a Postgres database
with --postgres, resuming from the cursor saved there when run again.
query reads a closed range of history as JSON lines, then exits.
tx waits for a transaction and prints its events, exiting 1 if it failed.
explain waits for an Aptos transaction and prints its status, gas, transfers,
balance changes, swaps and sales as text, or as one JSON line with --json,
exiting 1 if it failed.
transfers prints asset transfers as JSON lines: followed live, or with
--to-version read over a closed range, then exits.
swaps and sales print DEX swaps and NFT marketplace sales on Aptos the same
way, one JSON line per pool hop or token sold.
activity prints each transaction an Aptos account sent or received as a JSON
line of its direction, transfers, balance changes and events, and trace the
counterparty graph around it as one JSON line or Graphviz DOT; both read the
account's transactions from the Indexer and everything else from the fullnode.
holders prints every fungible store holding an Aptos asset at a past version,
largest first, as JSON lines or CSV, folded back from the Indexer and checked
against the chain there, then the snapshot's summary as one JSON line on stderr.
balances prints a wallet's balance of each asset once, then a JSON line each
time one changes, read at the head every --poll-interval.
watch follows the transactions each address sent, one walk per address, and
prints each event as a JSON line with the watched address as "address".
inspect prints each write or delete of one resource over a range as a JSON
line of the paths it changed, the first diffed against the value just before
the range; with --at, the resource's value at that version (Aptos only).
handles lists every event handle on an Aptos account as a JSON line, with
its descriptor and the tail flags that follow it.
name resolves a name to its address, or an address (0x...) to its primary
name, as one JSON line; name is null when the address has none.
check says whether each event type can be emitted, and exits 1 if one cannot.
record runs a bounded query and writes a recording for replayAdapter tests.
compare reads one range from two nodes and prints what one has that the other
does not, exiting 1 if they disagree.
monitor reads every --node-url each --interval and prints one record per round
of each node's head lag, requests, errors and 429s over --window, and how its
events over a recent range compare with the first node's, as a JSON line or
Prometheus text (default type: ${MONITOR_DEFAULT_TYPE} on Aptos).
serve runs a gateway (under Bun) that polls each descriptor its clients ask for
once and streams it to all of them as Server-Sent Events (/v1/events) and
WebSocket (/v1/ws), resumed by Last-Event-ID, with POST /v1/fetch, /v1/head and
/v1/earliest for GatewayAdapter and GET /v1/status. With --webhooks it also
posts to webhooks registered at /v1/webhooks, signed and at least once.
codegen reads module ABIs and writes TypeScript types and defineEvent
descriptors for their event structs; a bare address covers all its modules.
With --check it compares what it would write with --out instead, and exits 1
if the file differs or is missing.
With --chain sui it reads Sui packages, where every struct or enum with copy
and drop is an event.
coverage starts an Aptos localnet with the aptos CLI, publishes the Move package
from a funded account, runs the command against it (with STREAMD_NODE_URL,
STREAMD_FAUCET_URL, STREAMD_PACKAGE_ADDRESS and STREAMD_PUBLISHER_KEY set), then
prints one JSON line per #[event] struct of the package: whether the run emitted
it, how often, and the call that first did. It exits 1 if the command failed.
Every command but handles, inspect, explain, swaps, sales, activity, trace, holders and coverage takes --chain sui
to read Sui instead of Aptos.
tail, query, forward, store and watch take --sender, --function and --where, which
filter after fetching, so --limit counts the events that match; they also take
--decode, which checks integer fields before --where runs.

Options:
  --account <address>    events from transactions this account sent; transfers: what this
                         address or name (ANS, or SuiNS with --chain sui) sent or received
  --asset <asset>        transfers: only this asset, a fungible-asset or token object address,
                         a coin type or a legacy token id (a coin type with --chain sui)
                         balances: an asset to follow, repeatable (default: APT, or SUI)
                         swaps: only swaps with this asset on either side; trace: only
                         transfers of this asset
  --with-asset-info      transfers, activity: add each asset's symbol and decimals
  --protocol <name>      swaps: only this DEX, repeatable: hyperion, thala-clamm, thala-v2,
                         cellana, tapp, liquidswap, pancakeswap or thala-v1
  --marketplace <name>   sales: only this marketplace, repeatable: wapal or tradeport
  --indexer <url>        activity, trace, holders: the Indexer's GraphQL URL (default: the network's),
                         which gets the API key only when it is the network's and there is
                         no --node-url
  --hops <n>             trace: hops to follow from the address, 1 to 6 (default: 1)
  --direction <d>        trace: follow what each address sent (out), received (in) or both
                         (default: both)
  --min-amount <n>       trace: ignore transfers below this many base units
  --max-nodes <n>        trace: most addresses in the graph, root included (default: 100)
  --max-requests <n>     trace: most adapter calls; holders: most requests, Indexer and
                         fullnode alike (default: 1000)
  --verify <check>       holders: supply checks the total against the supply at the version
                         (default), stores also every holder's balance, none nothing
  --stop <address>       trace: keep this address but never expand it, repeatable
  --target <address>     trace: stop once this address joins, and add the path to it
  --token <token>        sales: only this token, an object address or legacy token id
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --creation-number <n>  event handle by creation number (with --account)
  --resource <type>      every change to this resource (with --account); inspect: the
                         resource to inspect (required)
  --at <n>               inspect: print the resource's value at this version, null if absent;
                         holders: the version to list holders at (required)
  --network <name>       mainnet, testnet, devnet, movement-mainnet, or movement-testnet
                         (default: mainnet)
  --node-url <url>       fullnode URL, overriding the network's default; repeatable on
                         commands that read events (not check, codegen, handles, name or
                         compare), spreading reads across the nodes, the first taking --api-key
  --archive <url>        those commands: send what the fullnodes refuse as pruned to this
                         archive node, or with default to Aptos mainnet's (Aptos only), which
                         gets the API key only when there is no --node-url
  --api-key <key>        fullnode API key, sent as a bearer token (or set ${API_KEY_ENV}, used
                         only on mainnet, testnet and devnet without --node-url)
  --from-version <n>     start at this version instead of the chain head
  --from-time <t>        tail, forward, alert, query, watch: start here, as 2h, 7d or an ISO date (UTC)
  --poll-interval <ms>   delay between polls (default: 1000); serve: each topic's
  --sender <address>     only events from transactions one of these signed, repeatable
  --function <a::m::f>   only events from a call to one of these entry functions, repeatable
  --where <expr>         only events whose data matches <field><op><value>, repeatable,
                         all must hold: field a dotted path such as legs.0.amount, op
                         ==, !=, <, <=, > or >=; an integer value compares as an integer,
                         any other is matched as text under == or !=
  --decode <paths>       turn these u64/u128/u256 data fields into integers before --where,
                         comma-separated and repeatable, * for every array element
                         (legs.*.amount); output and matches are unchanged, but a present
                         field that is not an unsigned integer is an error: query exits 1,
                         tail, forward, store and watch report it and skip the event
  --limit <n>            tail, transfers, swaps, sales, activity, watch: exit after printing n;
                         balances: after n changes; record, query, inspect: stop after n;
                         compare: page size; monitor: rounds
  --rules <file>         alert: the rules file (required)
  --to <url>             forward, alert: webhook URL (http or https, required); forward also
                         takes redis://[user:password@]host[:port][/db] or rediss:// (TLS),
                         Bun's own client under Bun, ioredis under Node (npm i ioredis);
                         or nats://[user:password@ | token@]host[:port], plaintext only;
                         or kafka://[user:password@]host[:port][,host[:port]]...[?sasl=<m>]
                         or kafkas:// (TLS), <m> plain, scram-sha-256 or scram-sha-512, through
                         @confluentinc/kafka-javascript or kafkajs (npm i either)
  --stream <key>         forward: the Redis stream each batch is added to (with redis://)
  --subject <name>       forward: the NATS subject each batch is published to, which a
                         JetStream stream must capture (with nats://)
  --topic <name>         forward: the Kafka topic each batch is produced to, which must exist
                         (with kafka:// or kafkas://)
  --cursor-file <path>   tail, transfers, swaps, sales, activity (following): save and
                         resume from here; watch: each address's position, as one JSON object;
                         forward: cursor file (default: ${DEFAULT_CURSOR_FILE}); alert: each rule's position
                         and open window (default: ${DEFAULT_ALERT_CURSOR_FILE})
  --format <name>        tail: json, or pretty for aligned lines in UTC (default: json);
                         inspect: json, or pretty for one line per changed path;
                         coverage: json, or pretty for one line per event struct;
                         trace: json, or dot for Graphviz;
                         holders: json, or csv with a header row (default: json)
  --retries <n>          forward, alert, serve: retries per batch or alert (default: ${DEFAULT_RETRIES})
  --timeout <ms>         forward, alert, serve: per-request timeout (default: ${DEFAULT_TIMEOUT});
                         tx, explain: wait for commit (default: 20000)
  --header <h>           forward, alert: request header 'Name: value', repeatable
  --secret <s>           forward, alert: sign each body with HMAC-SHA256 in ${SIGNATURE_HEADER},
                         a Redis entry's signature field or a NATS or Kafka header (or set
                         ${SECRET_ENV}, which keeps it out of ps)
  --db <file>            store: SQLite file to write (this or --postgres)
  --postgres <url>       store: Postgres database to write, postgres://user@host/db; Bun.SQL
                         under Bun, pg under Node (npm i pg); the URL is never printed, and
                         PGPASSWORD keeps the password out of it and out of ps
  --name <name>          store: name its cursor is saved under (default: the network and descriptor)
  --to-version <n>       record: end of the range (required); query: default the head;
                         inspect: a version or head (default: head);
                         forward: deliver up to here, then exit 0; transfers, swaps, sales,
                         activity: a version or head, read the range instead of following
                         (needs a start); trace: end of every address's range (default: head)
  --batch-size <n>       forward: most events per POST (default: 25); serve: per webhook POST
                         (default: each page); transfers, swaps, sales, activity (following):
                         transactions per page (default: 25)
  --order <asc|desc>     query; transfers, swaps, sales, activity with --to-version: oldest
                         or newest first (default: asc; desc without --limit buffers the
                         range to reverse it)
  --out <file>           codegen, record: write to this file instead of stdout
  --cache <file>         query, record: keep settled pages in this SQLite file and read them
                         from it on the next run of the same range (Aptos only)
  --stats                commands that read events (not check, codegen, handles, name or
                         compare): print each node's request counts and latencies to stderr
                         as one JSON line {"stats":...} on exit and on SIGUSR1 (not Windows);
                         reads made outside the adapter, such as name lookups and inspect's
                         resource reads, are not counted
  --json                 explain: print the explanation as one JSON line, bigints as
                         decimal strings
  --validators           codegen: also write an is<Type> runtime check per type
  --arbitraries          codegen: also write an arbitrary<Type>(rng) random payload generator
                         per type, a seeded streamdRng and streamdEvent for MemoryAdapter
  --sql <dialect>        codegen: also write streamdTables, a table per event type with a
                         column per field, and streamdSchema, its sqlite or postgres DDL;
                         storeTables in @streamd/store-sqlite or -postgres fills them
  --check                codegen: compare with the --out file, writing nothing; exit 1 if
                         it differs or is missing, naming the first line that differs
  --chain <name>         aptos or sui (default: aptos); with sui, --network is mainnet,
                         testnet or devnet, --node-url and --against are Sui gRPC URLs,
                         tx takes a base58 digest, and --handle, --field,
                         --creation-number, --resource and --api-key are refused
  --against <url>        compare: the second node's fullnode URL (required)
  --max-pages <n>        compare: most pages read from each node (default: 10)
  --interval <ms>        monitor: time between rounds, and each node call's timeout
                         (default: 10000)
  --window <duration>    monitor: how far back the window sums reach, as 30s, 5m, 1h or 1d
                         (default: 5m)
  --range <n>            monitor: versions compared with the first node per round (default: 100)
  --format <name>        monitor: json, or prometheus for text exposition (default: json)
  --listen <[host:]port> monitor: also serve the latest round's Prometheus text at /metrics,
                         on 127.0.0.1 unless a host is given; serve: where the gateway
                         listens (default: 127.0.0.1:${DEFAULT_SERVE_PORT})
  --token <t>            serve: require it as Authorization: Bearer or ?access_token= (or set
                         ${TOKEN_ENV}); required to listen on a host other than loopback
  --allow-origin <o>     serve: a browser origin given CORS headers and let open WebSockets,
                         repeatable, or * for any
  --max-topics <n>       serve: most descriptors followed at once (default: 32)
  --max-connections <n>  serve: most open streams, SSE and WebSocket (default: 1000)
  --webhooks <file>      serve: enable /v1/webhooks, keeping registered webhooks and their
                         cursors in <file> across restarts; --retries, --timeout and
                         --batch-size apply to their deliveries
  --webhook-allow-private
                         serve: let webhook targets be loopback, private or other
                         non-public addresses (refused by default)
  -h, --help             show this help
`;

function wrap<T>(fn: () => T): T {
  try {
    return fn();
  } catch (err) {
    throw new UsageError(err instanceof Error ? err.message : String(err));
  }
}

const APTOS_NETWORKS: readonly Network[] = ["mainnet", "testnet", "devnet"];

/**
 * The flag is an explicit choice, sent to whatever node the command uses. The
 * variable is meant for Aptos Labs' endpoints, so it is never sent to a custom
 * node or another chain.
 */
function apiKey(flag: string | undefined, network: Network, url: string | undefined): string | undefined {
  if (flag === "") throw new UsageError("--api-key must not be empty");
  if (flag !== undefined) return flag;
  if (url !== undefined || !APTOS_NETWORKS.includes(network)) return undefined;
  return process.env[API_KEY_ENV] || undefined;
}

const SUI_NETWORKS: readonly Network[] = ["mainnet", "testnet", "devnet"];

function chain(raw: string | undefined): boolean {
  if (raw === undefined || raw === "aptos") return false;
  if (raw === "sui") return true;
  throw new UsageError(`--chain must be aptos or sui: ${raw}`);
}

interface EndpointFlags {
  network?: string;
  "node-url"?: string[];
  "api-key"?: string;
  archive?: string;
  stats?: boolean;
}

/**
 * The chain, network and node URLs; the key is `withKey`'s, so each command
 * checks it where it always has. `oneNode` names a command that reads outside
 * the adapter, which takes one node only.
 */
function endpoint(values: EndpointFlags, sui: boolean, oneNode?: string): Endpoint {
  const options: Endpoint = { network: network(values.network) };
  const [url, ...extra] = (values["node-url"] ?? []).map(nodeUrl);
  if (url !== undefined) options.nodeUrl = url;
  if (extra.length > 0) {
    if (oneNode !== undefined) throw new UsageError(`--node-url is given once for ${oneNode}: it reads one node`);
    options.extraNodeUrls = extra;
  }
  if (values.archive !== undefined) options.archive = archive(values.archive, options.network, sui);
  if (values.stats === true) options.stats = true;
  if (sui) {
    if (!SUI_NETWORKS.includes(options.network)) {
      throw new UsageError(`--network with --chain sui must be one of ${SUI_NETWORKS.join(", ")}: ${options.network}`);
    }
    options.chain = "sui";
  }
  return options;
}

function withKey<T extends Endpoint>(options: T, flag: string | undefined): T {
  // Public Sui nodes take no key, so none is sent.
  if (options.chain === "sui") {
    if (flag !== undefined) throw new UsageError("--api-key is not sent to Sui nodes");
    return options;
  }
  const key = apiKey(flag, options.network, options.nodeUrl);
  if (key !== undefined) options.apiKey = key;
  return options;
}

function checkedAddress(raw: string, sui: boolean): string {
  if (!sui) return wrap(() => requireAddress(raw));
  const normal = normalizeAddress(raw);
  if (normal === undefined) throw new UsageError(`Not a valid Sui address: ${raw}`);
  return normal;
}

function integer(flag: string, raw: string | undefined, min: number, max: number) {
  if (raw === undefined) return undefined;
  const n = /^\d+$/.test(raw) ? Number(raw) : NaN;
  if (!(n >= min && n <= max)) {
    throw new UsageError(`--${flag} must be an integer from ${min} to ${max}: ${raw}`);
  }
  return n;
}

function version(raw: string | undefined, flag = "from-version"): bigint | undefined {
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw)) {
    throw new UsageError(`--${flag} must be a non-negative integer: ${raw}`);
  }
  return BigInt(raw);
}

const DURATION = /^(\d+)([smhd])$/;
const MS_PER: Record<string, bigint> = { s: 1000n, m: 60_000n, h: 3_600_000n, d: 86_400_000n };
const ISO_TIME = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?(Z|[+-]\d{2}:\d{2})?)?$/;

/** Microseconds since the epoch for a duration before `now`, or an ISO date (UTC unless it names an offset). */
export function parseTime(raw: string, now = Date.now()): bigint {
  const duration = DURATION.exec(raw);
  if (duration !== null) {
    const ms = BigInt(now) - BigInt(duration[1]!) * MS_PER[duration[2]!]!;
    return ms < 0n ? 0n : ms * 1000n;
  }
  const iso = ISO_TIME.exec(raw);
  const ms = iso === null ? NaN : Date.parse(raw.includes("T") && iso[1] === undefined ? `${raw}Z` : raw);
  if (Number.isNaN(ms) || ms < 0) {
    throw new UsageError(`--from-time must be a duration such as 2h or an ISO date: ${raw}`);
  }
  return BigInt(ms) * 1000n;
}

function network(raw: string | undefined): Network {
  if (raw === undefined) return "mainnet";
  if (!(NETWORKS as readonly string[]).includes(raw)) {
    throw new UsageError(`--network must be one of ${NETWORKS.join(", ")}: ${raw}`);
  }
  return raw as Network;
}

function httpUrl(raw: string): URL | undefined {
  const url = URL.canParse(raw) ? new URL(raw) : undefined;
  return url?.protocol === "http:" || url?.protocol === "https:" ? url : undefined;
}

function nodeUrl(raw: string): string {
  if (httpUrl(raw) === undefined) {
    throw new UsageError(`--node-url must be an http or https URL: ${raw}`);
  }
  return raw;
}

function archive(raw: string, network: Network, sui: boolean): string {
  if (sui) {
    throw new UsageError("--archive is not available with --chain sui: Sui's archive needs a native gRPC transport, which the CLI does not include");
  }
  if (raw === "default") {
    if (network !== "mainnet") {
      throw new UsageError(`--archive default is Aptos mainnet's archive; ${network} has none, so give an archive node's URL`);
    }
    return raw;
  }
  if (httpUrl(raw) === undefined) throw new UsageError(`--archive must be an http or https URL, or default: ${raw}`);
  return raw;
}

// A Redis URL is the one place credentials are expected, so it is checked here and never echoed.
function redisUrl(raw: string): string {
  const url = URL.canParse(raw) ? new URL(raw) : undefined;
  if (url === undefined || url.hostname === "") throw new UsageError("--to must be a valid redis:// or rediss:// URL with a host");
  if (url.search !== "" || url.hash !== "" || !/^(\/\d*)?$/.test(url.pathname)) {
    throw new UsageError("--to redis:// takes only a database number as its path, with no query or fragment");
  }
  // Lowercased, so a client matching the scheme case-sensitively never takes REDISS:// as plaintext.
  return url.protocol + raw.slice(url.protocol.length);
}

// Like a Redis URL, a NATS URL carries credentials, so it is checked here and never echoed.
function natsUrl(raw: string): string {
  const url = URL.canParse(raw) ? new URL(raw) : undefined;
  if (url === undefined || url.hostname === "") throw new UsageError("--to must be a valid nats:// URL with a host");
  if (url.search !== "" || url.hash !== "" || !/^\/?$/.test(url.pathname)) {
    throw new UsageError("--to nats:// takes no path, query or fragment");
  }
  try {
    decodeURIComponent(url.username);
    decodeURIComponent(url.password);
  } catch {
    throw new UsageError("--to nats:// has a malformed percent-escape in its user or password");
  }
  return url.protocol + raw.slice(url.protocol.length);
}

// Kafka's legal topic names: 1 to 249 of these, never "." or "..".
const TOPIC = /^[A-Za-z0-9._-]{1,249}$/;

function kafkaUrl(raw: string): string {
  try {
    parseKafkaUrl(raw);
  } catch (err) {
    throw new UsageError((err as Error).message);
  }
  const scheme = raw.indexOf(":");
  return raw.slice(0, scheme).toLowerCase() + raw.slice(scheme);
}

// Publishing needs a literal subject: dot-separated tokens, no wildcards, no whitespace or controls.
const SUBJECT = /^[^\s\u0000-\u001f\u007f.*>]+(\.[^\s\u0000-\u001f\u007f.*>]+)*$/;

function webhookUrl(raw: string | undefined, command: string): string {
  if (raw === undefined) throw new UsageError(`${command} needs --to <url>`);
  // The URL is not echoed: it may carry a token.
  const url = httpUrl(raw);
  if (url === undefined) throw new UsageError("--to must be an http or https URL");
  // fetch refuses these with an error quoting the whole URL.
  if (url.username !== "" || url.password !== "") {
    throw new UsageError("--to must not carry credentials; pass them with --header");
  }
  return raw;
}

function headers(raw: readonly string[] = []): Record<string, string> {
  const result: Record<string, string> = {};
  for (const header of raw) {
    const colon = header.indexOf(":");
    const name = header.slice(0, Math.max(colon, 0)).trim();
    const value = header.slice(colon + 1).trim();
    // Neither part is echoed: the value is usually a secret.
    if (!HEADER_NAME.test(name) || !HEADER_VALUE.test(value)) {
      throw new UsageError("--header must be 'Name: value' with a valid name and value");
    }
    // Lowercased so a given content-type replaces the default rather than joining it.
    result[name.toLowerCase()] = value;
  }
  return result;
}

interface DescriptorFlags {
  account?: string;
  handle?: string;
  field?: string;
  resource?: string;
  "creation-number"?: string;
}

function creationNumber(raw: string): bigint {
  const number = /^\d+$/.test(raw) ? BigInt(raw) : -1n;
  if (number < 0n || number > U64_MAX) throw new UsageError(`--creation-number must be a u64: ${raw}`);
  return number;
}

function refuseOnSui(flags: DescriptorFlags): void {
  for (const flag of ["handle", "field", "creation-number"] as const) {
    if (flags[flag] !== undefined) throw new UsageError(`--${flag} is not available with --chain sui: Sui has no event handles`);
  }
  if (flags.resource !== undefined) {
    throw new UsageError("--resource is not available with --chain sui: no Sui source serves resource changes");
  }
}

export function descriptor(
  command: string,
  flags: DescriptorFlags,
  types: string[],
  sui = false,
): EventDescriptor {
  if (sui) refuseOnSui(flags);
  const number = flags["creation-number"];
  if (flags.resource !== undefined || number !== undefined) {
    if (flags.resource !== undefined && number !== undefined) {
      throw new UsageError("set --resource or --creation-number, not both");
    }
    const flag = flags.resource !== undefined ? "--resource" : "--creation-number";
    if (flags.account === undefined) throw new UsageError(`${flag} needs --account`);
    if (flags.handle !== undefined || flags.field !== undefined) {
      throw new UsageError(`${flag} cannot be combined with --handle or --field`);
    }
    if (types.length > 0) throw new UsageError(`event types cannot be combined with ${flag}`);
    const accountAddress = wrap(() => requireAddress(flags.account!));
    return flags.resource !== undefined
      ? { kind: "resource", accountAddress, resourceType: wrap(() => requireResourceType(flags.resource)) as MoveEventType }
      : { kind: "handle", accountAddress, creationNumber: creationNumber(number!) };
  }
  const eventTypes = types.map((t) => wrap(() => requireEventType(t)));
  if (flags.handle !== undefined || flags.field !== undefined) {
    if (flags.handle === undefined || flags.field === undefined || flags.account === undefined) {
      throw new UsageError("--handle needs --account and --field");
    }
    if (eventTypes.length > 0) {
      throw new UsageError("event types cannot be combined with --handle");
    }
    return {
      kind: "handle",
      accountAddress: wrap(() => requireAddress(flags.account!)),
      eventHandleStruct: flags.handle,
      fieldName: flags.field,
    };
  }
  if (flags.account !== undefined) {
    const accountAddress = checkedAddress(flags.account, sui);
    return eventTypes.length > 0
      ? { kind: "account", accountAddress, eventTypes }
      : { kind: "account", accountAddress };
  }
  if (eventTypes.length === 0) {
    throw new UsageError(`${command} needs at least one event type, or --account`);
  }
  return { kind: "module", eventType: eventTypes as MoveEventType[] };
}

/** Sets `filter` and `decode` on `options` when their flags were given. */
function withFilter<T extends { filter?: CliFilter; decode?: string[] }>(options: T, flags: FilterFlags): T {
  const filter = wrap(() => parseFilter(flags));
  const decode = wrap(() => parseDecode(flags.decode));
  if (filter !== undefined) options.filter = filter;
  if (decode !== undefined) options.decode = decode;
  return options;
}

interface SourceFlags extends DescriptorFlags, EndpointFlags, FilterFlags {
  chain?: string;
  "from-time"?: string;
  "from-version"?: string;
  "poll-interval"?: string;
}

function startTime(values: { "from-time"?: string; "from-version"?: string }): bigint | undefined {
  if (values["from-time"] === undefined) return undefined;
  if (values["from-version"] !== undefined) throw new UsageError("set --from-time or --from-version, not both");
  return parseTime(values["from-time"]);
}

function source(command: string, values: SourceFlags, positionals: string[]): SourceOptions {
  const sui = chain(values.chain);
  const options: SourceOptions = {
    descriptor: descriptor(command, values, positionals, sui),
    ...endpoint(values, sui),
  };
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  withKey(options, values["api-key"]);
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  return withFilter(options, values);
}

function parseTail(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: TAIL_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const options: TailOptions = source("tail", values, positionals);
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  if (values["cursor-file"] === "") throw new UsageError("--cursor-file must not be empty");
  if (values["cursor-file"] !== undefined) options.cursorFile = values["cursor-file"];
  if (values.format !== undefined) {
    if (values.format !== "json" && values.format !== "pretty") {
      throw new UsageError(`--format must be json or pretty: ${values.format}`);
    }
    options.format = values.format;
  }
  return { command: "tail", options };
}

function parseForward(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: FORWARD_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (values.to !== undefined && /^(tls|nats\+tls):/i.test(values.to)) {
    throw new UsageError("--to tls:// is not supported: the NATS sink speaks plaintext only, so use nats://");
  }
  const redis = values.to !== undefined && /^rediss?:/i.test(values.to);
  const nats = values.to !== undefined && /^nats:/i.test(values.to);
  const kafka = values.to !== undefined && /^kafkas?:/i.test(values.to);
  const brokerUrl = redis ? redisUrl(values.to!) : nats ? natsUrl(values.to!) : kafka ? kafkaUrl(values.to!) : undefined;
  const options: ForwardOptions = {
    ...source("forward", values, positionals),
    ...delivery("forward", values, DEFAULT_CURSOR_FILE, brokerUrl),
  };
  if (kafka) {
    if (values.header !== undefined) throw new UsageError("--header is for webhooks: the Kafka sink sets only its own headers");
    if (values.topic === undefined || values.topic === "") throw new UsageError("--to kafka:// needs --topic <name>");
    if (!TOPIC.test(values.topic) || values.topic === "." || values.topic === "..") {
      throw new UsageError(`--topic must be 1 to 249 of A-Z, a-z, 0-9, ".", "_" and "-", not "." or "..": ${JSON.stringify(values.topic)}`);
    }
    options.topic = values.topic;
  } else if (values.topic !== undefined) {
    throw new UsageError("--topic needs a kafka:// or kafkas:// --to");
  }
  if (nats) {
    if (values.header !== undefined) throw new UsageError("--header is for webhooks: the NATS sink sets only its own headers");
    if (values.subject === undefined || values.subject === "") throw new UsageError("--to nats:// needs --subject <name>");
    if (!SUBJECT.test(values.subject)) {
      throw new UsageError(`--subject must be dot-separated tokens with no spaces or wildcards: ${JSON.stringify(values.subject)}`);
    }
    options.subject = values.subject;
  } else if (values.subject !== undefined) {
    throw new UsageError("--subject needs a nats:// --to");
  }
  if (redis) {
    if (values.header !== undefined) throw new UsageError("--header is for webhooks: a Redis stream entry carries no headers");
    if (values.stream === undefined || values.stream === "") throw new UsageError("--to redis:// needs --stream <key>");
    options.stream = values.stream;
  } else if (values.stream !== undefined) {
    throw new UsageError("--stream needs a redis:// or rediss:// --to");
  }
  const toVersion = version(values["to-version"], "to-version");
  if (toVersion !== undefined) {
    if (options.fromVersion === undefined && options.fromTime === undefined) {
      throw new UsageError("--to-version needs --from-version or --from-time, where the range starts");
    }
    if (options.fromVersion !== undefined && toVersion < options.fromVersion) {
      throw new UsageError("--to-version must not be below --from-version");
    }
    options.toVersion = toVersion;
  }
  const batchSize = integer("batch-size", values["batch-size"], 1, Number.MAX_SAFE_INTEGER);
  if (batchSize !== undefined) options.batchSize = batchSize;
  return { command: "forward", options };
}

interface DeliveryFlags {
  to?: string;
  "cursor-file"?: string;
  retries?: string;
  timeout?: string;
  header?: string[];
  secret?: string;
}

/**
 * The destination, its headers, signing, retry budget and cursor file, as
 * forward and alert take them; `to` is a webhook unless the caller has
 * already checked another kind.
 */
function delivery(command: string, values: DeliveryFlags, cursorFile: string, to = webhookUrl(values.to, command)) {
  const options: Pick<ForwardOptions, "to" | "cursorFile" | "retries" | "timeout" | "headers" | "secret"> = {
    to,
    cursorFile: values["cursor-file"] ?? cursorFile,
    retries: integer("retries", values.retries, 0, MAX_RETRIES) ?? DEFAULT_RETRIES,
    timeout: integer("timeout", values.timeout, 1, MAX_TIMEOUT) ?? DEFAULT_TIMEOUT,
    headers: headers(values.header),
  };
  if (options.cursorFile === "") throw new UsageError("--cursor-file must not be empty");
  if (values.secret === "") throw new UsageError("--secret must not be empty");
  const secret = values.secret ?? (process.env[SECRET_ENV] || undefined);
  if (secret !== undefined) {
    if (SIGNATURE_HEADER in options.headers) {
      throw new UsageError(`--header cannot set ${SIGNATURE_HEADER} when signing`);
    }
    options.secret = secret;
  }
  return options;
}

function parseAlert(args: string[]): ParsedCommand {
  const { values } = wrap(() => parseArgs({ args, options: ALERT_OPTIONS, allowPositionals: false, strict: true }));
  if (values.help) return { command: "help" };
  if (values.rules === undefined || values.rules === "") throw new UsageError("alert needs --rules <file>");
  const sui = chain(values.chain);
  const options: AlertOptions = {
    ...endpoint(values, sui),
    rules: values.rules,
    ...delivery("alert", values, DEFAULT_ALERT_CURSOR_FILE),
  };
  withKey(options, values["api-key"]);
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  return { command: "alert", options };
}

/** An empty host is allowed: the driver then reads `PGHOST` or uses localhost. */
function isPostgresUrl(value: string): boolean {
  if (!/^postgres(ql)?:\/\//i.test(value)) return false;
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

function withCache(options: { cache?: string }, cache: string | undefined, sui: boolean): void {
  if (cache === undefined) return;
  if (cache === "") throw new UsageError("--cache must not be empty");
  if (sui) throw new UsageError("--cache does nothing with --chain sui: Sui pages are never marked final");
  options.cache = cache;
}

function parseStore(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: STORE_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const { db, postgres } = values;
  if (db === undefined && postgres === undefined) throw new UsageError("store needs --db <file> or --postgres <url>");
  if (db !== undefined && postgres !== undefined) throw new UsageError("--db and --postgres cannot be used together");
  if (db === "") throw new UsageError("--db must not be empty");
  // The value may carry a password, so the refusal never repeats it.
  if (postgres !== undefined && !isPostgresUrl(postgres)) {
    throw new UsageError("--postgres must be a postgres:// or postgresql:// URL");
  }
  if (values.name === "") throw new UsageError("--name must not be empty");
  const options: StoreOptions = { ...source("store", values, positionals), ...(db !== undefined ? { db } : { postgres }) };
  if (values.name !== undefined) options.name = values.name;
  return { command: "store", options };
}

function parseRecord(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: RECORD_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const fromVersion = version(values["from-version"]);
  const toVersion = version(values["to-version"], "to-version");
  if (fromVersion === undefined || toVersion === undefined) {
    throw new UsageError("record needs --from-version and --to-version, so the query has an end");
  }
  if (toVersion < fromVersion) throw new UsageError("--to-version must not be below --from-version");
  if (values.out === "") throw new UsageError("--out must not be empty");
  const sui = chain(values.chain);
  const options: RecordOptions = {
    descriptor: descriptor("record", values, positionals, sui),
    ...endpoint(values, sui),
    fromVersion,
    toVersion,
  };
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  withKey(options, values["api-key"]);
  if (limit !== undefined) options.limit = limit;
  if (values.out !== undefined) options.out = values.out;
  withCache(options, values.cache, sui);
  return { command: "record", options };
}

function parseCompare(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: COMPARE_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (values.against === undefined) throw new UsageError("compare needs --against <url>, the node to compare with");
  if (httpUrl(values.against) === undefined) {
    throw new UsageError(`--against must be an http or https URL: ${values.against}`);
  }
  const fromVersion = version(values["from-version"]);
  const toVersion = version(values["to-version"], "to-version");
  if (fromVersion === undefined || toVersion === undefined) {
    throw new UsageError("compare needs --from-version and --to-version, the range to compare");
  }
  if (toVersion < fromVersion) throw new UsageError("--to-version must not be below --from-version");
  if (toVersion - fromVersion + 1n > MAX_COMPARE_RANGE) {
    throw new UsageError(`compare covers at most ${MAX_COMPARE_RANGE} versions at a time`);
  }
  const sui = chain(values.chain);
  const options: CompareOptions = {
    descriptor: descriptor("compare", values, positionals, sui),
    ...endpoint(values, sui, "compare"),
    against: values.against,
    fromVersion,
    toVersion,
  };
  withKey(options, values["api-key"]);
  const maxPages = integer("max-pages", values["max-pages"], 1, 1000);
  if (maxPages !== undefined) options.maxPages = maxPages;
  const limit = integer("limit", values.limit, 1, 1000);
  if (limit !== undefined) options.limit = limit;
  return { command: "compare", options };
}

const WINDOW = /^([1-9]\d*)([smhd])$/;
const LISTEN = /^(?:(.+):)?(\d+)$/;

function listenAddress(raw: string): { host: string; port: number } {
  const match = LISTEN.exec(raw);
  const port = match === null ? NaN : Number(match[2]);
  if (!(port >= 1 && port <= 65535)) throw new UsageError(`--listen must be a port or host:port, the port 1 to 65535: ${raw}`);
  // An IPv6 host is written in brackets, which listen() does not take.
  return { host: match![1]?.replace(/^\[(.*)\]$/, "$1") ?? "127.0.0.1", port };
}

function parseMonitor(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: MONITOR_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const sui = chain(values.chain);
  const base = endpoint(values, sui);
  if (base.nodeUrl === undefined || base.extraNodeUrls === undefined) {
    throw new UsageError("monitor needs --node-url at least twice, one per node to compare");
  }
  const none = positionals.length === 0 && Object.keys(DESCRIPTOR_OPTIONS).every((flag) => values[flag as keyof DescriptorFlags] === undefined);
  const window = values.window ?? "5m";
  const match = WINDOW.exec(window);
  if (match === null) throw new UsageError(`--window must be a duration such as 30s, 5m, 1h or 1d: ${window}`);
  const format = values.format ?? "json";
  if (format !== "json" && format !== "prometheus") throw new UsageError(`--format must be json or prometheus: ${format}`);
  const options: MonitorOptions = {
    descriptor: none && !sui ? { kind: "module", eventType: [MONITOR_DEFAULT_TYPE] } : descriptor("monitor", values, positionals, sui),
    ...base,
    nodeUrl: base.nodeUrl,
    extraNodeUrls: base.extraNodeUrls,
    interval: integer("interval", values.interval, 1, MAX_TIMEOUT) ?? 10_000,
    window: Number(match[1]) * Number(MS_PER[match[2]!]!),
    range: integer("range", values.range, 1, 10_000) ?? 100,
    format,
  };
  withKey(options, values["api-key"]);
  if (values.listen !== undefined) options.listen = listenAddress(values.listen);
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  return { command: "monitor", options };
}

const LOOPBACK = /^(?:localhost|127(?:\.\d{1,3}){3}|::1)$/;

function origin(raw: string): string {
  if (raw === "*") return raw;
  let parsed: URL | undefined;
  try {
    parsed = new URL(raw);
  } catch {
    parsed = undefined;
  }
  if (parsed === undefined || parsed.origin !== raw) throw new UsageError(`--allow-origin must be * or an origin such as https://app.example: ${raw}`);
  return raw;
}

function parseServe(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() => parseArgs({ args, options: SERVE_OPTIONS, allowPositionals: true, strict: true }));
  if (values.help) return { command: "help" };
  if (positionals.length > 0) throw new UsageError("serve takes no event types: clients name what they follow");
  const sui = chain(values.chain);
  const options: ServeOptions = {
    ...endpoint(values, sui),
    listen: values.listen === undefined ? { host: "127.0.0.1", port: DEFAULT_SERVE_PORT } : listenAddress(values.listen),
  };
  withKey(options, values["api-key"]);
  if (values.token === "") throw new UsageError("--token must not be empty");
  const token = values.token ?? (process.env[TOKEN_ENV] || undefined);
  if (token !== undefined) options.token = token;
  // A gateway spends its node's rate limit and API key for whoever reaches it.
  else if (!LOOPBACK.test(options.listen.host)) throw new UsageError(`serve needs --token (or ${TOKEN_ENV}) to listen on ${options.listen.host}`);
  if (values["allow-origin"] !== undefined) options.allowOrigins = values["allow-origin"].map(origin);
  const pollInterval = integer("poll-interval", values["poll-interval"], 1, MAX_TIMEOUT);
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  const maxTopics = integer("max-topics", values["max-topics"], 1, 10_000);
  if (maxTopics !== undefined) options.maxTopics = maxTopics;
  const maxConnections = integer("max-connections", values["max-connections"], 1, 1_000_000);
  if (maxConnections !== undefined) options.maxConnections = maxConnections;
  if (values.webhooks === undefined) {
    for (const flag of ["webhook-allow-private", "retries", "timeout", "batch-size"] as const) {
      if (values[flag] !== undefined) throw new UsageError(`--${flag} needs --webhooks`);
    }
    return { command: "serve", options };
  }
  if (values.webhooks === "") throw new UsageError("--webhooks must not be empty");
  const webhooks: NonNullable<ServeOptions["webhooks"]> = { file: values.webhooks };
  if (values["webhook-allow-private"]) webhooks.allowPrivateTargets = true;
  const retries = integer("retries", values.retries, 0, MAX_RETRIES);
  if (retries !== undefined) webhooks.retries = retries;
  const timeout = integer("timeout", values.timeout, 1, MAX_TIMEOUT);
  if (timeout !== undefined) webhooks.timeout = timeout;
  const batchSize = integer("batch-size", values["batch-size"], 1, 1000);
  if (batchSize !== undefined) webhooks.batchSize = batchSize;
  options.webhooks = webhooks;
  return { command: "serve", options };
}

function parseQuery(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: QUERY_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  if (fromVersion === undefined && fromTime === undefined) {
    throw new UsageError("query needs --from-version or --from-time");
  }
  const toVersion = version(values["to-version"], "to-version") ?? "head";
  if (toVersion !== "head" && fromVersion !== undefined && toVersion < fromVersion) {
    throw new UsageError("--to-version must not be below --from-version");
  }
  const order = values.order ?? "asc";
  if (order !== "asc" && order !== "desc") throw new UsageError(`--order must be asc or desc: ${order}`);
  const sui = chain(values.chain);
  const options: QueryOptions = {
    descriptor: descriptor("query", values, positionals, sui),
    ...endpoint(values, sui),
    toVersion,
    order,
  };
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  withKey(options, values["api-key"]);
  if (limit !== undefined) options.limit = limit;
  withCache(options, values.cache, sui);
  return { command: "query", options: withFilter(options, values) };
}

function parseBalances(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: BALANCES_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const [address, ...rest] = positionals;
  if (address === undefined || address === "" || rest.length > 0) {
    throw new UsageError("balances needs one address or name");
  }
  if (values.asset?.includes("")) throw new UsageError("--asset must not be empty");
  const options: BalancesCommandOptions = { address, ...endpoint(values, chain(values.chain)) };
  withKey(options, values["api-key"]);
  if (values.asset !== undefined) options.asset = values.asset;
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  return { command: "balances", options };
}

function parseWatch(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: WATCH_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length === 0) throw new UsageError("watch needs at least one address");
  const sui = chain(values.chain);
  const options: WatchCommandOptions = {
    addresses: positionals.map((raw) => checkedAddress(raw, sui)),
    ...endpoint(values, sui),
  };
  withKey(options, values["api-key"]);
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  if (values["cursor-file"] === "") throw new UsageError("--cursor-file must not be empty");
  if (values["cursor-file"] !== undefined) options.cursorFile = values["cursor-file"];
  return { command: "watch", options: withFilter(options, values) };
}

function parseInspect(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: INSPECT_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (chain(values.chain)) {
    throw new UsageError("inspect is not available with --chain sui: Sui reads object state only at the head");
  }
  const [raw, ...rest] = positionals;
  if (raw === undefined || rest.length > 0) throw new UsageError("inspect needs one address");
  if (values.resource === undefined) throw new UsageError("inspect needs --resource");
  const format = values.format ?? "json";
  if (format !== "json" && format !== "pretty") throw new UsageError(`--format must be json or pretty: ${format}`);
  const options: InspectOptions = {
    address: wrap(() => requireAddress(raw)),
    resourceType: wrap(() => requireResourceType(values.resource)),
    ...endpoint(values, false),
    format,
  };
  withKey(options, values["api-key"]);
  const at = version(values.at, "at");
  const fromVersion = version(values["from-version"]);
  if (at !== undefined) {
    for (const flag of ["from-version", "to-version", "limit"] as const) {
      if (values[flag] !== undefined) throw new UsageError(`set --at or --${flag}, not both`);
    }
    options.at = at;
    return { command: "inspect", options };
  }
  if (fromVersion === undefined) throw new UsageError("inspect needs --at or --from-version");
  const toVersion = values["to-version"] === "head" ? "head" : (version(values["to-version"], "to-version") ?? "head");
  if (toVersion !== "head" && toVersion < fromVersion) {
    throw new UsageError("--to-version must not be below --from-version");
  }
  options.fromVersion = fromVersion;
  options.toVersion = toVersion;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  return { command: "inspect", options };
}

function parseHandles(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: HANDLES_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const [address, ...rest] = positionals;
  if (address === undefined || address === "" || rest.length > 0) {
    throw new UsageError("handles needs one address or name");
  }
  if (chain(values.chain)) throw new UsageError("handles is not available with --chain sui: Sui has no event handles");
  const options: HandlesOptions = { address, ...endpoint(values, false, "handles") };
  return { command: "handles", options: withKey(options, values["api-key"]) };
}

function parseName(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: HANDLES_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const [value, ...rest] = positionals;
  if (value === undefined || value === "" || rest.length > 0) {
    throw new UsageError("name needs one name or address");
  }
  const options: NameOptions = { value, ...endpoint(values, chain(values.chain), "name") };
  return { command: "name", options: withKey(options, values["api-key"]) };
}

type WalkFlags = { [K in keyof typeof WALK_OPTIONS]?: string };

/**
 * The flags `transfers`, `swaps` and `sales` share: where the walk starts,
 * whether it follows or reads a range, and how.
 */
function recordWalk<T extends RecordWalkOptions>(options: T, values: WalkFlags): T {
  if (values["cursor-file"] === "") throw new UsageError("--cursor-file must not be empty");
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  const raw = values["to-version"];
  const toVersion = raw === "head" ? "head" : version(raw, "to-version");
  if (toVersion === undefined) {
    if (values.order !== undefined) throw new UsageError("--order needs --to-version");
  } else {
    for (const flag of ["cursor-file", "poll-interval", "batch-size"] as const) {
      if (values[flag] !== undefined) throw new UsageError(`--${flag} follows live, so it cannot be combined with --to-version`);
    }
    if (fromVersion === undefined && fromTime === undefined) {
      throw new UsageError("--to-version needs --from-version or --from-time, where the range starts");
    }
    if (toVersion !== "head" && fromVersion !== undefined && toVersion < fromVersion) {
      throw new UsageError("--to-version must not be below --from-version");
    }
    const order = values.order ?? "asc";
    if (order !== "asc" && order !== "desc") throw new UsageError(`--order must be asc or desc: ${order}`);
    options.toVersion = toVersion;
    options.order = order;
  }
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  const batchSize = integer("batch-size", values["batch-size"], 1, 1000);
  if (batchSize !== undefined) options.batchSize = batchSize;
  if (values["cursor-file"] !== undefined) options.cursorFile = values["cursor-file"];
  return options;
}

function parseTransfers(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: TRANSFERS_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length > 0) throw new UsageError(`transfers takes no event types: ${positionals[0]}`);
  for (const flag of ["asset", "account"] as const) {
    if (values[flag] === "") throw new UsageError(`--${flag} must not be empty`);
  }
  const options: TransfersOptions = recordWalk(withKey(endpoint(values, chain(values.chain)), values["api-key"]), values);
  if (values.asset !== undefined) options.asset = values.asset;
  if (values.account !== undefined) options.account = values.account;
  if (values["with-asset-info"]) options.withAssetInfo = true;
  return { command: "transfers", options };
}

/** `swaps` and `sales`: Aptos only, a repeatable name flag and one narrowing value. */
function aptosRecords(command: "swaps" | "sales", values: WalkFlags & EndpointFlags & { chain?: string; "api-key"?: string; help?: boolean }, positionals: string[]) {
  if (chain(values.chain)) {
    throw new UsageError(`${command} is not available with --chain sui: SuiClient has no ${command} yet`);
  }
  if (positionals.length > 0) throw new UsageError(`${command} takes no event types: ${positionals[0]}`);
  return recordWalk(withKey(endpoint(values, false), values["api-key"]), values);
}

function names(flag: string, raw: string[] | undefined): string[] | undefined {
  if (raw?.some((name) => name === "")) throw new UsageError(`--${flag} must not be empty`);
  return raw;
}

function parseSwaps(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: SWAPS_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (values.asset === "") throw new UsageError("--asset must not be empty");
  const protocol = names("protocol", values.protocol);
  const options: SwapsCommandOptions = aptosRecords("swaps", values, positionals);
  if (protocol !== undefined) options.protocol = protocol;
  if (values.asset !== undefined) options.asset = values.asset;
  return { command: "swaps", options };
}

function parseSales(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: SALES_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (values.token === "") throw new UsageError("--token must not be empty");
  const marketplace = names("marketplace", values.marketplace);
  const options: SalesCommandOptions = aptosRecords("sales", values, positionals);
  if (marketplace !== undefined) options.marketplace = marketplace;
  if (values.token !== undefined) options.token = values.token;
  return { command: "sales", options };
}

/** The one address or name `activity` and `trace` take, and their Indexer; Aptos only. */
function indexedTarget(command: "activity" | "trace" | "holders", values: { chain?: string; indexer?: string }, positionals: string[]) {
  if (chain(values.chain)) {
    throw new UsageError(`${command} is not available with --chain sui: SuiClient has no ${command} yet`);
  }
  const [address, ...rest] = positionals;
  if (address === undefined || address === "" || rest.length > 0) {
    throw new UsageError(`${command} needs one ${command === "holders" ? "asset" : "address or name"}`);
  }
  if (values.indexer !== undefined && httpUrl(values.indexer) === undefined) {
    throw new UsageError(`--indexer must be an http or https URL: ${values.indexer}`);
  }
  return { address, ...(values.indexer !== undefined && { indexer: values.indexer }) };
}

function parseActivity(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: ACTIVITY_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const target = indexedTarget("activity", values, positionals);
  const options: ActivityCommandOptions = recordWalk({ ...target, ...withKey(endpoint(values, false), values["api-key"]) }, values);
  if (values["with-asset-info"]) options.withAssetInfo = true;
  return { command: "activity", options };
}

function parseTrace(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: TRACE_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const target = indexedTarget("trace", values, positionals);
  const format = values.format ?? "json";
  if (format !== "json" && format !== "dot") throw new UsageError(`--format must be json or dot: ${format}`);
  const options: TraceCommandOptions = { ...target, format, ...withKey(endpoint(values, false), values["api-key"]) };
  const hops = integer("hops", values.hops, 1, 6);
  if (hops !== undefined) options.hops = hops;
  const { direction } = values;
  if (direction !== undefined) {
    if (direction !== "out" && direction !== "in" && direction !== "both") throw new UsageError(`--direction must be out, in or both: ${direction}`);
    options.direction = direction;
  }
  const fromVersion = version(values["from-version"]);
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  const raw = values["to-version"];
  const toVersion = raw === "head" ? "head" : version(raw, "to-version");
  if (toVersion !== undefined) {
    if (toVersion !== "head" && fromVersion !== undefined && toVersion < fromVersion) {
      throw new UsageError("--to-version must not be below --from-version");
    }
    options.toVersion = toVersion;
  }
  const minAmount = version(values["min-amount"], "min-amount");
  if (minAmount !== undefined) options.minAmount = minAmount;
  for (const flag of ["asset", "target"] as const) {
    if (values[flag] === "") throw new UsageError(`--${flag} must not be empty`);
  }
  if (values.asset !== undefined) options.asset = values.asset;
  if (values.target !== undefined) options.target = values.target;
  const maxNodes = integer("max-nodes", values["max-nodes"], 1, Number.MAX_SAFE_INTEGER);
  if (maxNodes !== undefined) options.maxNodes = maxNodes;
  const maxRequests = integer("max-requests", values["max-requests"], 1, Number.MAX_SAFE_INTEGER);
  if (maxRequests !== undefined) options.maxRequests = maxRequests;
  const stop = names("stop", values.stop);
  if (stop !== undefined) options.stop = stop;
  return { command: "trace", options };
}

function parseHolders(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: HOLDERS_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const { address: asset, ...indexer } = indexedTarget("holders", values, positionals);
  const at = version(values.at, "at");
  if (at === undefined) throw new UsageError("holders needs --at <version>");
  const format = values.format ?? "json";
  if (format !== "json" && format !== "csv") throw new UsageError(`--format must be json or csv: ${format}`);
  const options: HoldersCommandOptions = { asset, at, ...indexer, format, ...withKey(endpoint(values, false), values["api-key"]) };
  const { verify } = values;
  if (verify !== undefined) {
    if (verify !== "none" && verify !== "supply" && verify !== "stores") throw new UsageError(`--verify must be none, supply or stores: ${verify}`);
    options.verify = verify;
  }
  const maxRequests = integer("max-requests", values["max-requests"], 1, Number.MAX_SAFE_INTEGER);
  if (maxRequests !== undefined) options.maxRequests = maxRequests;
  return { command: "holders", options };
}

const TRANSACTION_HASH = /^0x[0-9a-fA-F]{64}$/;
// Base58 of 32 bytes; SuiClient checks the exact length.
const SUI_DIGEST = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

function parseTx(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: TX_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const [hash, ...rest] = positionals;
  if (hash === undefined || rest.length > 0) throw new UsageError("tx needs one transaction hash");
  const sui = chain(values.chain);
  if (sui && !SUI_DIGEST.test(hash)) throw new UsageError(`not a Sui transaction digest: ${hash}`);
  if (!sui && !TRANSACTION_HASH.test(hash)) throw new UsageError(`not a transaction hash: ${hash}`);
  const options: TxOptions = { hash, ...endpoint(values, sui) };
  const timeout = integer("timeout", values.timeout, 0, MAX_TIMEOUT);
  withKey(options, values["api-key"]);
  if (timeout !== undefined) options.timeout = timeout;
  return { command: "tx", options };
}

function parseExplain(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: EXPLAIN_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (chain(values.chain)) throw new UsageError("explain is not available with --chain sui: Sui has no transaction explainer yet");
  const [hash, ...rest] = positionals;
  if (hash === undefined || rest.length > 0) throw new UsageError("explain needs one transaction hash");
  if (!TRANSACTION_HASH.test(hash)) throw new UsageError(`not a transaction hash: ${hash}`);
  const options: ExplainCommandOptions = { hash, ...endpoint(values, false) };
  withKey(options, values["api-key"]);
  const timeout = integer("timeout", values.timeout, 0, MAX_TIMEOUT);
  if (timeout !== undefined) options.timeout = timeout;
  if (values.json) options.json = true;
  return { command: "explain", options };
}

function parseCheck(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: CHECK_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length === 0) throw new UsageError("check needs at least one event type");
  const sui = chain(values.chain);
  const options: CheckOptions = {
    descriptor: descriptor("check", {}, positionals, sui) as ModuleEventDescriptor,
    ...endpoint(values, sui, "check"),
  };
  withKey(options, values["api-key"]);
  return { command: "check", options };
}

function moduleRef(raw: string, sui: boolean): ModuleRef {
  const check = (value: string) => void checkedAddress(value, sui);
  if (!raw.includes("::")) {
    check(raw);
    return { address: raw };
  }
  const [address = "", module = "", ...rest] = raw.split("::");
  if (rest.length > 0 || !MODULE_NAME.test(module)) {
    throw new UsageError(`expected <address> or <address>::<module>: ${raw}`);
  }
  check(address);
  return { address, module };
}

function parseCodegen(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: CODEGEN_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length === 0) throw new UsageError("codegen needs at least one <address> or <address>::<module>");
  const sui = chain(values.chain);
  const options: CodegenOptions = {
    modules: positionals.map((p) => moduleRef(p, sui)),
    ...endpoint(values, sui, "codegen"),
  };
  withKey(options, values["api-key"]);
  if (values.out !== undefined) {
    if (values.out === "") throw new UsageError("--out must not be empty");
    options.out = values.out;
  }
  if (values.validators) options.validators = true;
  if (values.arbitraries) options.arbitraries = true;
  if (values.sql !== undefined) {
    if (values.sql !== "sqlite" && values.sql !== "postgres") throw new UsageError(`--sql must be sqlite or postgres: ${values.sql}`);
    options.sql = values.sql;
  }
  if (values.check) {
    if (options.out === undefined) throw new UsageError("--check needs --out <file> to compare with");
    options.check = true;
  }
  return { command: "codegen", options };
}

function parseCoverage(args: string[]): ParsedCommand {
  const split = args.indexOf("--");
  const own = split === -1 ? args : args.slice(0, split);
  const { values, positionals } = wrap(() =>
    parseArgs({ args: own, options: COVERAGE_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  const [packageDir, ...rest] = positionals;
  if (packageDir === undefined || packageDir === "" || rest.length > 0) {
    throw new UsageError("coverage needs one Move package directory, then -- and the test command");
  }
  const command = split === -1 ? [] : args.slice(split + 1);
  if (command.length === 0 || command[0] === "") throw new UsageError("coverage needs a test command after --");
  const format = values.format ?? "json";
  if (format !== "json" && format !== "pretty") throw new UsageError("--format must be json or pretty");
  return { command: "coverage", options: { packageDir, command, format } };
}

/** Parses `argv` (without the node and script paths) into a command. */
export function parseCommand(argv: readonly string[]): ParsedCommand {
  const [command, ...rest] = argv;
  if (command === undefined || command === "-h" || command === "--help") {
    return { command: "help" };
  }
  if (command === "tail") return parseTail(rest);
  if (command === "forward") return parseForward(rest);
  if (command === "alert") return parseAlert(rest);
  if (command === "store") return parseStore(rest);
  if (command === "check") return parseCheck(rest);
  if (command === "record") return parseRecord(rest);
  if (command === "query") return parseQuery(rest);
  if (command === "tx") return parseTx(rest);
  if (command === "explain") return parseExplain(rest);
  if (command === "transfers") return parseTransfers(rest);
  if (command === "swaps") return parseSwaps(rest);
  if (command === "sales") return parseSales(rest);
  if (command === "activity") return parseActivity(rest);
  if (command === "trace") return parseTrace(rest);
  if (command === "holders") return parseHolders(rest);
  if (command === "balances") return parseBalances(rest);
  if (command === "watch") return parseWatch(rest);
  if (command === "inspect") return parseInspect(rest);
  if (command === "handles") return parseHandles(rest);
  if (command === "name") return parseName(rest);
  if (command === "codegen") return parseCodegen(rest);
  if (command === "coverage") return parseCoverage(rest);
  if (command === "compare") return parseCompare(rest);
  if (command === "monitor") return parseMonitor(rest);
  if (command === "serve") return parseServe(rest);
  throw new UsageError(`unknown command: ${command}`);
}
