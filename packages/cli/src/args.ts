import { parseArgs } from "node:util";
import { normalizeAddress, requireAddress, requireEventType, requireResourceType } from "@streamd/core";
import type { EventDescriptor, ModuleEventDescriptor, MoveEventType } from "@streamd/core";
import type { ModuleRef } from "./codegen.js";
import { FILTER_OPTIONS, parseDecode, parseFilter } from "./filter.js";
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

export const SIGNATURE_HEADER = "x-streamd-signature";
export const SECRET_ENV = "STREAMD_WEBHOOK_SECRET";
export const API_KEY_ENV = "APTOS_API_KEY";

export interface ForwardOptions extends SourceOptions {
  /** Webhook URL; may carry a token, so it is never printed. */
  to: string;
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

/** Bounded when `toVersion` is set, through `getTransfers`; otherwise followed, through `transfers`. */
export interface TransfersOptions extends Endpoint {
  /** An asset as the chain's client takes it; validated there. */
  asset?: string;
  /** An address or name; validated and resolved by the chain's client. */
  account?: string;
  withAssetInfo?: true;
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

export interface CheckOptions extends Endpoint {
  descriptor: ModuleEventDescriptor;
}

export interface CodegenOptions extends Endpoint {
  modules: ModuleRef[];
  /** File to write; stdout when unset. */
  out?: string;
  /** Also write an `is<Type>` runtime check per generated type. */
  validators?: boolean;
}

export type ParsedCommand =
  | { command: "help" }
  | { command: "tail"; options: TailOptions }
  | { command: "forward"; options: ForwardOptions }
  | { command: "store"; options: StoreOptions }
  | { command: "check"; options: CheckOptions }
  | { command: "record"; options: RecordOptions }
  | { command: "query"; options: QueryOptions }
  | { command: "tx"; options: TxOptions }
  | { command: "transfers"; options: TransfersOptions }
  | { command: "balances"; options: BalancesCommandOptions }
  | { command: "watch"; options: WatchCommandOptions }
  | { command: "handles"; options: HandlesOptions }
  | { command: "name"; options: NameOptions }
  | { command: "codegen"; options: CodegenOptions }
  | { command: "compare"; options: CompareOptions };

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
const READ_OPTIONS = { ...ENDPOINT_OPTIONS, archive: { type: "string" } } as const;

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

const FORWARD_OPTIONS = {
  ...SOURCE_OPTIONS,
  ...FROM_TIME,
  to: { type: "string" },
  "cursor-file": { type: "string" },
  retries: { type: "string" },
  timeout: { type: "string" },
  header: { type: "string", multiple: true },
  secret: { type: "string" },
  "to-version": { type: "string" },
  "batch-size": { type: "string" },
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

const TRANSFERS_OPTIONS = {
  ...READ_OPTIONS,
  ...FROM_TIME,
  asset: { type: "string" },
  account: { type: "string" },
  "with-asset-info": { type: "boolean" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  order: { type: "string" },
  "poll-interval": { type: "string" },
  "batch-size": { type: "string" },
  "cursor-file": { type: "string" },
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

const CHECK_OPTIONS = {
  ...ENDPOINT_OPTIONS,
  help: { type: "boolean", short: "h" },
} as const;

const CODEGEN_OPTIONS = {
  ...ENDPOINT_OPTIONS,
  out: { type: "string" },
  validators: { type: "boolean" },
  help: { type: "boolean", short: "h" },
} as const;

const MODULE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

const DEFAULT_CURSOR_FILE = "streamd-forward.cursor";
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
  streamd store --db <file> [options] <eventType>...
  streamd store --db <file> --account <address> [...]
  streamd store --postgres <url> [options] <eventType>...
  streamd query (--from-version <n> | --from-time <t>) [--to-version <n>] [options] <eventType>...
  streamd tx <hash | digest>
  streamd transfers [--asset <asset>] [--account <address | name>] [--to-version <n | head>] [options]
  streamd balances <address | name> [--asset <asset>]... [options]
  streamd watch <address>... [--cursor-file <path>] [options]
  streamd handles <address | name>
  streamd name <name | address>
  streamd check <eventType>...
  streamd record --from-version <n> --to-version <n> [--out <file>] [--cache <file>] <eventType>...
  streamd codegen [--out <file>] [--validators] (<address> | <address>::<module>)...
  streamd compare --against <url> --from-version <n> --to-version <n> [options] <eventType>...

tail follows matching events and prints each as one JSON line on stdout.
forward POSTs each batch of matching events to a webhook as a JSON array,
saving its position to a cursor file once the webhook acknowledges it.
store follows matching events into a SQLite file, or a Postgres database
with --postgres, resuming from the cursor saved there when run again.
query reads a closed range of history as JSON lines, then exits.
tx waits for a transaction and prints its events, exiting 1 if it failed.
transfers prints asset transfers as JSON lines: followed live, or with
--to-version read over a closed range, then exits.
balances prints a wallet's balance of each asset once, then a JSON line each
time one changes, read at the head every --poll-interval.
watch follows the transactions each address sent, one walk per address, and
prints each event as a JSON line with the watched address as "address".
handles lists every event handle on an Aptos account as a JSON line, with
its descriptor and the tail flags that follow it.
name resolves a name to its address, or an address (0x...) to its primary
name, as one JSON line; name is null when the address has none.
check says whether each event type can be emitted, and exits 1 if one cannot.
record runs a bounded query and writes a recording for replayAdapter tests.
compare reads one range from two nodes and prints what one has that the other
does not, exiting 1 if they disagree.
codegen reads module ABIs and writes TypeScript types and defineEvent
descriptors for their event structs; a bare address covers all its modules.
With --chain sui it reads Sui packages, where every struct or enum with copy
and drop is an event.
Every command but handles takes --chain sui to read Sui instead of Aptos.
tail, query, forward, store and watch take --sender, --function and --where, which
filter after fetching, so --limit counts the events that match; they also take
--decode, which checks integer fields before --where runs.

Options:
  --account <address>    events from transactions this account sent; transfers: what this
                         address or name (ANS, or SuiNS with --chain sui) sent or received
  --asset <asset>        transfers: only this asset, a fungible-asset or token object address,
                         a coin type or a legacy token id (a coin type with --chain sui)
                         balances: an asset to follow, repeatable (default: APT, or SUI)
  --with-asset-info      transfers: add each asset's symbol and decimals
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --creation-number <n>  event handle by creation number (with --account)
  --resource <type>      every change to this resource (with --account)
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
  --from-time <t>        tail, forward, query, watch: start here, as 2h, 7d or an ISO date (UTC)
  --poll-interval <ms>   delay between polls (default: 1000)
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
  --limit <n>            tail, transfers, watch: exit after printing n; balances: after n changes;
                         record, query: stop after n; compare: page size
  --to <url>             forward: webhook URL (http or https, required)
  --cursor-file <path>   tail, transfers (following): save and resume from here; watch: each
                         address's position, as one JSON object; forward: cursor file
                         (default: ${DEFAULT_CURSOR_FILE})
  --format <name>        tail: json, or pretty for aligned lines in UTC (default: json)
  --retries <n>          forward: retries per batch (default: ${DEFAULT_RETRIES})
  --timeout <ms>         forward: per-request timeout (default: ${DEFAULT_TIMEOUT});
                         tx: wait for commit (default: 20000)
  --header <h>           forward: request header 'Name: value', repeatable
  --secret <s>           forward: sign each body with HMAC-SHA256 in ${SIGNATURE_HEADER}
                         (or set ${SECRET_ENV}, which keeps it out of ps)
  --db <file>            store: SQLite file to write (this or --postgres)
  --postgres <url>       store: Postgres database to write, postgres://user@host/db; Bun.SQL
                         under Bun, pg under Node (npm i pg); the URL is never printed, and
                         PGPASSWORD keeps the password out of it and out of ps
  --name <name>          store: name its cursor is saved under (default: the network and descriptor)
  --to-version <n>       record: end of the range (required); query: default the head;
                         forward: deliver up to here, then exit 0; transfers: a version or
                         head, read the range instead of following (needs a start)
  --batch-size <n>       forward: most events per POST (default: 25); transfers (following):
                         transactions per page (default: 25)
  --order <asc|desc>     query, transfers with --to-version: oldest or newest first (default: asc)
  --out <file>           codegen, record: write to this file instead of stdout
  --cache <file>         query, record: keep settled pages in this SQLite file and read them
                         from it on the next run of the same range (Aptos only)
  --validators           codegen: also write an is<Type> runtime check per type
  --chain <name>         aptos or sui (default: aptos); with sui, --network is mainnet,
                         testnet or devnet, --node-url and --against are Sui gRPC URLs,
                         tx takes a base58 digest, and --handle, --field,
                         --creation-number, --resource and --api-key are refused
  --against <url>        compare: the second node's fullnode URL (required)
  --max-pages <n>        compare: most pages read from each node (default: 10)
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

function webhookUrl(raw: string | undefined): string {
  if (raw === undefined) throw new UsageError("forward needs --to <url>");
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

function descriptor(
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
  const options: ForwardOptions = {
    ...source("forward", values, positionals),
    to: webhookUrl(values.to),
    cursorFile: values["cursor-file"] ?? DEFAULT_CURSOR_FILE,
    retries: integer("retries", values.retries, 0, MAX_RETRIES) ?? DEFAULT_RETRIES,
    timeout: integer("timeout", values.timeout, 1, MAX_TIMEOUT) ?? DEFAULT_TIMEOUT,
    headers: headers(values.header),
  };
  if (options.cursorFile === "") throw new UsageError("--cursor-file must not be empty");
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
  if (values.secret === "") throw new UsageError("--secret must not be empty");
  const secret = values.secret ?? (process.env[SECRET_ENV] || undefined);
  if (secret !== undefined) {
    if (SIGNATURE_HEADER in options.headers) {
      throw new UsageError(`--header cannot set ${SIGNATURE_HEADER} when signing`);
    }
    options.secret = secret;
  }
  return { command: "forward", options };
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

function parseTransfers(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: TRANSFERS_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length > 0) throw new UsageError(`transfers takes no event types: ${positionals[0]}`);
  for (const flag of ["asset", "account", "cursor-file"] as const) {
    if (values[flag] === "") throw new UsageError(`--${flag} must not be empty`);
  }
  const sui = chain(values.chain);
  const options: TransfersOptions = endpoint(values, sui);
  withKey(options, values["api-key"]);
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
  if (values.asset !== undefined) options.asset = values.asset;
  if (values.account !== undefined) options.account = values.account;
  if (values["with-asset-info"]) options.withAssetInfo = true;
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (limit !== undefined) options.limit = limit;
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  const batchSize = integer("batch-size", values["batch-size"], 1, 1000);
  if (batchSize !== undefined) options.batchSize = batchSize;
  if (values["cursor-file"] !== undefined) options.cursorFile = values["cursor-file"];
  return { command: "transfers", options };
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
  return { command: "codegen", options };
}

/** Parses `argv` (without the node and script paths) into a command. */
export function parseCommand(argv: readonly string[]): ParsedCommand {
  const [command, ...rest] = argv;
  if (command === undefined || command === "-h" || command === "--help") {
    return { command: "help" };
  }
  if (command === "tail") return parseTail(rest);
  if (command === "forward") return parseForward(rest);
  if (command === "store") return parseStore(rest);
  if (command === "check") return parseCheck(rest);
  if (command === "record") return parseRecord(rest);
  if (command === "query") return parseQuery(rest);
  if (command === "tx") return parseTx(rest);
  if (command === "transfers") return parseTransfers(rest);
  if (command === "balances") return parseBalances(rest);
  if (command === "watch") return parseWatch(rest);
  if (command === "handles") return parseHandles(rest);
  if (command === "name") return parseName(rest);
  if (command === "codegen") return parseCodegen(rest);
  if (command === "compare") return parseCompare(rest);
  throw new UsageError(`unknown command: ${command}`);
}
