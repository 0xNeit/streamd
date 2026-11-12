import { parseArgs } from "node:util";
import { normalizeAddress, requireAddress, requireEventType, requireResourceType } from "@streamd/core";
import type { EventDescriptor, ModuleEventDescriptor, MoveEventType } from "@streamd/core";
import type { ModuleRef } from "./codegen.js";

export type Network = "mainnet" | "testnet" | "devnet" | "movement-mainnet" | "movement-testnet";

/** Which chain and node a command reads. */
export interface Endpoint {
  /** Unset for Aptos. */
  chain?: "sui";
  /** With `chain: "sui"`, only mainnet, testnet or devnet. */
  network: Network;
  /** A fullnode URL, or a gRPC URL for Sui. */
  nodeUrl?: string;
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
  /** SQLite file to write. */
  db: string;
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
  limit?: number;
  order: "asc" | "desc";
}

export interface RecordOptions extends Endpoint {
  descriptor: EventDescriptor;
  fromVersion: bigint;
  toVersion: bigint;
  limit?: number;
  /** File to write; stdout when unset. */
  out?: string;
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
  "node-url": { type: "string" },
  "api-key": { type: "string" },
} as const;

const SOURCE_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  ...ENDPOINT_OPTIONS,
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
  ...ENDPOINT_OPTIONS,
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  out: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const QUERY_OPTIONS = {
  ...FROM_TIME,
  ...DESCRIPTOR_OPTIONS,
  ...ENDPOINT_OPTIONS,
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  order: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const TX_OPTIONS = {
  ...ENDPOINT_OPTIONS,
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
  streamd query (--from-version <n> | --from-time <t>) [--to-version <n>] [options] <eventType>...
  streamd tx <hash | digest>
  streamd check <eventType>...
  streamd record --from-version <n> --to-version <n> [--out <file>] <eventType>...
  streamd codegen [--out <file>] [--validators] (<address> | <address>::<module>)...
  streamd compare --against <url> --from-version <n> --to-version <n> [options] <eventType>...

tail follows matching events and prints each as one JSON line on stdout.
forward POSTs each batch of matching events to a webhook as a JSON array,
saving its position to a cursor file once the webhook acknowledges it.
store follows matching events into a SQLite file, resuming from the cursor
saved there when run again.
query reads a closed range of history as JSON lines, then exits.
tx waits for a transaction and prints its events, exiting 1 if it failed.
check says whether each event type can be emitted, and exits 1 if one cannot.
record runs a bounded query and writes a recording for replayAdapter tests.
compare reads one range from two nodes and prints what one has that the other
does not, exiting 1 if they disagree.
codegen reads module ABIs and writes TypeScript types and defineEvent
descriptors for their event structs; a bare address covers all its modules.
With --chain sui it reads Sui packages, where every struct or enum with copy
and drop is an event.
Every command takes --chain sui to read Sui instead of Aptos.

Options:
  --account <address>    events from transactions this account sent
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --creation-number <n>  event handle by creation number (with --account)
  --resource <type>      every change to this resource (with --account)
  --network <name>       mainnet, testnet, devnet, movement-mainnet, or movement-testnet
                         (default: mainnet)
  --node-url <url>       fullnode URL, overriding the network's default
  --api-key <key>        fullnode API key, sent as a bearer token (or set ${API_KEY_ENV}, used
                         only on mainnet, testnet and devnet without --node-url)
  --from-version <n>     start at this version instead of the chain head
  --from-time <t>        tail, forward, query: start here, as 2h, 7d or an ISO date (UTC)
  --poll-interval <ms>   delay between polls (default: 1000)
  --limit <n>            tail: exit after printing n events; record, query: stop after n;
                         compare: page size
  --to <url>             forward: webhook URL (http or https, required)
  --cursor-file <path>   tail: save and resume from here; forward: cursor file
                         (default: ${DEFAULT_CURSOR_FILE})
  --format <name>        tail: json, or pretty for aligned lines in UTC (default: json)
  --retries <n>          forward: retries per batch (default: ${DEFAULT_RETRIES})
  --timeout <ms>         forward: per-request timeout (default: ${DEFAULT_TIMEOUT});
                         tx: wait for commit (default: 20000)
  --header <h>           forward: request header 'Name: value', repeatable
  --secret <s>           forward: sign each body with HMAC-SHA256 in ${SIGNATURE_HEADER}
                         (or set ${SECRET_ENV}, which keeps it out of ps)
  --db <file>            store: SQLite file to write (required)
  --name <name>          store: name its cursor is saved under (default: the network and descriptor)
  --to-version <n>       record: end of the range (required); query: default the head;
                         forward: deliver up to here, then exit 0
  --batch-size <n>       forward: most events per POST (default: 25)
  --order <asc|desc>     query: oldest or newest first (default: asc)
  --out <file>           codegen, record: write to this file instead of stdout
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
  "node-url"?: string;
  "api-key"?: string;
}

function endpoint(values: EndpointFlags, sui: boolean): Endpoint {
  const options: Endpoint = { network: network(values.network) };
  const url = nodeUrl(values["node-url"]);
  if (url !== undefined) options.nodeUrl = url;
  if (sui) {
    if (!SUI_NETWORKS.includes(options.network)) {
      throw new UsageError(`--network with --chain sui must be one of ${SUI_NETWORKS.join(", ")}: ${options.network}`);
    }
    // Public Sui nodes take no key, so none is sent.
    if (values["api-key"] !== undefined) throw new UsageError("--api-key is not sent to Sui nodes");
    options.chain = "sui";
  } else {
    const key = apiKey(values["api-key"], options.network, url);
    if (key !== undefined) options.apiKey = key;
  }
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

function nodeUrl(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined;
  if (httpUrl(raw) === undefined) {
    throw new UsageError(`--node-url must be an http or https URL: ${raw}`);
  }
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
    const accountAddress = checkedAddress(flags.account, false);
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

interface SourceFlags extends DescriptorFlags, EndpointFlags {
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
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  return options;
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

function parseStore(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: STORE_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (values.db === undefined) throw new UsageError("store needs --db <file>");
  if (values.db === "") throw new UsageError("--db must not be empty");
  if (values.name === "") throw new UsageError("--name must not be empty");
  const options: StoreOptions = { ...source("store", values, positionals), db: values.db };
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
  if (limit !== undefined) options.limit = limit;
  if (values.out !== undefined) options.out = values.out;
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
    ...endpoint(values, sui),
    against: values.against,
    fromVersion,
    toVersion,
  };
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
  if (limit !== undefined) options.limit = limit;
  return { command: "query", options };
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
    ...endpoint(values, sui),
  };
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
    ...endpoint(values, sui),
  };
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
  if (command === "codegen") return parseCodegen(rest);
  if (command === "compare") return parseCompare(rest);
  throw new UsageError(`unknown command: ${command}`);
}
