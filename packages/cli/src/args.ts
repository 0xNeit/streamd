import { parseArgs } from "node:util";
import { requireAddress, requireEventType, requireResourceType } from "@streamd/core";
import type { EventDescriptor, ModuleEventDescriptor, MoveEventType } from "@streamd/core";
import type { ModuleRef } from "./codegen.js";

export type Network = "mainnet" | "testnet" | "devnet" | "movement-mainnet" | "movement-testnet";

/** Where events come from: shared by `tail` and `forward`. */
export interface SourceOptions {
  descriptor: EventDescriptor;
  network: Network;
  nodeUrl?: string;
  fromVersion?: bigint;
  /** Microseconds since the epoch; never set with `fromVersion`. */
  fromTime?: bigint;
  pollInterval?: number;
}

export interface TailOptions extends SourceOptions {
  limit?: number;
}

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
}

export interface StoreOptions extends SourceOptions {
  /** SQLite file to write. */
  db: string;
  /** Name its cursor is saved under; derived from the network and descriptor when unset. */
  name?: string;
}

export interface QueryOptions {
  descriptor: EventDescriptor;
  network: Network;
  nodeUrl?: string;
  /** Exactly one of `fromVersion` and `fromTime` is set. */
  fromVersion?: bigint;
  fromTime?: bigint;
  toVersion: bigint | "head";
  limit?: number;
  order: "asc" | "desc";
}

export interface RecordOptions {
  descriptor: EventDescriptor;
  network: Network;
  nodeUrl?: string;
  fromVersion: bigint;
  toVersion: bigint;
  limit?: number;
  /** File to write; stdout when unset. */
  out?: string;
}

export interface CheckOptions {
  descriptor: ModuleEventDescriptor;
  network: Network;
  nodeUrl?: string;
}

export interface CodegenOptions {
  modules: ModuleRef[];
  network: Network;
  nodeUrl?: string;
  /** File to write; stdout when unset. */
  out?: string;
}

export type ParsedCommand =
  | { command: "help" }
  | { command: "tail"; options: TailOptions }
  | { command: "forward"; options: ForwardOptions }
  | { command: "store"; options: StoreOptions }
  | { command: "check"; options: CheckOptions }
  | { command: "record"; options: RecordOptions }
  | { command: "query"; options: QueryOptions }
  | { command: "codegen"; options: CodegenOptions };

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

const SOURCE_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  network: { type: "string" },
  "node-url": { type: "string" },
  "from-version": { type: "string" },
  "poll-interval": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const FROM_TIME = { "from-time": { type: "string" } } as const;

const TAIL_OPTIONS = { ...SOURCE_OPTIONS, ...FROM_TIME, limit: { type: "string" } } as const;

const FORWARD_OPTIONS = {
  ...SOURCE_OPTIONS,
  ...FROM_TIME,
  to: { type: "string" },
  "cursor-file": { type: "string" },
  retries: { type: "string" },
  timeout: { type: "string" },
  header: { type: "string", multiple: true },
} as const;

const STORE_OPTIONS = {
  ...SOURCE_OPTIONS,
  db: { type: "string" },
  name: { type: "string" },
} as const;

const RECORD_OPTIONS = {
  ...DESCRIPTOR_OPTIONS,
  network: { type: "string" },
  "node-url": { type: "string" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  out: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const QUERY_OPTIONS = {
  ...FROM_TIME,
  ...DESCRIPTOR_OPTIONS,
  network: { type: "string" },
  "node-url": { type: "string" },
  "from-version": { type: "string" },
  "to-version": { type: "string" },
  limit: { type: "string" },
  order: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const CHECK_OPTIONS = {
  network: { type: "string" },
  "node-url": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const CODEGEN_OPTIONS = {
  network: { type: "string" },
  "node-url": { type: "string" },
  out: { type: "string" },
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
  streamd check <eventType>...
  streamd record --from-version <n> --to-version <n> [--out <file>] <eventType>...
  streamd codegen [--out <file>] <address>::<module>...

tail follows matching events and prints each as one JSON line on stdout.
forward POSTs each batch of matching events to a webhook as a JSON array,
saving its position to a cursor file once the webhook acknowledges it.
store follows matching events into a SQLite file, resuming from the cursor
saved there when run again.
query reads a closed range of history as JSON lines, then exits.
check says whether each event type can be emitted, and exits 1 if one cannot.
record runs a bounded query and writes a recording for replayAdapter tests.
codegen reads module ABIs and writes TypeScript types and defineEvent
descriptors for their event structs.

Options:
  --account <address>    events from transactions this account sent
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --creation-number <n>  event handle by creation number (with --account)
  --resource <type>      every change to this resource (with --account)
  --network <name>       mainnet, testnet, devnet, movement-mainnet, or movement-testnet
                         (default: mainnet)
  --node-url <url>       fullnode URL, overriding the network's default
  --from-version <n>     start at this version instead of the chain head
  --from-time <t>        tail, forward, query: start here, as 2h, 7d or an ISO date (UTC)
  --poll-interval <ms>   delay between polls (default: 1000)
  --limit <n>            tail: exit after printing n events; record, query: stop after n
  --to <url>             forward: webhook URL (http or https, required)
  --cursor-file <path>   forward: cursor file (default: ${DEFAULT_CURSOR_FILE})
  --retries <n>          forward: retries per batch (default: ${DEFAULT_RETRIES})
  --timeout <ms>         forward: per-request timeout (default: ${DEFAULT_TIMEOUT})
  --header <h>           forward: request header 'Name: value', repeatable
  --db <file>            store: SQLite file to write (required)
  --name <name>          store: name its cursor is saved under (default: the network and descriptor)
  --to-version <n>       record: end of the range (required); query: default the head
  --order <asc|desc>     query: oldest or newest first (default: asc)
  --out <file>           codegen, record: write to this file instead of stdout
  -h, --help             show this help
`;

function wrap<T>(fn: () => T): T {
  try {
    return fn();
  } catch (err) {
    throw new UsageError(err instanceof Error ? err.message : String(err));
  }
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

function descriptor(
  command: string,
  flags: DescriptorFlags,
  types: string[],
): EventDescriptor {
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
    const accountAddress = wrap(() => requireAddress(flags.account!));
    return eventTypes.length > 0
      ? { kind: "account", accountAddress, eventTypes }
      : { kind: "account", accountAddress };
  }
  if (eventTypes.length === 0) {
    throw new UsageError(`${command} needs at least one event type, or --account`);
  }
  return { kind: "module", eventType: eventTypes as MoveEventType[] };
}

interface SourceFlags extends DescriptorFlags {
  network?: string;
  "node-url"?: string;
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
  const options: SourceOptions = {
    descriptor: descriptor(command, values, positionals),
    network: network(values.network),
  };
  const url = nodeUrl(values["node-url"]);
  const fromVersion = version(values["from-version"]);
  const fromTime = startTime(values);
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (url !== undefined) options.nodeUrl = url;
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
  const options: RecordOptions = {
    descriptor: descriptor("record", values, positionals),
    network: network(values.network),
    fromVersion,
    toVersion,
  };
  const url = nodeUrl(values["node-url"]);
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (url !== undefined) options.nodeUrl = url;
  if (limit !== undefined) options.limit = limit;
  if (values.out !== undefined) options.out = values.out;
  return { command: "record", options };
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
  const options: QueryOptions = {
    descriptor: descriptor("query", values, positionals),
    network: network(values.network),
    toVersion,
    order,
  };
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (fromTime !== undefined) options.fromTime = fromTime;
  const url = nodeUrl(values["node-url"]);
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (url !== undefined) options.nodeUrl = url;
  if (limit !== undefined) options.limit = limit;
  return { command: "query", options };
}

function parseCheck(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: CHECK_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length === 0) throw new UsageError("check needs at least one event type");
  const options: CheckOptions = {
    descriptor: descriptor("check", {}, positionals) as ModuleEventDescriptor,
    network: network(values.network),
  };
  const url = nodeUrl(values["node-url"]);
  if (url !== undefined) options.nodeUrl = url;
  return { command: "check", options };
}

function moduleRef(raw: string): ModuleRef {
  const [address = "", module = "", ...rest] = raw.split("::");
  if (rest.length > 0 || !MODULE_NAME.test(module)) {
    throw new UsageError(`expected <address>::<module>: ${raw}`);
  }
  wrap(() => requireAddress(address));
  return { address, module };
}

function parseCodegen(args: string[]): ParsedCommand {
  const { values, positionals } = wrap(() =>
    parseArgs({ args, options: CODEGEN_OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };
  if (positionals.length === 0) throw new UsageError("codegen needs at least one <address>::<module>");
  const options: CodegenOptions = {
    modules: positionals.map(moduleRef),
    network: network(values.network),
  };
  const url = nodeUrl(values["node-url"]);
  if (url !== undefined) options.nodeUrl = url;
  if (values.out !== undefined) {
    if (values.out === "") throw new UsageError("--out must not be empty");
    options.out = values.out;
  }
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
  if (command === "codegen") return parseCodegen(rest);
  throw new UsageError(`unknown command: ${command}`);
}
