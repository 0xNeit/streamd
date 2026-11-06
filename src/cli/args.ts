import { parseArgs } from "node:util";
import { requireAddress, requireEventType } from "../adapters/validate.js";
import type { EventDescriptor, MoveEventType } from "../types.js";

export type Network = "mainnet" | "testnet" | "devnet";

/** Where events come from: shared by `tail` and `forward`. */
export interface SourceOptions {
  descriptor: EventDescriptor;
  network: Network;
  nodeUrl?: string;
  fromVersion?: bigint;
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

export type ParsedCommand =
  | { command: "help" }
  | { command: "tail"; options: TailOptions }
  | { command: "forward"; options: ForwardOptions };

/** Bad command-line input: reported with usage, exit code 2. */
export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsageError";
  }
}

const NETWORKS: readonly Network[] = ["mainnet", "testnet", "devnet"];
const MAX_TIMEOUT = 2_147_483_647;

const SOURCE_OPTIONS = {
  account: { type: "string" },
  handle: { type: "string" },
  field: { type: "string" },
  network: { type: "string" },
  "node-url": { type: "string" },
  "from-version": { type: "string" },
  "poll-interval": { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

const TAIL_OPTIONS = { ...SOURCE_OPTIONS, limit: { type: "string" } } as const;

const FORWARD_OPTIONS = {
  ...SOURCE_OPTIONS,
  to: { type: "string" },
  "cursor-file": { type: "string" },
  retries: { type: "string" },
  timeout: { type: "string" },
  header: { type: "string", multiple: true },
} as const;

const DEFAULT_CURSOR_FILE = "streamd-forward.cursor";
const DEFAULT_RETRIES = 5;
const MAX_RETRIES = 100;
const DEFAULT_TIMEOUT = 10_000;
// RFC 9110 token characters.
const HEADER_NAME = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
// What fetch accepts in a value; anything else fails every attempt with an
// error naming the offending character.
const HEADER_VALUE = /^[\t\x20-\x7e\x80-\xff]*$/;

export const USAGE = `Usage:
  streamd tail [options] <eventType>...
  streamd tail --account <address> [options] [<eventType>...]
  streamd tail --account <address> --handle <struct> --field <name> [options]
  streamd forward --to <url> [options] <eventType>...
  streamd forward --to <url> --account <address> [...]

tail follows matching events and prints each as one JSON line on stdout.
forward POSTs each batch of matching events to a webhook as a JSON array,
saving its position to a cursor file once the webhook acknowledges it.

Options:
  --account <address>    events from transactions this account sent
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --network <name>       mainnet, testnet, or devnet (default: mainnet)
  --node-url <url>       fullnode URL, overriding the network's default
  --from-version <n>     start at this version instead of the chain head
  --poll-interval <ms>   delay between polls (default: 1000)
  --limit <n>            tail: exit after printing n events
  --to <url>             forward: webhook URL (http or https, required)
  --cursor-file <path>   forward: cursor file (default: ${DEFAULT_CURSOR_FILE})
  --retries <n>          forward: retries per batch (default: ${DEFAULT_RETRIES})
  --timeout <ms>         forward: per-request timeout (default: ${DEFAULT_TIMEOUT})
  --header <h>           forward: request header 'Name: value', repeatable
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

function version(raw: string | undefined): bigint | undefined {
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw)) {
    throw new UsageError(`--from-version must be a non-negative integer: ${raw}`);
  }
  return BigInt(raw);
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
}

function descriptor(
  command: string,
  flags: DescriptorFlags,
  types: string[],
): EventDescriptor {
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
  "from-version"?: string;
  "poll-interval"?: string;
}

function source(command: string, values: SourceFlags, positionals: string[]): SourceOptions {
  const options: SourceOptions = {
    descriptor: descriptor(command, values, positionals),
    network: network(values.network),
  };
  const url = nodeUrl(values["node-url"]);
  const fromVersion = version(values["from-version"]);
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  if (url !== undefined) options.nodeUrl = url;
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
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

/** Parses `argv` (without the node and script paths) into a command. */
export function parseCommand(argv: readonly string[]): ParsedCommand {
  const [command, ...rest] = argv;
  if (command === undefined || command === "-h" || command === "--help") {
    return { command: "help" };
  }
  if (command === "tail") return parseTail(rest);
  if (command === "forward") return parseForward(rest);
  throw new UsageError(`unknown command: ${command}`);
}
