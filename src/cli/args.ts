import { parseArgs } from "node:util";
import { requireAddress, requireEventType } from "../adapters/validate.js";
import type { EventDescriptor, MoveEventType } from "../types.js";

export type Network = "mainnet" | "testnet" | "devnet";

export interface TailOptions {
  descriptor: EventDescriptor;
  network: Network;
  nodeUrl?: string;
  fromVersion?: bigint;
  cursor?: string;
  pollInterval?: number;
  limit?: number;
}

export type ParsedCommand =
  | { command: "help" }
  | { command: "tail"; options: TailOptions };

/** Bad command-line input: reported with usage, exit code 2. */
export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsageError";
  }
}

const NETWORKS: readonly Network[] = ["mainnet", "testnet", "devnet"];
const MAX_TIMEOUT = 2_147_483_647;

const OPTIONS = {
  account: { type: "string" },
  handle: { type: "string" },
  field: { type: "string" },
  network: { type: "string" },
  "node-url": { type: "string" },
  "from-version": { type: "string" },
  cursor: { type: "string" },
  "poll-interval": { type: "string" },
  limit: { type: "string" },
  help: { type: "boolean", short: "h" },
} as const;

export const USAGE = `Usage:
  streamd tail [options] <eventType>...
  streamd tail --account <address> [options] [<eventType>...]
  streamd tail --account <address> --handle <struct> --field <name> [options]

Follows matching events and prints each as one JSON line on stdout.

Options:
  --account <address>    events from transactions this account sent
  --handle <struct>      legacy event handle struct (with --account and --field)
  --field <name>         event handle field name
  --network <name>       mainnet, testnet, or devnet (default: mainnet)
  --node-url <url>       fullnode URL, overriding the network's default
  --from-version <n>     start at this version instead of the chain head
  --cursor <token>       resume from a cursor reported by an earlier run
  --poll-interval <ms>   delay between polls (default: 1000)
  --limit <n>            exit after printing n events
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

function nodeUrl(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined;
  const url = URL.canParse(raw) ? new URL(raw) : undefined;
  if (url?.protocol !== "http:" && url?.protocol !== "https:") {
    throw new UsageError(`--node-url must be an http or https URL: ${raw}`);
  }
  return raw;
}

interface DescriptorFlags {
  account?: string;
  handle?: string;
  field?: string;
}

function descriptor(flags: DescriptorFlags, types: string[]): EventDescriptor {
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
    throw new UsageError("tail needs at least one event type, or --account");
  }
  return { kind: "module", eventType: eventTypes as MoveEventType[] };
}

/** Parses `argv` (without the node and script paths) into a command. */
export function parseCommand(argv: readonly string[]): ParsedCommand {
  const [command, ...rest] = argv;
  if (command === undefined || command === "-h" || command === "--help") {
    return { command: "help" };
  }
  if (command !== "tail") throw new UsageError(`unknown command: ${command}`);

  const { values, positionals } = wrap(() =>
    parseArgs({ args: [...rest], options: OPTIONS, allowPositionals: true, strict: true }),
  );
  if (values.help) return { command: "help" };

  const options: TailOptions = {
    descriptor: descriptor(values, positionals),
    network: network(values.network),
  };
  const url = nodeUrl(values["node-url"]);
  const fromVersion = version(values["from-version"]);
  const pollInterval = integer("poll-interval", values["poll-interval"], 0, MAX_TIMEOUT);
  const limit = integer("limit", values.limit, 1, Number.MAX_SAFE_INTEGER);
  if (url !== undefined) options.nodeUrl = url;
  if (fromVersion !== undefined) options.fromVersion = fromVersion;
  if (values.cursor !== undefined) options.cursor = values.cursor;
  if (pollInterval !== undefined) options.pollInterval = pollInterval;
  if (limit !== undefined) options.limit = limit;
  return { command: "tail", options };
}
