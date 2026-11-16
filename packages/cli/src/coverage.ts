import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Ed25519Account } from "@aptos-labs/ts-sdk";
import { normalizeAddress } from "@streamd/core";
import type { MoveEventType, StreamdEvent } from "@streamd/core";
import type { Localnet } from "@streamd/testing";
import { parseModuleAbi } from "./abi.js";
import type { ModuleAbi } from "./abi.js";
import type { CoverageOptions } from "./args.js";
import { message } from "./format.js";

/** The call behind the first event of one struct. */
export interface FirstEmit {
  version: bigint;
  eventIndex: number;
  transactionHash?: string;
  /** The entry function, or `null` when the transaction had none. */
  function: string | null;
  sender?: string;
}

/** One `#[event]` struct of the package and what the run did with it. */
export interface CoverageEntry {
  /** `<address>::<module>::<Struct>`, the address long form. */
  event: string;
  count: number;
  first?: FirstEmit;
}

const UNBOUND = /^\s*(["']?)([A-Za-z_][A-Za-z0-9_]*)\1\s*=\s*(["'])_\3\s*(?:#.*)?$/;
const SECTION = /^\s*\[([^\]]*)\]\s*(?:#.*)?$/;

/** The named addresses a `Move.toml`'s `[addresses]` section leaves as `"_"`, to be bound at publish. */
export function unboundAddresses(moveToml: string): string[] {
  const names: string[] = [];
  let inAddresses = false;
  for (const line of moveToml.split(/\r?\n/)) {
    const section = SECTION.exec(line);
    if (section !== null) {
      inAddresses = section[1]!.trim() === "addresses";
      continue;
    }
    const unbound = inAddresses ? UNBOUND.exec(line) : null;
    if (unbound !== null) names.push(unbound[2]!);
  }
  return names;
}

function typeKey(type: string): string {
  const bare = type.split("<", 1)[0]!.trim();
  const [address, ...rest] = bare.split("::");
  return [normalizeAddress(address!) ?? address, ...rest].join("::");
}

/** Every `#[event]` struct in the modules, as `<long address>::<module>::<Struct>`, sorted. */
export function eventStructTypes(abis: readonly ModuleAbi[]): MoveEventType[] {
  const types = abis.flatMap((abi) => abi.structs.filter((s) => s.isEvent).map((s) => typeKey(`${abi.address}::${abi.name}::${s.name}`)));
  return [...new Set(types)].sort() as MoveEventType[];
}

/**
 * Counts the events per struct, a generic instantiation under its struct, and
 * keeps the earliest by `(version, eventIndex)`; events of other types are ignored.
 */
export function coverageReport(types: readonly string[], events: readonly StreamdEvent[]): CoverageEntry[] {
  const entries = new Map<string, { count: number; first?: StreamdEvent }>(types.map((type) => [type, { count: 0 }]));
  for (const event of events) {
    const entry = entries.get(typeKey(event.type));
    if (entry === undefined) continue;
    entry.count++;
    const first = entry.first;
    if (first === undefined || event.version < first.version || (event.version === first.version && event.eventIndex < first.eventIndex)) {
      entry.first = event;
    }
  }
  return [...entries].map(([event, { count, first }]) => ({
    event,
    count,
    ...(first !== undefined && {
      first: {
        version: first.version,
        eventIndex: first.eventIndex,
        ...(first.transactionHash !== undefined && { transactionHash: first.transactionHash }),
        function: first.entryFunction?.function ?? null,
        ...(first.sender !== undefined && { sender: first.sender }),
      },
    }),
  }));
}

/** One JSON line: `event`, `emitted`, `count` and, when emitted, `first` with its version as a string. */
export function coverageLine(entry: CoverageEntry): string {
  const { event, count, first } = entry;
  const record = { event, emitted: count > 0, count, ...(first !== undefined && { first: { ...first, version: first.version.toString() } }) };
  return `${JSON.stringify(record)}\n`;
}

/** One line for `--format pretty`. */
export function prettyCoverageLine(entry: CoverageEntry): string {
  const { event, count, first } = entry;
  if (first === undefined) return `missed   ${event}\n`;
  return `emitted  ${event}  x${count}  first at ${first.version} by ${first.function ?? "(no entry function)"}\n`;
}

export interface CoverageIO {
  /** Starts the localnet the run uses; `runCoverage` stops it. */
  start(): Promise<Localnet>;
  /** Runs the test command with `env` added to its environment, resolving its exit code. */
  run(command: string[], env: Record<string, string>): Promise<number>;
  /** Writes one output line; a returned promise is awaited before the next line. */
  write(line: string): void | Promise<void>;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  /** Aborted on an interrupt: the command is not started, or no report follows it. */
  signal: AbortSignal;
}

/**
 * Publishes the package to a fresh localnet from a funded account, runs the
 * test command against it, then prints one line per `#[event]` struct the
 * package declares. Resolves 0 when the command passed, 1 when it or any step
 * before the report failed, 130 when interrupted; the report still prints
 * after a failed command.
 */
export async function runCoverage(options: CoverageOptions, io: CoverageIO): Promise<number> {
  const manifest = join(options.packageDir, "Move.toml");
  let toml: string;
  try {
    toml = await readFile(manifest, "utf8");
  } catch (err) {
    io.error(`streamd: cannot read ${manifest}: ${message(err)}`);
    return 1;
  }
  let localnet: Localnet;
  try {
    localnet = await io.start();
  } catch (err) {
    io.error(`streamd: cannot start a localnet: ${message(err)}`);
    return 1;
  }
  try {
    const account = await localnet.fundedAccount();
    const address = account.accountAddress.toStringLong();
    const namedAddresses = Object.fromEntries(unboundAddresses(toml).map((name) => [name, address]));
    const published = await localnet.publish(options.packageDir, { account, namedAddresses });
    if (io.signal.aborted) return interrupted(io);
    const code = await io.run(options.command, {
      STREAMD_NODE_URL: localnet.nodeUrl,
      STREAMD_FAUCET_URL: localnet.faucetUrl ?? "",
      STREAMD_PACKAGE_ADDRESS: address,
      // fundedAccount generates an Ed25519 account; the key is a throwaway on this localnet.
      STREAMD_PUBLISHER_KEY: (account as Ed25519Account).privateKey.toString(),
    });
    // Ctrl-C reaches the node as well, so after one there is nothing left to read.
    if (io.signal.aborted) return interrupted(io);
    const modules = await localnet.aptos.getAccountModules({ accountAddress: address });
    const types = eventStructTypes(modules.map(parseModuleAbi));
    const events = types.length === 0
      ? []
      : await localnet.client.getEvents({ kind: "module", eventType: types }, { fromVersion: published.version, toVersion: "head" });
    const report = coverageReport(types, events);
    const line = options.format === "pretty" ? prettyCoverageLine : coverageLine;
    for (const entry of report) await io.write(line(entry));
    const emitted = report.filter((entry) => entry.count > 0).length;
    io.error(`streamd: ${emitted} of ${report.length} event structs emitted${code === 0 ? "" : `; the test command exited ${code}`}`);
    return code === 0 ? 0 : 1;
  } catch (err) {
    io.error(`streamd: ${message(err)}`);
    return 1;
  } finally {
    await localnet.stop();
  }
}

function interrupted(io: CoverageIO): number {
  io.error("streamd: interrupted; no coverage report");
  return 130;
}
