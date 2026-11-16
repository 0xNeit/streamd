import { normalizeAddress } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import type { ModuleAbi } from "./abi.js";

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

const UNBOUND = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"_"\s*(?:#.*)?$/;
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
    if (unbound !== null) names.push(unbound[1]!);
  }
  return names;
}

function typeKey(type: string): string {
  const bare = type.split("<", 1)[0]!.trim();
  const [address, ...rest] = bare.split("::");
  return [normalizeAddress(address!) ?? address, ...rest].join("::");
}

/** Every `#[event]` struct in the modules, as `<long address>::<module>::<Struct>`, sorted. */
export function eventStructTypes(abis: readonly ModuleAbi[]): string[] {
  const types = abis.flatMap((abi) => abi.structs.filter((s) => s.isEvent).map((s) => typeKey(`${abi.address}::${abi.name}::${s.name}`)));
  return [...new Set(types)].sort();
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

/** One aligned line for `--format pretty`. */
export function prettyCoverageLine(entry: CoverageEntry): string {
  const { event, count, first } = entry;
  if (first === undefined) return `missed   ${event}\n`;
  return `emitted  ${event}  x${count}  first at ${first.version} by ${first.function ?? "(no entry function)"}\n`;
}
