import { StreamdError, normalizeAddress } from "@streamd/core";
import type { MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import type { AbiChanges, EventStructSignature, FunctionSignature, PackageUpgrade } from "./types.js";

type Raw = Record<string, unknown>;

const isStrings = (v: unknown): v is string[] => Array.isArray(v) && v.every((s) => typeof s === "string");
const isRecords = (v: unknown): v is Raw[] => Array.isArray(v) && v.every((r) => typeof r === "object" && r !== null);

function fieldsOf(v: unknown, bad: () => never): { name: string; type: string }[] {
  if (!isRecords(v)) bad();
  return (v as Raw[]).map((f) => {
    if (typeof f.name !== "string" || typeof f.type !== "string") bad();
    return { name: f.name as string, type: f.type as string };
  });
}

function typeParams(v: unknown, bad: () => never): Raw[] {
  if (!isRecords(v) || !v.every((p) => isStrings(p.constraints))) bad();
  return v as Raw[];
}

function eventOf(prefix: string, s: Raw, bad: () => never): EventStructSignature {
  if (typeof s.name !== "string" || !isStrings(s.abilities)) bad();
  const signature: EventStructSignature = {
    name: `${prefix}${s.name as string}`,
    abilities: s.abilities as string[],
    typeParameters: typeParams(s.generic_type_params, bad).map((p) => ({
      constraints: p.constraints as string[],
      isPhantom: p.is_phantom === true,
    })),
    fields: fieldsOf(s.fields ?? [], bad),
  };
  if (s.variants !== undefined) {
    if (!isRecords(s.variants)) bad();
    signature.variants = (s.variants as Raw[]).map((v) => {
      if (typeof v.name !== "string") bad();
      return { name: v.name as string, fields: fieldsOf(v.fields, bad) };
    });
  }
  return signature;
}

function functionOf(prefix: string, f: Raw, bad: () => never): FunctionSignature {
  if (typeof f.name !== "string" || typeof f.visibility !== "string" || !isStrings(f.params) || !isStrings(f.return)) bad();
  return {
    name: `${prefix}${f.name as string}`,
    visibility: f.visibility as string,
    typeParameters: typeParams(f.generic_type_params, bad).map((p) => ({ constraints: p.constraints as string[] })),
    params: f.params as string[],
    returns: f.return as string[],
  };
}

interface Signatures {
  events: Map<string, EventStructSignature>;
  entryFunctions: Map<string, FunctionSignature>;
  views: Map<string, FunctionSignature>;
}

/** The event structs, entry functions and views of `modules`, by full name. */
function signatures(modules: readonly MoveModuleBytecode[]): Signatures {
  const out: Signatures = { events: new Map(), entryFunctions: new Map(), views: new Map() };
  for (const [i, m] of modules.entries()) {
    const abi = (m as { abi?: unknown } | null)?.abi as Raw | undefined;
    const label = typeof abi?.name === "string" ? abi.name : `#${i}`;
    const bad = (): never => {
      throw new StreamdError(`Fullnode returned module ${label} without a readable ABI`);
    };
    if (typeof abi !== "object" || abi === null || typeof abi.name !== "string") bad();
    const at = typeof abi!.address === "string" ? normalizeAddress(abi!.address) : undefined;
    if (at === undefined || !isRecords(abi!.structs) || !isRecords(abi!.exposed_functions)) bad();
    const prefix = `${at}::${abi!.name as string}::`;
    for (const s of abi!.structs as Raw[]) {
      // Nodes from before module events omit is_event.
      if (s.is_event === true) {
        const e = eventOf(prefix, s, bad);
        out.events.set(e.name, e);
      }
    }
    for (const f of abi!.exposed_functions as Raw[]) {
      if (f.is_entry !== true && f.is_view !== true) continue;
      const fn = functionOf(prefix, f, bad);
      if (f.is_entry === true) out.entryFunctions.set(fn.name, fn);
      if (f.is_view === true) out.views.set(fn.name, fn);
    }
  }
  return out;
}

function changes<T>(before: Map<string, T>, after: Map<string, T>): AbiChanges<T> {
  const result: AbiChanges<T> = { added: [], changed: [], removed: [] };
  const names = [...new Set([...before.keys(), ...after.keys()])].sort();
  for (const name of names) {
    const [b, a] = [before.get(name), after.get(name)];
    if (b === undefined) result.added.push(a!);
    else if (a === undefined) result.removed.push(b);
    else if (JSON.stringify(a) !== JSON.stringify(b)) result.changed.push({ before: b, after: a });
  }
  return result;
}

/**
 * How the event structs, entry functions and views of one address's modules
 * differ from `before` to `after`, both as the fullnode lists them. Throws a
 * `StreamdError` for a module without a readable ABI, since a diff that
 * skipped it would report its signatures as removed.
 */
export function diffModules(
  before: readonly MoveModuleBytecode[],
  after: readonly MoveModuleBytecode[],
): Omit<PackageUpgrade, "address" | "isUpgrade"> {
  const [b, a] = [signatures(before), signatures(after)];
  return {
    events: changes(b.events, a.events),
    entryFunctions: changes(b.entryFunctions, a.entryFunctions),
    views: changes(b.views, a.views),
  };
}
