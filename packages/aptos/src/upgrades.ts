import { getAptosFullNode } from "@aptos-labs/ts-sdk";
import type { Aptos } from "@aptos-labs/ts-sdk";
import { RequestLimiter, StreamdError, StreamdNetworkError, normalizeAddress, toStreamdError } from "@streamd/core";
import type { EventAdapter, ModuleEventDescriptor, StreamdEvent } from "@streamd/core";
import type { MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { addr, malformed, pageSource } from "./records.js";
import type { Fields } from "./records.js";
import type { AbiChanges, EventStructSignature, FunctionSignature, PackageUpgrade } from "./types.js";

/** Only `0x1::code` can emit it, so its `code_address` is the framework's word. */
const PUBLISH = "0x1::code::PublishPackage";
export const UPGRADE_DESCRIPTOR: ModuleEventDescriptor = { kind: "module", eventType: PUBLISH };

/** Every module at `address` as of `version`, as the fullnode lists them; stops paging once `signal` aborts. */
export type ModuleReader = (address: string, version: bigint, signal: AbortSignal) => Promise<MoveModuleBytecode[]>;

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

/**
 * `ModuleReader` over `aptos`'s fullnode, every page of `/accounts/{address}/modules`
 * at `ledger_version`. Not the SDK's `getAccountModules` or `getAccountModulesPage`,
 * which drop `ledgerVersion` (ts-sdk 6.3.1) and so read the head, and the first of
 * which also stops at 1000 modules in all. A repeated cursor fails rather than pages forever.
 */
export function moduleReader(aptos: Aptos): ModuleReader {
  return async (address, version, signal) => {
    const modules: MoveModuleBytecode[] = [];
    const seen = new Set<string>();
    let cursor: string | undefined;
    do {
      signal.throwIfAborted();
      let page;
      try {
        page = await getAptosFullNode<{}, MoveModuleBytecode[]>({
          aptosConfig: aptos.config,
          originMethod: "upgrades",
          path: `accounts/${address}/modules`,
          params: { ledger_version: version.toString(), limit: 1000, ...(cursor !== undefined && { start: cursor }) },
        });
      } catch (err) {
        throw toStreamdError(err, `modules of ${address} at version ${version}`);
      }
      if (!Array.isArray(page.data)) throw new StreamdError(`Fullnode returned no module list for ${address} at version ${version}`);
      modules.push(...page.data);
      const next: unknown = page.headers?.["x-aptos-cursor"];
      cursor = typeof next === "string" && next !== "" ? next : undefined;
      if (cursor !== undefined && seen.has(cursor)) throw new StreamdNetworkError("Fullnode repeated a module listing cursor");
      if (cursor !== undefined) seen.add(cursor);
    } while (cursor !== undefined);
    return modules;
  };
}

/**
 * `adapter`'s `PublishPackage` pages as `PackageUpgrade` records for the
 * address `account` gives: each publish there is diffed between the modules
 * `read` returns at the version before it and at its own. The reads run two
 * at a time through a limiter of their own, as the page fetch already holds
 * a slot of the client's, whose `requestTimeout` bounds the whole page; a
 * failed read fails the page. A second publish there in one transaction on
 * the page adds no record, since the diff already spans the transaction.
 */
export function upgradeSource(adapter: EventAdapter, account: () => Promise<string>, read: ModuleReader): EventAdapter {
  const limiter = new RequestLimiter(2);
  // Pages are read one at a time, so one still reading when the next starts was
  // given up on (timed out or aborted): its queued reads leave, and it reads no more.
  let page = new AbortController();
  return pageSource(adapter, async (events) => {
    // A network error, so a caller still holding the page retries rather than stops.
    page.abort(new StreamdNetworkError("Superseded by a later page"));
    const { signal } = (page = new AbortController());
    if (events.length === 0) return [];
    const at = await account();
    const records: StreamdEvent<PackageUpgrade>[] = [];
    let newest: bigint | undefined;
    for (const event of events) {
      if (event.type !== PUBLISH) continue;
      const d = event.data as Fields | null;
      if (typeof d !== "object" || d === null || typeof d.is_upgrade !== "boolean") throw malformed(event);
      if (addr(d.code_address, event) !== at || event.version === newest) continue;
      newest = event.version;
      const [before, after] = await Promise.all([
        event.version === 0n ? [] : limiter.run(() => read(at, event.version - 1n, signal), signal, signal),
        limiter.run(() => read(at, event.version, signal), signal, signal),
      ]);
      records.push({ ...event, data: { address: at, isUpgrade: d.is_upgrade, ...diffModules(before, after) } });
    }
    return records;
  });
}
