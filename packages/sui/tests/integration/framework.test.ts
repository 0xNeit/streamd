import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { defineEvent, type ModuleEventDescriptor } from "@streamd/core";
import * as framework from "../../src/framework.js";
import { checkDescriptor, networks, SuiClient } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const ADDRESS = /^0x[0-9a-f]{64}$/;
const DECIMAL = /^\d+$/;

/** Every catalog descriptor, generic ones instantiated with SUI, which checks only the outer type. */
function descriptors(): ModuleEventDescriptor[] {
  return Object.values(framework).flatMap((value): ModuleEventDescriptor[] => {
    if (typeof value === "function") return [(value as (...a: string[]) => ModuleEventDescriptor)(...Array(value.length).fill("0x2::sui::SUI"))];
    return (value as { kind?: unknown }).kind === "module" ? [value as ModuleEventDescriptor] : [];
  });
}

interface LoadedValidators {
  mod: Record<string, (value: unknown) => boolean>;
  cleanup: () => void;
}

interface CliCodegenModule {
  generateEventTypes: (
    targets: Array<{ address: string; module?: string }>,
    load: (address: string, module: string) => Promise<LoadedModuleAbi>,
    options: { validators: boolean; dialect: unknown },
  ) => Promise<string>;
}

interface CliSuiAbiModule {
  suiCodegen: (service: unknown) => {
    targets: (refs: Array<{ address: string; module?: string }>) => Promise<Array<{ address: string; module?: string }>>;
    load: (address: string, module: string) => Promise<LoadedModuleAbi>;
  };
  SUI_DIALECT: unknown;
}

interface AbiMoveType {
  kind: string;
  of?: AbiMoveType;
  name?: string;
  address?: string;
  module?: string;
}

interface LoadedStructAbi {
  name: string;
  isEvent: boolean;
  typeParams: number;
  fields: Array<{ name: string; type: AbiMoveType }>;
}

interface LoadedModuleAbi {
  address: string;
  name: string;
  structs: LoadedStructAbi[];
}

type SuiCodegen = ReturnType<CliSuiAbiModule["suiCodegen"]>;

async function loadValidators(
  grpc: SuiGrpcClient,
  targets: Array<{ address: string; module?: string }>,
  cg?: SuiCodegen,
): Promise<LoadedValidators> {
  const { generateEventTypes } = (await import(new URL("../../../cli/src/codegen.js", import.meta.url).href)) as CliCodegenModule;
  const { suiCodegen, SUI_DIALECT } = (await import(new URL("../../../cli/src/sui-abi.js", import.meta.url).href)) as CliSuiAbiModule;
  const codegen = cg ?? suiCodegen(grpc.movePackageService);
  const resolved = await codegen.targets(targets);
  const source = await generateEventTypes(resolved, codegen.load, { validators: true, dialect: SUI_DIALECT });
  const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".validators-"));
  try {
    const file = join(dir, "events.ts");
    writeFileSync(file, source);
    const mod = (await import(file)) as Record<string, (value: unknown) => boolean>;
    return { mod, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
  } catch (err) {
    rmSync(dir, { recursive: true, force: true });
    throw err;
  }
}

const isOptionField = (t: AbiMoveType): boolean =>
  t.kind === "struct" && t.module === "option" && t.name === "Option" && t.address !== undefined && BigInt(t.address) === 1n;

const isBytesField = (t: AbiMoveType): boolean =>
  t.kind === "vector" && t.of?.kind === "primitive" && t.of.name === "u8";

const findGuard = (mod: Record<string, (value: unknown) => boolean>, structName: string) =>
  mod[`is${structName}`] ?? Object.entries(mod).find(([k]) => k.startsWith("is") && k.endsWith(`_${structName}`))?.[1];

interface MatchedLiveEvent {
  digest: string;
  address: string;
  module: string;
  structName: string;
  eventType: `${string}::${string}::${string}`;
  field: string;
}

async function findRecentLiveEvents(
  grpc: SuiGrpcClient,
  cg: SuiCodegen,
): Promise<{ option?: MatchedLiveEvent; bytes?: MatchedLiveEvent }> {
  const modules = new Map<string, Promise<LoadedModuleAbi | undefined>>();
  const loadMod = (address: string, module: string) => {
    const k = `${address}::${module}`;
    if (!modules.has(k)) modules.set(k, cg.targets([{ address, module }]).then(() => cg.load(address, module)).catch(() => undefined));
    return modules.get(k)!;
  };
  const { response: info } = await grpc.ledgerService.getServiceInfo({});
  let seq = info.checkpointHeight ?? 0n;
  let option: MatchedLiveEvent | undefined;
  let bytes: MatchedLiveEvent | undefined;
  for (let i = 0; i < 80 && seq > 0n; i++, seq--) {
    const { response } = await grpc.ledgerService.getCheckpoint({
      checkpointId: { oneofKind: "sequenceNumber", sequenceNumber: seq },
      readMask: { paths: ["transactions.digest", "transactions.events.events.event_type"] },
    });
    for (const tx of response.checkpoint?.transactions ?? []) {
      const digest = tx.digest;
      if (!digest) continue;
      for (const ev of tx.events?.events ?? []) {
        const rawType = ev.eventType;
        if (!rawType || rawType.includes("<")) continue;
        const [address, module, structName, ...rest] = rawType.split("::");
        if (!address || !module || !structName || rest.length > 0) continue;
        const s = (await loadMod(address, module))?.structs.find((x) => x.name === structName && x.isEvent && x.typeParams === 0);
        if (!s) continue;
        const eventType = rawType as `${string}::${string}::${string}`;
        const optField = !option ? s.fields.find((f) => isOptionField(f.type)) : undefined;
        const bytesField = !bytes ? s.fields.find((f) => isBytesField(f.type)) : undefined;
        if (optField) option = { digest, address, module, structName, eventType, field: optField.name };
        if (bytesField) bytes = { digest, address, module, structName, eventType, field: bytesField.name };
        if (option && bytes) return { option, bytes };
      }
    }
  }
  return { ...(option && { option }), ...(bytes && { bytes }) };
}

describe.skipIf(!enabled)("@streamd/sui/framework (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("names only types mainnet's packages can emit", async () => {
    const list = descriptors();
    expect(list.length).toBeGreaterThan(20);
    const check = await checkDescriptor(grpc, { kind: "module", eventType: list.flatMap((d) => d.eventType) });
    expect(check.types.filter((t) => t.status !== "event")).toEqual([]);
    expect(check.ok).toBe(true);
  }, 30_000);

  it("reads events whose data has the shapes the catalog declares", async () => {
    using sui = new SuiClient({ sui: grpc });
    const stakes = await sui.getEvents(framework.StakingRequestEvent, { fromVersion: -20_000_000n, limit: 2, maxPages: 30 });
    expect(stakes.length).toBeGreaterThan(0);
    for (const { data } of stakes) {
      expect(Object.keys(data).sort()).toEqual(["amount", "epoch", "pool_id", "staker_address", "validator_address"]);
      expect(data.pool_id).toMatch(ADDRESS);
      expect(data.amount).toMatch(DECIMAL);
    }

    // A generic event: take a type argument from any listing, then ask for exactly that instantiation.
    const [any] = await sui.getEvents({ kind: "module", eventType: "0x2::kiosk::ItemListed" }, { fromVersion: -20_000_000n, limit: 1, maxPages: 30 });
    const arg = /<(.+)>$/.exec(any!.type)![1]!;
    const listed = await sui.getEvents(framework.ItemListed(arg), { fromVersion: any!.version, limit: 1, maxPages: 5 });
    expect(listed[0]!.type).toBe(any!.type);
    expect(Object.keys(listed[0]!.data).sort()).toEqual(["id", "kiosk", "price"]);
    expect(listed[0]!.data.kiosk).toMatch(ADDRESS);
    expect(listed[0]!.data.price).toMatch(DECIMAL);

    const { mod, cleanup } = await loadValidators(grpc, [
      { address: "0x3", module: "validator" },
      { address: "0x2", module: "kiosk" },
    ]);
    try {
      for (const { data } of stakes) expect(mod.isStakingRequestEvent!(data)).toBe(true);
      expect(mod.isStakingRequestEvent!({ ...stakes[0]!.data, amount: 123 })).toBe(false);
      expect(mod.isItemListed!(listed[0]!.data)).toBe(true);
      expect(mod.isItemListed!({ ...listed[0]!.data, kiosk: "bad" })).toBe(false);
    } finally {
      cleanup();
    }
  }, 120_000);

  it("validates live mainnet Option and vector<u8> events through generated validators", async () => {
    using sui = new SuiClient({ sui: grpc });
    const { suiCodegen } = (await import(new URL("../../../cli/src/sui-abi.js", import.meta.url).href)) as CliSuiAbiModule;
    const cg = suiCodegen(grpc.movePackageService);
    const found = await findRecentLiveEvents(grpc, cg);
    expect(found.option).toBeDefined();
    expect(found.bytes).toBeDefined();
    const cases: Array<{ hit: MatchedLiveEvent; invalidValue: unknown }> = [
      { hit: found.option!, invalidValue: { vec: [] } },
      { hit: found.bytes!, invalidValue: "not-base64!" },
    ];
    for (const { hit, invalidValue } of cases) {
      const events = await sui.eventsOf(hit.digest, defineEvent<Record<string, unknown>>(hit.eventType));
      expect(events.length).toBeGreaterThan(0);
      const { mod, cleanup } = await loadValidators(grpc, [{ address: hit.address, module: hit.module }], cg);
      try {
        const guard = findGuard(mod, hit.structName);
        expect(guard).toBeTypeOf("function");
        for (const ev of events) {
          expect(guard!(ev.data)).toBe(true);
          expect(guard!({ ...ev.data, [hit.field]: invalidValue })).toBe(false);
        }
      } finally {
        cleanup();
      }
    }
  }, 120_000);
});
