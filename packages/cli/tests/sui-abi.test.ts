import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { StreamdError } from "@streamd/core";
import { generateEventTypes } from "../src/codegen.js";
import type { ModuleRef } from "../src/codegen.js";
import { SUI_DIALECT, suiCodegen } from "../src/sui-abi.js";
import type { SuiPackageService } from "../src/sui-abi.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const STD = long("1");
const SUI = long("2");
const V1 = long("a1");
const V2 = long("a2");
const OTHER = long("b1");

const P = { address: 1, bool: 2, u8: 3, u16: 4, u32: 5, u64: 6, u128: 7, u256: 8 } as const;
const prim = (name: keyof typeof P) => ({ type: P[name], typeParameterInstantiation: [] });
const vec = (of: unknown) => ({ type: 9, typeParameterInstantiation: [of] });
const param = (index: number) => ({ type: 11, typeParameter: index, typeParameterInstantiation: [] });
const dt = (typeName: string, ...args: unknown[]) => ({ type: 10, typeName, typeParameterInstantiation: args });

interface Opts {
  abilities?: number[];
  definingId?: string;
  typeParameters?: number;
}
interface Raw {
  name: string;
  definingId?: string;
  abilities: number[];
  kind: number;
  typeParameters: unknown[];
  fields: unknown[];
  variants: unknown[];
}
const struct = (name: string, fields: [string, unknown][], { abilities = [1, 2], definingId, typeParameters = 0 }: Opts = {}): Raw => ({
  name,
  definingId,
  abilities,
  kind: 1,
  typeParameters: Array.from({ length: typeParameters }, () => ({ constraints: [], isPhantom: false })),
  fields: fields.map(([n, type], position) => ({ name: n, position, type })),
  variants: [],
});
const enumOf = (name: string, variants: [string, [string, unknown][]][], definingId?: string): Raw => ({
  ...struct(name, [], { definingId }),
  kind: 2,
  fields: [],
  variants: variants.map(([n, fields]) => ({ name: n, fields: fields.map(([f, type]) => ({ name: f, type })) })),
});

/** A package at `storageId`; datatypes default to `definingId` = `storageId`. */
function pkg(storageId: string, originalId: string, modules: Record<string, Raw[]>) {
  return {
    storageId,
    originalId,
    modules: Object.entries(modules).map(([name, datatypes]) => ({
      name,
      datatypes: datatypes.map((d) => ({ ...d, definingId: d.definingId ?? storageId })),
    })),
  };
}

const STD_PKG = pkg(STD, STD, {
  option: [struct("Option", [["vec", vec(param(0))]], { abilities: [1, 2, 3], typeParameters: 1 })],
  string: [struct("String", [["bytes", vec(prim("u8"))]], { abilities: [1, 2, 3] })],
  ascii: [struct("String", [["bytes", vec(prim("u8"))]], { abilities: [1, 2, 3] })],
  type_name: [struct("TypeName", [["name", dt(`${STD}::ascii::String`)]], { abilities: [1, 2, 3] })],
});
const SUI_PKG = pkg(SUI, SUI, {
  object: [struct("ID", [["bytes", prim("address")]], { abilities: [1, 2, 3] }), struct("UID", [["id", dt(`${SUI}::object::ID`)]], { abilities: [3] })],
  balance: [struct("Balance", [["value", prim("u64")]], { abilities: [3], typeParameters: 1 })],
  url: [struct("Url", [["url", dt(`${STD}::ascii::String`)]], { abilities: [1, 2, 3] })],
});

// The first version of a protocol, and its upgrade, which adds NewInfo and Later.
const POOL_V1 = [
  struct("SwapEvent", [
    ["pool", dt(`${SUI}::object::ID`)],
    ["amount", prim("u64")],
    ["small", prim("u8")],
    ["flag", prim("bool")],
    ["who", prim("address")],
    ["memo", dt(`${STD}::string::String`)],
    ["symbol", dt(`${STD}::ascii::String`)],
    ["coin", dt(`${STD}::type_name::TypeName`)],
    ["link", dt(`${SUI}::url::Url`)],
    ["maybe", dt(`${STD}::option::Option`, prim("u64"))],
    ["bytes", vec(prim("u8"))],
    ["ids", vec(dt(`${STD}::option::Option`, dt(`${SUI}::object::ID`)))],
    ["info", dt(`${V1}::pool::Info`)],
  ]),
  struct("Info", [["fee", prim("u128")]], { abilities: [1, 2, 3] }),
  struct("Pool", [["id", dt(`${SUI}::object::UID`)], ["reserve", dt(`${SUI}::balance::Balance`, param(0))]], { abilities: [3, 4], typeParameters: 1 }),
  enumOf("Side", [["Bid", [["price", prim("u64")]]], ["Ask", []]]),
  struct("Order", [["side", dt(`${V1}::pool::Side`)], ["value", param(0)]], { typeParameters: 1 }),
];
const V1_PKG = pkg(V1, V1, { pool: POOL_V1, admin: [struct("Cap", [["id", dt(`${SUI}::object::UID`)]], { abilities: [3, 4] })] });
const V2_PKG = pkg(V2, V1, {
  pool: [
    ...POOL_V1.map((d) => ({ ...d, definingId: V1 })),
    // Bytecode names the upgrade's own types by the original id, V1.
    struct("NewInfo", [["rate", prim("u32")]], { abilities: [1, 2, 3] }),
    struct("Later", [["inner", dt(`${V1}::pool::NewInfo`)]]),
  ],
  admin: [{ ...struct("Cap", [["id", dt(`${SUI}::object::UID`)]], { abilities: [3, 4] }), definingId: V1 }],
});
// Another package, linked against the upgrade.
const OTHER_PKG = pkg(OTHER, OTHER, { uses: [struct("Uses", [["info", dt(`${V1}::pool::NewInfo`)]])] });

function service(packages = [STD_PKG, SUI_PKG, V1_PKG, V2_PKG, OTHER_PKG]) {
  const reads: string[] = [];
  const listed: string[] = [];
  const svc: SuiPackageService = {
    async getPackage({ packageId }) {
      reads.push(packageId);
      const found = packages.find((p) => p.storageId === packageId);
      if (found === undefined) throw Object.assign(new Error("not found"), { code: "NOT_FOUND" });
      return { response: { package: found } };
    },
    // One version per page, ascending, as the node orders them.
    async listPackageVersions({ packageId, pageToken }) {
      listed.push(packageId);
      const versions = packages.filter((p) => p.originalId === packageId).map((p, i) => ({ packageId: p.storageId, version: BigInt(i + 1) }));
      const page = pageToken === undefined ? 0 : pageToken[0]!;
      const more = page + 1 < versions.length;
      return { response: { versions: versions.slice(page, page + 1), ...(more && { nextPageToken: new Uint8Array([page + 1]) }) } };
    },
  };
  return { svc, reads, listed };
}

async function generate(refs: ModuleRef[], svc = service().svc, validators = false) {
  const { targets, load } = suiCodegen(svc);
  return generateEventTypes(await targets(refs), load, { dialect: SUI_DIALECT, validators });
}

describe("suiCodegen", () => {
  it("maps every field as the Sui node writes it, naming types by their defining package", async () => {
    const source = await generate([{ address: "0xa2", module: "pool" }]);
    expect(source).toContain("// u64, u128 and u256 values are decimal strings, and vector<u8> values base64, as the Sui node returns them.");
    expect(source).toContain(
      [
        `/** \`${V1}::pool::SwapEvent\`, an event */`,
        "export interface SwapEvent {",
        "  pool: string;",
        "  amount: string;",
        "  small: number;",
        "  flag: boolean;",
        "  who: string;",
        "  memo: string;",
        "  symbol: string;",
        "  coin: string;",
        "  link: string;",
        "  maybe: string | null;",
        "  bytes: string;",
        "  ids: (string | null)[];",
        "  info: Info;",
        "}",
        `export const SwapEvent = defineEvent<SwapEvent>("${V1}::pool::SwapEvent");`,
        'export const SwapEventDecode = ["amount"] as const;',
      ].join("\n"),
    );
    expect(source).toContain(`export const Later = defineEvent<Later>("${V2}::pool::Later");`);
    expect(source).toContain(`/** \`${V2}::pool::NewInfo\`, an event */\nexport interface NewInfo {\n  rate: number;\n}`);
    expect(source).toContain(`export const Info = defineEvent<Info>("${V1}::pool::Info");`);
    expect(source).not.toContain("Pool");
    expect(source).not.toContain("interface ID");
    expect(source).not.toContain("ViewRequest");
  });

  it("writes enums as unions on @variant and generic events as descriptor functions", async () => {
    const source = await generate([{ address: V1, module: "pool" }]);
    expect(source).toContain('export type Side =\n  | { "@variant": "Bid"; price: string }\n  | { "@variant": "Ask" };');
    expect(source).toContain(`export const Side = defineEvent<Side>("${V1}::pool::Side");`);
    expect(source).toContain(
      `export const Order = <T0 = unknown>(t0: string) =>\n  defineEvent<Order<T0>>(\`${V1}::pool::Order<\${t0}>\`);`,
    );
  });

  it("expands a bare package to every module with an emittable type, split by defining id", async () => {
    const { svc, reads } = service();
    const { targets } = suiCodegen(svc);
    expect(await targets([{ address: "0xa2" }])).toEqual([
      { address: V1, module: "pool" },
      { address: V2, module: "pool" },
    ]);
    expect(reads).toEqual([V2]);
  });

  it("resolves a type a later version introduced through its package's versions, reading each package once", async () => {
    const { svc, reads, listed } = service();
    const source = await generate([{ address: OTHER }], svc);
    // Declared in a module not named, so a type without a descriptor.
    expect(source).toContain(`/** \`${V2}::pool::NewInfo\` */\nexport interface NewInfo {`);
    expect(source).not.toContain("export const NewInfo");
    // Two pages: the upgrade is the second version listed.
    expect(listed).toEqual([V1, V1]);
    expect(reads.sort()).toEqual([OTHER, V1, V2].sort());
  });

  it("refuses a node that pages versions without end", async () => {
    const { svc } = service();
    const endless: SuiPackageService = {
      getPackage: svc.getPackage,
      listPackageVersions: async () => ({ response: { versions: [], nextPageToken: new Uint8Array([1]) } }),
    };
    await expect(generate([{ address: OTHER }], endless)).rejects.toThrow(`Sui node listed more than 100 pages of versions of package ${V1}`);
  });

  it("checks the Sui JSON shapes with --validators", async () => {
    const source = await generate([{ address: V1, module: "pool" }], service().svc, true);
    const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".validators-"));
    try {
      const file = join(dir, "pool.ts");
      writeFileSync(file, source);
      const mod = (await import(file)) as Record<string, (v: unknown) => boolean>;
      const good = {
        pool: long("c"), amount: "5", small: 1, flag: true, who: long("d"), memo: "m", symbol: "S",
        coin: "00ab::coin::COIN", link: "https://x", maybe: null, bytes: "AQID", ids: [long("e"), null], info: { fee: "7" },
      };
      expect(mod.isSwapEvent!(good)).toBe(true);
      expect(mod.isSwapEvent!({ ...good, maybe: "9" })).toBe(true);
      expect(mod.isSwapEvent!({ ...good, maybe: 9 })).toBe(false);
      expect(mod.isSwapEvent!({ ...good, bytes: "0x0102" })).toBe(false);
      expect(mod.isSwapEvent!({ ...good, pool: "nope" })).toBe(false);
      expect(mod.isSide!({ "@variant": "Bid", price: "1" })).toBe(true);
      expect(mod.isSide!({ "@variant": "Ask" })).toBe(true);
      expect(mod.isSide!({ __variant__: "Ask" })).toBe(false);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses missing packages and modules, and targets with nothing to emit", async () => {
    const notPackage: SuiPackageService = {
      getPackage: async () => Promise.reject(Object.assign(new Error("object is not a package"), { code: "INVALID_ARGUMENT" })),
      listPackageVersions: async () => ({ response: { versions: [] } }),
    };
    await expect(generate([{ address: "0x99" }])).rejects.toThrow(`Package ${long("99")} not found`);
    await expect(generate([{ address: "0x6" }], notPackage)).rejects.toThrow(`Package ${long("6")} not found`);
    await expect(generate([{ address: V1, module: "nope" }])).rejects.toThrow(`Module ${V1}::nope not found`);
    await expect(generate([{ address: V1, module: "admin" }])).rejects.toThrow(`Module ${V1}::admin declares no struct or enum with copy and drop`);
    const failing: SuiPackageService = { ...notPackage, getPackage: async () => Promise.reject(Object.assign(new Error("down"), { code: "UNAVAILABLE" })) };
    await expect(generate([{ address: V1 }], failing)).rejects.toThrow(`Sui node request for package ${V1} failed: down`);
  });

  it("refuses a malformed package and a type no version of its package has", async () => {
    const bad = (mutate: (p: ReturnType<typeof pkg>) => unknown) =>
      generate([{ address: V1 }], service([STD_PKG, SUI_PKG, mutate(structuredClone(V1_PKG)) as ReturnType<typeof pkg>]).svc);
    await expect(bad((p) => ({ ...p, modules: undefined }))).rejects.toThrow(`Sui node returned a malformed package ${V1}: no module list`);
    await expect(bad((p) => ((p.modules[0]!.datatypes[0]!.abilities as unknown) = undefined, p))).rejects.toThrow("has no abilities");
    await expect(bad((p) => ((p.modules[0]!.datatypes[0]!.name = "bad name"), p))).rejects.toThrow("bad datatype name");
    await expect(bad((p) => ((p.modules[0]!.datatypes[0]!.definingId = "x"), p))).rejects.toThrow("bad defining id of SwapEvent");
    const orphan = pkg(OTHER, OTHER, { uses: [struct("Uses", [["x", dt(`${V1}::pool::Missing`)]])] });
    await expect(generate([{ address: OTHER }], service([orphan, V1_PKG]).svc)).rejects.toThrow(
      `Type ${V1}::pool::Missing not found in any version of its package`,
    );
    await expect(generate([{ address: OTHER }], service([orphan, V1_PKG]).svc)).rejects.toBeInstanceOf(StreamdError);
  });

  it("emits a file that typechecks", async () => {
    const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".sui-codegen-"));
    try {
      writeFileSync(join(dir, "events.ts"), await generate([{ address: V2 }], service().svc, true));
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022", module: "ESNext", moduleResolution: "bundler", strict: true,
            noUnusedLocals: true, noUnusedParameters: true, noEmit: true, skipLibCheck: true, types: [],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["events.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
      expect(result.exitCode).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});
