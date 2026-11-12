import { normalizeAddress, StreamdError } from "@streamd/core";
import type { FieldAbi, ModuleAbi, MoveType, Primitive, StructAbi } from "./abi.js";
import type { Builtin, Dialect, LoadModule, ModuleRef } from "./codegen.js";

/** The part of a `SuiGrpcClient`'s `movePackageService` codegen reads. */
export interface SuiPackageService {
  getPackage(input: { packageId: string }): PromiseLike<{ response: { package?: unknown } }>;
  listPackageVersions(input: { packageId: string; pageSize?: number; pageToken?: Uint8Array }): PromiseLike<{
    response: { versions?: { packageId?: string; version?: bigint }[]; nextPageToken?: Uint8Array };
  }>;
}

interface Body {
  type?: unknown;
  typeName?: unknown;
  typeParameterInstantiation?: unknown;
  typeParameter?: unknown;
}

interface Datatype {
  name: string;
  definingId: string;
  emittable: boolean;
  isEnum: boolean;
  typeParams: number;
  fields: { name: string; type: Body }[];
  variants: { name: string; fields: { name: string; type: Body }[] }[];
}

interface Package {
  originalId: string;
  modules: Map<string, Datatype[]>;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
// The protobuf enums' values.
const COPY = 1;
const DROP = 2;
const ENUM = 2;
const VECTOR = 9;
const DATATYPE = 10;
const TYPE_PARAMETER = 11;
const PRIMITIVES: (Primitive | undefined)[] = [undefined, "address", "bool", "u8", "u16", "u32", "u64", "u128", "u256"];

const long = (address: string) => normalizeAddress(address) ?? address;

function malformed(id: string, what: string): never {
  throw new StreamdError(`Sui node returned a malformed package ${id}: ${what}`);
}

function identifier(value: unknown, id: string, what: string): string {
  if (typeof value !== "string" || !IDENTIFIER.test(value)) malformed(id, `bad ${what} name`);
  return value as string;
}

function address(value: unknown, id: string, what: string): string {
  const normalized = typeof value === "string" ? normalizeAddress(value) : undefined;
  if (normalized === undefined) malformed(id, `bad ${what}`);
  return normalized!;
}

function fields(value: unknown, id: string, owner: string): Datatype["fields"] {
  if (!Array.isArray(value)) malformed(id, `${owner} has no field list`);
  return (value as { name?: unknown; type?: unknown }[]).map((f) => {
    const name = identifier(f?.name, id, `field of ${owner}`);
    if (f.type === null || typeof f.type !== "object") malformed(id, `field ${owner}.${name} has no type`);
    return { name, type: f.type as Body };
  });
}

function datatype(raw: Record<string, unknown>, id: string): Datatype {
  if (raw === null || typeof raw !== "object") malformed(id, "bad datatype");
  const name = identifier(raw.name, id, "datatype");
  const { abilities, typeParameters, variants } = raw;
  if (!Array.isArray(abilities) || !Array.isArray(typeParameters)) malformed(id, `datatype ${name} has no abilities or type parameters`);
  const isEnum = raw.kind === ENUM;
  if (isEnum && !Array.isArray(variants)) malformed(id, `enum ${name} has no variant list`);
  return {
    name,
    definingId: address(raw.definingId, id, `defining id of ${name}`),
    emittable: (abilities as unknown[]).includes(COPY) && (abilities as unknown[]).includes(DROP),
    isEnum,
    typeParams: (typeParameters as unknown[]).length,
    fields: isEnum ? [] : fields(raw.fields, id, name),
    variants: isEnum
      ? (variants as Record<string, unknown>[]).map((v) => {
          const variant = identifier(v?.name, id, `variant of ${name}`);
          return { name: variant, fields: fields(v.fields, id, `${name}::${variant}`) };
        })
      : [],
  };
}

function parsePackage(raw: unknown, id: string): Package {
  const pkg = raw as { originalId?: unknown; modules?: unknown } | undefined;
  if (pkg === undefined || pkg === null || !Array.isArray(pkg.modules)) malformed(id, "no module list");
  return {
    originalId: address(pkg!.originalId, id, "original id"),
    modules: new Map(
      (pkg!.modules as Record<string, unknown>[]).map((m) => {
        const name = identifier(m?.name, id, "module");
        if (!Array.isArray(m.datatypes)) malformed(id, `module ${name} has no datatype list`);
        return [name, (m.datatypes as Record<string, unknown>[]).map((d) => datatype(d, id))];
      }),
    ),
  };
}

const describe = (err: unknown) => (err instanceof Error ? err.message : String(err));

/**
 * Reads Sui packages for `generateEventTypes`. Each module becomes one
 * `ModuleAbi` per defining package id, since an upgraded package carries types
 * earlier versions introduced and events name every type by the version that
 * introduced it. Every datatype with `copy` and `drop` in a module `targets`
 * returned counts as an event: that is all `sui::event::emit` asks, and the
 * node serves no bytecode to say which ones a module actually emits. Types
 * other modules declare are only referenced, so they get no descriptor.
 */
export function suiCodegen(service: SuiPackageService): {
  targets: (refs: ModuleRef[]) => Promise<ModuleRef[]>;
  load: LoadModule;
} {
  const packages = new Map<string, Promise<Package>>();
  const named = new Set<string>();
  const read = (packageId: string): Promise<Package> => {
    const id = long(packageId);
    if (!packages.has(id)) {
      packages.set(
        id,
        Promise.resolve(service.getPackage({ packageId: id })).then(
          ({ response }) => parsePackage(response?.package, id),
          (err: unknown) => {
            // The id is validated, so INVALID_ARGUMENT means the object there is not a package.
            const code = (err as { code?: unknown } | null)?.code;
            if (code === "NOT_FOUND" || code === "INVALID_ARGUMENT") throw new StreamdError(`Package ${id} not found`);
            throw new StreamdError(`Sui node request for package ${id} failed: ${describe(err)}`);
          },
        ),
      );
    }
    return packages.get(id)!;
  };

  const latest = async (originalId: string): Promise<string | undefined> => {
    let best: { id: string; version: bigint } | undefined;
    let pageToken: Uint8Array | undefined;
    do {
      let response;
      try {
        ({ response } = await service.listPackageVersions({ packageId: originalId, pageSize: 100, ...(pageToken && { pageToken }) }));
      } catch (err) {
        throw new StreamdError(`Sui node request for the versions of package ${originalId} failed: ${describe(err)}`);
      }
      for (const v of response?.versions ?? []) {
        if (typeof v?.version === "bigint" && (best === undefined || v.version > best.version)) {
          best = { id: address(v.packageId, originalId, "version id"), version: v.version };
        }
      }
      pageToken = response?.nextPageToken?.length ? response.nextPageToken : undefined;
    } while (pageToken !== undefined);
    return best?.id;
  };

  const find = (pkg: Package, module: string, name: string) => pkg.modules.get(module)?.find((d) => d.name === name);

  // Bytecode names types by the original package id; events name them by the defining id.
  const definingIds = new Map<string, Promise<string>>();
  const definingId = (runtime: string, module: string, name: string): Promise<string> => {
    const key = `${runtime}::${module}::${name}`;
    if (!definingIds.has(key)) {
      definingIds.set(
        key,
        (async () => {
          for (const pending of packages.values()) {
            const pkg = await pending.catch(() => undefined);
            const found = pkg?.originalId === runtime ? find(pkg, module, name) : undefined;
            if (found !== undefined) return found.definingId;
          }
          const found = find(await read(runtime), module, name) ?? (await latestFind(runtime, module, name));
          if (found === undefined) throw new StreamdError(`Type ${key} not found in any version of its package`);
          return found.definingId;
        })(),
      );
    }
    return definingIds.get(key)!;
  };
  const latestFind = async (runtime: string, module: string, name: string) => {
    const id = await latest(runtime);
    return id === undefined ? undefined : find(await read(id), module, name);
  };

  const moveType = async (body: Body, depth = 0): Promise<MoveType> => {
    const args = Array.isArray(body?.typeParameterInstantiation) ? (body.typeParameterInstantiation as Body[]) : [];
    const unsupported = { kind: "unsupported", text: String(body?.typeName ?? body?.type).slice(0, 200) } as const;
    if (depth > 32) return unsupported;
    const primitive = typeof body?.type === "number" ? PRIMITIVES[body.type] : undefined;
    if (primitive !== undefined) return { kind: "primitive", name: primitive };
    if (body?.type === VECTOR && args.length === 1) return { kind: "vector", of: await moveType(args[0]!, depth + 1) };
    if (body?.type === TYPE_PARAMETER && Number.isInteger(body.typeParameter) && (body.typeParameter as number) >= 0) {
      return { kind: "param", index: body.typeParameter as number };
    }
    if (body?.type !== DATATYPE || typeof body.typeName !== "string") return unsupported;
    const [runtime = "", module = "", name = "", ...rest] = body.typeName.split("::");
    const at = normalizeAddress(runtime);
    if (at === undefined || rest.length > 0 || !IDENTIFIER.test(module) || !IDENTIFIER.test(name)) return unsupported;
    const resolved = await Promise.all(args.map((a) => moveType(a, depth + 1)));
    return { kind: "struct", address: await definingId(at, module, name), module, name, args: resolved };
  };

  const fieldAbis = (list: Datatype["fields"]): Promise<FieldAbi[]> =>
    Promise.all(list.map(async (f) => ({ name: f.name, type: await moveType(f.type) })));

  const struct = async (d: Datatype, isEvent: boolean): Promise<StructAbi> => ({
    name: d.name,
    isEvent: isEvent && d.emittable,
    isEnum: d.isEnum,
    isResource: false,
    typeParams: d.typeParams,
    fields: await fieldAbis(d.fields),
    variants: await Promise.all(d.variants.map(async (v) => ({ name: v.name, fields: await fieldAbis(v.fields) }))),
  });

  const load: LoadModule = async (packageId, module) => {
    const id = long(packageId);
    const datatypes = (await read(id)).modules.get(module);
    if (datatypes === undefined) throw new StreamdError(`Module ${id}::${module} not found`);
    const own = datatypes.filter((d) => d.definingId === id);
    return {
      address: id,
      name: module,
      structs: await Promise.all(own.map((d) => struct(d, named.has(`${id}::${module}`)))),
      views: [],
    } satisfies ModuleAbi;
  };

  const targets = async (refs: ModuleRef[]): Promise<ModuleRef[]> => {
    const out = new Map<string, ModuleRef>();
    for (const ref of refs) {
      const pkg = await read(ref.address);
      const modules = ref.module === undefined ? [...pkg.modules.keys()] : [ref.module];
      let found = false;
      for (const module of modules) {
        const datatypes = pkg.modules.get(module);
        if (datatypes === undefined) throw new StreamdError(`Module ${long(ref.address)}::${module} not found`);
        for (const d of datatypes) {
          if (!d.emittable) continue;
          found = true;
          named.add(`${d.definingId}::${module}`);
          out.set(`${d.definingId}::${module}`, { address: d.definingId, module });
        }
      }
      if (!found) {
        const what = ref.module === undefined ? `Package ${long(ref.address)}` : `Module ${long(ref.address)}::${ref.module}`;
        throw new StreamdError(`${what} declares no struct or enum with copy and drop`);
      }
    }
    return [...out.values()];
  };

  return { targets, load };
}

const STRING: Builtin = { type: () => "string", check: (v) => `typeof ${v} === "string"` };
const ID: Builtin = { type: () => "string", check: (v, c) => c.use("address", v) };

const SUI_BUILTINS: Record<string, Builtin> = {
  [`${long("0x1")}::string::String`]: STRING,
  [`${long("0x1")}::ascii::String`]: STRING,
  [`${long("0x1")}::type_name::TypeName`]: STRING,
  [`${long("0x2")}::url::Url`]: STRING,
  [`${long("0x2")}::object::ID`]: ID,
  [`${long("0x2")}::object::UID`]: ID,
  [`${long("0x2")}::balance::Balance`]: { opaque: true, type: () => "string", check: (v, c) => c.use("uint", v) },
  [`${long("0x1")}::option::Option`]: {
    type: ([inner]) => `${inner} | null`,
    check: (v, c) => `(${v} === null || ${c.inner(c.args[0]!, v)})`,
  },
};

/** How a Sui node writes Move values as JSON, as `SuiAdapter` delivers them. */
export const SUI_DIALECT: Dialect = {
  builtin: (t) => SUI_BUILTINS[`${long(t.address)}::${t.module}::${t.name}`],
  variantKey: "@variant",
  bytes:
    "const streamdIsBytes = (v: unknown) => typeof v === \"string\" && /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(v);",
  note: "// u64, u128 and u256 values are decimal strings, and vector<u8> values base64, as the Sui node returns them.",
};
