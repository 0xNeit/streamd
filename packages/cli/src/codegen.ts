import { normalizeAddress, StreamdError } from "@streamd/core";
import type { FieldAbi, ModuleAbi, MoveType, StructAbi, ViewAbi } from "./abi.js";

/** One module, or every module at `address` when `module` is unset. */
export interface ModuleRef {
  address: string;
  module?: string;
}

/** Reads one module's ABI; called at most once per module. */
export type LoadModule = (address: string, module: string) => Promise<ModuleAbi>;

/** Reads every module's ABI at an account. */
export type ListModules = (address: string) => Promise<ModuleAbi[]>;

export interface GenerateOptions {
  /** Expands targets that name no module. */
  list?: ListModules;
  /** Writes a `handle` descriptor builder per `EventHandle` field of a resource; default true. */
  handles?: boolean;
  /** Writes an `is<Type>` runtime check per generated type; default false. */
  validators?: boolean;
}

interface Entry {
  key: string;
  module: ModuleAbi;
  struct: StructAbi;
}

interface ViewEntry {
  module: ModuleAbi;
  view: ViewAbi;
}

interface HandleEntry {
  module: ModuleAbi;
  struct: StructAbi;
  field: string;
  payload: MoveType;
}

const FRAMEWORK = normalizeAddress("0x1")!;
const NARROW = new Set(["u8", "u16", "u32", "i8", "i16", "i32"]);
const DECODABLE = new Set(["u64", "u128", "u256"]);

const long = (address: string) => normalizeAddress(address) ?? address;
const moduleKey = (address: string, module: string) => `${long(address)}::${module}`;
const qualified = (e: Entry) => `${e.module.address}::${e.module.name}::${e.struct.name}`;

const BUILTINS: Record<string, "String" | "Option" | "Object"> = {
  "string::String": "String",
  "option::Option": "Option",
  "object::Object": "Object",
};

/** Framework structs whose JSON is not their fields, so no interface is generated. */
function builtin(t: Extract<MoveType, { kind: "struct" }>) {
  return long(t.address) === FRAMEWORK ? BUILTINS[`${t.module}::${t.name}`] : undefined;
}

/** The payload type of a legacy `0x1::event::EventHandle<T>` field, if `t` is one. */
const handlePayload = (t: MoveType): MoveType | undefined =>
  t.kind === "struct" && long(t.address) === FRAMEWORK && t.module === "event" && t.name === "EventHandle"
    ? t.args[0]
    : undefined;

const fieldsOf = (s: StructAbi): FieldAbi[] =>
  s.isEnum ? s.variants.flatMap((v) => v.fields) : s.fields;

/** Every struct to generate: the targets' events, then what they reference. */
async function collect(
  targets: ModuleRef[],
  load: LoadModule,
  { list, handles: withHandles = true }: GenerateOptions,
): Promise<{ entries: Map<string, Entry>; views: ViewEntry[]; handles: HandleEntry[] }> {
  const modules = new Map<string, Promise<ModuleAbi>>();
  const getModule = (address: string, name: string) => {
    const key = moduleKey(address, name);
    if (!modules.has(key)) {
      modules.set(
        key,
        load(address, name).then((abi) => {
          if (moduleKey(abi.address, abi.name) !== key) {
            throw new StreamdError(`Fullnode returned ${abi.address}::${abi.name} for ${address}::${name}`);
          }
          return abi;
        }),
      );
    }
    return modules.get(key)!;
  };

  const entries = new Map<string, Entry>();
  const add = (module: ModuleAbi, struct: StructAbi): Entry[] => {
    const key = `${moduleKey(module.address, module.name)}::${struct.name}`;
    if (entries.has(key)) return [];
    const entry = { key, module, struct };
    entries.set(key, entry);
    return [entry];
  };

  const pending: Entry[] = [];
  const views: ViewEntry[] = [];
  const handles: HandleEntry[] = [];
  const seen = new Set<string>();
  const take = (abi: ModuleAbi): boolean => {
    const events = abi.structs.filter((s) => s.isEvent);
    const held = !withHandles
      ? []
      : abi.structs.flatMap((struct) =>
          struct.isResource && !struct.isEnum
            ? struct.fields.flatMap((f) => {
                const payload = handlePayload(f.type);
                return payload === undefined ? [] : [{ module: abi, struct, field: f.name, payload }];
              })
            : [],
        );
    if (events.length === 0 && abi.views.length === 0 && held.length === 0) return false;
    if (seen.has(moduleKey(abi.address, abi.name))) return true;
    seen.add(moduleKey(abi.address, abi.name));
    for (const s of events) pending.push(...add(abi, s));
    for (const view of abi.views) views.push({ module: abi, view });
    handles.push(...held);
    return true;
  };
  for (const { address, module } of targets) {
    if (module !== undefined) {
      if (!take(await getModule(address, module))) {
        throw new StreamdError(`Module ${address}::${module} declares no event structs, event handles or view functions`);
      }
      continue;
    }
    if (list === undefined) throw new StreamdError(`No module listing to expand ${address}`);
    let found = false;
    for (const abi of await list(address)) {
      if (long(abi.address) !== long(address)) {
        throw new StreamdError(`Fullnode listed ${abi.address}::${abi.name} for account ${address}`);
      }
      modules.set(moduleKey(abi.address, abi.name), Promise.resolve(abi));
      if (take(abi)) found = true;
    }
    if (!found) {
      throw new StreamdError(`Account ${address} has no modules with event structs, event handles or view functions`);
    }
  }

  const visit = async (owner: Entry, field: string, t: MoveType): Promise<void> => {
    const where = `field ${field} of ${qualified(owner)}`;
    if (t.kind === "unsupported") throw new StreamdError(`Unsupported Move type in ${where}: ${t.text}`);
    if (t.kind === "param" && t.index >= owner.struct.typeParams) {
      throw new StreamdError(`Malformed ABI: ${where} names T${t.index}`);
    }
    if (t.kind === "vector") return visit(owner, field, t.of);
    if (t.kind !== "struct") return;
    const kind = builtin(t);
    if (kind === "Object") return; // its type argument never appears in JSON
    for (const arg of t.args) await visit(owner, field, arg);
    if (kind !== undefined) return;
    const abi = await getModule(t.address, t.module);
    const struct = abi.structs.find((s) => s.name === t.name);
    if (struct === undefined) {
      throw new StreamdError(`Struct ${t.name} not found in ${t.address}::${t.module}, needed by ${where}`);
    }
    if (struct.typeParams !== t.args.length) {
      throw new StreamdError(`Malformed ABI: ${where} gives ${t.name} the wrong number of type arguments`);
    }
    pending.push(...add(abi, struct));
  };

  for (const h of handles) {
    await visit({ key: "", module: h.module, struct: h.struct }, h.field, h.payload);
  }
  for (let entry = pending.shift(); entry !== undefined; entry = pending.shift()) {
    for (const f of fieldsOf(entry.struct)) await visit(entry, f.name, f.type);
  }
  return { entries, views, handles };
}

const pascal = (snake: string) =>
  snake.split("_").map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join("");

// Lowercase names could be TypeScript keywords, and T0-style names would shadow type parameters.
const unsafe = (name: string) => !/^[A-Z]/.test(name) || /^T\d+$/.test(name);

const count = (names: string[]) => {
  const counts = new Map<string, number>();
  for (const n of names) counts.set(n, (counts.get(n) ?? 0) + 1);
  return counts;
};

/**
 * The Move name where it is unique, `<Module>_<Struct>` where two modules share
 * it, and `<Module>_<Struct>_<address>` where even that collides. The underscore
 * keeps qualified names apart from Move's own CamelCase names, and the result
 * depends only on the set of structs, not on the order of the targets.
 */
function assignNames(entries: Entry[]): Map<string, string> {
  const plain = count(entries.map((e) => e.struct.name));
  const local = entries.map((e) => {
    const name = e.struct.name;
    return plain.get(name) === 1 && !unsafe(name) ? name : `${pascal(e.module.name)}_${name}`;
  });
  const taken = count(local);
  const names = new Map(
    entries.map((e, i) => {
      const name = local[i]!;
      return [e.key, name === e.struct.name || taken.get(name) === 1 ? name : `${name}_${e.module.address}`];
    }),
  );
  if (new Set(names.values()).size !== names.size) {
    throw new StreamdError("Could not give every generated type a distinct name");
  }
  return names;
}

function tsType(t: MoveType, names: Map<string, string>): string {
  switch (t.kind) {
    case "primitive":
      if (t.name === "bool") return "boolean";
      return NARROW.has(t.name) ? "number" : "string";
    case "param":
      return `T${t.index}`;
    case "vector":
      if (t.of.kind === "primitive" && t.of.name === "u8") return "string";
      return `${tsType(t.of, names)}[]`;
    case "struct": {
      const kind = builtin(t);
      if (kind === "String") return "string";
      if (kind === "Object") return "{ inner: string }";
      const args = t.args.map((a) => tsType(a, names));
      if (kind === "Option") return `{ vec: [] | [${args[0]}] }`;
      const name = names.get(`${moduleKey(t.address, t.module)}::${t.name}`)!;
      return args.length === 0 ? name : `${name}<${args.join(", ")}>`;
    }
    case "unsupported":
      throw new StreamdError(`Unsupported Move type: ${t.text}`);
  }
}

/** The fields `decode` can turn into a `bigint`: signed values are refused by it, so they stay out. */
function decodableFields(struct: Entry["struct"]): string[] {
  return struct.fields
    .filter((f) => f.type.kind === "primitive" && DECODABLE.has(f.type.name))
    .map((f) => f.name);
}

function renderDecodeList(name: string, fields: string[], names: Map<string, string>): string {
  const list = `${name}Decode`;
  const free = [...names.values()].includes(list) ? `${list}_` : list;
  return `export const ${free} = [${fields.map((f) => JSON.stringify(f)).join(", ")}] as const;`;
}

function render(e: Entry, names: Map<string, string>): string {
  const name = names.get(e.key)!;
  const count = e.struct.typeParams;
  const params = Array.from({ length: count }, (_, i) => `T${i}`);
  const generics = count === 0 ? "" : `<${params.map((p) => `${p} = unknown`).join(", ")}>`;
  const field = (f: FieldAbi) => `${f.name}: ${tsType(f.type, names)}`;
  const out = [`/** \`${qualified(e)}\`${e.struct.isEvent ? ", an event" : ""} */`];

  if (e.struct.isEnum) {
    const variants = e.struct.variants.map(
      (v) => `  | { ${[`__variant__: "${v.name}"`, ...v.fields.map(field)].join("; ")} }`,
    );
    out.push(`export type ${name}${generics} =`, ...(variants.length ? variants : ["  never"]));
    out[out.length - 1] += ";";
  } else {
    out.push(`export interface ${name}${generics} {`, ...e.struct.fields.map((f) => `  ${field(f)};`), "}");
  }

  if (e.struct.isEvent) {
    if (count === 0) {
      out.push(`export const ${name} = defineEvent<${name}>("${qualified(e)}");`);
    } else {
      const args = params.map((p) => p.toLowerCase());
      out.push(
        `export const ${name} = ${generics}(${args.map((a) => `${a}: string`).join(", ")}) =>`,
        `  defineEvent<${name}<${params.join(", ")}>>(\`${qualified(e)}<${args.map((a) => `\${${a}}`).join(", ")}>\`);`,
      );
    }
    const fields = decodableFields(e.struct);
    if (fields.length > 0) out.push(renderDecodeList(name, fields, names));
  }
  return out.join("\n");
}

const camel = (snake: string) => {
  const name = pascal(snake);
  return name.charAt(0).toLowerCase() + name.slice(1);
};

/** The TypeScript type `/view` accepts for `t`, or `undefined` when JSON cannot express it. */
function argumentType(t: MoveType): string | undefined {
  switch (t.kind) {
    case "primitive":
      if (t.name === "signer") return undefined;
      if (t.name === "bool") return "boolean";
      if (t.name === "address") return "string";
      return NARROW.has(t.name) ? "number" : "string | bigint";
    case "param":
      return "unknown";
    case "vector": {
      if (t.of.kind === "primitive" && t.of.name === "u8") return "string";
      const inner = argumentType(t.of);
      if (inner === undefined) return undefined;
      return inner.includes("|") ? `(${inner})[]` : `${inner}[]`;
    }
    case "struct": {
      const kind = builtin(t);
      if (kind === "String" || kind === "Object") return "string";
      if (kind !== "Option") return undefined;
      const inner = argumentType(t.args[0]!);
      return inner === undefined ? undefined : `{ vec: [] | [${inner}] }`;
    }
    case "unsupported":
      return undefined;
  }
}

function renderView({ module, view }: ViewEntry, name: string): string {
  const fn = `${module.address}::${module.name}::${view.name}`;
  const types = view.params.map(argumentType);
  if (types.includes(undefined)) {
    return `// \`${fn}\` takes an argument the JSON view API cannot send, so it has no builder.`;
  }
  const typeArgs = Array.from({ length: view.typeParams }, (_, i) => `t${i}`);
  const args = types.map((_, i) => `arg${i}`);
  const params = [...typeArgs.map((a) => `${a}: string`), ...types.map((t, i) => `arg${i}: ${t}`)];
  return [
    `/** \`${fn}\`, a view */`,
    `export const ${name} = (${params.join(", ")}): ViewRequest => ({`,
    `  function: "${fn}",`,
    `  typeArguments: [${typeArgs.join(", ")}],`,
    `  functionArguments: [${args.join(", ")}],`,
    "});",
  ].join("\n");
}

const usesParam = (t: MoveType): boolean =>
  t.kind === "param" || (t.kind === "vector" && usesParam(t.of)) || (t.kind === "struct" && t.args.some(usesParam));

function renderHandle(h: HandleEntry, name: string, names: Map<string, string>): string {
  const count = h.struct.typeParams;
  const args = Array.from({ length: count }, (_, i) => `t${i}`);
  const payload = tsType(h.payload, names);
  const generics = usesParam(h.payload)
    ? `<${Array.from({ length: count }, (_, i) => `T${i} = unknown`).join(", ")}>`
    : "";
  const struct = `${h.module.address}::${h.module.name}::${h.struct.name}`;
  const tag = count === 0 ? `"${struct}"` : `\`${struct}<${args.map((a) => `\${${a}}`).join(", ")}>\``;
  const params = ["accountAddress: string", ...args.map((a) => `${a}: string`)];
  return [
    `/** \`${h.field}\` on \`${struct}\`, a legacy event handle */`,
    `export const ${name} = ${generics}(${params.join(", ")}): HandleEventDescriptor<${payload}> => ({`,
    '  kind: "handle",',
    "  accountAddress,",
    `  eventHandleStruct: ${tag},`,
    `  fieldName: "${h.field}",`,
    "});",
  ].join("\n");
}

const HELPERS = {
  record:
    "const streamdIsRecord = (v: unknown): v is Record<string, unknown> =>\n  typeof v === \"object\" && v !== null && !Array.isArray(v);",
  uint: "const streamdIsUint = (v: unknown) => (typeof v === \"string\" && /^\\d+$/.test(v)) || (typeof v === \"bigint\" && v >= 0n);",
  int: "const streamdIsInt = (v: unknown) => (typeof v === \"string\" && /^-?\\d+$/.test(v)) || typeof v === \"bigint\";",
  address: "const streamdIsAddress = (v: unknown) => typeof v === \"string\" && /^0x[0-9a-fA-F]{1,64}$/.test(v);",
  bytes: "const streamdIsBytes = (v: unknown) => typeof v === \"string\" && /^0x(?:[0-9a-fA-F]{2})*$/.test(v);",
} as const;
type Helper = keyof typeof HELPERS;

const SIGNED_WIDE = new Set(["i64", "i128", "i256"]);

/**
 * A boolean expression checking `value` against `t`, as the fullnode writes it
 * in JSON; `depth` keeps nested callbacks' parameter names apart.
 */
function check(t: MoveType, value: string, guards: Map<string, string>, used: Set<Helper>, depth = 0): string {
  const use = (helper: Helper, arg = value) => (used.add(helper), `streamd${helper === "record" ? "IsRecord" : `Is${pascal(helper)}`}(${arg})`);
  switch (t.kind) {
    case "primitive":
      if (t.name === "bool") return `typeof ${value} === "boolean"`;
      if (NARROW.has(t.name)) return `Number.isInteger(${value})`;
      if (DECODABLE.has(t.name)) return use("uint");
      if (SIGNED_WIDE.has(t.name)) return use("int");
      return use("address");
    case "param":
      // A type argument is only known at runtime, so its values are not checked.
      return "true";
    case "vector": {
      if (t.of.kind === "primitive" && t.of.name === "u8") return use("bytes");
      const item = `v${depth}`;
      return `Array.isArray(${value}) && ${value}.every((${item}: unknown) => ${check(t.of, item, guards, used, depth + 1)})`;
    }
    case "struct": {
      const kind = builtin(t);
      if (kind === "String") return `typeof ${value} === "string"`;
      if (kind === "Object") return `${use("record")} && ${use("address", `${value}["inner"]`)}`;
      if (kind === "Option") {
        const item = `v${depth}`;
        const inner = check(t.args[0]!, item, guards, used, depth + 1);
        const vec = `${value}["vec"]`;
        return `${use("record")} && Array.isArray(${vec}) && ${vec}.length <= 1 && ${vec}.every((${item}: unknown) => ${inner})`;
      }
      return `${guards.get(`${moduleKey(t.address, t.module)}::${t.name}`)!}(${value})`;
    }
    case "unsupported":
      throw new StreamdError(`Unsupported Move type: ${t.text}`);
  }
}

function renderGuard(e: Entry, name: string, guard: string, guards: Map<string, string>, used: Set<Helper>): string {
  const field = (f: FieldAbi) => check(f.type, `value[${JSON.stringify(f.name)}]`, guards, used);
  const all = (fields: FieldAbi[], first: string) =>
    [first, ...fields.map(field).filter((c) => c !== "true")].join(" && ");
  used.add("record");
  const doc = `/** Whether \`value\` has the shape of \`${qualified(e)}\`; extra fields pass, type arguments are not checked. */`;
  if (!e.struct.isEnum) {
    return [doc, `export function ${guard}(value: unknown): value is ${name} {`, `  return ${all(e.struct.fields, "streamdIsRecord(value)")};`, "}"].join("\n");
  }
  const cases = e.struct.variants.map((v) => {
    const checks = all(v.fields, "").replace(/^ && /, "");
    return `    case ${JSON.stringify(v.name)}:\n      return ${checks || "true"};`;
  });
  return [
    doc,
    `export function ${guard}(value: unknown): value is ${name} {`,
    "  if (!streamdIsRecord(value)) return false;",
    '  switch (value["__variant__"]) {',
    ...cases,
    "    default:",
    "      return false;",
    "  }",
    "}",
  ].join("\n");
}

/**
 * `<Struct>_<Field>` where unique, `<Module>_<Struct>_<Field>` where two modules
 * share it, and the address appended where even that collides.
 */
function handleNames(handles: HandleEntry[], taken: Set<string>): string[] {
  const plain = handles.map((h) => `${h.struct.name}_${pascal(h.field)}`);
  const counts = count(plain);
  const names = handles.map((h, i) =>
    counts.get(plain[i]!) === 1 && !taken.has(plain[i]!) ? plain[i]! : `${pascal(h.module.name)}_${plain[i]}`,
  );
  const again = count(names);
  return names.map((name, i) => (again.get(name) === 1 && !taken.has(name) ? name : `${name}_${handles[i]!.module.address}`));
}

/** `<function>View` where unique, `<module>_<function>View` where two targets share it. */
function viewNames(views: ViewEntry[]): string[] {
  const plain = count(views.map((v) => v.view.name));
  const names = views.map(({ module, view }) =>
    plain.get(view.name) === 1 ? `${camel(view.name)}View` : `${camel(module.name)}_${camel(view.name)}View`,
  );
  const taken = count(names);
  return names.map((name, i) => (taken.get(name) === 1 ? name : `${name}_${views[i]!.module.address}`));
}

/**
 * TypeScript source declaring a type and a `defineEvent` descriptor for every
 * event struct in `targets`, plus types for the structs they reference, a
 * `ViewRequest` builder for each of their `#[view]` functions, and a `handle`
 * descriptor builder for each `EventHandle` field of their resources.
 */
export async function generateEventTypes(
  targets: ModuleRef[],
  load: LoadModule,
  options: GenerateOptions = {},
): Promise<string> {
  const collected = await collect(targets, load, options);
  const entries = [...collected.entries.values()].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const key = ({ module, view }: ViewEntry) => `${moduleKey(module.address, module.name)}::${view.name}`;
  const views = collected.views.sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
  const names = assignNames(entries);
  const builders = viewNames(views);
  const handleKey = (h: HandleEntry) => `${moduleKey(h.module.address, h.module.name)}::${h.struct.name}::${h.field}`;
  const handles = collected.handles.sort((a, b) => (handleKey(a) < handleKey(b) ? -1 : handleKey(a) > handleKey(b) ? 1 : 0));
  const handleBuilders = handleNames(handles, new Set([...names.values(), ...builders]));
  const taken = new Set([...names.values(), ...builders, ...handleBuilders]);
  const guards = new Map<string, string>();
  if (options.validators) {
    for (const e of entries) {
      let guard = `is${names.get(e.key)!}`;
      while (taken.has(guard)) guard += "_";
      taken.add(guard);
      guards.set(e.key, guard);
    }
  }
  const used = new Set<Helper>();
  const checks = options.validators ? entries.map((e) => renderGuard(e, names.get(e.key)!, guards.get(e.key)!, guards, used)) : [];
  const header = [
    `// Generated by streamd codegen from the on-chain ABIs of ${targets
      .map((t) => (t.module === undefined ? t.address : `${t.address}::${t.module}`))
      .join(", ")}.`,
    "// Do not edit: regenerate after a module upgrade.",
    "// u64, u128, u256, i64, i128 and i256 values are decimal strings, as the fullnode returns them.",
    ...(entries.some((e) => e.struct.isEvent) ? ['import { defineEvent } from "@streamd/core";'] : []),
    ...(views.length > 0 ? ['import type { ViewRequest } from "@streamd/core";'] : []),
    ...(handles.length > 0 ? ['import type { HandleEventDescriptor } from "@streamd/core";'] : []),
  ];
  const sections = [
    header.join("\n"),
    ...entries.map((e) => render(e, names)),
    ...views.map((v, i) => renderView(v, builders[i]!)),
    ...handles.map((h, i) => renderHandle(h, handleBuilders[i]!, names)),
    ...(checks.length > 0 ? [(Object.keys(HELPERS) as Helper[]).filter((h) => used.has(h)).map((h) => HELPERS[h]).join("\n"), ...checks] : []),
  ];
  return `${sections.join("\n\n")}\n`;
}
