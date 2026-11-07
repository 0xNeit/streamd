import { normalizeAddress, StreamdError } from "@streamd/core";
import type { FieldAbi, ModuleAbi, MoveType, StructAbi } from "./abi.js";

export interface ModuleRef {
  address: string;
  module: string;
}

/** Reads one module's ABI; called at most once per module. */
export type LoadModule = (address: string, module: string) => Promise<ModuleAbi>;

interface Entry {
  key: string;
  module: ModuleAbi;
  struct: StructAbi;
}

const FRAMEWORK = normalizeAddress("0x1")!;
const NARROW = new Set(["u8", "u16", "u32", "i8", "i16", "i32"]);

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

const fieldsOf = (s: StructAbi): FieldAbi[] =>
  s.isEnum ? s.variants.flatMap((v) => v.fields) : s.fields;

/** Every struct to generate: the targets' events, then what they reference. */
async function collect(targets: ModuleRef[], load: LoadModule): Promise<Map<string, Entry>> {
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
  for (const { address, module } of targets) {
    const abi = await getModule(address, module);
    const events = abi.structs.filter((s) => s.isEvent);
    if (events.length === 0) {
      throw new StreamdError(`Module ${address}::${module} declares no event structs`);
    }
    for (const s of events) pending.push(...add(abi, s));
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

  for (let entry = pending.shift(); entry !== undefined; entry = pending.shift()) {
    for (const f of fieldsOf(entry.struct)) await visit(entry, f.name, f.type);
  }
  return entries;
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
  }
  return out.join("\n");
}

/**
 * TypeScript source declaring a type and a `defineEvent` descriptor for every
 * event struct in `targets`, plus types for the structs they reference.
 */
export async function generateEventTypes(targets: ModuleRef[], load: LoadModule): Promise<string> {
  const entries = [...(await collect(targets, load)).values()].sort((a, b) =>
    a.key < b.key ? -1 : a.key > b.key ? 1 : 0,
  );
  const names = assignNames(entries);
  const header = [
    `// Generated by streamd codegen from the on-chain ABIs of ${targets
      .map((t) => `${t.address}::${t.module}`)
      .join(", ")}.`,
    "// Do not edit: regenerate after a module upgrade.",
    "// u64, u128, u256, i64, i128 and i256 values are decimal strings, as the fullnode returns them.",
    'import { defineEvent } from "@streamd/core";',
  ];
  return `${[header.join("\n"), ...entries.map((e) => render(e, names))].join("\n\n")}\n`;
}
