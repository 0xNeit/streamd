import { describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { MemoryAdapter, StreamdClient, StreamdError } from "@streamd/core";
import type { ModuleEventDescriptor, StreamdEvent } from "@streamd/core";
import { parseModuleAbi } from "../src/abi.js";
import type { ModuleAbi } from "../src/abi.js";
import { generateEventTypes } from "../src/codegen.js";

const DEX = "0x00000000000000000000000000000000000000000000000000000000000cafe0";
const RUNS = 10_000;

const struct = (name: string, fields: [string, string][], isEvent = false, typeParams = 0) => ({
  name,
  is_event: isEvent,
  is_enum: false,
  generic_type_params: Array.from({ length: typeParams }, () => ({ constraints: [] })),
  fields: fields.map(([n, type]) => ({ name: n, type })),
  variants: [],
});

const shapes = parseModuleAbi({
  abi: {
    address: DEX,
    name: "shapes",
    structs: [
      struct("Everything", [
        ["small", "u8"], ["mid", "u16"], ["wide", "u32"], ["signed", "i32"], ["tiny", "i8"], ["big", "u128"], ["huge", "u256"],
        ["amount", "u64"], ["neg", "i64"], ["flag", "bool"], ["who", "address"], ["bytes", "vector<u8>"],
        ["name", "0x1::string::String"], ["maybe", "0x1::option::Option<u64>"], ["obj", "0x1::object::Object<0x1::object::ObjectCore>"],
        ["list", "vector<vector<u16>>"], ["inner", `${DEX}::shapes::Inner`], ["boxed", `${DEX}::shapes::Boxed<u64, bool>`],
        ["pick", `${DEX}::shapes::Pick`], ["picks", `vector<0x1::option::Option<${DEX}::shapes::Pick>>`], ["empty", `${DEX}::shapes::Empty`],
        ["objs", "vector<0x1::object::Object<0x1::object::ObjectCore>>"], ["boxedObj", `${DEX}::shapes::Boxed<0x1::object::Object<0x1::object::ObjectCore>, u8>`], ["maybeObj", "0x1::option::Option<0x1::object::Object<0x1::object::ObjectCore>>"],
      ], true),
      struct("Inner", [["x", "u64"], ["names", "vector<0x1::string::String>"]]),
      // T1 is phantom: no field mentions it.
      struct("Boxed", [["item", "T0"], ["items", "vector<T0>"], ["count", "u64"]], false, 2),
      struct("Empty", []),
      // T0 reaches a field only through Object<T0>, which generates nothing from it.
      struct("Holder", [["obj", "0x1::object::Object<T0>"], ["n", "u8"]], true, 1),
      struct("Opts", [["xs", "vector<0x1::option::Option<T0>>"], ["o", "0x1::option::Option<T0>"]], true, 1),
      struct("Proto", [["__proto__", "u8"]], true),
      struct("Moved", [["boxed", `${DEX}::shapes::Boxed<T0, u8>`]], true, 1),
      {
        name: "Pick", is_event: false, is_enum: true, generic_type_params: [], fields: [],
        variants: [{ name: "None", fields: [] }, { name: "Some", fields: [{ name: "amount", type: "u64" }] }],
      },
    ],
  },
});

type Mod = Record<string, (...args: any[]) => any>;

async function generated(abis: ModuleAbi[], options: { validators?: boolean; arbitraries?: boolean }) {
  // Inside the package, so the generated import of @streamd/core resolves at runtime.
  const dir = mkdtempSync(join(fileURLToPath(new URL("..", import.meta.url)), ".arbitraries-"));
  const file = join(dir, "shapes.ts");
  const load = async (_address: string, module: string) => abis.find((a) => a.name === module)!;
  try {
    const source = await generateEventTypes(abis.map((a) => ({ address: a.address, module: a.name })), load, options);
    writeFileSync(file, source);
    return { dir, source, mod: (await import(file)) as Mod };
  } catch (err) {
    rmSync(dir, { recursive: true, force: true });
    throw err;
  }
}

async function withShapes(run: (mod: Mod, source: string, dir: string) => Promise<void> | void) {
  const { dir, source, mod } = await generated([shapes], { validators: true, arbitraries: true });
  try {
    await run(mod, source, dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const sample = (mod: Mod, seed: number, n = RUNS) => {
  const rng = mod.streamdRng!(seed);
  return Array.from({ length: n }, () => mod.arbitraryEverything!(rng));
};

describe("generateEventTypes arbitraries", () => {
  it("generates values every validator accepts, for every type", async () => {
    await withShapes((mod) => {
      const rng = mod.streamdRng!(7);
      const u8 = (r: () => number) => Number(r() % 256);
      for (let i = 0; i < RUNS; i++) {
        expect(mod.isEverything!(mod.arbitraryEverything!(rng))).toBe(true);
        expect(mod.isInner!(mod.arbitraryInner!(rng))).toBe(true);
        expect(mod.isPick!(mod.arbitraryPick!(rng))).toBe(true);
        expect(mod.isEmpty!(mod.arbitraryEmpty!(rng))).toBe(true);
        const boxed = mod.arbitraryBoxed!(rng, mod.arbitraryInner, u8);
        expect(mod.isBoxed!(boxed) && mod.isInner!(boxed.item) && boxed.items.every(mod.isInner!)).toBe(true);
        const moved = mod.arbitraryMoved!(rng, mod.arbitraryPick);
        expect(mod.isMoved!(moved) && mod.isPick!(moved.boxed.item)).toBe(true);
        expect(mod.isHolder!(mod.arbitraryHolder!(rng, u8))).toBe(true);
        expect(mod.isOpts!(mod.arbitraryOpts!(rng, u8))).toBe(true);
        const proto = mod.arbitraryProto!(rng);
        expect(Object.hasOwn(proto, "__proto__") && Object.getPrototypeOf(proto) === Object.prototype && mod.isProto!(proto)).toBe(true);
      }
    });
  });

  it("keeps every integer in range, in the fullnode's forms, and reaches the edges", async () => {
    await withShapes((mod) => {
      const all = sample(mod, 11);
      const seen = (pick: (e: any) => unknown) => new Set(all.map((e) => String(pick(e))));
      const ranges: [string, bigint, bigint][] = [
        ["small", 0n, 255n], ["mid", 0n, 65535n], ["wide", 0n, 4294967295n], ["signed", -(2n ** 31n), 2n ** 31n - 1n],
        ["tiny", -128n, 127n], ["amount", 0n, 2n ** 64n - 1n], ["big", 0n, 2n ** 128n - 1n], ["huge", 0n, 2n ** 256n - 1n],
        ["neg", -(2n ** 63n), 2n ** 63n - 1n],
      ];
      for (const [field, min, max] of ranges) {
        const values = all.map((e) => BigInt(e[field]));
        expect(values.every((v) => v >= min && v <= max)).toBe(true);
        expect(seen((e) => e[field])).toContain(String(min));
        expect(seen((e) => e[field])).toContain(String(max));
        expect(seen((e) => e[field]).size).toBeGreaterThan(100);
      }
      const nested = all.flatMap((e) => [...e.list.flat(), Number(e.boxed.count)]);
      expect(nested.every((v) => Number.isInteger(v) && v >= 0 && v <= 2 ** 64)).toBe(true);
      expect(all.flatMap((e) => e.list.flat()).every((v: number) => v <= 65535)).toBe(true);
      for (const field of ["small", "signed", "tiny"]) expect(all.every((e) => typeof e[field] === "number")).toBe(true);
      for (const field of ["amount", "big", "neg"]) expect(all.every((e) => typeof e[field] === "string")).toBe(true);
      // Magnitudes vary rather than sitting near the maximum.
      expect(all.some((e) => e.amount.length < 10 && e.amount !== "0" && e.amount !== "1")).toBe(true);
      expect(seen((e) => e.bytes)).toContain("0x");
      expect(seen((e) => e.name)).toContain("");
      expect(all.some((e) => /[^\x00-\x7f]/.test(e.name))).toBe(true);
      expect(seen((e) => e.list.length)).toEqual(new Set(["0", "1", "2", "3", "4"]));
      expect(all.every((e) => e.bytes.length <= 66)).toBe(true);
      expect(seen((e) => e.maybe.vec.length)).toEqual(new Set(["0", "1"]));
      expect(seen((e) => e.pick.__variant__)).toEqual(new Set(["None", "Some"]));
      expect(all.some((e) => e.objs.length > 0 && e.maybeObj.vec.length > 0)).toBe(true);
      expect(all.every((e) => [...e.objs, ...e.maybeObj.vec, e.boxedObj.item].every((o: any) => typeof o?.inner === "string"))).toBe(true);
      // Addresses as the fullnode writes them: lowercase, leading zeros dropped.
      expect(all.every((e) => /^0x(?:0|[1-9a-f][0-9a-f]{0,63})$/.test(e.who))).toBe(true);
      expect(seen((e) => e.who)).toContain("0x1");
      expect(all.some((e) => e.who.length === 66)).toBe(true);
      expect(all.some((e) => e.who.length === 65)).toBe(true);
    });
  });

  it("repeats a seed's values and differs across seeds", async () => {
    await withShapes((mod) => {
      const text = (seed: number) => JSON.stringify(sample(mod, seed, 50));
      expect(text(3)).toBe(text(3));
      expect(text(3)).not.toBe(text(4));
      const rng = mod.streamdRng!(0);
      // mulberry32's first outputs for seed 0, so a changed algorithm shows here.
      expect([rng(), rng(), rng()]).toEqual([1144304738, 1416247, 958946056]);
    });
  });

  it("builds events MemoryAdapter serves back through getEvents", async () => {
    await withShapes(async (mod) => {
      const rng = mod.streamdRng!(5);
      const descriptor = mod.Everything as unknown as ModuleEventDescriptor<unknown>;
      const events: StreamdEvent[] = Array.from({ length: 500 }, (_, i) =>
        mod.streamdEvent!(descriptor, mod.arbitraryEverything!(rng), BigInt(1 + Math.floor(i / 2)), i % 2),
      );
      expect(events[0]).toMatchObject({ type: `${DEX}::shapes::Everything`, sequenceNumber: 0n, version: 1n, eventIndex: 0 });
      const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
      const read = await client.getEvents(descriptor, { fromVersion: 0n });
      expect(read.map((e) => e.data)).toEqual(events.map((e) => e.data));
      expect(read.every((e) => mod.isEverything!(e.data))).toBe(true);
      const both = { kind: "module", eventType: [`${DEX}::shapes::Everything`, `${DEX}::shapes::Moved`] };
      expect(() => mod.streamdEvent!(both, {}, 1n)).toThrow("streamdEvent takes a descriptor of one event type");
      const moved = mod.streamdEvent!(mod.Moved!("u8"), mod.arbitraryMoved!(rng, mod.arbitraryPick), 9n, 1);
      expect(moved.type).toBe(`${DEX}::shapes::Moved<u8>`);
      const generic = new StreamdClient({ adapter: new MemoryAdapter([moved]) });
      expect((await generic.getEvents(mod.Moved!("u8"), { fromVersion: 0n })).map((e) => e.data)).toEqual([moved.data]);
    });
  });

  it("writes nothing without the option, steps around clashes, and refuses a StreamdRng struct", async () => {
    const plain = await generated([shapes], { validators: true });
    rmSync(plain.dir, { recursive: true, force: true });
    expect(plain.source).not.toContain("arbitrary");
    expect(plain.source).not.toContain("StreamdRng");
    // A vector or Option of a type parameter checks nothing per item, so no callback is written for it.
    expect(plain.source).toContain('Array.isArray(value["items"]) && streamdIsUint(value["count"])');
    expect(plain.source).not.toContain("=> true)");
    // The generator for FooView would be arbitraryFooView, which the builder for view arbitrary_foo already is.
    const clash = parseModuleAbi({
      abi: {
        address: DEX,
        name: "clash",
        structs: [struct("FooView", [["a", "u8"]], true)],
        exposed_functions: [
          { name: "arbitrary_foo", visibility: "public", is_entry: false, is_view: true, generic_type_params: [], params: [], return: [] },
        ],
      },
    });
    const clashed = await generated([clash], { arbitraries: true });
    try {
      expect(clashed.source).toContain("export const arbitraryFooView = ");
      expect(clashed.source).toContain("export function arbitraryFooView_(rng: StreamdRng): FooView {");
      // Only the helpers a generator calls are written.
      expect(clashed.source).toContain("function streamdInt");
      expect(clashed.source).not.toContain("streamdAddress");
      expect(clashed.mod.arbitraryFooView_!(clashed.mod.streamdRng!(1))).toEqual({ a: expect.any(Number) });
    } finally {
      rmSync(clashed.dir, { recursive: true, force: true });
    }
    for (const name of ["StreamdRng", "StreamdEvent", "ModuleEventDescriptor"]) {
      const named = parseModuleAbi({ abi: { address: DEX, name: "named", structs: [struct(name, [["a", "u8"]], true)] } });
      const load = async () => named;
      await expect(generateEventTypes([{ address: DEX, module: "named" }], load, { arbitraries: true })).rejects.toThrow(
        new StreamdError(`${DEX}::named::${name} is named ${name}, which the generated generators use`),
      );
      await expect(generateEventTypes([{ address: DEX, module: "named" }], load, {})).resolves.toContain(name);
    }
  });

  it("typechecks under strict settings, inferring generic generators' types", async () => {
    const phantom = await generated([shapes], { arbitraries: true });
    rmSync(phantom.dir, { recursive: true, force: true });
    expect(phantom.source).toContain("_t1: (rng: StreamdRng) => T1");
    expect(phantom.source).toContain("export function arbitraryHolder<T0>(rng: StreamdRng, _t0: (rng: StreamdRng) => T0): Holder<T0> {");
    // An interface with a phantom type parameter beside a used one fails noUnusedParameters
    // on its own, so Boxed's T1 is used here; Holder, whose only parameter is phantom, passes.
    const boxed = shapes.structs.find((s) => s.name === "Boxed")!;
    const used = {
      ...shapes,
      structs: shapes.structs.map((s) => (s === boxed ? { ...s, fields: [...s.fields, { name: "tag", type: { kind: "param", index: 1 } as const }] } : s)),
    };
    const { dir, source } = await generated([used], { validators: true, arbitraries: true });
    try {
      expect(source).toContain("export function arbitraryEmpty(_rng: StreamdRng): Empty {");
      writeFileSync(
        join(dir, "use.ts"),
        [
          'import { arbitraryBoxed, arbitraryEverything, arbitraryInner, arbitraryMoved, arbitraryPick, Everything, streamdEvent, streamdRng, type Boxed, type Inner, type Moved, type Pick } from "./shapes.js";',
          "const rng = streamdRng(1);",
          "const boxed: Boxed<Inner, number> = arbitraryBoxed(rng, arbitraryInner, (r) => r() % 8);",
          "const moved: Moved<Pick> = arbitraryMoved(rng, arbitraryPick);",
          "const event = streamdEvent(Everything, arbitraryEverything(rng), 1n);",
          "const amount: string = event.data.amount;",
          "void boxed; void moved; void amount;",
        ].join("\n"),
      );
      writeFileSync(
        join(dir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            target: "ES2022", module: "ESNext", moduleResolution: "bundler", strict: true, noUnusedLocals: true,
            noUnusedParameters: true, noPropertyAccessFromIndexSignature: true, noUncheckedIndexedAccess: true,
            noEmit: true, skipLibCheck: true, types: [], lib: ["ES2022"],
            paths: { "@streamd/core": [fileURLToPath(new URL("../../core/dist/index.d.ts", import.meta.url))] },
          },
          files: ["shapes.ts", "use.ts"],
        }),
      );
      const tsc = fileURLToPath(new URL("../../../node_modules/.bin/tsc", import.meta.url));
      const result = Bun.spawnSync([tsc, "-p", dir]);
      expect(result.stdout.toString() + result.stderr.toString()).toBe("");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});
