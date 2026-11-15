import { describe, expect, it } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import type { MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { diffModules } from "../src/upgrades.js";
import type { PackageUpgrade } from "../src/types.js";
import { makeAptos, testClient } from "./helpers.js";

const A = `0x${"ab".repeat(32)}`;

type Fn = { name: string; visibility?: string; is_entry?: boolean; is_view?: boolean; params?: string[]; return?: string[] };
type St = { name: string; is_event?: boolean; fields?: { name: string; type: string }[]; variants?: unknown[] };

const fn = (f: Fn) => ({
  visibility: "public",
  is_entry: false,
  is_view: false,
  generic_type_params: [],
  params: [],
  return: [],
  ...f,
});
const st = (s: St) => ({
  is_native: false,
  is_event: false,
  is_enum: false,
  abilities: ["drop", "store"],
  generic_type_params: [],
  fields: [],
  ...s,
});
const mod = (name: string, functions: Fn[], structs: St[] = [], address = "0xabababababababababababababababababababababababababababababababab") =>
  ({
    bytecode: "0x",
    abi: { address, name, friends: [], exposed_functions: functions.map(fn), structs: structs.map(st) },
  }) as unknown as MoveModuleBytecode;

const none = { added: [], changed: [], removed: [] };

describe("diffModules", () => {
  it("reports added, changed and removed signatures in each category", () => {
    const before = [
      mod(
        "pool",
        [
          { name: "swap", is_entry: true, params: ["&signer", "u64"] },
          { name: "close", is_entry: true },
          { name: "price", is_view: true, return: ["u64"] },
          { name: "old_view", is_view: true },
        ],
        [
          { name: "Swapped", is_event: true, fields: [{ name: "amount", type: "u64" }] },
          { name: "Closed", is_event: true },
        ],
      ),
    ];
    const after = [
      mod(
        "pool",
        [
          { name: "swap", is_entry: true, params: ["&signer", "u64", "u64"] },
          { name: "deposit", is_entry: true, visibility: "private" },
          { name: "price", is_view: true, return: ["u128"] },
          { name: "reserves", is_view: true, return: ["u64", "u64"] },
        ],
        [
          { name: "Swapped", is_event: true, fields: [{ name: "amount", type: "u64" }, { name: "fee", type: "u64" }] },
          { name: "Deposited", is_event: true },
        ],
      ),
    ];
    const diff = diffModules(before, after);
    const p = `${A}::pool::`;
    expect(diff.entryFunctions.added.map((f) => [f.name, f.visibility])).toEqual([[`${p}deposit`, "private"]]);
    expect(diff.entryFunctions.removed.map((f) => f.name)).toEqual([`${p}close`]);
    expect(diff.entryFunctions.changed).toEqual([
      {
        before: { name: `${p}swap`, visibility: "public", typeParameters: [], params: ["&signer", "u64"], returns: [] },
        after: { name: `${p}swap`, visibility: "public", typeParameters: [], params: ["&signer", "u64", "u64"], returns: [] },
      },
    ]);
    expect(diff.views.added.map((f) => f.name)).toEqual([`${p}reserves`]);
    expect(diff.views.removed.map((f) => f.name)).toEqual([`${p}old_view`]);
    expect(diff.views.changed.map((c) => [c.before.returns, c.after.returns])).toEqual([[["u64"], ["u128"]]]);
    expect(diff.events.added.map((e) => e.name)).toEqual([`${p}Deposited`]);
    expect(diff.events.removed.map((e) => e.name)).toEqual([`${p}Closed`]);
    expect(diff.events.changed).toHaveLength(1);
    expect(diff.events.changed[0]!.after).toEqual({
      name: `${p}Swapped`,
      abilities: ["drop", "store"],
      typeParameters: [],
      fields: [{ name: "amount", type: "u64" }, { name: "fee", type: "u64" }],
    });
  });

  it("counts a struct gaining #[event] as an added event and a function becoming a view as an added view", () => {
    const before = [mod("m", [{ name: "get" }], [{ name: "E" }])];
    const after = [mod("m", [{ name: "get", is_view: true }], [{ name: "E", is_event: true }])];
    const diff = diffModules(before, after);
    expect(diff.events.added.map((e) => e.name)).toEqual([`${A}::m::E`]);
    expect(diff.views.added.map((f) => f.name)).toEqual([`${A}::m::get`]);
    expect(diffModules(after, before).events.removed.map((e) => e.name)).toEqual([`${A}::m::E`]);
  });

  it("reports nothing for unchanged modules, ignoring public functions and plain structs", () => {
    const modules = [mod("m", [{ name: "f", is_entry: true }, { name: "helper" }], [{ name: "Plain" }, { name: "E", is_event: true }])];
    const changedHelper = [mod("m", [{ name: "f", is_entry: true }, { name: "helper", params: ["u8"] }], [{ name: "Plain", fields: [{ name: "x", type: "u8" }] }, { name: "E", is_event: true }])];
    expect(diffModules(modules, modules)).toEqual({ events: none, entryFunctions: none, views: none });
    expect(diffModules(modules, changedHelper)).toEqual({ events: none, entryFunctions: none, views: none });
  });

  it("reports a whole new module as added, keyed in long form and sorted", () => {
    const after = [
      mod("zeta", [{ name: "b", is_entry: true }, { name: "a", is_entry: true }], [], "0xAB"),
      mod("alpha", [{ name: "c", is_entry: true }], [{ name: "Ev", is_event: true, variants: [{ name: "V1", fields: [{ name: "x", type: "u8" }] }] }], "0xab"),
    ];
    const diff = diffModules([], after);
    const short = `0x${"0".repeat(62)}ab`;
    expect(diff.entryFunctions.added.map((f) => f.name)).toEqual([`${short}::alpha::c`, `${short}::zeta::a`, `${short}::zeta::b`]);
    expect(diff.events.added).toEqual([
      { name: `${short}::alpha::Ev`, abilities: ["drop", "store"], typeParameters: [], fields: [], variants: [{ name: "V1", fields: [{ name: "x", type: "u8" }] }] },
    ]);
  });

  it("keeps type parameter constraints and phantoms", () => {
    const generic = (phantom: boolean) =>
      ({
        bytecode: "0x",
        abi: {
          address: A,
          name: "g",
          friends: [],
          exposed_functions: [{ ...fn({ name: "go", is_entry: true }), generic_type_params: [{ constraints: ["store"] }] }],
          structs: [{ ...st({ name: "E", is_event: true }), generic_type_params: [{ constraints: [], is_phantom: phantom }] }],
        },
      }) as unknown as MoveModuleBytecode;
    const diff = diffModules([generic(false)], [generic(true)]);
    expect(diff.events.changed.map((c) => [c.before.typeParameters, c.after.typeParameters])).toEqual([
      [[{ constraints: [], isPhantom: false }], [{ constraints: [], isPhantom: true }]],
    ]);
    expect(diffModules([], [generic(false)]).entryFunctions.added[0]!.typeParameters).toEqual([{ constraints: ["store"] }]);
  });

  it("refuses a module without an ABI or with a malformed one", () => {
    const noAbi = { bytecode: "0x" } as MoveModuleBytecode;
    expect(() => diffModules([], [noAbi])).toThrow(StreamdError);
    const broken = mod("m", [{ name: "f", is_entry: true }]);
    (broken.abi!.exposed_functions[0] as unknown as Record<string, unknown>).params = "u64";
    expect(() => diffModules([broken], [])).toThrow("Fullnode returned module m without a readable ABI");
    const badAddress = mod("m", [], [], "not-an-address");
    expect(() => diffModules([], [badAddress])).toThrow(StreamdError);
    const badEvent = mod("m", [], [{ name: "E", is_event: true, fields: [{ name: "x" } as never] }]);
    expect(() => diffModules([], [badEvent])).toThrow(StreamdError);
  });
});

const B = `0x${"cd".repeat(32)}`;
const PUBLISH = "0x1::code::PublishPackage";

const publish = (version: number, eventIndex: number, codeAddress: string, isUpgrade = true): StreamdEvent => ({
  type: PUBLISH,
  data: { code_address: codeAddress, is_upgrade: isUpgrade },
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex,
  transactionHash: `0x${version.toString(16).padStart(64, "0")}`,
  sender: codeAddress,
});

const V1 = [mod("pool", [{ name: "swap", is_entry: true }])];
const V2 = [mod("pool", [{ name: "swap", is_entry: true }, { name: "price", is_view: true }])];

/** An offline SDK instance whose module reads answer from `answer`, recording each `[address, version]`. */
function reader(answer: (address: string, version: bigint) => MoveModuleBytecode[] | Promise<MoveModuleBytecode[]>, aptos: Aptos = makeAptos()) {
  const reads: [string, bigint][] = [];
  Object.assign(aptos, {
    getAccountModules: async ({ accountAddress, options }: { accountAddress: string; options: { ledgerVersion: bigint } }) => {
      reads.push([accountAddress, options.ledgerVersion]);
      return answer(accountAddress, options.ledgerVersion);
    },
  });
  return { aptos, reads };
}
const byVersion = (from: number) => (_a: string, v: bigint) => (v >= BigInt(from) ? V2 : V1);

describe("getUpgrades", () => {
  it("diffs each publish at the address between the version before it and its own", async () => {
    const { aptos, reads } = reader(byVersion(10));
    const events = [publish(5, 0, B), publish(10, 1, A), publish(10, 2, A), publish(12, 0, B)];
    const client = testClient({ adapter: new MemoryAdapter(events), aptos });
    const records = await client.getUpgrades(A, { fromVersion: 0n });
    expect(records).toHaveLength(1);
    const [record] = records;
    expect(record!.version).toBe(10n);
    expect(record!.eventIndex).toBe(1);
    expect(record!.transactionHash).toBe(events[1]!.transactionHash);
    expect(record!.data).toEqual({
      address: A,
      isUpgrade: true,
      events: none,
      entryFunctions: none,
      views: {
        added: [{ name: `${A}::pool::price`, visibility: "public", typeParameters: [], params: [], returns: [] }],
        changed: [],
        removed: [],
      },
    } satisfies PackageUpgrade);
    expect(reads.sort((x, y) => Number(x[1] - y[1]))).toEqual([[A, 9n], [A, 10n]]);
  });

  it("reads nothing before version 0 and takes the address in any form", async () => {
    const { aptos, reads } = reader(() => V1);
    const client = testClient({ adapter: new MemoryAdapter([publish(0, 0, "0xABAB" + "ab".repeat(30), false)]), aptos });
    const [record] = await client.getUpgrades(A.toUpperCase().replace("0X", "0x"), { fromVersion: 0n });
    expect(record!.data.isUpgrade).toBe(false);
    expect(record!.data.entryFunctions.added.map((f) => f.name)).toEqual([`${A}::pool::swap`]);
    expect(reads).toEqual([[A, 0n]]);
  });

  it("applies the range, where and limit to the records", async () => {
    const { aptos } = reader(byVersion(20));
    const events = [publish(10, 0, A), publish(20, 0, A), publish(30, 0, A)];
    const client = testClient({ adapter: new MemoryAdapter(events), aptos });
    const ranged = await client.getUpgrades(A, { fromVersion: 15n, toVersion: 30n });
    expect(ranged.map((r) => r.version)).toEqual([20n, 30n]);
    const changing = await client.getUpgrades(A, { fromVersion: 0n, where: (e) => (e.data as PackageUpgrade).views.added.length > 0, limit: 1 });
    expect(changing.map((r) => r.version)).toEqual([20n]);
  });

  it("refuses a PublishPackage without its fields", async () => {
    const { aptos } = reader(() => V1);
    const bad = { ...publish(3, 0, A), data: { code_address: A } };
    const client = testClient({ adapter: new MemoryAdapter([bad]), aptos });
    expect(client.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("does not have its struct's fields");
    const noAddress = { ...publish(3, 0, A), data: { code_address: "zz", is_upgrade: true } };
    const other = testClient({ adapter: new MemoryAdapter([noAddress]), aptos });
    expect(other.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow(StreamdError);
  });

  it("rejects a failed read as a network error keeping its status", async () => {
    const { aptos } = reader(() => {
      throw Object.assign(new Error("Ledger version(9) has been pruned"), { status: 410 });
    });
    const client = testClient({ adapter: new MemoryAdapter([publish(10, 0, A)]), aptos });
    const err = await client.getUpgrades(A, { fromVersion: 0n }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(410);
    expect((err as Error).message).toContain(`modules of ${A} at version`);
  });

  it("reads outside the client's limiter and within requestTimeout", async () => {
    const { aptos } = reader(byVersion(10));
    const single = testClient({ adapter: new MemoryAdapter([publish(10, 0, A)]), aptos, maxConcurrentRequests: 1 });
    expect(await single.getUpgrades(A, { fromVersion: 0n })).toHaveLength(1);
    const stuck = reader(() => new Promise<never>(() => {}));
    const slow = testClient({ adapter: new MemoryAdapter([publish(10, 0, A)]), aptos: stuck.aptos, requestTimeout: 20 });
    expect(slow.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("timed out");
  });

  it("refuses an address that is not a string at the call", () => {
    const client = testClient({ adapter: new MemoryAdapter([]) });
    expect(() => client.getUpgrades(42 as never)).toThrow("upgrades needs an address or Aptos name");
    expect(() => client.upgrades(undefined as never, () => {})).toThrow("upgrades needs an address or Aptos name");
  });
});

describe("upgrades", () => {
  it("follows live, retries a failed read, and resolves a name once", async () => {
    const mainnet = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    let fail = true;
    const { aptos, reads } = reader((_a, v) => {
      if (fail) {
        fail = false;
        throw Object.assign(new Error("busy"), { status: 503 });
      }
      return v >= 10n ? V2 : V1;
    }, mainnet);
    const lookups: string[] = [];
    Object.assign(aptos, { getTargetAddress: async ({ name }: { name: string }) => (lookups.push(name), A) });
    const memory = new MemoryAdapter([publish(4, 0, B), publish(10, 0, A)]);
    const client = testClient({ adapter: memory, aptos });
    const got: StreamdEvent<PackageUpgrade>[] = [];
    const errors: unknown[] = [];
    const sub = client.upgrades("alice.apt", (e) => void got.push(e), {
      fromVersion: 0n,
      pollInterval: 5,
      maxBackoff: 5,
      onError: (e) => void errors.push(e),
    });
    const until = async (ok: () => boolean) => {
      for (let i = 0; i < 200 && !ok(); i++) await new Promise((r) => setTimeout(r, 5));
    };
    await until(() => got.length === 1);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).cause).toBeInstanceOf(StreamdNetworkError);
    expect(got[0]!.version).toBe(10n);
    expect(got[0]!.data.views.added).toHaveLength(1);
    memory.push(publish(15, 0, A, true), publish(16, 0, B));
    await until(() => got.length === 2);
    sub.unsubscribe();
    expect(got.map((e) => e.version)).toEqual([10n, 15n]);
    expect(got[1]!.data.views).toEqual(none);
    expect(lookups).toEqual(["alice.apt"]);
    expect(reads.some(([a]) => a === B)).toBe(false);
    expect(client.listenerCount()).toBe(0);
  });
});
