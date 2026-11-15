import { describe, expect, it } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import type { MoveModuleBytecode } from "@aptos-labs/ts-sdk";
import { UPGRADE_DESCRIPTOR, diffModules, upgradeSource } from "../src/upgrades.js";
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

type Listing = { modules: MoveModuleBytecode[]; cursor?: string };

/**
 * An SDK instance over a fake fullnode that serves `/accounts/{address}/modules`
 * from `page`, by the request's `ledger_version` and `start`, recording each.
 * A thrown error carrying `status` becomes that HTTP status.
 */
function fakeNode(page: (address: string, version: bigint, start: string | undefined) => Listing | Promise<Listing>, network = Network.DEVNET) {
  const requests: [string, bigint, string | undefined][] = [];
  const client = {
    async provider(req: { url: string; params?: Record<string, unknown> }) {
      const match = /\/accounts\/([^/]+)\/modules$/.exec(req.url.replace(/\?.*$/, ""));
      if (match === null || typeof req.params?.ledger_version !== "string" || req.params.limit !== 1000) throw new Error(`unexpected request ${req.url}`);
      const start = req.params.start as string | undefined;
      requests.push([match[1]!, BigInt(req.params.ledger_version), start]);
      try {
        const { modules, cursor } = await page(match[1]!, BigInt(req.params.ledger_version), start);
        return { status: 200, statusText: "OK", data: modules, headers: cursor === undefined ? {} : { "x-aptos-cursor": cursor } };
      } catch (err) {
        const status = (err as { status?: number }).status;
        if (status === undefined) throw err;
        return { status, statusText: "Error", data: { message: (err as Error).message }, headers: {} };
      }
    },
  };
  const aptos = new Aptos(new AptosConfig({ network, fullnode: "http://fake/v1", client: client as never, clientConfig: { http2: false } }));
  return { aptos, requests };
}

/** `fakeNode` answering one page per listing from `answer`. */
function reader(answer: (address: string, version: bigint) => MoveModuleBytecode[] | Promise<MoveModuleBytecode[]>, network?: Network) {
  const node = fakeNode(async (address, version) => ({ modules: await answer(address, version) }), network);
  return { aptos: node.aptos, reads: node.requests };
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
    expect(reads.map(([a, v]): [string, bigint] => [a, v]).sort((x, y) => Number(x[1] - y[1]))).toEqual([[A, 9n], [A, 10n]]);
  });

  it("reads nothing before version 0 and takes the address in any form", async () => {
    const { aptos, reads } = reader(() => V1);
    const client = testClient({ adapter: new MemoryAdapter([publish(0, 0, "0xABAB" + "ab".repeat(30), false)]), aptos });
    const [record] = await client.getUpgrades(A.toUpperCase().replace("0X", "0x"), { fromVersion: 0n });
    expect(record!.data.isUpgrade).toBe(false);
    expect(record!.data.entryFunctions.added.map((f) => f.name)).toEqual([`${A}::pool::swap`]);
    expect(reads.map(([a, v]) => [a, v])).toEqual([[A, 0n]]);
  });

  it("applies the range, where and limit to the records", async () => {
    const { aptos } = reader(byVersion(20));
    const events = [publish(10, 0, A), publish(20, 0, A), publish(30, 0, A)];
    const client = testClient({ adapter: new MemoryAdapter(events), aptos });
    const ranged = await client.getUpgrades(A, { fromVersion: 15n, toVersion: 30n });
    expect(ranged.map((r) => r.version)).toEqual([20n, 30n]);
    const changing = await client.getUpgrades(A, { fromVersion: 0n, where: (e) => (e.data as PackageUpgrade).views.added.length > 0 });
    expect(changing.map((r) => r.version)).toEqual([20n]);
    const first = await client.getUpgrades(A, { fromVersion: 0n, limit: 1 });
    expect(first.map((r) => r.version)).toEqual([10n]);
  });

  it("refuses a PublishPackage without its fields", async () => {
    const { aptos } = reader(() => V1);
    const bad = { ...publish(3, 0, A), data: { code_address: A } };
    const client = testClient({ adapter: new MemoryAdapter([bad]), aptos });
    await expect(client.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("does not have its struct's fields");
    const noAddress = { ...publish(3, 0, A), data: { code_address: "zz", is_upgrade: true } };
    const other = testClient({ adapter: new MemoryAdapter([noAddress]), aptos });
    await expect(other.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow(StreamdError);
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
    await expect(slow.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("timed out");
  });

  it("reads every page of a long module listing at the publish's versions", async () => {
    const { aptos, requests } = fakeNode((_a, version, start) =>
      start === undefined
        ? { modules: V1, cursor: `next-${version}` }
        : { modules: version === 0n ? [mod("extra", [{ name: "go", is_entry: true }])] : [] },
    );
    const client = testClient({ adapter: new MemoryAdapter([publish(0, 0, A, false)]), aptos });
    const [record] = await client.getUpgrades(A, { fromVersion: 0n });
    expect(record!.data.entryFunctions.added.map((f) => f.name)).toEqual([`${A}::extra::go`, `${A}::pool::swap`]);
    expect(requests).toEqual([[A, 0n, undefined], [A, 0n, "next-0"]]);
  });

  it("refuses a listing that is not a list, and ends on an empty cursor", async () => {
    const bad = fakeNode(() => ({ modules: {} as never }));
    const client = testClient({ adapter: new MemoryAdapter([publish(0, 0, A, false)]), aptos: bad.aptos });
    await expect(client.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("no module list");
    const empty = fakeNode(() => ({ modules: V1, cursor: "" }));
    const other = testClient({ adapter: new MemoryAdapter([publish(0, 0, A, false)]), aptos: empty.aptos });
    expect(await other.getUpgrades(A, { fromVersion: 0n })).toHaveLength(1);
    expect(empty.requests).toHaveLength(1);
  });

  it("refuses a module listing whose cursor repeats", async () => {
    const { aptos, requests } = fakeNode(() => ({ modules: V1, cursor: "same" }));
    const client = testClient({ adapter: new MemoryAdapter([publish(0, 0, A, false)]), aptos });
    await expect(client.getUpgrades(A, { fromVersion: 0n })).rejects.toThrow("repeated a module listing cursor");
    expect(requests).toHaveLength(2);
  });

  it("stops paging a listing once its page is given up on", async () => {
    let release = () => {};
    const gate = new Promise<void>((r) => (release = r));
    const { aptos, requests } = fakeNode(async (_a, _v, start) => {
      await gate;
      return { modules: V1, ...(start === undefined && { cursor: "next" }) };
    });
    let n = 0;
    const pages = [[publish(0, 0, A, false)], []];
    const client = testClient({ adapter: new MemoryAdapter([]), aptos });
    const wrap = (client as unknown as { upgradesOf(a: string): (s: unknown) => { fetchEvents: (d: unknown, o: unknown) => Promise<unknown> } }).upgradesOf(A);
    const source = wrap({ fetchEvents: async () => ({ events: pages[n++]!, nextCursor: "", hasMore: false }) });
    const abandoned = source.fetchEvents(UPGRADE_DESCRIPTOR, { fromVersion: 0n });
    abandoned.catch(() => {});
    await new Promise((r) => setTimeout(r, 5));
    await source.fetchEvents(UPGRADE_DESCRIPTOR, { fromVersion: 0n });
    release();
    await new Promise((r) => setTimeout(r, 5));
    expect(requests).toEqual([[A, 0n, undefined]]);
  });

  it("refuses an address that is not a string at the call", () => {
    const client = testClient({ adapter: new MemoryAdapter([]) });
    expect(() => client.getUpgrades(42 as never)).toThrow("upgrades needs an address or Aptos name");
    expect(() => client.upgrades(undefined as never, () => {})).toThrow("upgrades needs an address or Aptos name");
  });
});

describe("upgrades", () => {
  it("follows live, retries a failed read, and resolves a name once", async () => {
    let fail = true;
    const { aptos, reads } = reader((_a, v) => {
      if (fail) {
        fail = false;
        throw Object.assign(new Error("busy"), { status: 503 });
      }
      return v >= 10n ? V2 : V1;
    }, Network.MAINNET);
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
      for (let i = 0; !ok(); i++) {
        if (i === 600) throw new Error("timed out waiting");
        await new Promise((r) => setTimeout(r, 5));
      }
    };
    await until(() => got.length === 1);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).cause).toBeInstanceOf(StreamdNetworkError);
    expect(got[0]!.version).toBe(10n);
    expect(got[0]!.data.views.added).toHaveLength(1);
    memory.push(publish(15, 0, A, true), publish(16, 0, B));
    await until(() => got.length === 2);
    expect(client.listenerCount()).toBe(0);
    sub.unsubscribe();
    expect(got.map((e) => e.version)).toEqual([10n, 15n]);
    expect(got[1]!.data.views).toEqual(none);
    expect(lookups).toEqual(["alice.apt"]);
    expect(reads.some(([a]) => a === B)).toBe(false);
  });
});

describe("upgradeSource", () => {
  const range = { fromVersion: 0n, limit: 10 };

  it("ignores other event types, and delivers a page fetched again in full", async () => {
    const reads: bigint[] = [];
    const stray = { ...publish(7, 0, A), type: "0xbad::code::PublishPackage" };
    // Ending on the duplicate, so a page refetched after its last record still has that record first.
    const events = [stray, publish(8, 0, A), publish(8, 3, A)];
    const source = upgradeSource({ fetchEvents: async () => ({ events, nextCursor: "", hasMore: false }) }, async () => A, async (_a, v) => (reads.push(v), V1));
    const first = await source.fetchEvents(UPGRADE_DESCRIPTOR, range);
    // A caller retrying the page, after a failed where or a pause, gets it all again.
    const again = await source.fetchEvents(UPGRADE_DESCRIPTOR, range);
    expect(first.events.map((e) => e.version)).toEqual([8n]);
    expect(again.events.map((e) => e.version)).toEqual([8n]);
    expect(reads.sort((x, y) => Number(x - y))).toEqual([7n, 7n, 8n, 8n]);
  });

  it("stops a page given up on from reading more once the next page starts", async () => {
    const reads: bigint[] = [];
    let release = () => {};
    const gate = new Promise<void>((r) => (release = r));
    const stuck = [publish(10, 0, A), publish(20, 0, A), publish(30, 0, A)];
    let n = 0;
    const source = upgradeSource(
      { fetchEvents: async () => ({ events: n++ === 0 ? stuck : [], nextCursor: "", hasMore: false }) },
      async () => A,
      async (_a, v) => {
        reads.push(v);
        if (v < 11n) await gate;
        return V1;
      },
    );
    const abandoned = source.fetchEvents(UPGRADE_DESCRIPTOR, range);
    abandoned.catch(() => {});
    await new Promise((r) => setTimeout(r, 5));
    await source.fetchEvents(UPGRADE_DESCRIPTOR, range);
    release();
    await expect(abandoned).rejects.toBeInstanceOf(StreamdNetworkError);
    await new Promise((r) => setTimeout(r, 5));
    expect(reads.sort((x, y) => Number(x - y))).toEqual([9n, 10n]);
  });
});
