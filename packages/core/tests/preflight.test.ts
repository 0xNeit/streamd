import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { checkDescriptor } from "../src/preflight.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { ModuleEventDescriptor, ResourceDescriptor } from "../src/types.js";

const ADDR = "0x" + "a".repeat(64);
const ONE = "0x" + "0".repeat(63) + "1";

interface Reply {
  status?: number;
  data: unknown;
}

function fakeNode(route: (path: string) => Reply) {
  const paths: string[] = [];
  const client = {
    async provider(req: { url: string }) {
      const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "").replace(/\?.*$/, "");
      paths.push(path);
      const reply = route(path);
      const status = reply.status ?? 200;
      return { status, statusText: status === 200 ? "OK" : "Error", data: reply.data, headers: {} };
    },
  };
  const aptos = new Aptos(
    new AptosConfig({
      network: Network.LOCAL,
      fullnode: "http://fake/v1",
      client: client as never,
      clientConfig: { http2: false },
    }),
  );
  return { aptos, paths };
}

function struct(name: string, isEvent: boolean, abilities: string[]) {
  return { name, is_event: isEvent, is_native: false, is_enum: false, abilities, generic_type_params: [], fields: [] };
}

const coinModule = {
  bytecode: "0x00",
  abi: {
    address: ONE,
    name: "coin",
    friends: [],
    exposed_functions: [],
    structs: [
      struct("CoinDeposit", true, ["drop", "store"]),
      struct("DepositEvent", false, ["drop", "store"]),
      struct("CoinStore", false, ["key"]),
      struct("OnlyStore", false, ["store"]),
      struct("OnlyDrop", false, ["drop"]),
    ],
  },
};

function serveCoin() {
  return fakeNode((path) => {
    if (path === `accounts/${ONE}/module/coin`) return { data: coinModule };
    return { status: 404, data: { message: "Module not found", error_code: "module_not_found" } };
  });
}

const mod = (eventType: ModuleEventDescriptor["eventType"]): ModuleEventDescriptor => ({
  kind: "module",
  eventType,
});

describe("checkDescriptor", () => {
  it("reports each status and ok across a list sharing one module request", async () => {
    const { aptos, paths } = serveCoin();
    const check = await checkDescriptor(
      aptos,
      mod([
        "0x1::coin::CoinDeposit",
        "0x1::coin::DepositEvent",
        "0x1::coin::CoinStore",
        "0x1::coin::OnlyStore",
        "0x1::coin::OnlyDrop",
        "0x1::coin::Nope",
        "0x1::nope::Thing",
        "0x1::coin::CoinDeposit",
      ]),
    );
    expect(check).toEqual({
      ok: false,
      types: [
        { eventType: "0x1::coin::CoinDeposit", status: "event" },
        { eventType: "0x1::coin::DepositEvent", status: "legacy" },
        { eventType: "0x1::coin::CoinStore", status: "not-event" },
        { eventType: "0x1::coin::OnlyStore", status: "not-event" },
        { eventType: "0x1::coin::OnlyDrop", status: "not-event" },
        { eventType: "0x1::coin::Nope", status: "missing-struct" },
        { eventType: "0x1::nope::Thing", status: "missing-module" },
      ],
    });
    expect(paths).toEqual([`accounts/${ONE}/module/coin`, `accounts/${ONE}/module/nope`]);
  });

  it("is ok for event and legacy types, checking a generic type by its outer struct", async () => {
    const { aptos } = serveCoin();
    const check = await checkDescriptor(
      aptos,
      mod(["0x1::coin::CoinDeposit<0x1::aptos_coin::AptosCoin>", "0x1::coin::DepositEvent"]),
    );
    expect(check.ok).toBe(true);
    expect(check.types[0]).toEqual({
      eventType: "0x1::coin::CoinDeposit<0x1::aptos_coin::AptosCoin>",
      status: "event",
    });
  });

  it("accepts a single type string", async () => {
    const { aptos } = serveCoin();
    expect(await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit"))).toEqual({
      ok: true,
      types: [{ eventType: "0x1::coin::CoinDeposit", status: "event" }],
    });
  });

  it("rejects bad input as StreamdError before any request", async () => {
    const bad: unknown[] = [
      { kind: "account", accountAddress: ADDR },
      null,
      mod([]),
      mod("not a type" as never),
      mod("zz::coin::CoinDeposit"),
      mod("0x1::co-in::CoinDeposit"),
      mod("0x1::coin::Coin%2FDeposit"),
      mod("0x1::coin::Coin/../x"),
      mod(["0x1::coin::CoinDeposit", "0x1::coin::"]),
    ];
    for (const descriptor of bad) {
      const { aptos, paths } = serveCoin();
      const err = await checkDescriptor(aptos, descriptor as ModuleEventDescriptor).catch((e) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
      expect(paths).toEqual([]);
    }
  });

  it("rejects a failed request as StreamdNetworkError with its status", async () => {
    const { aptos } = fakeNode(() => ({ status: 500, data: { message: "boom" } }));
    const err = await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit")).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(500);
  });

  it("rejects a module response without an ABI as StreamdError", async () => {
    const { aptos } = fakeNode(() => ({ data: { bytecode: "0x00" } }));
    const err = await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit")).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("reads a struct with no is_event as not declaring #[event]", async () => {
    const { is_event: _, ...old } = struct("DepositEvent", false, ["drop", "store"]);
    const { aptos } = fakeNode(() => ({ data: { ...coinModule, abi: { ...coinModule.abi, structs: [null, old] } } }));
    expect((await checkDescriptor(aptos, mod("0x1::coin::DepositEvent"))).types[0]!.status).toBe("legacy");
  });

  it("rejects malformed ABI struct entries as StreamdError", async () => {
    const bad = [
      [null, struct("CoinDeposit", "yes" as never, ["drop", "store"])],
      [{ ...struct("CoinDeposit", false, []), abilities: "drop store" }],
    ];
    for (const structs of bad) {
      const { aptos } = fakeNode(() => ({ data: { ...coinModule, abi: { ...coinModule.abi, structs } } }));
      const err = await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit")).catch((e) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
  });

  it("does not answer one node's check from another's", async () => {
    await checkDescriptor(serveCoin().aptos, mod("0x1::coin::CoinDeposit"));
    const { aptos } = fakeNode(() => ({ status: 404, data: { message: "Module not found" } }));
    const check = await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit"));
    expect(check.types[0]!.status).toBe("missing-module");
  });
});

describe("checkDescriptor on handles and resources", () => {
  const STORE = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
  const STORE_PATH = `accounts/${ADDR}/resource/${encodeURIComponent(STORE)}`;
  const handle = (creation: string, addr = ADDR) => ({
    counter: "3",
    guid: { id: { addr, creation_num: creation } },
  });
  const coin = {
    bytecode: "0x00",
    abi: {
      address: ONE,
      name: "coin",
      friends: [],
      exposed_functions: [],
      structs: [struct("CoinStore", false, ["key"]), struct("Plain", false, ["store"])],
    },
  };

  function node(resource: Reply, resources: unknown[] = []) {
    return fakeNode((path) => {
      if (path === `accounts/${ONE}/module/coin`) return { data: coin };
      if (path === `accounts/${ADDR}/resources`) return { data: resources };
      if (path === STORE_PATH) return resource;
      return { status: 404, data: { message: "nope" } };
    });
  }

  const byField = (fieldName: string) => ({
    kind: "handle" as const,
    accountAddress: ADDR,
    eventHandleStruct: STORE,
    fieldName,
  });
  const byNumber = (creationNumber: bigint) => ({
    kind: "handle" as const,
    accountAddress: ADDR,
    creationNumber,
  });
  const res = (resourceType: string) => ({
    kind: "resource" as const,
    accountAddress: ADDR,
    resourceType: resourceType as ResourceDescriptor["resourceType"],
  });
  const ok404: Reply = { status: 404, data: { message: "nope" } };

  it("finds a handle by struct and field in one request", async () => {
    const { aptos, paths } = node({ data: { type: STORE, data: { deposits: handle("2") } } });
    expect(await checkDescriptor(aptos, byField("deposits"))).toEqual({
      ok: true,
      types: [{ eventType: STORE, status: "handle" }],
    });
    expect(paths).toEqual([STORE_PATH]);
  });

  it("reports a field that is absent or not a handle", async () => {
    const { aptos } = node({ data: { type: STORE, data: { n: "1", other: handle("2") } } });
    for (const field of ["n", "missing"]) {
      expect(await checkDescriptor(aptos, byField(field))).toEqual({
        ok: false,
        types: [{ eventType: STORE, status: "not-handle" }],
      });
    }
  });

  it("reports a resource the account does not hold", async () => {
    const { aptos } = node(ok404);
    expect(await checkDescriptor(aptos, byField("deposits"))).toEqual({
      ok: false,
      types: [{ eventType: STORE, status: "missing-resource" }],
    });
  });

  it("finds a creation number nested in a resource", async () => {
    const { aptos, paths } = node(ok404, [
      { type: "0x1::a::B", data: { outer: { list: [{ inner: handle("7") }] } } },
    ]);
    expect(await checkDescriptor(aptos, byNumber(7n))).toEqual({
      ok: true,
      types: [{ eventType: "7", status: "handle" }],
    });
    expect(paths).toEqual([`accounts/${ADDR}/resources`]);
  });

  it("does not match a creation number held at another address or a different number", async () => {
    const { aptos } = node(ok404, [
      { type: "0x1::a::B", data: { h: handle("7", ONE), i: handle("8") } },
    ]);
    expect(await checkDescriptor(aptos, byNumber(7n))).toEqual({
      ok: false,
      types: [{ eventType: "7", status: "missing-handle" }],
    });
  });

  it("matches a guid address whatever its case", async () => {
    const { aptos } = node(ok404, [
      { type: "0x1::a::B", data: { h: handle("7", `0x${"A".repeat(64)}`) } },
    ]);
    expect((await checkDescriptor(aptos, byNumber(7n))).ok).toBe(true);
  });

  it("reports a missing handle on an account with no resources", async () => {
    const { aptos } = node(ok404, []);
    expect((await checkDescriptor(aptos, byNumber(1n))).types[0]!.status).toBe("missing-handle");
  });

  it("treats a 404 listing resources as a missing handle", async () => {
    const { aptos } = fakeNode(() => ok404);
    expect((await checkDescriptor(aptos, byNumber(1n))).types[0]!.status).toBe("missing-handle");
  });

  it("reports a held resource and a valid one not held, both ok", async () => {
    const held = node({ data: { type: STORE, data: {} } });
    expect(await checkDescriptor(held.aptos, res(STORE))).toEqual({
      ok: true,
      types: [{ eventType: STORE, status: "resource" }],
    });
    expect(held.paths).toEqual([`accounts/${ONE}/module/coin`, STORE_PATH]);
    const absent = node(ok404);
    expect(await checkDescriptor(absent.aptos, res(STORE))).toEqual({
      ok: true,
      types: [{ eventType: STORE, status: "not-held" }],
    });
  });

  it.each([
    ["0x1::coin::Plain", "not-resource"],
    ["0x1::coin::Nope", "missing-struct"],
    ["0x1::nope::Thing", "missing-module"],
  ])("reports %s as %s without reading the resource", async (type, status) => {
    const { aptos, paths } = node(ok404);
    expect(await checkDescriptor(aptos, res(type))).toEqual({
      ok: false,
      types: [{ eventType: type, status: status as "not-resource" }],
    });
    expect(paths).toHaveLength(1);
  });

  it("rejects a resource reply without data and malformed abilities", async () => {
    const { aptos } = node({ data: { type: STORE } });
    await expect(checkDescriptor(aptos, byField("x"))).rejects.toThrow("without data");
    const bad = fakeNode(() => ({
      data: { bytecode: "0x", abi: { structs: [{ name: "Plain", abilities: "key" }] } },
    }));
    await expect(checkDescriptor(bad.aptos, res("0x1::coin::Plain"))).rejects.toThrow("malformed ABI");
  });

  it("rejects when the node fails", async () => {
    const failing = () => fakeNode(() => ({ status: 500, data: { message: "boom" } })).aptos;
    await expect(checkDescriptor(failing(), byField("x"))).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(checkDescriptor(failing(), byNumber(1n))).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(checkDescriptor(failing(), res(STORE))).rejects.toBeInstanceOf(StreamdNetworkError);
  });

  it("throws on invalid descriptors before any request", async () => {
    const { aptos, paths } = node(ok404);
    const bad = [
      { kind: "handle", accountAddress: "nope", creationNumber: 1n },
      { kind: "handle", accountAddress: ADDR, creationNumber: -1n },
      { kind: "handle", accountAddress: ADDR, eventHandleStruct: STORE },
      { kind: "handle", accountAddress: ADDR, eventHandleStruct: "nope", fieldName: "f" },
      { kind: "resource", accountAddress: ADDR, resourceType: "nope" },
      { kind: "resource", accountAddress: "nope", resourceType: STORE },
      { kind: "account", accountAddress: ADDR },
      null,
    ];
    for (const descriptor of bad) {
      await expect(checkDescriptor(aptos, descriptor as never)).rejects.toBeInstanceOf(StreamdError);
    }
    expect(paths).toEqual([]);
  });
});
