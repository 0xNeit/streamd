import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { checkDescriptor } from "../src/preflight.js";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import type { ModuleEventDescriptor } from "../src/types.js";

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
      { kind: "handle", accountAddress: ADDR, eventHandleStruct: "0x1::a::B", fieldName: "f" },
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

  it("does not answer one node's check from another's", async () => {
    await checkDescriptor(serveCoin().aptos, mod("0x1::coin::CoinDeposit"));
    const { aptos } = fakeNode(() => ({ status: 404, data: { message: "Module not found" } }));
    const check = await checkDescriptor(aptos, mod("0x1::coin::CoinDeposit"));
    expect(check.types[0]!.status).toBe("missing-module");
  });
});
