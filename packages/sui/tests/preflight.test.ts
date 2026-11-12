import { describe, expect, it } from "bun:test";
import type { SuiGrpcClient } from "@mysten/sui/grpc";
import { StreamdError, StreamdNetworkError, type EventDescriptor, type ModuleEventDescriptor } from "@streamd/core";
import { checkDescriptor } from "../src/preflight.js";
import { LONG, grpcError } from "./helpers.js";

const COPY = 1;
const DROP = 2;
const STORE = 3;
const KEY = 4;
const PKG = LONG("a");
const V2 = LONG("a2");

const STRUCT = 1;
const ENUM = 2;
const dt = (name: string, abilities: number[], definingId = PKG, kind = STRUCT) => ({ name, abilities, definingId, kind });

/** A package service answering `getPackage` from `packages` (modules by name, or an error to throw); anything unlisted is `NOT_FOUND`. */
function client(packages: Record<string, Record<string, unknown[]> | Error | { response: unknown }>) {
  const reads: string[] = [];
  const movePackageService = {
    getPackage: async ({ packageId }: { packageId: string }) => {
      reads.push(packageId);
      const value = packages[packageId];
      if (value === undefined) throw grpcError("NOT_FOUND");
      if (value instanceof Error) throw value;
      if ("response" in value) return value;
      return { response: { package: { modules: Object.entries(value).map(([name, datatypes]) => ({ name, datatypes })) } } };
    },
  };
  return { sui: { movePackageService } as unknown as SuiGrpcClient, reads };
}

const ONE = {
  [PKG]: {
    pool: [dt("Swapped", [COPY, DROP]), dt("Flag", [COPY, DROP, STORE]), dt("Pool", [KEY, STORE]), dt("Ticket", [DROP]), dt("Side", [COPY, DROP], PKG, ENUM)],
  },
  [V2]: { pool: [dt("Swapped", [COPY, DROP], PKG), dt("Added", [COPY, DROP], V2)] },
  [LONG("5")]: grpcError("INVALID_ARGUMENT", "object is not a package"),
};

const mod = (eventType: string | string[]) => ({ kind: "module", eventType }) as ModuleEventDescriptor;

async function rejects(promise: Promise<unknown>, type: new (...a: never[]) => Error, text: string) {
  const err = await promise.then(() => undefined, (e: unknown) => e);
  expect(err).toBeInstanceOf(type);
  expect((err as Error).message).toContain(text);
  return err;
}

describe("checkDescriptor", () => {
  it("calls copy + drop datatypes events and the rest not", async () => {
    const { sui, reads } = client(ONE);
    const check = await checkDescriptor(sui, mod(["0xa::pool::Swapped", "0xa::pool::Flag", "0xa::pool::Pool", "0xa::pool::Ticket", "0xa::pool::Side"]));
    expect(check).toEqual({
      ok: false,
      types: [
        { eventType: `${PKG}::pool::Swapped`, status: "event" },
        { eventType: `${PKG}::pool::Flag`, status: "event" },
        { eventType: `${PKG}::pool::Pool`, status: "not-event" },
        { eventType: `${PKG}::pool::Ticket`, status: "not-event" },
        { eventType: `${PKG}::pool::Side`, status: "event" },
      ],
    });
    expect(reads).toEqual([PKG]);
    expect((await checkDescriptor(sui, mod("0xa::pool::Swapped"))).ok).toBe(true);
  });

  it("reports missing packages, modules and structs", async () => {
    const { sui } = client(ONE);
    const check = await checkDescriptor(sui, mod(["0xb::pool::Swapped", "0x5::coin::Coin", "0xa::nope::Swapped", "0xa::pool::Nope"]));
    expect(check.ok).toBe(false);
    expect(check.types.map((t) => t.status)).toEqual(["missing-package", "missing-package", "missing-module", "missing-struct"]);
  });

  it("names the defining package of a type an upgrade inherited", async () => {
    const { sui } = client(ONE);
    const check = await checkDescriptor(sui, mod([`${V2}::pool::Swapped`, `${V2}::pool::Added`]));
    expect(check).toEqual({
      ok: false,
      types: [
        { eventType: `${V2}::pool::Swapped`, status: "defined-elsewhere", definingId: PKG },
        { eventType: `${V2}::pool::Added`, status: "event" },
      ],
    });
  });

  it("dedupes short and long forms, and checks a generic instantiation by its outer datatype", async () => {
    const { sui, reads } = client({ [LONG("2")]: { coin: [dt("CurrencyCreated", [COPY, DROP], LONG("2"))] } });
    const check = await checkDescriptor(sui, mod(["0x2::coin::CurrencyCreated<0x2::sui::SUI>", `${LONG("2")}::coin::CurrencyCreated<0x2::sui::SUI>`]));
    expect(check).toEqual({
      ok: true,
      types: [{ eventType: `${LONG("2")}::coin::CurrencyCreated<${LONG("2")}::sui::SUI>`, status: "event" }],
    });
    expect(reads).toEqual([LONG("2")]);
  });

  it("validates an account's address and checks its eventTypes", async () => {
    const { sui, reads } = client(ONE);
    await rejects(checkDescriptor(sui, { kind: "account", accountAddress: "alice" }), StreamdError, "Not a valid Sui address");
    expect(await checkDescriptor(sui, { kind: "account", accountAddress: "0xb0b" })).toEqual({ ok: true, types: [] });
    expect(reads).toEqual([]);
    const typed = await checkDescriptor(sui, { kind: "account", accountAddress: "0xb0b", eventTypes: ["0xa::pool::Swapped"] });
    expect(typed).toEqual({ ok: true, types: [{ eventType: `${PKG}::pool::Swapped`, status: "event" }] });
    expect(await checkDescriptor(sui, { kind: "account", accountAddress: "0xb0b", eventTypes: [] })).toEqual({ ok: false, types: [] });
  });

  it("refuses what it cannot check before any read", async () => {
    const { sui, reads } = client(ONE);
    const handle = { kind: "handle", accountAddress: "0x1", eventHandleStruct: "0x1::a::B", fieldName: "c" } as EventDescriptor;
    const resource = { kind: "resource", accountAddress: "0x1", resourceType: "0x1::a::B" } as EventDescriptor;
    for (const d of [handle, resource]) await rejects(checkDescriptor(sui, d as never), StreamdError, "module and account");
    await rejects(checkDescriptor(sui, mod(["0xa::pool::Swapped", "not a type"])), StreamdError, "Not a valid Sui event type: not a type");
    await rejects(checkDescriptor(sui, mod([])), StreamdError, "at least one eventType");
    await rejects(checkDescriptor({} as SuiGrpcClient, mod("0xa::pool::Swapped")), StreamdError, "needs a SuiGrpcClient");
    expect(reads).toEqual([]);
  });

  it("rejects a transport failure as a network error and a malformed package as a StreamdError", async () => {
    const { sui } = client({
      [PKG]: grpcError("UNAVAILABLE", "down"),
      [LONG("b")]: grpcError("RESOURCE_EXHAUSTED", "slow down"),
      [LONG("c")]: { response: { package: {} } },
      [LONG("d")]: { pool: [{ name: "X" }] },
      [LONG("e")]: { pool: null } as never,
    });
    const err = await rejects(checkDescriptor(sui, mod("0xa::pool::X")), StreamdNetworkError, "down");
    expect((err as StreamdNetworkError).statusCode).toBeUndefined();
    const limited = await rejects(checkDescriptor(sui, mod("0xb::pool::X")), StreamdNetworkError, "slow down");
    expect((limited as StreamdNetworkError).statusCode).toBe(429);
    for (const p of ["c", "d", "e"]) {
      const e = await rejects(checkDescriptor(sui, mod(`0x${p}::pool::X`)), StreamdError, "Sui node returned");
      expect(e).not.toBeInstanceOf(StreamdNetworkError);
    }
  });
});
