import { describe, expect, it } from "bun:test";
import { AccountAddress, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { ownedBy } from "../src/ownership.js";
import type { StreamdEvent } from "../src/types.js";

const WALLET = `0x${"a11ce".padStart(63, "0")}`;
const WALLET_LONG = `0x${"a11ce".padStart(64, "0")}`;
const long = (hex: string) => `0x${hex.padStart(64, "0")}`;

interface Call {
  function: string;
  typeArguments: string[];
  functionArguments: string[];
  ledgerVersion: unknown;
}

function fakeAptos(answer: (call: Call) => unknown) {
  const calls: Call[] = [];
  const aptos = {
    viewJson: async ({ payload, options }: { payload: Omit<Call, "ledgerVersion">; options: { ledgerVersion: unknown } }) => {
      const call = { ...payload, ledgerVersion: options.ledgerVersion };
      calls.push(call);
      return answer(call);
    },
  } as unknown as Aptos;
  return { aptos, calls };
}

function apiError(status: number, vmErrorCode: number | null = null): Error {
  return Object.assign(new Error(`status ${status}`), { status, data: { vm_error_code: vmErrorCode } });
}

const ev = (data: unknown, version = 10n): StreamdEvent => ({
  type: "0x1::fungible_asset::Deposit",
  data,
  version,
  sequenceNumber: 0n,
  eventIndex: 0,
});

const owned = (...objects: string[]) => (call: Call) => [objects.map(long).includes(call.functionArguments[0])];

describe("ownedBy", () => {
  it("reads owns<ObjectCore> for the named store at the event's version", async () => {
    const { aptos, calls } = fakeAptos(owned("5"));
    const where = ownedBy(aptos, WALLET);
    expect(await where(ev({ store: "0x5", amount: "1" }, 42n))).toBe(true);
    expect(calls).toEqual([
      {
        function: "0x1::object::owns",
        typeArguments: ["0x1::object::ObjectCore"],
        functionArguments: [long("5"), WALLET_LONG],
        ledgerVersion: 42n,
      },
    ]);
    expect(await where(ev({ store: "0x6" }, 42n))).toBe(false);
  });

  it("reads object, token, token_address and Object<T> as { inner }", async () => {
    const { aptos } = fakeAptos(owned("7"));
    const where = ownedBy(aptos, WALLET);
    expect(await where(ev({ object: "0x7", from: "0x1", to: "0x2" }))).toBe(true);
    expect(await where(ev({ token: "0x7" }))).toBe(true);
    expect(await where(ev({ token_address: "0x7" }))).toBe(true);
    expect(await where(ev({ object: { inner: "0x7" } }))).toBe(true);
  });

  it("makes no read for an event naming no store or object", async () => {
    const { aptos, calls } = fakeAptos(() => [true]);
    const where = ownedBy(aptos, WALLET);
    expect(await where(ev({ amount: "1" }))).toBe(false);
    expect(await where(ev({ store: "not an address", object: 5 }))).toBe(false);
    expect(await where(ev(null))).toBe(false);
    expect(calls).toEqual([]);
  });

  it("checks fields in order and stops at the first owned one", async () => {
    const { aptos, calls } = fakeAptos(owned("2"));
    const where = ownedBy(aptos, WALLET, { fields: ["a", "b", "c"] });
    expect(await where(ev({ a: "0x1", b: "0x2", c: "0x3", store: "0x9" }))).toBe(true);
    expect(calls.map((c) => c.functionArguments[0])).toEqual([long("1"), long("2")]);
  });

  it("answers false for an object missing at the version", async () => {
    const { aptos } = fakeAptos(() => {
      throw apiError(400, 4016);
    });
    expect(await ownedBy(aptos, WALLET)(ev({ store: "0x5" }))).toBe(false);
  });

  it("rejects another 400 with a plain StreamdError", async () => {
    const { aptos } = fakeAptos(() => {
      throw apiError(400, 4008);
    });
    const err = await Promise.resolve(ownedBy(aptos, WALLET)(ev({ store: "0x5" }))).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("rejects other failures as network errors and reads again on retry", async () => {
    let fail = true;
    const { aptos, calls } = fakeAptos(() => {
      if (fail) throw apiError(503);
      return [true];
    });
    const where = ownedBy(aptos, WALLET);
    const err = await Promise.resolve(where(ev({ store: "0x5" }))).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(503);
    fail = false;
    expect(await where(ev({ store: "0x5" }))).toBe(true);
    expect(calls).toHaveLength(2);
  });

  it("rejects a response that is not one boolean", async () => {
    const { aptos } = fakeAptos(() => ["yes"]);
    await expect(Promise.resolve(ownedBy(aptos, WALLET)(ev({ store: "0x5" })))).rejects.toThrow("returned no boolean");
  });

  it("reads each (object, version) once, concurrent callers included", async () => {
    const { aptos, calls } = fakeAptos(owned("5"));
    const where = ownedBy(aptos, WALLET);
    const answers = await Promise.all([
      where(ev({ store: "0x5" }, 1n)),
      where(ev({ store: "0x0005" }, 1n)),
      where(ev({ store: "0x5" }, 2n)),
    ]);
    expect(answers).toEqual([true, true, true]);
    expect(await where(ev({ store: "0x5" }, 1n))).toBe(true);
    expect(calls.map((c) => c.ledgerVersion)).toEqual([1n, 2n]);
  });

  it("keeps at most maxConcurrentRequests reads in flight", async () => {
    let active = 0;
    let peak = 0;
    const aptos = {
      viewJson: async () => {
        peak = Math.max(peak, ++active);
        await new Promise((r) => setTimeout(r, 1));
        active--;
        return [false];
      },
    } as unknown as Aptos;
    const where = ownedBy(aptos, WALLET, { maxConcurrentRequests: 2 });
    await Promise.all([1n, 2n, 3n, 4n, 5n].map((v) => where(ev({ store: "0x5" }, v))));
    expect(peak).toBe(2);
  });

  it("resolves a name once and reads with the resolved wallet", async () => {
    const { aptos, calls } = fakeAptos(owned("5"));
    let lookups = 0;
    Object.assign(aptos, { getTargetAddress: async () => (lookups++, AccountAddress.from(WALLET_LONG)) });
    const where = ownedBy(aptos, "alice.apt");
    expect(lookups).toBe(0);
    expect(await Promise.all([where(ev({ store: "0x5" }, 1n)), where(ev({ store: "0x6" }, 2n))])).toEqual([true, false]);
    expect(await where(ev({ store: "0x5" }, 3n))).toBe(true);
    expect(lookups).toBe(1);
    expect(calls.map((c) => c.functionArguments[1])).toEqual([WALLET_LONG, WALLET_LONG, WALLET_LONG]);
  });

  it("retries a failed name resolution on the next evaluation", async () => {
    const { aptos } = fakeAptos(owned("5"));
    let lookups = 0;
    Object.assign(aptos, {
      getTargetAddress: async () => {
        if (lookups++ === 0) throw apiError(503);
        return AccountAddress.from(WALLET_LONG);
      },
    });
    const where = ownedBy(aptos, "alice.apt");
    await expect(where(ev({ store: "0x5" }))).rejects.toBeInstanceOf(StreamdNetworkError);
    expect(await where(ev({ store: "0x5" }))).toBe(true);
    expect(lookups).toBe(2);
  });

  it("validates its arguments at construction", () => {
    const { aptos } = fakeAptos(() => [true]);
    expect(() => ownedBy(aptos, "0xzz")).toThrow(StreamdError);
    expect(() => ownedBy(aptos, "no")).toThrow(StreamdError);
    expect(() => ownedBy({} as Aptos, WALLET)).toThrow("needs an Aptos instance");
    expect(() => ownedBy(aptos, WALLET, { fields: [] })).toThrow("fields");
    expect(() => ownedBy(aptos, WALLET, { fields: [""] })).toThrow("fields");
    expect(() => ownedBy(aptos, WALLET, { maxConcurrentRequests: 0 })).toThrow("maxConcurrentRequests");
    expect(() => ownedBy(aptos, WALLET, { maxConcurrentRequests: 1.5 })).toThrow("maxConcurrentRequests");
  });
});
