import { describe, expect, it } from "bun:test";
import { AccountAddress, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "../src/errors.js";
import { resolveName } from "../src/names.js";

const TARGET = `0x${"a11ce".padStart(64, "0")}`;

function fakeAptos(answer: (name: string) => unknown) {
  const names: string[] = [];
  const aptos = {
    getTargetAddress: async ({ name }: { name: string }) => {
      names.push(name);
      return answer(name);
    },
  } as unknown as Aptos;
  return { aptos, names };
}

function apiError(status: number): Error {
  return Object.assign(new Error(`status ${status}`), { status });
}

describe("resolveName", () => {
  it("returns an address in long form without a read", async () => {
    const { aptos, names } = fakeAptos(() => {
      throw new Error("unexpected read");
    });
    expect(await resolveName(aptos, "0x1")).toBe(`0x${"1".padStart(64, "0")}`);
    expect(names).toEqual([]);
  });

  it("resolves a name to its target in long form", async () => {
    const { aptos, names } = fakeAptos(() => AccountAddress.from(TARGET));
    expect(await resolveName(aptos, "alice.apt")).toBe(TARGET);
    expect(await resolveName(aptos, "sub.alice")).toBe(TARGET);
    expect(names).toEqual(["alice.apt", "sub.alice"]);
  });

  it("rejects a name with no target as a StreamdError naming it", async () => {
    const { aptos } = fakeAptos(() => undefined);
    const err = await resolveName(aptos, "gone.apt").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toContain("gone.apt");
  });

  it("rejects an SDK refusal and a 400 as plain StreamdErrors", async () => {
    for (const thrown of [new Error("The ANS contract is not deployed to custom"), apiError(400)]) {
      const { aptos } = fakeAptos(() => {
        throw thrown;
      });
      const err = await resolveName(aptos, "alice.apt").catch((e) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err).not.toBeInstanceOf(StreamdNetworkError);
    }
  });

  it("rejects other failures as a StreamdNetworkError with the status", async () => {
    const { aptos } = fakeAptos(() => {
      throw apiError(503);
    });
    const err = await resolveName(aptos, "alice.apt").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBe(503);
  });

  it("refuses malformed input before any read", async () => {
    const { aptos, names } = fakeAptos(() => AccountAddress.ONE);
    for (const bad of ["0xzz", "ab", "-ab.apt", "a.b.c.apt", "Alice.apt", ""]) {
      await expect(resolveName(aptos, bad)).rejects.toBeInstanceOf(StreamdError);
    }
    expect(names).toEqual([]);
  });

  it("needs an Aptos instance for a name", async () => {
    await expect(resolveName({} as Aptos, "alice.apt")).rejects.toThrow("needs an Aptos instance");
  });
});
