import { describe, expect, it } from "bun:test";
import { AccountAddress, Network, type Aptos } from "@aptos-labs/ts-sdk";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import { primaryName, resolveName } from "../src/names.js";

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

  it("rejects a 400 as a plain StreamdError", async () => {
    const { aptos } = fakeAptos(() => {
      throw apiError(400);
    });
    const err = await resolveName(aptos, "alice.apt").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
  });

  it("resolves on the networks the SDK has a router for", async () => {
    for (const network of [Network.MAINNET, Network.TESTNET, Network.LOCAL]) {
      const { aptos } = fakeAptos(() => AccountAddress.from(TARGET));
      Object.assign(aptos, { config: { network } });
      expect(await resolveName(aptos, "alice.apt")).toBe(TARGET);
    }
  });

  it("refuses a network without ANS before any read", async () => {
    const { aptos, names } = fakeAptos(() => AccountAddress.ONE);
    Object.assign(aptos, { config: { network: Network.CUSTOM } });
    const err = await resolveName(aptos, "alice.apt").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toContain("custom");
    expect(names).toEqual([]);
  });

  it("rejects a transport failure with no status as a StreamdNetworkError", async () => {
    const { aptos } = fakeAptos(() => {
      throw new TypeError("fetch failed");
    });
    const err = await resolveName(aptos, "alice.apt").catch((e) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect(err.statusCode).toBeUndefined();
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

describe("primaryName", () => {
  function reverse(
    primary: (address: string) => unknown,
    forward: (name: string) => unknown = () => AccountAddress.from(TARGET),
    config?: { network: Network },
  ) {
    const asked: string[] = [];
    const names: string[] = [];
    const aptos = {
      config,
      getPrimaryName: async ({ address }: { address: string }) => {
        asked.push(address);
        return primary(address);
      },
      getTargetAddress: async ({ name }: { name: string }) => {
        names.push(name);
        return forward(name);
      },
    } as unknown as Aptos;
    return { aptos, asked, names };
  }
  const OTHER = `0x${"b0b".padStart(64, "0")}`;

  it("returns a domain with .apt, asking in long form", async () => {
    const one = `0x${"1".padStart(64, "0")}`;
    const { aptos, asked, names } = reverse(() => "alice", () => AccountAddress.from(one));
    expect(await primaryName(aptos, "0x1")).toBe("alice.apt");
    expect(asked).toEqual([one]);
    expect(names).toEqual(["alice.apt"]);
  });

  it("returns a subdomain with .apt", async () => {
    expect(await primaryName(reverse(() => "sub.alice").aptos, TARGET)).toBe("sub.alice.apt");
  });

  it("gives undefined with no forward read when there is no name", async () => {
    for (const none of [undefined, ""]) {
      const { aptos, names } = reverse(() => none);
      expect(await primaryName(aptos, TARGET)).toBeUndefined();
      expect(names).toEqual([]);
    }
  });

  it("gives undefined when the name now points elsewhere", async () => {
    const { aptos } = reverse(() => "alice", () => AccountAddress.from(OTHER));
    expect(await primaryName(aptos, TARGET)).toBeUndefined();
  });

  it("gives undefined when the name no longer resolves", async () => {
    expect(await primaryName(reverse(() => "alice", () => undefined).aptos, TARGET)).toBeUndefined();
  });

  it("rejects a node failure on either read as a network error", async () => {
    const down = () => {
      throw apiError(503);
    };
    await expect(primaryName(reverse(down).aptos, TARGET)).rejects.toBeInstanceOf(StreamdNetworkError);
    await expect(primaryName(reverse(() => "alice", down).aptos, TARGET)).rejects.toBeInstanceOf(
      StreamdNetworkError,
    );
  });

  it("rejects a 400 from the lookup as a StreamdError", async () => {
    const refused = () => {
      throw apiError(400);
    };
    const err = await primaryName(reverse(refused).aptos, TARGET).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err).not.toBeInstanceOf(StreamdNetworkError);
    expect(err.message).toContain("could not be read");
  });

  it("rejects on a network without ANS before any read", async () => {
    const { aptos, asked } = reverse(() => "alice", undefined, { network: Network.CUSTOM });
    await expect(primaryName(aptos, TARGET)).rejects.toThrow("no ANS on network custom");
    expect(asked).toEqual([]);
  });

  it("throws on invalid input before any read", async () => {
    const { aptos, asked } = reverse(() => "alice");
    for (const bad of ["alice.apt", "0xzz", "", undefined as unknown as string]) {
      await expect(primaryName(aptos, bad)).rejects.toBeInstanceOf(StreamdError);
    }
    await expect(primaryName({} as Aptos, TARGET)).rejects.toThrow("needs an Aptos instance");
    expect(asked).toEqual([]);
  });
});
