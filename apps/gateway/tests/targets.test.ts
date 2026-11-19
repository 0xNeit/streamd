import { describe, expect, it } from "bun:test";
import { StreamdError } from "@streamd/core";
import { checkTarget, isPrivateAddress, parseTarget } from "../src/targets.js";
import type { Lookup } from "../src/targets.js";

const resolvesTo = (...addresses: string[]): Lookup => async () =>
  addresses.map((address) => ({ address, family: address.includes(":") ? 6 : 4 }));

describe("isPrivateAddress", () => {
  it("refuses loopback, private, link-local, CGNAT, multicast, reserved and unspecified addresses", () => {
    for (const address of [
      "127.0.0.1", "10.2.3.4", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254",
      "100.64.0.1", "0.0.0.0", "224.0.0.1", "255.255.255.255", "198.18.0.1",
      "::1", "::", "fd00::1", "fe80::1", "ff02::1", "::ffff:127.0.0.1", "::ffff:10.0.0.1", "64:ff9b::a00:1",
      "::7f00:1", "::ffff:0:7f00:1", "2002:7f00:1::", "2001:0:4136::1", "64:ff9b:1::1", "fec0::1", "192.88.99.1",
    ]) {
      expect([address, isPrivateAddress(address)]).toEqual([address, true]);
    }
  });

  it("accepts public addresses", () => {
    for (const address of ["8.8.8.8", "172.32.0.1", "1.1.1.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"]) {
      expect([address, isPrivateAddress(address)]).toEqual([address, false]);
    }
  });
});

describe("parseTarget", () => {
  it("takes an http or https URL", () => {
    expect(parseTarget("https://example.com/hook?x=1").href).toBe("https://example.com/hook?x=1");
  });

  it("refuses other schemes, credentials, relative and oversized URLs", () => {
    for (const value of ["ftp://example.com/", "file:///etc/passwd", "https://user:pw@example.com/", "/hook", "", 7, `https://example.com/${"a".repeat(2048)}`]) {
      expect(() => parseTarget(value)).toThrow(StreamdError);
    }
  });
});

describe("checkTarget", () => {
  it("refuses a literal private host, in any spelling the URL parser normalises", async () => {
    for (const value of ["http://127.0.0.1:8080/", "http://[::1]/", "http://0x7f000001/", "http://2130706433/", "http://[::ffff:169.254.169.254]/"]) {
      await expect(checkTarget(parseTarget(value), { lookup: resolvesTo("8.8.8.8") })).rejects.toThrow("not a public address");
    }
  });

  it("refuses a name when any address it resolves to is private", async () => {
    const url = parseTarget("https://hooks.example.com/");
    await expect(checkTarget(url, { lookup: resolvesTo("8.8.8.8", "10.0.0.5") })).rejects.toThrow("hooks.example.com is not a public address");
    await expect(checkTarget(url, { lookup: resolvesTo() })).rejects.toThrow("cannot resolve");
    await expect(checkTarget(url, { lookup: async () => { throw new Error("ENOTFOUND"); } })).rejects.toThrow("cannot resolve hooks.example.com");
  });

  it("accepts a name resolving only to public addresses, and anything under allowPrivate", async () => {
    await checkTarget(parseTarget("https://hooks.example.com/"), { lookup: resolvesTo("93.184.215.14", "2606:2800::1") });
    await checkTarget(parseTarget("http://127.0.0.1:9/"), { allowPrivate: true });
  });
});
