import { describe, expect, it } from "bun:test";
import { StreamdError, StreamdNetworkError } from "@streamd/core";
import type { NameOptions } from "../src/args.js";
import { runName } from "../src/name.js";
import type { NameIO } from "../src/name.js";

const ALICE = `0x${"a1".padStart(64, "0")}`;

function harness(io: Partial<NameIO> = {}) {
  const controller = new AbortController();
  const lines: string[] = [];
  const errors: string[] = [];
  const calls: string[] = [];
  const full: NameIO = {
    resolveName: async (v) => (calls.push(`resolve ${v}`), ALICE),
    primaryName: async (a) => (calls.push(`primary ${a}`), "alice.apt"),
    write: (l) => void lines.push(l),
    error: (m) => errors.push(m),
    signal: controller.signal,
    ...io,
  };
  const run = (value: string, options: Partial<NameOptions> = {}) => runName({ network: "mainnet", value, ...options }, full);
  return { run, lines, errors, calls, controller };
}

describe("runName", () => {
  it("resolves a name to its address, printing the name as given", async () => {
    const h = harness();
    expect(await h.run("alice")).toBe(0);
    expect(h.calls).toEqual(["resolve alice"]);
    expect(h.lines).toEqual([`{"name":"alice","address":"${ALICE}"}\n`]);
  });

  it("looks up an address's primary name, printing the address in long form", async () => {
    const h = harness();
    expect(await h.run("0xa1")).toBe(0);
    expect(h.calls).toEqual(["resolve 0xa1", `primary ${ALICE}`]);
    expect(h.lines).toEqual([`{"name":"alice.apt","address":"${ALICE}"}\n`]);
  });

  it("prints a null name and exits 0 for an address with no primary name", async () => {
    const h = harness({ primaryName: async () => undefined });
    expect(await h.run(ALICE)).toBe(0);
    expect(h.lines).toEqual([`{"name":null,"address":"${ALICE}"}\n`]);
    expect(h.errors).toEqual([]);
  });

  it("exits 1 with the library's message for a refused name, a malformed address or a failed read", async () => {
    const refused = harness({
      resolveName: () => Promise.reject(new StreamdError("SuiNS name nobody.sui is not registered")),
    });
    expect(await refused.run("nobody.sui")).toBe(1);
    expect(refused.errors).toEqual(["streamd: SuiNS name nobody.sui is not registered"]);
    expect(refused.lines).toEqual([]);

    const failed = harness({
      primaryName: () => Promise.reject(new StreamdNetworkError("Request for the primary name failed", undefined, new Error("ECONNRESET"))),
    });
    expect(await failed.run(ALICE)).toBe(1);
    expect(failed.errors[0]).toStartWith("streamd: Request for the primary name failed");
    expect(failed.lines).toEqual([]);
  });

  it("exits 1 when the write fails, and 0 when the output closed", async () => {
    const failing = harness({ write: () => Promise.reject(new Error("disk full")) });
    expect(await failing.run("alice.apt")).toBe(1);
    expect(failing.errors).toEqual(["streamd: disk full"]);

    const closed = harness({
      write: () => {
        closed.controller.abort();
        return Promise.reject(new Error("aborted"));
      },
    });
    expect(await closed.run("alice.apt")).toBe(0);
    expect(closed.errors).toEqual([]);
  });
});
