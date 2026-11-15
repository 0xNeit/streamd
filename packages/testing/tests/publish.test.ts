import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StreamdError } from "@streamd/core";
import { buildPublishPayload, parsePublishPayload } from "../src/publish.js";

const FAKE = new URL("./fixtures/fake-aptos.ts", import.meta.url).pathname;
const GOOD = { function_id: "0x1::code::publish_package_txn", args: [{ type: "hex", value: "0x0102" }, { type: "hex", value: ["0xa11ceb0b"] }] };

afterEach(() => {
  delete process.env.FAKE_APTOS;
  delete process.env.FAKE_APTOS_LOG;
});

describe("buildPublishPayload", () => {
  it("returns the metadata and modules the CLI wrote", async () => {
    expect(await buildPublishPayload(FAKE, "/pkg", { counter: "0x1" })).toEqual({
      metadataBytes: "0x0102",
      moduleBytecode: ["0xa11ceb0b", "0xa11ceb0c"],
    });
  });

  it("rejects a compile failure with the CLI's output, named addresses in long form", async () => {
    process.env.FAKE_APTOS = "compile-error";
    const err = await buildPublishPayload(FAKE, "/pkg", { counter: "0xA", other: "0x2" }).catch((e) => e);
    expect(err).toBeInstanceOf(StreamdError);
    expect(err.message).toContain("Compiling /pkg failed");
    expect(err.message).toContain("unbound module");
    expect(err.message).toContain(`"--named-addresses","counter=0x${"0".repeat(63)}a,other=0x${"0".repeat(63)}2"`);
    expect(err.message).toContain('"--skip-fetch-latest-git-deps"');
  });

  it("refuses a named address that is not a Move identifier or not an address, before the CLI runs", async () => {
    process.env.FAKE_APTOS = "compile-error";
    for (const named of <Record<string, string>[]>[{ "a=b": "0x1" }, { "1x": "0x1" }, { counter: "alice.apt" }, { counter: "0x1,b=0x2" }]) {
      const err = await buildPublishPayload(FAKE, "/pkg", named).catch((e) => e);
      expect(err).toBeInstanceOf(StreamdError);
      expect(err.message).not.toContain("Compiling");
    }
  });

  it("rejects when the CLI wrote no payload, or not a publish", async () => {
    process.env.FAKE_APTOS = "no-payload";
    await expect(buildPublishPayload(FAKE, "/pkg")).rejects.toThrow("wrote no readable publish payload");
    process.env.FAKE_APTOS = "bad-payload";
    await expect(buildPublishPayload(FAKE, "/pkg")).rejects.toThrow("not a 0x1::code::publish_package_txn call");
  });

  it("runs compiles of one package one after another", async () => {
    const dir = await mkdtemp(join(tmpdir(), "streamd-publish-test-"));
    try {
      process.env.FAKE_APTOS_LOG = join(dir, "same.log");
      await Promise.all([buildPublishPayload(FAKE, "/pkg"), buildPublishPayload(FAKE, "/pkg/../pkg")]);
      expect(await readFile(join(dir, "same.log"), "utf8")).toBe("start\nend\nstart\nend\n");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("says the CLI must be installed when it is missing", async () => {
    await expect(buildPublishPayload("/nonexistent/aptos", "/pkg")).rejects.toThrow("The aptos CLI was not found");
  });
});

describe("parsePublishPayload", () => {
  it("takes the two hex arguments", () => {
    expect(parsePublishPayload(GOOD)).toEqual({ metadataBytes: "0x0102", moduleBytecode: ["0xa11ceb0b"] });
  });

  it("refuses every other shape", () => {
    const bad: unknown[] = [
      null,
      "x",
      { ...GOOD, function_id: "0x1::code::publish_package" },
      { ...GOOD, args: [GOOD.args[0]] },
      { ...GOOD, args: [{ type: "u8", value: "0x01" }, GOOD.args[1]] },
      { ...GOOD, args: [{ type: "hex", value: "0x1" }, GOOD.args[1]] },
      { ...GOOD, args: [{ type: "hex", value: "0xzz" }, GOOD.args[1]] },
      { ...GOOD, args: [GOOD.args[0], { type: "hex", value: [] }] },
      { ...GOOD, args: [GOOD.args[0], { type: "hex", value: "0xa11ceb0b" }] },
      { ...GOOD, args: [GOOD.args[0], { type: "hex", value: ["0xa11ceb0b", 7] }] },
    ];
    for (const json of bad) expect(() => parsePublishPayload(json)).toThrow(StreamdError);
  });
});
