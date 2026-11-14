import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeAddress, StreamdError } from "@streamd/core";
import { parseModuleAbi } from "../src/abi.js";
import { checkGenerated } from "../src/codegen-check.js";
import { generateEventTypes } from "../src/codegen.js";

const dir = mkdtempSync(join(tmpdir(), "streamd-codegen-check-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

let n = 0;
function file(content: string | Uint8Array): string {
  const path = join(dir, `out-${n++}.ts`);
  writeFileSync(path, content);
  return path;
}

const SOURCE = "line one\nline two\nline three\n";

describe("checkGenerated", () => {
  it("matches identical bytes and leaves the file alone", async () => {
    const path = file(SOURCE);
    expect(await checkGenerated(path, SOURCE)).toBeUndefined();
    expect(readFileSync(path, "utf8")).toBe(SOURCE);
  });

  it("names the file and the first line that differs, both sides as JSON", async () => {
    const path = file("line one\nline 2\nline three\n");
    expect(await checkGenerated(path, SOURCE)).toBe(
      `${path} differs from the generated output at line 2\n  on disk:   "line 2"\n  generated: "line two"`,
    );
    expect(readFileSync(path, "utf8")).toBe("line one\nline 2\nline three\n");
  });

  it("reports a missing or an extra line against the end of the file", async () => {
    const short = file("line one\nline two");
    expect(await checkGenerated(short, SOURCE)).toBe(
      `${short} differs from the generated output at line 3\n  on disk:   (end of file)\n  generated: "line three"`,
    );
    const missing = file("line one\n");
    expect(await checkGenerated(missing, SOURCE)).toBe(
      `${missing} differs from the generated output at line 2\n  on disk:   ""\n  generated: "line two"`,
    );
    const extra = file(`${SOURCE}more\n`);
    expect(await checkGenerated(extra, SOURCE)).toBe(
      `${extra} differs from the generated output at line 4\n  on disk:   "more"\n  generated: ""`,
    );
    const longer = file(`${SOURCE}\n`);
    expect(await checkGenerated(longer, SOURCE)).toBe(
      `${longer} differs from the generated output at line 5\n  on disk:   ""\n  generated: (end of file)`,
    );
  });

  it("counts line endings and a byte order mark as drift", async () => {
    const crlf = file(SOURCE.replaceAll("\n", "\r\n"));
    expect(await checkGenerated(crlf, SOURCE)).toBe(
      `${crlf} differs from the generated output at line 1\n  on disk:   "line one\\r"\n  generated: "line one"`,
    );
    const bom = file(`﻿${SOURCE}`);
    expect(await checkGenerated(bom, SOURCE)).toStartWith(`${bom} differs from the generated output at line 1\n`);
  });

  it("reports bytes that differ when no decoded line does", async () => {
    const path = file(new Uint8Array([0xff, 0x0a]));
    expect(await checkGenerated(path, "�\n")).toBe(`${path} differs from the generated output`);
  });

  it("cuts long lines to 120 characters", async () => {
    const long = "x".repeat(200);
    const path = file(`${"y".repeat(200)}\n`);
    expect(await checkGenerated(path, `${long}\n`)).toBe(
      `${path} differs from the generated output at line 1\n  on disk:   "${"y".repeat(120)}..."\n  generated: "${"x".repeat(120)}..."`,
    );
  });

  it("says a missing file does not exist, and rejects on any other read failure", async () => {
    const path = join(dir, "never-written.ts");
    expect(await checkGenerated(path, SOURCE)).toBe(`${path} does not exist`);
    expect(readdirSync(dir)).not.toContain("never-written.ts");
    await expect(checkGenerated(dir, SOURCE)).rejects.toThrow();
  });

  it("compares the output of the flags the file was generated with", async () => {
    const fixtures = fileURLToPath(new URL("./fixtures/abi/", import.meta.url));
    const abis = readdirSync(fixtures).map((f) => parseModuleAbi(JSON.parse(readFileSync(join(fixtures, f), "utf8"))));
    const load = async (address: string, module: string) => {
      const found = abis.find((a) => normalizeAddress(a.address) === normalizeAddress(address) && a.name === module);
      if (found === undefined) throw new StreamdError(`Module ${address}::${module} not found`);
      return found;
    };
    const targets = [{ address: "0x1", module: "fungible_asset" }];
    const withValidators = await generateEventTypes(targets, load, { validators: true });
    const plain = await generateEventTypes(targets, load);
    const path = file(withValidators);
    expect(await checkGenerated(path, withValidators)).toBeUndefined();
    expect(await checkGenerated(path, plain)).toStartWith(`${path} differs from the generated output at line `);
  });
});
