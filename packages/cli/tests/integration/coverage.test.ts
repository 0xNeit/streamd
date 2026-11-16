import { describe, expect, it } from "bun:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

/**
 * Runs `streamd coverage` on a real localnet started with the installed
 * `aptos` CLI. Run with `bun run test:localnet`; skipped otherwise.
 */
const enabled = process.env.STREAMD_LOCALNET === "1";
const MAIN = new URL("../../src/main.ts", import.meta.url).pathname;
const FIXTURE = new URL("../fixtures/coverage", import.meta.url).pathname;

describe.skipIf(!enabled)("streamd coverage (aptos CLI)", () => {
  it("reports the structs the publish and the test command emitted, and the one they never did", async () => {
    const { stdout, stderr } = await promisify(execFile)("bun", [MAIN, "coverage", FIXTURE, "--", "bun", `${FIXTURE}/drive.ts`]);
    const lines = stdout.trim().split("\n").map((line) => JSON.parse(line));
    const address = lines[0].event.split("::")[0];
    expect(address).toMatch(/^0x[0-9a-f]{64}$/);
    expect(lines.map((line) => [line.event.slice(address.length), line.emitted, line.count, line.first?.function])).toEqual([
      ["::coverage::Bumped", true, 1, `${address}::coverage::bump`],
      ["::coverage::Created", true, 1, "0x1::code::publish_package_txn"],
      ["::coverage::Never", false, 0, undefined],
      ["::coverage::Tagged", true, 1, `${address}::coverage::tag`],
    ]);
    expect(stderr).toContain("called ");
    expect(stderr).toContain("streamd: 3 of 4 event structs emitted\n");
  }, 180_000);
});
