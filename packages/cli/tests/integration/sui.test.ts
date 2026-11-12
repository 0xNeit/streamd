import { describe, expect, it } from "bun:test";
import { fileURLToPath } from "node:url";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const MAIN = fileURLToPath(new URL("../../src/main.ts", import.meta.url));
// Cetus CLMM swaps: several a minute on mainnet.
const SWAP = "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent";

async function streamd(...args: string[]): Promise<{ code: number; lines: string[]; stderr: string }> {
  const child = Bun.spawn(["bun", MAIN, ...args], { stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  return { code, lines: stdout.split("\n").filter((l) => l !== ""), stderr };
}

describe.skipIf(!enabled)("streamd --chain sui (mainnet)", () => {
  it("queries a recent range, then prints one of its transactions with tx", async () => {
    const query = await streamd("query", "--chain", "sui", "--from-time", "30m", "--limit", "3", SWAP);
    expect(query.code).toBe(0);
    expect(query.lines).toHaveLength(3);
    const events = query.lines.map((l) => JSON.parse(l) as { type: string; version: string; transactionHash: string });
    expect(events.every((e) => e.type === SWAP)).toBe(true);
    expect(BigInt(events[1]!.version) >= BigInt(events[0]!.version)).toBe(true);

    const tx = await streamd("tx", "--chain", "sui", events[0]!.transactionHash);
    expect(tx.code).toBe(0);
    const inTx = tx.lines.map((l) => JSON.parse(l) as { version: string; transactionHash: string; type: string });
    expect(inTx.length).toBeGreaterThan(0);
    expect(inTx.every((e) => e.transactionHash === events[0]!.transactionHash && e.version === events[0]!.version)).toBe(true);
    expect(inTx.some((e) => e.type === SWAP)).toBe(true);
  }, 60_000);

  it("tails from a time and stops at the limit", async () => {
    const tail = await streamd("tail", "--chain", "sui", "--from-time", "10m", "--limit", "2", SWAP);
    expect(tail.code).toBe(0);
    expect(tail.lines).toHaveLength(2);
    expect(tail.lines.map((l) => (JSON.parse(l) as { type: string }).type)).toEqual([SWAP, SWAP]);
  }, 60_000);

  it("checks types through the package service", async () => {
    const check = await streamd("check", "--chain", "sui", "0x3::validator::StakingRequestEvent", "0x2::coin::NoSuchStruct");
    expect(check.code).toBe(1);
    expect(check.lines).toEqual([
      `event          0x${"3".padStart(64, "0")}::validator::StakingRequestEvent`,
      `missing-struct 0x${"2".padStart(64, "0")}::coin::NoSuchStruct`,
    ]);
  }, 30_000);

  it("refuses an event handle before reaching the node", async () => {
    const tail = await streamd("tail", "--chain", "sui", "--account", "0x2", "--handle", "0x2::m::S", "--field", "f");
    expect(tail.code).toBe(2);
    expect(tail.stderr).toContain("--handle is not available with --chain sui");
  });
});
