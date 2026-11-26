import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { MemoryAdapter, StreamdClient, recordingAdapter } from "@streamd/core";
import type { StreamdEvent } from "@streamd/core";
import { runRecord } from "../src/record.js";

const MAIN = resolve(dirname(fileURLToPath(import.meta.url)), "../src/main.ts");
const TYPE = "0x1::transaction_fee::FeeStatement";
const dir = mkdtempSync(join(tmpdir(), "streamd-replay-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const event = (version: number): StreamdEvent => ({
  type: TYPE,
  data: { total_charge_gas_units: String(version) },
  version: BigInt(version),
  sequenceNumber: 0n,
  eventIndex: 0,
});

/** A recording of `streamd record --from-version 1 --to-version 5` over five events. */
async function recordQuery(): Promise<string> {
  let text = "";
  const code = await runRecord(
    { network: "mainnet", descriptor: { kind: "module", eventType: TYPE }, fromVersion: 1n, toVersion: 5n },
    { adapter: new MemoryAdapter([1, 2, 3, 4, 5].map(event)), write: (t) => (text = t), error: () => {}, signal: new AbortController().signal },
  );
  expect(code).toBe(0);
  const path = join(dir, "query.json");
  writeFileSync(path, text);
  return path;
}

async function cli(...args: string[]) {
  // An unreachable node: any read that got past the replay would fail loudly.
  const child = Bun.spawn([process.execPath, MAIN, ...args], {
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, APTOS_API_KEY: "" },
  });
  const [out, err, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  return { out, err, code, versions: out.trim() === "" ? [] : out.trim().split("\n").map((l) => (JSON.parse(l) as { version: string }).version) };
}

describe("--replay", () => {
  it("answers query from the recording with the flags it was made with", async () => {
    const file = await recordQuery();
    const r = await cli("query", "--replay", file, "--from-version", "1", "--to-version", "5", TYPE);
    expect(r.err).toBe("");
    expect(r.code).toBe(0);
    expect(r.versions).toEqual(["1", "2", "3", "4", "5"]);
  });

  it("ends a query it cannot answer with exit 1, naming the request", async () => {
    const file = await recordQuery();
    const r = await cli("query", "--replay", file, "--from-version", "2", "--to-version", "5", TYPE);
    expect(r.code).toBe(1);
    expect(r.err).toContain("No recorded fetchEvents call matches this request");
    expect(r.err).toContain(TYPE);
    expect(r.err).toContain('["fromVersion","2n"]');
  });

  it("follows a recording with tail, then exits 1 at the first request past it", async () => {
    const recorder = recordingAdapter(new MemoryAdapter([1, 2, 3].map(event)));
    const client = new StreamdClient({ adapter: recorder });
    const seen: bigint[] = [];
    const sub = client.on({ kind: "module", eventType: TYPE }, (e) => void seen.push(e.version), { fromVersion: 1n, pollInterval: 1 });
    while (seen.length < 3) await new Promise((r) => setTimeout(r, 5));
    sub.unsubscribe();
    client.destroy();
    const path = join(dir, "tail.json");
    // Only the pages that delivered events: the recording ends where they did.
    const recording = recorder.toJSON();
    recording.calls = recording.calls.filter((c) => c.method !== "fetchEvents" || (c.result as { events: unknown[] }).events.length > 0);
    writeFileSync(path, JSON.stringify(recording));
    const r = await cli("tail", "--replay", path, "--from-version", "1", "--poll-interval", "1", TYPE);
    expect(r.versions).toEqual(["1", "2", "3"]);
    expect(r.code).toBe(1);
    expect(r.err).toContain("No recorded fetchEvents call matches this request");
  });

  it("follows a record file with tail through its bounded pages, then exits 1 past them", async () => {
    const file = await recordQuery();
    const r = await cli("tail", "--replay", file, "--from-version", "1", "--poll-interval", "1", TYPE);
    expect(r.versions).toEqual(["1", "2", "3", "4", "5"]);
    expect(r.code).toBe(1);
    expect(r.err).toContain("No recorded fetchEvents call matches this request");
  });

  it("stops a followed replay at --limit with exit 0", async () => {
    const file = await recordQuery();
    const r = await cli("tail", "--replay", file, "--from-version", "1", "--limit", "2", TYPE);
    expect(r.code).toBe(0);
    expect(r.versions).toEqual(["1", "2"]);
  });

  it("ends tx with exit 1 when the recording holds no lookup", async () => {
    const file = await recordQuery();
    const r = await cli("tx", "--replay", file, `0x${"ab".repeat(32)}`, "--timeout", "0");
    expect(r.code).toBe(1);
    expect(r.err).not.toBe("");
  });

  it("refuses an unreadable or malformed file with exit 1", async () => {
    const missing = await cli("query", "--replay", join(dir, "none.json"), "--from-version", "1", TYPE);
    expect(missing.code).toBe(1);
    expect(missing.err).toContain("cannot read --replay file");
    const bad = join(dir, "bad.json");
    writeFileSync(bad, "{}");
    const malformed = await cli("query", "--replay", bad, "--from-version", "1", TYPE);
    expect(malformed.code).toBe(1);
    expect(malformed.err).toContain("is not a recording");
  });
});
