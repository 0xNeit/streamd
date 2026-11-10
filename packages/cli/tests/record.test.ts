import { describe, expect, it } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError, replayAdapter } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import type { RecordOptions } from "../src/args.js";
import { runRecord, writeAtomically } from "../src/record.js";

const TYPE = "0x1::coin::DepositEvent";

function ev(version: number): StreamdEvent {
  return { type: TYPE, data: { n: String(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

const options = (extra: Partial<RecordOptions> = {}): RecordOptions => ({
  descriptor: { kind: "module", eventType: [TYPE] },
  network: "mainnet",
  fromVersion: 1n,
  toVersion: 3n,
  ...extra,
});

async function run(adapter: EventAdapter, opts = options(), signal = new AbortController().signal) {
  const written: string[] = [];
  const errors: string[] = [];
  const code = await runRecord(opts, { adapter, write: (t) => written.push(t), error: (m) => errors.push(m), signal });
  return { code, written, errors };
}

describe("runRecord", () => {
  const events = [ev(1), ev(2), ev(3), ev(4)];

  it("writes a recording that replays the same query with no source", async () => {
    const { code, written, errors } = await run(new MemoryAdapter(events));
    expect(code).toBe(0);
    expect(written).toHaveLength(1);
    expect(errors[0]).toMatch(/recorded 3 events in \d+ calls/);
    expect(errors[0]).toContain("{ fromVersion: 1n, toVersion: 3n }");

    const client = new StreamdClient({ adapter: replayAdapter(written[0]!) });
    const replayed = await client.getEvents(options().descriptor, { fromVersion: 1n, toVersion: 3n });
    expect(replayed.map((e) => e.version)).toEqual([1n, 2n, 3n]);
    client.destroy();
  });

  it("keeps a limit in the recording and in the replay filter it prints", async () => {
    const { written, errors } = await run(new MemoryAdapter(events), options({ limit: 2 }));
    expect(errors[0]).toContain("limit: 2");
    const client = new StreamdClient({ adapter: replayAdapter(written[0]!) });
    const replayed = await client.getEvents(options().descriptor, { fromVersion: 1n, toVersion: 3n, limit: 2 });
    expect(replayed.map((e) => e.version)).toEqual([1n, 2n]);
    client.destroy();
  });

  it("writes a valid recording of nothing when no event matches", async () => {
    const { code, written } = await run(new MemoryAdapter([]));
    expect(code).toBe(0);
    expect(() => replayAdapter(written[0]!)).not.toThrow();
  });

  it("retries a failed page and leaves it out of the recording", async () => {
    let failures = 2;
    const memory = new MemoryAdapter(events);
    const adapter: EventAdapter = {
      fetchEvents: async (d, o) => {
        if (failures-- > 0) throw new StreamdNetworkError("rate limited", 429);
        return memory.fetchEvents(d, o);
      },
    };
    const { code, written } = await run(adapter);
    expect(code).toBe(0);
    expect(JSON.stringify(JSON.parse(written[0]!).calls)).not.toContain("rate limited");
  }, 15_000);

  it("writes nothing and exits 1 when the query fails", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdError("not a network error");
      },
    };
    const { code, written, errors } = await run(adapter);
    expect(code).toBe(1);
    expect(written).toEqual([]);
    expect(errors).toEqual(["streamd: not a network error"]);
  });

  it("writes nothing and exits 130 when interrupted", async () => {
    const controller = new AbortController();
    controller.abort();
    const { code, written, errors } = await run(new MemoryAdapter(events), options(), controller.signal);
    expect(code).toBe(130);
    expect(written).toEqual([]);
    expect(errors).toEqual([]);
  });
});

describe("writeAtomically", () => {
  const dir = () => mkdtempSync(join(tmpdir(), "streamd-record-"));

  it("creates or replaces the file and leaves nothing else", () => {
    const d = dir();
    const path = join(d, "r.json");
    writeAtomically(path, "one");
    writeAtomically(path, "two");
    expect(readFileSync(path, "utf8")).toBe("two");
    expect(readdirSync(d)).toEqual(["r.json"]);
  });

  it("leaves the old file and no temporary behind on failure", () => {
    const d = dir();
    const missing = join(d, "no-such-dir", "r.json");
    expect(() => writeAtomically(missing, "x")).toThrow();
    expect(existsSync(missing)).toBe(false);
    const path = join(d, "keep.json");
    writeFileSync(path, "old");
    expect(readFileSync(path, "utf8")).toBe("old");
  });
});
