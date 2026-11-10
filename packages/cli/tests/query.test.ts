import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import type { QueryOptions } from "../src/args.js";
import { runQuery } from "../src/query.js";

const TYPE = "0x1::coin::DepositEvent";

function ev(version: number): StreamdEvent {
  return { type: TYPE, data: { n: String(version) }, version: BigInt(version), sequenceNumber: 0n, eventIndex: 0 };
}

const options = (extra: Partial<QueryOptions> = {}): QueryOptions => ({
  descriptor: { kind: "module", eventType: [TYPE] },
  network: "mainnet",
  fromVersion: 2n,
  toVersion: 4n,
  order: "asc",
  ...extra,
});

async function run(adapter: EventAdapter, opts = options(), signal = new AbortController().signal) {
  const lines: string[] = [];
  const errors: string[] = [];
  const client = new StreamdClient({ adapter });
  const code = await runQuery(opts, { client, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal });
  client.destroy();
  const versions = lines.map((l) => (JSON.parse(l) as { version: string }).version);
  return { code, lines, errors, versions };
}

describe("runQuery", () => {
  const events = [ev(1), ev(2), ev(3), ev(4), ev(5)];

  it("prints the range as JSON lines and exits 0", async () => {
    const { code, versions, lines, errors } = await run(new MemoryAdapter(events));
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(versions).toEqual(["2", "3", "4"]);
    expect(lines[0]!.endsWith("\n")).toBe(true);
  });

  it("ends at the head when asked", async () => {
    const { versions } = await run(new MemoryAdapter(events), options({ toVersion: "head" }));
    expect(versions).toEqual(["2", "3", "4", "5"]);
  });

  it("applies a limit, and gives the newest first for desc", async () => {
    expect((await run(new MemoryAdapter(events), options({ limit: 2 }))).versions).toEqual(["2", "3"]);
    const desc = await run(new MemoryAdapter(events), options({ limit: 2, order: "desc" }));
    expect(desc.versions).toEqual(["4", "3"]);
  });

  it("exits 0 with no output when nothing matches", async () => {
    const { code, lines } = await run(new MemoryAdapter([]));
    expect(code).toBe(0);
    expect(lines).toEqual([]);
  });

  it("retries a failed page", async () => {
    let failures = 1;
    const memory = new MemoryAdapter(events);
    const adapter: EventAdapter = {
      fetchEvents: async (d, o) => {
        if (failures-- > 0) throw new StreamdNetworkError("rate limited", 429);
        return memory.fetchEvents(d, o);
      },
    };
    const { code, versions } = await run(adapter);
    expect(code).toBe(0);
    expect(versions).toEqual(["2", "3", "4"]);
  }, 10_000);

  it("reports a failure and exits 1", async () => {
    const adapter: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdError("bad request");
      },
    };
    const { code, errors } = await run(adapter);
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: bad request"]);
  });

  it("exits 1 when a write fails", async () => {
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    const code = await runQuery(options(), {
      client,
      write: () => {
        throw new Error("disk full");
      },
      error: (m) => errors.push(m),
      signal: new AbortController().signal,
    });
    expect(code).toBe(1);
    expect(errors).toEqual(["streamd: disk full"]);
    client.destroy();
  });

  it("exits 0, quietly, when the signal stops it", async () => {
    const controller = new AbortController();
    controller.abort();
    const { code, errors, lines } = await run(new MemoryAdapter(events), options(), controller.signal);
    expect(code).toBe(0);
    expect(errors).toEqual([]);
    expect(lines).toEqual([]);
  });

  it("waits for each write before the next", async () => {
    const order: string[] = [];
    let release = () => {};
    const client = new StreamdClient({ adapter: new MemoryAdapter(events) });
    const running = runQuery(options({ limit: 2 }), {
      client,
      write: async (line) => {
        order.push(`start ${(JSON.parse(line) as { version: string }).version}`);
        if (order.length === 1) await new Promise<void>((r) => (release = r));
      },
      error: () => {},
      signal: new AbortController().signal,
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(order).toEqual(["start 2"]);
    release();
    await running;
    expect(order).toEqual(["start 2", "start 3"]);
    client.destroy();
  });
});
