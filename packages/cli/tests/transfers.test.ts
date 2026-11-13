import { describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, EventDescriptor, StreamdEvent, Transfer } from "@streamd/core";
import type { TransfersOptions } from "../src/args.js";
import { runTransfers } from "../src/transfers.js";
import type { TransfersIO } from "../src/transfers.js";

const TYPE = "0x1::fungible_asset::Deposit";
const DESCRIPTOR: EventDescriptor = { kind: "module", eventType: TYPE };

function record(version: number): StreamdEvent<Transfer> {
  return {
    type: TYPE,
    data: { asset: "0xa", amount: 10n ** 20n + BigInt(version), from: "0xf", to: "0xt" },
    version: BigInt(version),
    sequenceNumber: 0n,
    eventIndex: 0,
  };
}

/** A chain client whose transfers are plain events from `adapter`, recording what each call was given. */
function harness(adapter: EventAdapter, signal = new AbortController().signal, maxErrors?: number) {
  const inner = new StreamdClient({ adapter, ...(maxErrors !== undefined && { defaults: { maxErrors } }) });
  const seen: Record<string, unknown>[] = [];
  const lines: string[] = [];
  const errors: string[] = [];
  const strip = (options: Record<string, unknown>) => {
    seen.push(options);
    const { asset: _a, account: _b, withAssetInfo: _c, ...rest } = options;
    return rest;
  };
  const client = {
    transfers: (callback: (event: StreamdEvent<Transfer>) => unknown, options: Record<string, unknown> = {}) =>
      inner.on(DESCRIPTOR, callback as never, strip(options)),
    getTransfers: async (filter: Record<string, unknown> = {}) =>
      inner.getEvents(DESCRIPTOR, strip(filter)) as Promise<StreamdEvent<Transfer>[]>,
  } as unknown as TransfersIO["client"];
  const io: TransfersIO = { client, write: (l) => void lines.push(l), error: (m) => errors.push(m), signal };
  const run = async (options: Partial<TransfersOptions>) => {
    const code = await runTransfers({ network: "mainnet", ...options }, io);
    inner.destroy();
    return code;
  };
  const versions = () => lines.map((l) => (JSON.parse(l) as { version: string }).version);
  return { run, io, seen, lines, errors, versions };
}

const events = [record(1), record(2), record(3), record(4)];

describe("runTransfers following", () => {
  it("prints records as JSON lines with amounts as decimal strings, and exits 0 at the limit", async () => {
    const h = harness(new MemoryAdapter(events));
    expect(await h.run({ fromVersion: 2n, limit: 2, pollInterval: 1 })).toBe(0);
    expect(h.versions()).toEqual(["2", "3"]);
    expect(JSON.parse(h.lines[0]!).data).toEqual({ asset: "0xa", amount: "100000000000000000002", from: "0xf", to: "0xt" });
    expect(h.lines[0]!.endsWith("\n")).toBe(true);
    expect(h.errors).toEqual([]);
  });

  it("passes the narrowing and listener options, and nothing unset", async () => {
    const h = harness(new MemoryAdapter(events));
    await h.run({ asset: "0xa", account: "alice.apt", withAssetInfo: true, fromTime: 7n, pollInterval: 1, batchSize: 500, limit: 1 });
    const { signal, onError, ...given } = h.seen[0]!;
    expect(given).toEqual({ asset: "0xa", account: "alice.apt", withAssetInfo: true, fromTime: 7n, pollInterval: 1, batchSize: 500 });
    expect(signal).toBeInstanceOf(AbortSignal);
    expect(typeof onError).toBe("function");
    const bare = harness(new MemoryAdapter(events));
    await bare.run({ fromVersion: 1n, limit: 1, pollInterval: 1 });
    expect(Object.keys(bare.seen[0]!).sort()).toEqual(["fromVersion", "onError", "pollInterval", "signal"]);
  });

  it("saves its position to a cursor file once a page is done, and resumes from it", async () => {
    const file = join(mkdtempSync(join(tmpdir(), "streamd-transfers-")), "t.cursor");
    const controller = new AbortController();
    const first = harness(new MemoryAdapter(events), controller.signal);
    const done = first.run({ fromVersion: 1n, pollInterval: 1, cursorFile: file });
    setTimeout(() => controller.abort(), 30);
    expect(await done).toBe(0);
    expect(first.versions()).toEqual(["1", "2", "3", "4"]);
    const cursor = readFileSync(file, "utf8").trim();
    expect(cursor).not.toBe("");
    const second = harness(new MemoryAdapter([...events, record(5)]));
    expect(await second.run({ fromVersion: 1n, limit: 1, pollInterval: 1, cursorFile: file })).toBe(0);
    expect(second.seen[0]!.cursor).toBe(cursor);
    expect(second.versions()).toEqual(["5"]);
  });

  it("exits 1 on an unreadable cursor file, before subscribing", async () => {
    const file = join(mkdtempSync(join(tmpdir(), "streamd-transfers-")), "t.cursor");
    writeFileSync(file, "\n");
    const h = harness(new MemoryAdapter(events));
    expect(await h.run({ cursorFile: file })).toBe(1);
    expect(h.errors[0]).toContain("cannot read cursor file");
    expect(h.seen).toEqual([]);
  });

  it("exits 0 when the signal aborts", async () => {
    const controller = new AbortController();
    const h = harness(new MemoryAdapter([]), controller.signal);
    const done = h.run({ pollInterval: 1 });
    setTimeout(() => controller.abort(), 20);
    expect(await done).toBe(0);
  });

  it("exits 1 when the subscription gives up, reporting why", async () => {
    const failing: EventAdapter = {
      fetchEvents: async () => {
        throw new StreamdNetworkError("node down");
      },
      getHeadVersion: async () => 1n,
    };
    const h = harness(failing, undefined, 1);
    expect(await h.run({ fromVersion: 1n, pollInterval: 1 })).toBe(1);
    expect(h.errors.some((e) => e.includes("node down"))).toBe(true);
  });

  it("exits 1 with the client's message when it refuses the options", async () => {
    const h = harness(new MemoryAdapter(events));
    h.io.client.transfers = () => {
      throw new StreamdError("Not a valid Aptos address or name: ???");
    };
    expect(await h.run({ account: "???" })).toBe(1);
    expect(h.errors).toEqual(["streamd: Not a valid Aptos address or name: ???"]);
  });

  it("exits 1 when a write fails", async () => {
    const h = harness(new MemoryAdapter(events));
    h.io.write = () => {
      throw new Error("disk full");
    };
    expect(await h.run({ fromVersion: 1n, pollInterval: 1 })).toBe(1);
    expect(h.errors).toEqual(["streamd: disk full"]);
  });
});

describe("runTransfers over a range", () => {
  it("reads the range with the narrowing, order, limit and retries, then exits 0", async () => {
    const h = harness(new MemoryAdapter(events));
    expect(await h.run({ asset: "0xa", fromVersion: 1n, toVersion: 3n, order: "desc", limit: 2 })).toBe(0);
    expect(h.versions()).toEqual(["3", "2"]);
    const { signal, ...given } = h.seen[0]!;
    expect(given).toEqual({ asset: "0xa", fromVersion: 1n, toVersion: 3n, orderBy: "desc", limit: 2, retries: 5 });
    expect(signal).toBeInstanceOf(AbortSignal);
  });

  it("ends at the head and defaults to ascending", async () => {
    const h = harness(new MemoryAdapter(events));
    expect(await h.run({ fromVersion: 2n, toVersion: "head" })).toBe(0);
    expect(h.versions()).toEqual(["2", "3", "4"]);
    expect(h.seen[0]!.orderBy).toBe("asc");
  });

  it("exits 1 when the query fails, and 0 when it was interrupted", async () => {
    const h = harness(new MemoryAdapter(events));
    h.io.client.getTransfers = async () => {
      throw new StreamdError("Unregistered name: nobody.apt");
    };
    expect(await h.run({ fromVersion: 1n, toVersion: "head" })).toBe(1);
    expect(h.errors).toEqual(["streamd: Unregistered name: nobody.apt"]);
    const controller = new AbortController();
    controller.abort();
    const aborted = harness(new MemoryAdapter(events), controller.signal);
    expect(await aborted.run({ fromVersion: 1n, toVersion: "head" })).toBe(0);
    expect(aborted.errors).toEqual([]);
  });
});
