import { describe, expect, it } from "bun:test";
import type { HolderSnapshot } from "@streamd/aptos";
import { StreamdError } from "@streamd/core";
import type { HoldersCommandOptions } from "../src/args.js";
import { csvRow, runHolders } from "../src/holders.js";

const ASSET = `0x${"a1".repeat(32)}`;
const S1 = `0x${"1".repeat(64)}`;
const S2 = `0x${"2".repeat(64)}`;
const O1 = `0x${"3".repeat(64)}`;

function snapshot(): HolderSnapshot {
  return {
    asset: ASSET,
    version: 100n,
    indexedVersion: 150n,
    holders: [
      { store: S1, owner: O1, balance: 10n ** 20n },
      { store: S2, owner: O1, balance: 7n },
    ],
    total: 10n ** 20n + 7n,
    supply: 10n ** 20n + 7n,
    verified: "supply",
    dispatchable: false,
    requests: 6,
  };
}

function harness(result: () => Promise<HolderSnapshot> = async () => snapshot(), write?: (text: string) => void) {
  const calls: { asset: string; version: bigint; options: Record<string, unknown> }[] = [];
  const out: string[] = [];
  const errors: string[] = [];
  const summaries: string[] = [];
  const controller = new AbortController();
  const indexer = { name: "indexer" };
  const io = {
    client: {
      holdersAt: (asset: string, version: bigint, options: Record<string, unknown>) => {
        calls.push({ asset, version, options });
        return result();
      },
    },
    indexer,
    write: write ?? ((text: string) => void out.push(text)),
    error: (m: string) => errors.push(m),
    summary: (line: string) => summaries.push(line),
    signal: controller.signal,
  };
  const run = (options: Partial<HoldersCommandOptions> = {}) =>
    runHolders({ network: "mainnet", asset: ASSET, at: 100n, format: "json", ...options }, io as never);
  return { run, calls, out, errors, summaries, controller, indexer };
}

describe("runHolders", () => {
  it("passes the indexer, retries and signal, and only the checks given", async () => {
    const h = harness();
    await h.run();
    await h.run({ verify: "stores", maxRequests: 40 });
    expect(h.calls[0]).toEqual({ asset: ASSET, version: 100n, options: { indexer: h.indexer, retries: 5, signal: h.controller.signal } });
    expect(h.calls[1]!.options).toMatchObject({ verify: "stores", maxRequests: 40 });
  });

  it("prints one JSON line per holder, then the summary", async () => {
    const h = harness();
    expect(await h.run()).toBe(0);
    expect(h.out).toEqual([
      `{"store":"${S1}","owner":"${O1}","balance":"100000000000000000000"}\n`,
      `{"store":"${S2}","owner":"${O1}","balance":"7"}\n`,
    ]);
    expect(JSON.parse(h.summaries[0]!)).toEqual({
      snapshot: {
        asset: ASSET, version: "100", indexedVersion: "150", total: "100000000000000000007", supply: "100000000000000000007",
        verified: "supply", dispatchable: false, requests: 6, holders: 2,
      },
    });
    expect(h.errors).toEqual([]);
  });

  it("prints CSV with a header and CRLF records", async () => {
    const h = harness();
    expect(await h.run({ format: "csv" })).toBe(0);
    expect(h.out.join("")).toBe(`store,owner,balance\r\n${S1},${O1},100000000000000000000\r\n${S2},${O1},7\r\n`);
  });

  it("leaves supply out of the summary when it was not checked", async () => {
    const h = harness(async () => {
      const { supply: _, ...rest } = snapshot();
      return { ...rest, verified: "none", holders: [] };
    });
    expect(await h.run({ format: "csv" })).toBe(0);
    expect(h.out).toEqual(["store,owner,balance\r\n"]);
    expect(JSON.parse(h.summaries[0]!).snapshot).not.toHaveProperty("supply");
  });

  it("exits 1 with the message when the snapshot rejects", async () => {
    const h = harness(() => Promise.reject(new StreamdError("holdersAt does not fold APT")));
    expect(await h.run()).toBe(1);
    expect(h.errors).toEqual(["streamd: holdersAt does not fold APT"]);
    expect(h.out).toEqual([]);
    expect(h.summaries).toEqual([]);
  });

  it("exits 130 when interrupted before the snapshot resolved", async () => {
    const h = harness(() => Promise.reject(new Error("aborted")));
    h.controller.abort();
    expect(await h.run()).toBe(130);
    expect(h.errors).toEqual([]);
  });

  it("exits 0 without a summary when the output closes, and 1 when a write fails otherwise", async () => {
    const closed = harness(undefined, () => {
      closed.controller.abort();
      throw new Error("EPIPE");
    });
    expect(await closed.run()).toBe(0);
    expect(closed.summaries).toEqual([]);
    const broken = harness(undefined, () => {
      throw new Error("disk full");
    });
    expect(await broken.run()).toBe(1);
    expect(broken.errors).toEqual(["streamd: disk full"]);
  });
});

describe("csvRow", () => {
  it("quotes a field holding a comma, quote, CR or LF, doubling its quotes", () => {
    expect(csvRow(["a,b", 'say "hi"', "x\ny", "x\ry", "plain"])).toBe(`"a,b","say ""hi""","x\ny","x\ry",plain\r\n`);
    expect(csvRow([""])).toBe("\r\n");
  });
});
