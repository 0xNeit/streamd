import { afterEach, describe, expect, it } from "bun:test";
import { sendOnce } from "@streamd/aptos";
import { MemoryAdapter, StreamdClient, StreamdError, StreamdNetworkError } from "@streamd/core";
import type { EventAdapter, StreamdEvent } from "@streamd/core";
import type { InspectOptions } from "../src/args.js";
import { resourceReader } from "../src/chain.js";
import { changeLine, changeRecord, diffValues, prettyChange, runInspect, valueLine } from "../src/inspect.js";

const TYPE = "0x1::block::BlockResource";

function change(data: unknown, overrides: Partial<StreamdEvent> = {}): StreamdEvent {
  return { type: TYPE, data, version: 100n, sequenceNumber: 0n, eventIndex: 3, ...overrides };
}

describe("diffValues", () => {
  it("lists changed leaves by dotted path, keys sorted", () => {
    const before = { z: "1", a: { counter: "5", guid: { id: "x" } } };
    const after = { z: "2", a: { counter: "6", guid: { id: "x" } } };
    expect(diffValues(before, after)).toEqual([
      { path: "a.counter", before: "5", after: "6" },
      { path: "z", before: "1", after: "2" },
    ]);
  });

  it("is empty for equal values", () => {
    expect(diffValues({ a: [1, { b: true }] }, { a: [1, { b: true }] })).toEqual([]);
  });

  it("reports added and removed keys with one side missing", () => {
    const changes = diffValues({ gone: { x: 1 }, kept: 1 }, { kept: 1, added: [1] });
    expect(changes).toEqual([{ path: "added", after: [1] }, { path: "gone", before: { x: 1 } }]);
    expect("before" in changes[0]!).toBe(false);
    expect("after" in changes[1]!).toBe(false);
  });

  it("walks arrays by index as they grow and shrink", () => {
    expect(diffValues({ items: [{ amount: "1" }] }, { items: [{ amount: "2" }, { amount: "3" }] })).toEqual([
      { path: "items.0.amount", before: "1", after: "2" },
      { path: "items.1", after: { amount: "3" } },
    ]);
    expect(diffValues({ items: ["a", "b"] }, { items: ["a"] })).toEqual([{ path: "items.1", before: "b" }]);
    const eleven = Array.from({ length: 11 }, (_, i) => i);
    expect(diffValues(eleven, eleven.map((n) => n + 1)).map((c) => c.path)).toEqual(eleven.map(String));
  });

  it("reports a change of shape whole at its path", () => {
    expect(diffValues({ v: { a: 1 } }, { v: [1] })).toEqual([{ path: "v", before: { a: 1 }, after: [1] }]);
    expect(diffValues({ v: { a: 1 } }, { v: "s" })).toEqual([{ path: "v", before: { a: 1 }, after: "s" }]);
    expect(diffValues({ v: null }, { v: { a: 1 } })).toEqual([{ path: "v", before: null, after: { a: 1 } }]);
  });
});

describe("changeRecord", () => {
  it("diffs an update and carries the transaction context", () => {
    const event = change({ height: "2" }, { transactionHash: "0xab", timestamp: 1_700_000_000_000_000n, sender: "0x1" });
    expect(changeRecord(event, { height: "1" })).toEqual({
      version: 100n,
      transactionHash: "0xab",
      timestamp: 1_700_000_000_000_000n,
      sender: "0x1",
      change: "updated",
      changes: [{ path: "height", before: "1", after: "2" }],
    });
  });

  it("keeps an unchanged write as an update with no changes", () => {
    expect(changeRecord(change({ a: 1 }), { a: 1 })).toEqual({ version: 100n, change: "updated", changes: [] });
  });

  it("gives a creation, an unknown before and a delete their own kinds", () => {
    expect(changeRecord(change({ a: 1 }), null)).toEqual({ version: 100n, change: "created", value: { a: 1 } });
    expect(changeRecord(change({ a: 1 }), undefined)).toEqual({ version: 100n, change: "written", value: { a: 1 } });
    expect(changeRecord(change(null), { a: 1 })).toEqual({ version: 100n, change: "deleted" });
    expect(changeRecord(change(null), undefined)).toEqual({ version: 100n, change: "deleted" });
  });
});

describe("changeLine", () => {
  it("prints one JSON line with bigints as decimal strings and missing sides left out", () => {
    const record = changeRecord(change({ n: 2n, extra: "x" }, { timestamp: 5n }), { n: 1n });
    expect(changeLine(record)).toBe(
      '{"version":"100","timestamp":"5","change":"updated","changes":[{"path":"extra","after":"x"},{"path":"n","before":"1","after":"2"}]}\n',
    );
  });
});

describe("prettyChange", () => {
  it("prints a header and one line per changed path", () => {
    const record = changeRecord(change({ a: "2", b: "x" }, { timestamp: 1_700_000_000_000_000n }), { a: "1", c: true });
    expect(prettyChange(record)).toBe(
      [
        "100  2023-11-14 22:13:20  updated",
        '  a: "1" -> "2"',
        '  b: (absent) -> "x"',
        "  c: true -> (absent)",
        "",
      ].join("\n"),
    );
  });

  it("prints whole values, deletes and empty updates", () => {
    expect(prettyChange(changeRecord(change({ a: 1 }), null))).toBe('100  -  created\n  {"a":1}\n');
    expect(prettyChange(changeRecord(change({ a: 1 }), undefined))).toBe('100  -  written\n  {"a":1}\n');
    expect(prettyChange(changeRecord(change(null), { a: 1 }))).toBe("100  -  deleted\n");
    expect(prettyChange(changeRecord(change({ a: 1 }), { a: 1 }))).toBe("100  -  updated\n  (no field changed)\n");
  });

  it("shows control characters from on-chain text as ?", () => {
    const out = prettyChange(changeRecord(change({ name: "a\u009b31mb" }), { name: "a" }));
    expect(out).toBe('100  -  updated\n  name: "a" -> "a?31mb"\n');
  });
});

describe("valueLine", () => {
  it("prints the version and value as JSON, null when absent", () => {
    expect(valueLine(7n, { n: 1n }, false)).toBe('{"version":"7","value":{"n":"1"}}\n');
    expect(valueLine(7n, null, false)).toBe('{"version":"7","value":null}\n');
  });

  it("prints indented JSON for pretty, control characters as ?", () => {
    expect(valueLine(7n, { s: "x\u0085y" }, true)).toBe('{\n  "s": "x?y"\n}\n');
  });
});

describe("runInspect", () => {
  const LONG = `0x${"0".repeat(63)}1`;
  const options = (extra: Partial<InspectOptions> = {}): InspectOptions => ({
    address: LONG,
    resourceType: TYPE,
    network: "mainnet",
    format: "json",
    fromVersion: 10n,
    toVersion: 20n,
    ...extra,
  });
  const at = (version: number, data: unknown): StreamdEvent => change(data, { version: BigInt(version) });
  const history = [at(5, { h: "0" }), at(10, { h: "1" }), at(12, null), at(15, { h: "2" }), at(25, { h: "3" })];

  async function run(
    opts: InspectOptions,
    read: (version: bigint) => Promise<unknown>,
    adapter: EventAdapter = new MemoryAdapter(history),
    write?: (text: string) => void | Promise<void>,
  ) {
    const out: string[] = [];
    const errors: string[] = [];
    const reads: bigint[] = [];
    const client = new StreamdClient({ adapter });
    const code = await runInspect(opts, {
      client,
      read: (version) => (reads.push(version), read(version)),
      write: write ?? ((text) => void out.push(text)),
      error: (m) => errors.push(m),
      signal: new AbortController().signal,
    });
    client.destroy();
    return { code, out, errors, reads, records: opts.format === "json" ? out.map((l) => JSON.parse(l) as Record<string, unknown>) : [] };
  }

  it("diffs each change in range against the one before, the first against the value before the range", async () => {
    const { code, records, reads, errors } = await run(options(), async () => ({ h: "0" }));
    expect([code, errors, reads]).toEqual([0, [], [9n]]);
    expect(records).toEqual([
      { version: "10", change: "updated", changes: [{ path: "h", before: "0", after: "1" }] },
      { version: "12", change: "deleted" },
      { version: "15", change: "created", value: { h: "2" } },
    ]);
  });

  it("takes the resource as absent before version 0, with no read", async () => {
    const { records, reads } = await run(options({ fromVersion: 0n, toVersion: 5n }), async () => {
      throw new Error("not read");
    });
    expect(reads).toEqual([]);
    expect(records).toEqual([{ version: "5", change: "created", value: { h: "0" } }]);
  });

  it("prints nothing for a range with no change", async () => {
    const { code, out } = await run(options({ fromVersion: 16n, toVersion: 24n }), async () => ({ h: "2" }));
    expect([code, out]).toEqual([0, []]);
  });

  it("stops after --limit records", async () => {
    const { records } = await run(options({ limit: 2 }), async () => ({ h: "0" }));
    expect(records.map((r) => r.version)).toEqual(["10", "12"]);
  });

  it("prints the first change whole, with a note, when the value before is pruned", async () => {
    const pruned = async () => {
      throw new StreamdNetworkError("pruned", 410);
    };
    const { code, records, errors } = await run(options({ toVersion: 11n }), pruned);
    expect(code).toBe(0);
    expect(records).toEqual([{ version: "10", change: "written", value: { h: "1" } }]);
    expect(errors).toEqual(["streamd: version 9 is pruned on this node; pass --archive to diff it, so the first change is printed whole"]);
    const archived = await run(options({ toVersion: 11n, archive: "default" }), pruned);
    expect(archived.errors[0]).toContain("pruned on this node and the archive");
  });

  it("exits 1 when the value before fails any other way, before reading the range", async () => {
    let fetched = false;
    const adapter = new MemoryAdapter(history);
    const watched: EventAdapter = { ...adapter, fetchEvents: (d, o) => ((fetched = true), adapter.fetchEvents(d, o)) };
    const { code, out, errors } = await run(options(), async () => Promise.reject(new StreamdError("Fullnode request failed")), watched);
    expect([code, out, errors, fetched]).toEqual([1, [], ["streamd: Fullnode request failed"], false]);
    const down = await run(options(), async () => Promise.reject(new StreamdNetworkError("down", 500)));
    expect([down.code, down.out, down.errors]).toEqual([1, [], ["streamd: down"]]);
  });

  it("prints pretty records", async () => {
    const { out } = await run(options({ format: "pretty", toVersion: 11n }), async () => ({ h: "0" }));
    expect(out).toEqual(['10  -  updated\n  h: "0" -> "1"\n']);
  });

  it("prints the value at --at, null when absent", async () => {
    const present = await run(options({ at: 7n }), async () => ({ h: "0" }));
    expect([present.code, present.out, present.reads]).toEqual([0, ['{"version":"7","value":{"h":"0"}}\n'], [7n]]);
    const absent = await run(options({ at: 3n }), async () => null);
    expect(absent.out).toEqual(['{"version":"3","value":null}\n']);
    const failed = await run(options({ at: 3n }), async () => Promise.reject(new StreamdNetworkError("pruned", 410)));
    expect([failed.code, failed.errors]).toEqual([1, ["streamd: pruned"]]);
  });

  it("waits for each write, and exits 1 when one rejects", async () => {
    let release!: () => void;
    const out: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter(history) });
    const running = runInspect(options(), {
      client,
      read: async () => ({ h: "0" }),
      write: (text) => (out.push(text), out.length === 1 ? new Promise<void>((resolve) => (release = resolve)) : undefined),
      error: () => {},
      signal: new AbortController().signal,
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(out).toHaveLength(1);
    release();
    expect(await running).toBe(0);
    expect(out).toHaveLength(3);
    const rejected = await run(options(), async () => ({ h: "0" }), undefined, () => Promise.reject(new Error("EPIPE")));
    const atRejected = await run(options({ at: 3n }), async () => null, undefined, () => Promise.reject(new Error("EPIPE")));
    expect([atRejected.code, atRejected.errors]).toEqual([1, ["streamd: EPIPE"]]);
    expect([rejected.code, rejected.errors]).toEqual([1, ["streamd: EPIPE"]]);
    client.destroy();
  });

  it("exits 0 with no message when the signal aborts mid-range", async () => {
    const controller = new AbortController();
    const errors: string[] = [];
    const client = new StreamdClient({ adapter: new MemoryAdapter(history) });
    const code = await runInspect(options(), {
      client,
      read: async () => ({ h: "0" }),
      write: () => {
        controller.abort();
        throw new Error("EPIPE");
      },
      error: (m) => errors.push(m),
      signal: controller.signal,
    });
    client.destroy();
    expect([code, errors]).toEqual([0, []]);
  });

  it("exits 1 when a write fails", async () => {
    const { code, errors } = await run(options(), async () => ({ h: "0" }), undefined, () => {
      throw new Error("EIO");
    });
    expect([code, errors]).toEqual([1, ["streamd: EIO"]]);
  });
});

describe("resourceReader", () => {
  const realFetch = globalThis.fetch;
  afterEach(() => {
    globalThis.fetch = realFetch;
  });
  const STORE = "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>";
  const LONG = `0x${"0".repeat(63)}1`;
  const PRUNED = { error_code: "version_pruned", message: "Ledger version(1) has been pruned" };

  /** Answers each URL by its host, recording what was asked. */
  function serve(answers: Record<string, [number, unknown]>): URL[] {
    const seen: URL[] = [];
    globalThis.fetch = (async (input: string | URL) => {
      const url = new URL(String(input));
      seen.push(url);
      const [status, body] = answers[url.host] ?? [500, { message: "no answer" }];
      return Response.json(body, { status });
    }) as unknown as typeof fetch;
    return seen;
  }

  const reader = (extra: Partial<InspectOptions> = {}) =>
    resourceReader({ network: "mainnet", nodeUrl: "https://a.example/v1", ...extra }, LONG, STORE, sendOnce());

  it("reads the resource's data at the version, the type encoded in the path", async () => {
    const seen = serve({ "a.example": [200, { type: STORE, data: { coin: { value: "5" } } }] });
    expect(await reader()(42n)).toEqual({ coin: { value: "5" } });
    expect(seen[0]!.pathname).toBe(`/v1/accounts/${LONG}/resource/${encodeURIComponent(STORE)}`);
    expect(seen[0]!.searchParams.get("ledger_version")).toBe("42");
  });

  it("gives null for a resource the account did not hold, and fails on any other 404", async () => {
    serve({ "a.example": [404, { error_code: "resource_not_found", message: "Resource not found" }] });
    expect(await reader()(42n)).toBeNull();
    serve({ "a.example": [404, { error_code: "version_not_found", message: "Ledger version not found" }] });
    const err = await reader()(42n).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(StreamdNetworkError);
    expect((err as StreamdNetworkError).statusCode).toBe(404);
  });

  it("rejects a body with no data", async () => {
    serve({ "a.example": [200, { type: STORE }] });
    await expect(reader()(42n)).rejects.toThrow(/malformed/);
  });

  it("asks the archive only for a pruned version, and fails with the 410 without one", async () => {
    serve({ "a.example": [410, PRUNED] });
    const err = await reader()(1n).catch((e: unknown) => e);
    expect((err as StreamdNetworkError).statusCode).toBe(410);
    const seen = serve({ "a.example": [410, PRUNED], "archive.example": [200, { type: STORE, data: { n: "1" } }] });
    expect(await reader({ archive: "https://archive.example/v1" })(1n)).toEqual({ n: "1" });
    expect(seen.map((u) => u.host)).toEqual(["a.example", "archive.example"]);
    const other = serve({ "a.example": [500, { message: "down" }] });
    await expect(reader({ archive: "https://archive.example/v1" })(1n)).rejects.toBeInstanceOf(StreamdNetworkError);
    expect(other.map((u) => u.host)).toEqual(["a.example"]);
  });

  it("sends the key to Aptos Labs' archive only without --node-url", async () => {
    const keys: (string | null)[] = [];
    globalThis.fetch = (async (input: string | URL, init?: RequestInit) => {
      keys.push(new Headers(init?.headers).get("authorization"));
      const archive = String(input).startsWith("https://archive.mainnet.aptoslabs.com/");
      return Response.json(archive ? { data: {} } : PRUNED, { status: archive ? 200 : 410 });
    }) as unknown as typeof fetch;
    await resourceReader({ network: "mainnet", archive: "default", apiKey: "k" }, LONG, STORE, sendOnce())(1n);
    await resourceReader({ network: "mainnet", nodeUrl: "https://a.example/v1", archive: "default", apiKey: "k" }, LONG, STORE, sendOnce())(1n);
    expect(keys).toEqual(["Bearer k", "Bearer k", "Bearer k", null]);
  });
});
