import { describe, expect, it } from "bun:test";
import { MemoryAdapter, StreamdError } from "@streamd/core";
import type { Transfer } from "@streamd/core";
import { TraceBudgetError, countedAdapter, tracePath, walkTransfers } from "../src/trace.js";
import type { TracedTransaction, WalkOptions } from "../src/trace.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;
const [A, B, C, D, E] = ["a", "b", "c", "d", "e"].map((h) => long(h + "0"));
const APT = long("a");
const USDC = long("bc");

/** A ledger of transfers by version; each node reads the transactions it took part in. */
function ledger(transactions: [number, Transfer[]][]) {
  const reads: string[] = [];
  const read = async (address: string): Promise<TracedTransaction[]> => {
    reads.push(address);
    return transactions
      .filter(([, ts]) => ts.some((t) => t.from === address || t.to === address))
      .map(([v, ts]) => ({ version: BigInt(v), transfers: ts.filter((t) => t.from === address || t.to === address) }));
  };
  return { read, reads };
}
const t = (from: string | undefined, to: string | undefined, amount: bigint, asset = APT): Transfer => ({ asset, amount, from, to });

const defaults: WalkOptions = { hops: 1, direction: "both", minAmount: 0n, maxNodes: 100, stop: new Set() };
const shape = (graph: Awaited<ReturnType<typeof walkTransfers>>) => ({
  nodes: graph.nodes.map((n) => [n.address, n.hop, n.expanded]),
  edges: graph.edges.map((e) => [e.from, e.to, e.amount, e.transfers]),
  stoppedBy: graph.stoppedBy,
});

describe("walkTransfers", () => {
  const chain = ledger([
    [1, [t(A, B, 100n)]],
    [2, [t(B, C, 60n)]],
    [3, [t(C, D, 30n)]],
    [4, [t(E, A, 5n)]],
  ]);

  it("reads only the root at one hop, both ways", async () => {
    expect(shape(await walkTransfers(chain.read, A, defaults))).toEqual({
      nodes: [[A, 0, true], [B, 1, false], [E, 1, false]],
      edges: [[A, B, 100n, 1], [E, A, 5n, 1]],
      stoppedBy: undefined,
    });
  });

  it("follows hops and counts a transfer seen from both ends once", async () => {
    const { read, reads } = ledger([
      [1, [t(A, B, 100n)]],
      [2, [t(B, C, 60n)]],
      [3, [t(C, D, 30n)]],
      [4, [t(E, A, 5n)]],
    ]);
    const graph = await walkTransfers(read, A, { ...defaults, hops: 2 });
    expect(shape(graph)).toEqual({
      nodes: [[A, 0, true], [B, 1, true], [E, 1, true], [C, 2, false]],
      edges: [[A, B, 100n, 1], [E, A, 5n, 1], [B, C, 60n, 1]],
      stoppedBy: undefined,
    });
    expect(reads).toEqual([A, B, E]);
  });

  it("keeps two identical transfers in one transaction as two, from either end", async () => {
    const { read } = ledger([[7, [t(A, B, 10n), t(A, B, 10n)]], [8, [t(A, B, 10n)]]]);
    const graph = await walkTransfers(read, A, { ...defaults, hops: 2 });
    expect(graph.edges).toEqual([{ from: A, to: B, asset: APT, amount: 30n, transfers: 3, firstVersion: 7n, lastVersion: 8n }]);
  });

  it("follows one direction", async () => {
    expect(shape(await walkTransfers(chain.read, B, { ...defaults, hops: 3, direction: "out" })).edges).toEqual([
      [B, C, 60n, 1],
      [C, D, 30n, 1],
    ]);
    expect(shape(await walkTransfers(chain.read, B, { ...defaults, hops: 3, direction: "in" })).edges).toEqual([
      [A, B, 100n, 1],
      [E, A, 5n, 1],
    ]);
  });

  it("ignores small transfers, other assets, lone legs and transfers to itself", async () => {
    const { read } = ledger([[1, [t(A, B, 9n), t(A, C, 10n, USDC), t(A, undefined, 50n), t(undefined, A, 50n), t(A, A, 50n), t(A, D, 10n)]]]);
    expect(shape(await walkTransfers(read, A, { ...defaults, minAmount: 10n })).edges).toEqual([
      [A, C, 10n, 1],
      [A, D, 10n, 1],
    ]);
    expect(shape(await walkTransfers(read, A, { ...defaults, asset: APT })).edges).toEqual([
      [A, B, 9n, 1],
      [A, D, 10n, 1],
    ]);
  });

  it("keeps stop-listed nodes unexpanded", async () => {
    const graph = await walkTransfers(chain.read, A, { ...defaults, hops: 3, stop: new Set([B]) });
    expect(shape(graph).nodes).toEqual([[A, 0, true], [B, 1, false], [E, 1, true]]);
  });

  it("stops at the node cap, dropping the edge that would pass it", async () => {
    const graph = await walkTransfers(chain.read, A, { ...defaults, hops: 3, maxNodes: 3 });
    expect(shape(graph)).toEqual({
      nodes: [[A, 0, true], [B, 1, true], [E, 1, false]],
      edges: [[A, B, 100n, 1], [E, A, 5n, 1]],
      stoppedBy: "nodes",
    });
  });

  it("stops as soon as the target joins", async () => {
    const graph = await walkTransfers(chain.read, A, { ...defaults, hops: 6, target: C });
    expect(shape(graph).stoppedBy).toBe("target");
    expect(graph.nodes.at(-1)!.address).toBe(C);
    expect(tracePath({ ...graph, requests: 0 }, A, C)!.map((e) => [e.from, e.to])).toEqual([[A, B], [B, C]]);
    expect(shape(await walkTransfers(chain.read, A, { ...defaults, target: A })).stoppedBy).toBe("target");
  });

  it("stops when the budget is spent, leaving that node unexpanded, and rethrows anything else", async () => {
    let calls = 0;
    const read = async (address: string) => {
      if (++calls > 1) throw new TraceBudgetError("spent");
      return chain.read(address);
    };
    const graph = await walkTransfers(read, A, { ...defaults, hops: 2 });
    expect(shape(graph).nodes).toEqual([[A, 0, true], [B, 1, false], [E, 1, false]]);
    expect(graph.stoppedBy).toBe("requests");
    await expect(walkTransfers(async () => Promise.reject(new StreamdError("boom")), A, defaults)).rejects.toThrow("boom");
  });
});

describe("tracePath", () => {
  it("finds the shortest directed chain, none against the flow, and [] for itself", async () => {
    const { read } = ledger([[1, [t(A, B, 1n)]], [2, [t(B, C, 1n)]], [3, [t(A, C, 1n)]]]);
    const graph = { ...(await walkTransfers(read, A, { ...defaults, hops: 2 })), requests: 0 };
    expect(tracePath(graph, A, C)!.map((e) => [e.from, e.to])).toEqual([[A, C]]);
    expect(tracePath(graph, "0xb0", C)!.map((e) => [e.from, e.to])).toEqual([[B, C]]);
    expect(tracePath(graph, C, A)).toBeUndefined();
    expect(tracePath(graph, A, A)).toEqual([]);
    expect(() => tracePath(graph, "nope", A)).toThrow(StreamdError);
  });
});

describe("countedAdapter", () => {
  it("counts every method, a class's included, and refuses calls past the cap", async () => {
    const { adapter, requests } = countedAdapter(new MemoryAdapter([]), 2);
    await adapter.getHeadVersion!({ kind: "module", eventType: "0x1::a::B" });
    await adapter.fetchEvents({ kind: "module", eventType: "0x1::a::B" }, {});
    await expect(adapter.fetchEvents({ kind: "module", eventType: "0x1::a::B" }, {})).rejects.toThrow(TraceBudgetError);
    expect(requests()).toBe(2);
  });
});
