import { describe, expect, it } from "bun:test";
import { AccountAddress, Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { MemoryAdapter, StreamdError } from "@streamd/core";
import type { AccountEventDescriptor, EventAdapter, StreamdEvent, Transfer } from "@streamd/core";
import { testClient } from "./helpers.js";
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

describe("AptosClient.trace", () => {
  const COIN = "0x1::aptos_coin::AptosCoin";
  const coin = (v: number, from: string, to: string, amount: string): StreamdEvent[] =>
    (["Withdraw", "Deposit"] as const).map((side, i) => ({
      type: `0x1::coin::Coin${side}`,
      data: { coin_type: COIN, account: side === "Withdraw" ? from : to, amount },
      version: BigInt(v),
      sequenceNumber: 0n,
      eventIndex: i,
      sender: from,
    }));
  const HISTORY = [...coin(1, A, B, "100"), ...coin(2, B, C, "60"), ...coin(3, C, D, "30"), ...coin(9, A, E, "1")];

  /** An Indexer-like source: an `account` read gets the transactions that account took part in. */
  function indexer(events: StreamdEvent[]): EventAdapter & { reads: string[] } {
    const reads: string[] = [];
    return {
      reads,
      fetchEvents: (d, o) => {
        const who = (d as AccountEventDescriptor).accountAddress;
        reads.push(who);
        const versions = new Set(events.filter((e) => (e.data as { account: string }).account === who).map((e) => e.version));
        return new MemoryAdapter(events.filter((e) => versions.has(e.version))).fetchEvents(d, o);
      },
      getHeadVersion: async () => 5n,
      view: async () => [],
    };
  }

  it("reads every node over one range fixed at the start, counting each adapter call", async () => {
    const source = indexer(HISTORY);
    const graph = await testClient({ adapter: source }).trace(A, { hops: 2 });
    expect(graph.root).toBe(A);
    expect(graph.nodes.map((n) => [n.address, n.hop, n.expanded])).toEqual([[A, 0, true], [B, 1, true], [C, 2, false]]);
    expect(graph.edges).toEqual([
      { from: A, to: B, asset: COIN, amount: 100n, transfers: 1, firstVersion: 1n, lastVersion: 1n },
      { from: B, to: C, asset: COIN, amount: 60n, transfers: 1, firstVersion: 2n, lastVersion: 2n },
    ]);
    expect(graph.stoppedBy).toBeUndefined();
    expect(source.reads).toEqual([A, B]);
    expect(graph.requests).toBe(3);
    const late = await testClient({ adapter: source }).trace(A, { toVersion: 9n, fromVersion: -8n });
    expect(late.edges.map((e) => e.to)).toEqual([B, E]);
    expect((await testClient({ adapter: source }).trace(A, { fromVersion: -3n })).edges).toEqual([]);
  });

  it("stops when maxRequests is spent, leaving the node it was reading unexpanded", async () => {
    const graph = await testClient({ adapter: indexer(HISTORY) }).trace(A, { hops: 3, maxRequests: 2 });
    expect(graph.stoppedBy).toBe("requests");
    expect(graph.requests).toBe(2);
    expect(graph.nodes.map((n) => [n.address, n.expanded])).toEqual([[A, true], [B, false]]);
  });

  it("resolves a name root once", async () => {
    const aptos = new Aptos(new AptosConfig({ network: Network.MAINNET, clientConfig: { http2: false } }));
    let lookups = 0;
    aptos.getTargetAddress = async () => (lookups++, AccountAddress.from(B));
    const graph = await testClient({ adapter: indexer(HISTORY), aptos }).trace("bob.apt", { direction: "out" });
    expect(graph.root).toBe(B);
    expect(graph.edges.map((e) => [e.from, e.to])).toEqual([[B, C]]);
    expect(lookups).toBe(1);
  });

  it("refuses bad options and adapters before reading anything", async () => {
    const source = indexer(HISTORY);
    const client = testClient({ adapter: source });
    for (const [options, message] of [
      [{ hops: 0 }, "hops"],
      [{ hops: 7 }, "hops"],
      [{ direction: "up" }, "direction"],
      [{ minAmount: 1 }, "minAmount"],
      [{ maxNodes: 1.5 }, "maxNodes"],
      [{ maxRequests: 0 }, "maxRequests"],
      [{ stop: ["nope"] }, "Not a valid"],
      [{ stop: "0x1" }, "stop"],
      [{ target: 3 }, "Not a valid"],
      [{ asset: "nope::x" }, "coin type"],
      [{ toVersion: -1n }, "toVersion"],
      [{ fromVersion: 1 }, "fromVersion"],
    ] as const) {
      await expect(client.trace(A, options as never)).rejects.toThrow(message);
    }
    await expect(client.trace(undefined as never)).rejects.toThrow("trace needs an address");
    await expect(client.trace("not an address!")).rejects.toThrow("Not a valid Aptos address or name");
    expect(source.reads).toEqual([]);
    const headless = testClient({ adapter: { ...indexer(HISTORY), getHeadVersion: undefined } });
    await expect(headless.trace(A)).rejects.toThrow("getHeadVersion");
    expect((await headless.trace(A, { toVersion: 5n })).edges).toHaveLength(1);
  });
});
