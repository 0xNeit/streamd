import { describe, expect, it } from "bun:test";
import type { FundFlowGraph } from "@streamd/aptos";
import { StreamdError } from "@streamd/core";
import type { TraceCommandOptions } from "../src/args.js";
import { formatDot, pathTo, runTrace } from "../src/trace.js";

const A = `0x${"a".repeat(64)}`;
const B = `0x${"b".repeat(64)}`;
const C = `0x${"c".repeat(64)}`;

function graph(): FundFlowGraph {
  return {
    root: A,
    nodes: [
      { address: A, hop: 0, expanded: true },
      { address: B, hop: 1, expanded: false },
      { address: C, hop: 1, expanded: false },
    ],
    edges: [
      { from: A, to: B, asset: "0xa", amount: 10n ** 20n, transfers: 2, firstVersion: 5n, lastVersion: 9n },
      { from: C, to: A, asset: 'odd"type\\x', amount: 1n, transfers: 1, firstVersion: 7n, lastVersion: 7n },
    ],
    stoppedBy: "requests",
    requests: 12,
  };
}

function harness(result: () => Promise<FundFlowGraph> = async () => graph()) {
  const calls: { address: string; options: Record<string, unknown> }[] = [];
  const out: string[] = [];
  const errors: string[] = [];
  const controller = new AbortController();
  const io = {
    client: {
      trace: (address: string, options: Record<string, unknown>) => {
        calls.push({ address, options });
        return result();
      },
    },
    write: (text: string) => void out.push(text),
    error: (m: string) => errors.push(m),
    signal: controller.signal,
  };
  const run = (options: Partial<TraceCommandOptions>) =>
    runTrace({ network: "mainnet", address: "alice.apt", format: "json", ...options }, io as never);
  return { run, calls, out, errors, controller, io };
}

describe("runTrace", () => {
  it("passes every option given and nothing unset", async () => {
    const h = harness();
    expect(await h.run({})).toBe(0);
    expect(Object.keys(h.calls[0]!.options).sort()).toEqual(["retries", "signal"]);
    await h.run({ hops: 2, direction: "out", fromVersion: 1n, toVersion: "head", minAmount: 5n, asset: "0xa", maxNodes: 9, maxRequests: 50, stop: [B], target: C });
    const { signal, ...given } = h.calls[1]!.options;
    expect(h.calls[1]!.address).toBe("alice.apt");
    expect(given).toEqual({ hops: 2, direction: "out", fromVersion: 1n, toVersion: "head", minAmount: 5n, asset: "0xa", maxNodes: 9, maxRequests: 50, stop: [B], target: C, retries: 5 });
    expect(signal).toBe(h.io.signal);
  });

  it("prints one JSON line with bigints as strings, and path only under --target", async () => {
    const h = harness();
    await h.run({});
    expect(h.out[0]!.endsWith("\n")).toBe(true);
    expect(h.out[0]!.trim().includes("\n")).toBe(false);
    const plain = JSON.parse(h.out[0]!);
    expect(plain.edges[0].amount).toBe("100000000000000000000");
    expect(plain.stoppedBy).toBe("requests");
    expect("path" in plain).toBe(false);
    await h.run({ target: B });
    expect(JSON.parse(h.out[1]!).path).toEqual([expect.objectContaining({ from: A, to: B, amount: "100000000000000000000" })]);
    await h.run({ target: B, direction: "in" });
    expect(JSON.parse(h.out[2]!).path).toBeNull();
  });

  it("prints DOT with the path in bold", async () => {
    const h = harness();
    expect(await h.run({ format: "dot", target: B })).toBe(0);
    expect(h.out[0]).toContain(`"${A}" -> "${B}" [label="0xa\\n100000000000000000000 (2 transfers)", style=bold];`);
  });

  it("exits 1 with the client's message, and 130 when interrupted", async () => {
    const refused = harness(async () => {
      throw new StreamdError("trace stop must be a list of addresses");
    });
    expect(await refused.run({ stop: ["alice.apt"] })).toBe(1);
    expect(refused.errors).toEqual(["streamd: trace stop must be a list of addresses"]);
    const interrupted = harness(async () => {
      interrupted.controller.abort();
      throw new Error("aborted");
    });
    expect(await interrupted.run({})).toBe(130);
    expect(interrupted.out).toEqual([]);
  });
});

describe("pathTo", () => {
  it("follows the direction funds moved, either way for both", () => {
    expect(pathTo(graph(), B, "out")).toHaveLength(1);
    expect(pathTo(graph(), B, "in")).toBeNull();
    expect(pathTo(graph(), C, "in")).toEqual([graph().edges[1]!]);
    expect(pathTo(graph(), C, "out")).toBeNull();
    expect(pathTo(graph(), C)).toEqual([graph().edges[1]!]);
  });
});

describe("formatDot", () => {
  it("draws every node and edge, the root heavier, unexpanded nodes dashed, escaping quotes and backslashes", () => {
    expect(formatDot(graph())).toBe(
      [
        "digraph trace {",
        '  comment="stopped by requests";',
        `  "${A}" [label="${A}\\nhop 0", penwidth=2];`,
        `  "${B}" [label="${B}\\nhop 1", style=dashed];`,
        `  "${C}" [label="${C}\\nhop 1", style=dashed];`,
        `  "${A}" -> "${B}" [label="0xa\\n100000000000000000000 (2 transfers)"];`,
        `  "${C}" -> "${A}" [label="odd\\"type\\\\x\\n1 (1 transfer)"];`,
        "}",
        "",
      ].join("\n"),
    );
  });

  it("is an empty graph for a root with no transfers", () => {
    expect(formatDot({ root: A, nodes: [{ address: A, hop: 0, expanded: true }], edges: [], requests: 2 })).toBe(
      `digraph trace {\n  "${A}" [label="${A}\\nhop 0", penwidth=2];\n}\n`,
    );
  });
});
