import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { checkAdapter } from "@streamd/core";
import type { EventDescriptor } from "@streamd/core";
import { FullnodeScanAdapter } from "../src/index.js";

const TYPE = "0x1::test::Ping";
const desc: EventDescriptor = { kind: "module", eventType: TYPE };
const walkOptions = { fromVersion: 2n, limit: 2 };

describe("checkAdapter against FullnodeScanAdapter", () => {
  const hashOf = (v: number) => `0x${v.toString(16).padStart(64, "0")}`;
  const transactions = Array.from({ length: 10 }, (_, v) => ({
    version: String(v),
    hash: hashOf(v),
    type: "user_transaction",
    success: true,
    vm_status: "Executed successfully",
    events: [{ type: TYPE, data: {}, sequence_number: "0" }],
  }));
  const block = (h: number) => ({
    block_height: String(h),
    block_timestamp: String(h + 2),
    first_version: String(2 * h),
    last_version: String(2 * h + 1),
  });

  /**
   * A real `Aptos` over a stubbed transport serving versions 0..9, one Ping
   * each, blocks 0..4 of two versions each, both transaction lookups and a view.
   */
  function stubbedFullnode(): FullnodeScanAdapter {
    const route = (path: string, params: Record<string, unknown>): unknown => {
      if (path === "") {
        return { ledger_version: "9", oldest_ledger_version: "0", block_height: "4", oldest_block_height: "0" };
      }
      if (path === "transactions") {
        const start = Number(params.start ?? 0);
        const limit = Number(params.limit ?? 25);
        return transactions.slice(start, start + limit);
      }
      const at = /^(blocks\/by_height|transactions\/by_version|transactions\/by_hash)\/(\w+)$/.exec(path);
      if (at?.[1] === "blocks/by_height") return block(Number(at[2]));
      if (at?.[1] === "transactions/by_version") return transactions[Number(at[2])];
      if (at?.[1] === "transactions/by_hash") return transactions.find((t) => t.hash === at[2]);
      if (path === "view") return ["1"];
      throw new Error(`unexpected path ${path}`);
    };
    const client = {
      async provider(req: { url: string; params?: Record<string, unknown> }) {
        const path = req.url.replace(/^http:\/\/fake\/v1\/?/, "");
        return { status: 200, statusText: "OK", data: route(path, req.params ?? {}), headers: {} };
      },
    };
    return new FullnodeScanAdapter(
      new Aptos(
        new AptosConfig({
          // The SDK caches ledger info per network name; one no other test uses keeps this head.
          network: Network.CUSTOM,
          fullnode: "http://fake/v1",
          client: client as never,
          clientConfig: { http2: false },
        }),
      ),
      { scanBatch: 2 },
    );
  }

  it("passes every rule on a module descriptor", async () => {
    const report = await checkAdapter(stubbedFullnode(), desc, {
      ...walkOptions,
      blockHeight: 3n,
      view: { request: { function: "0x1::timestamp::now_microseconds" } },
    });
    expect(report.results.filter((r) => r.status !== "pass")).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.pagesFetched).toBe(3);
  });
});
