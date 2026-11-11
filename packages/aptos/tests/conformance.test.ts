import { describe, it, expect } from "bun:test";
import { Aptos, AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { checkAdapter } from "@streamd/core";
import type { EventDescriptor } from "@streamd/core";
import { FullnodeScanAdapter } from "../src/index.js";

const TYPE = "0x1::test::Ping";
const desc: EventDescriptor = { kind: "module", eventType: TYPE };
const walkOptions = { fromVersion: 2n, limit: 2 };

describe("checkAdapter against FullnodeScanAdapter", () => {
  /** A real `Aptos` over a stubbed transport serving versions 0..9, one Ping each. */
  function stubbedFullnode(): FullnodeScanAdapter {
    const route = (path: string, params: Record<string, unknown>): unknown => {
      if (path === "") return { ledger_version: "9", oldest_ledger_version: "0" };
      if (path === "transactions") {
        const start = Number(params.start ?? 0);
        const limit = Number(params.limit ?? 25);
        return Array.from({ length: 10 }, (_, v) => ({
          version: String(v),
          type: "user_transaction",
          events: [{ type: TYPE, data: {}, sequence_number: "0" }],
        })).slice(start, start + limit);
      }
      if (path === "blocks/by_height/3") {
        return { block_height: "3", block_timestamp: "5", first_version: "6", last_version: "7" };
      }
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
    const report = await checkAdapter(stubbedFullnode(), desc, { ...walkOptions, blockHeight: 3n });
    expect(report.results.filter((r) => r.status !== "pass")).toEqual([]);
    expect(report.ok).toBe(true);
    expect(report.pagesFetched).toBe(3);
  });
});
