import { describe, expect, it } from "bun:test";
import { eventId, fromFunction } from "@streamd/core";
import type { MoveEventType } from "@streamd/core";
import { SuiClient, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

/** Cetus CLMM swaps: frequent, and reached through many routers, so some PTBs call the pool module more than once. */
const SWAP = "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::SwapEvent" as MoveEventType;

describe.skipIf(!enabled)("SuiClient entry functions (mainnet)", () => {
  it("attaches the emitting command, which fromFunction and eventsOf agree on", async () => {
    using sui = new SuiClient({ sui: networks.mainnet });
    const events = await sui.getEvents({ kind: "module", eventType: SWAP }, { fromVersion: -50_000n, limit: 40, maxPages: 20 });
    const called = events.filter((e) => e.entryFunction !== undefined);
    expect(called.length).toBeGreaterThan(0);
    for (const { entryFunction } of called) {
      expect(entryFunction!.function).toMatch(/^0x[0-9a-f]{64}::\w+::\w+$/);
      expect(entryFunction!.arguments.length).toBeGreaterThan(0);
    }
    const target = called[0]!.entryFunction!.function;
    const filtered = await sui.getEvents({ kind: "module", eventType: SWAP }, {
      fromVersion: events[0]!.version,
      toVersion: events.at(-1)!.version,
      where: fromFunction(target),
    });
    expect(filtered.every((e) => e.entryFunction?.function === target)).toBe(true);
    const ids = new Set(filtered.map(eventId));
    for (const e of called.filter((c) => c.entryFunction!.function === target)) expect(ids.has(eventId(e))).toBe(true);
    const looked = await sui.eventsOf(called[0]!.transactionHash!);
    expect(looked.find((e) => e.eventIndex === called[0]!.eventIndex)?.entryFunction).toEqual(called[0]!.entryFunction);
  }, 120_000);
});
