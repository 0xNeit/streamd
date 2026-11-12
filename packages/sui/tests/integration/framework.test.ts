import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import type { ModuleEventDescriptor } from "@streamd/core";
import * as framework from "../../src/framework.js";
import { checkDescriptor, networks, SuiClient } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

const ADDRESS = /^0x[0-9a-f]{64}$/;
const DECIMAL = /^\d+$/;

/** Every catalog descriptor, generic ones instantiated with SUI, which checks only the outer type. */
function descriptors(): ModuleEventDescriptor[] {
  return Object.values(framework).flatMap((value): ModuleEventDescriptor[] => {
    if (typeof value === "function") return [(value as (...a: string[]) => ModuleEventDescriptor)(...Array(value.length).fill("0x2::sui::SUI"))];
    return (value as { kind?: unknown }).kind === "module" ? [value as ModuleEventDescriptor] : [];
  });
}

describe.skipIf(!enabled)("@streamd/sui/framework (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("names only types mainnet's packages can emit", async () => {
    const list = descriptors();
    expect(list.length).toBeGreaterThan(20);
    const check = await checkDescriptor(grpc, { kind: "module", eventType: list.flatMap((d) => d.eventType) });
    expect(check.types.filter((t) => t.status !== "event")).toEqual([]);
    expect(check.ok).toBe(true);
  }, 30_000);

  it("reads events whose data has the shapes the catalog declares", async () => {
    using sui = new SuiClient({ sui: grpc });
    const stakes = await sui.getEvents(framework.StakingRequestEvent, { fromVersion: -20_000_000n, limit: 2, maxPages: 30 });
    expect(stakes.length).toBeGreaterThan(0);
    for (const { data } of stakes) {
      expect(Object.keys(data).sort()).toEqual(["amount", "epoch", "pool_id", "staker_address", "validator_address"]);
      expect(data.pool_id).toMatch(ADDRESS);
      expect(data.amount).toMatch(DECIMAL);
    }

    // A generic event: take a type argument from any listing, then ask for exactly that instantiation.
    const [any] = await sui.getEvents({ kind: "module", eventType: "0x2::kiosk::ItemListed" }, { fromVersion: -20_000_000n, limit: 1, maxPages: 30 });
    const arg = /<(.+)>$/.exec(any!.type)![1]!;
    const listed = await sui.getEvents(framework.ItemListed(arg), { fromVersion: any!.version, limit: 1, maxPages: 5 });
    expect(listed[0]!.type).toBe(any!.type);
    expect(Object.keys(listed[0]!.data).sort()).toEqual(["id", "kiosk", "price"]);
    expect(listed[0]!.data.kiosk).toMatch(ADDRESS);
    expect(listed[0]!.data.price).toMatch(DECIMAL);
  }, 120_000);
});
