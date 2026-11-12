import { describe, expect, it } from "bun:test";
import { SuiGrpcClient } from "@mysten/sui/grpc";
import { checkDescriptor, networks } from "../../src/index.js";

/** Hits Sui mainnet; skipped unless `STREAMD_INTEGRATION=1`. */
const enabled = process.env.STREAMD_INTEGRATION === "1";

// SuiNS core: published at ORIGINAL, upgraded since; its Admin struct dates from the first version.
const ORIGINAL = "0xd22b24490e0bae52676651b4f56660a5ff8022a2576e0089f79b3c88d44e08f0";

describe.skipIf(!enabled)("checkDescriptor (mainnet)", () => {
  const grpc = new SuiGrpcClient(networks.mainnet);

  it("reports each status from the package service", async () => {
    const check = await checkDescriptor(grpc, {
      kind: "module",
      eventType: [
        "0x3::validator::StakingRequestEvent",
        "0x2::coin::Coin<0x2::sui::SUI>",
        "0x2::coin::NoSuchStruct",
        "0x2::no_such_module::X",
        "0x1234::m::X",
        "0x6::clock::Clock",
      ],
    });
    expect(check.ok).toBe(false);
    expect(check.types.map((t) => t.status)).toEqual(["event", "not-event", "missing-struct", "missing-module", "missing-package", "missing-package"]);
    expect(check.types[0]!.eventType).toBe(`0x${"3".padStart(64, "0")}::validator::StakingRequestEvent`);
  }, 30_000);

  it("names the defining package of a type an upgrade inherited", async () => {
    const { versions } = (await grpc.movePackageService.listPackageVersions({ packageId: ORIGINAL, pageSize: 100 })).response;
    const latest = versions.at(-1)!.packageId!;
    expect(latest).not.toBe(ORIGINAL);
    const check = await checkDescriptor(grpc, { kind: "module", eventType: [`${latest}::admin::Admin`, `${ORIGINAL}::admin::Admin`] });
    expect(check.types).toEqual([
      { eventType: `${latest}::admin::Admin`, status: "defined-elsewhere", definingId: ORIGINAL },
      { eventType: `${ORIGINAL}::admin::Admin`, status: "not-event" },
    ]);
  }, 30_000);
});
