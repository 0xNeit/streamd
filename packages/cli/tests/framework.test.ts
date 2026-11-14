import { describe, expect, it } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { CATALOG_PATH, FRAMEWORK_MODULES, generateCatalog } from "../scripts/framework.js";

describe("framework catalog", () => {
  it("matches the generator's output over the committed ABI snapshots", async () => {
    expect(readFileSync(CATALOG_PATH, "utf8")).toBe(await generateCatalog());
  });

  it("qualifies names that two framework modules share", async () => {
    const source = await generateCatalog();
    expect(source).toContain('export const FungibleAsset_Deposit = defineEvent<FungibleAsset_Deposit>("0x1::fungible_asset::Deposit");');
    expect(source).toContain("export const Coin_Deposit = <T0 = unknown>(t0: string) =>");
    expect(source).not.toContain("export const Deposit ");
  });

  it("types the token, governance and multisig modules", async () => {
    const source = await generateCatalog();
    for (const type of [
      "0x3::token::TokenDeposit",
      "0x4::token::Mutation",
      "0x4::collection::Mint",
      "0x1::aptos_governance::CreateProposal",
      "0x1::voting::ResolveProposal",
      "0x1::multisig_account::AddOwners",
      "0x1::staking_contract::Distribute",
      "0x1::vesting::Vest",
      "0x1::code::PublishPackage",
      "0x1::object_code_deployment::Upgrade",
      "0x1::reconfiguration::NewEpoch",
    ]) {
      expect(source).toContain(`("${type}")`);
    }
  });

  it("gives every export one name, keeping the ones that existed before names were shared", async () => {
    const source = await generateCatalog();
    const consts = [...source.matchAll(/^export const (\w+)/gm)].map((m) => m[1]);
    expect(new Set(consts).size).toBe(consts.length);
    expect(source).toContain("DelegationPool_CreateProposal as CreateProposal");
    expect(source).toContain("DelegationPool_Vote as Vote");
    expect(source).toContain("DelegationPool_SetBeneficiaryForOperator as SetBeneficiaryForOperator");
    expect(source).toContain("Voting_CreateProposal");
  });

  it("has a descriptor for every event in the snapshots of every listed module", async () => {
    const source = await generateCatalog();
    const dir = new URL("../scripts/framework-abi/", import.meta.url);
    const listed = Object.entries(FRAMEWORK_MODULES).flatMap(([a, ms]) => ms.map((m) => `${a}-${m}.json`));
    expect(readdirSync(dir)).toEqual(expect.arrayContaining(listed));
    let events = 0;
    for (const file of listed) {
      const { abi } = JSON.parse(readFileSync(new URL(file, dir), "utf8"));
      for (const s of abi.structs.filter((s: { is_event: boolean }) => s.is_event)) {
        expect(source).toMatch(new RegExp(`${abi.address}::${abi.name}::${s.name}["<]`));
        events++;
      }
    }
    expect(events).toBeGreaterThan(100);
  });
});
