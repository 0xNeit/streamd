import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { CATALOG_PATH, generateCatalog } from "../scripts/framework.js";

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
    expect(source).toContain("Voting_CreateProposal");
  });
});
