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
});
