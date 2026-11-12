import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { generateSuiCatalog, SUI_CATALOG_PATH } from "../scripts/sui-framework.js";

const long = (hex: string) => `0x${hex.padStart(64, "0")}`;

describe("Sui framework catalog", () => {
  it("matches the generator's output over the committed package snapshots", async () => {
    expect(readFileSync(SUI_CATALOG_PATH, "utf8")).toBe(await generateSuiCatalog());
  });

  it("types the staking and kiosk events by their long-form ids", async () => {
    const source = await generateSuiCatalog();
    expect(source).toContain(
      `export const StakingRequestEvent = defineEvent<StakingRequestEvent>("${long("3")}::validator::StakingRequestEvent");`,
    );
    expect(source).toContain(
      `export const ItemListed = <T0 = unknown>(t0: string) =>\n  defineEvent<ItemListed<T0>>(\`${long("2")}::kiosk::ItemListed<\${t0}>\`);`,
    );
    expect(source).toContain('export const ItemListedDecode = ["price"] as const;');
  });

  it("gives every export one name and no descriptor to types it only references", async () => {
    const source = await generateSuiCatalog();
    const consts = [...source.matchAll(/^export const (\w+)/gm)].map((m) => m[1]);
    expect(new Set(consts).size).toBe(consts.length);
    expect(source).toContain("export interface VecMap<T0 = unknown, T1 = unknown> {");
    expect(source).not.toContain("export const VecMap");
    expect(source).toContain("Token_RuleKey");
    expect(source).toContain("TransferPolicy_RuleKey");
  });
});
