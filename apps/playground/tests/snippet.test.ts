import { describe, expect, it } from "bun:test";
import { descriptorName, snippet } from "../src/snippet.js";

const SOURCE = [
  'export const Deposit = defineEvent<Deposit>("0x1::fungible_asset::Deposit");',
  'export const FungibleAsset_Withdraw = defineEvent<FungibleAsset_Withdraw>("0x1::fungible_asset::Withdraw");',
  "export const Moved = <T0>(t0: string) =>",
  "  defineEvent<Moved<T0>>(`0xcafe::pool::Moved<${t0}>`);",
].join("\n");

describe("descriptorName", () => {
  it("finds the generated export, whatever form the address was typed in", () => {
    expect(descriptorName(SOURCE, "0x1::fungible_asset::Deposit")).toBe("Deposit");
    expect(descriptorName(SOURCE, ` 0x${"0".repeat(63)}1::fungible_asset::Withdraw `)).toBe("FungibleAsset_Withdraw");
  });

  it("has none for a generic event, a type not generated, or a different module", () => {
    expect(descriptorName(SOURCE, "0xcafe::pool::Moved")).toBeUndefined();
    expect(descriptorName(SOURCE, "0x1::fungible_asset::Frozen")).toBeUndefined();
    expect(descriptorName(SOURCE, "0x1::coin::Deposit")).toBeUndefined();
  });
});

describe("snippet", () => {
  it("imports the generated descriptor and reads the key from the environment", () => {
    const text = snippet({ network: "mainnet", eventType: "0x1::fungible_asset::Deposit", name: "Deposit" });
    expect(text).toContain('import { Deposit } from "./events.js";');
    expect(text).toContain("network: Network.MAINNET");
    expect(text).toContain("client.on(Deposit, (event) => {");
    expect(text).toContain("process.env.APTOS_API_KEY");
    expect(text.endsWith("});\n")).toBe(true);
  });

  it("inlines a module descriptor when there is nothing to import", () => {
    const text = snippet({ network: "testnet", eventType: " 0xcafe::pool::Moved ", name: undefined });
    expect(text).not.toContain("./events.js");
    expect(text).not.toContain("events.ts");
    expect(text).toContain("network: Network.TESTNET");
    expect(text).toContain('client.on({ kind: "module", eventType: "0xcafe::pool::Moved" }, (event) => {');
  });

  it("escapes the typed event type", () => {
    const text = snippet({ network: "mainnet", eventType: '0x1::m::E"); evil("' });
    expect(text).toContain('eventType: "0x1::m::E\\"); evil(\\""');
  });
});
