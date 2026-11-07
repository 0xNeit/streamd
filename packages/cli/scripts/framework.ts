// Writes packages/core/src/framework.ts, the `@streamd/core/framework` catalog,
// from the ABI snapshots in ./framework-abi: `bun packages/cli/scripts/framework.ts`.
// To refresh a snapshot, save `/v1/accounts/0x1/module/<name>` from a mainnet
// fullnode keeping only `abi.address`, `abi.name` and `abi.structs`.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { normalizeAddress, StreamdError } from "@streamd/core";
import { parseModuleAbi } from "../src/abi.js";
import { generateEventTypes } from "../src/codegen.js";

export const FRAMEWORK_MODULES = [
  "account",
  "aptos_account",
  "block",
  "coin",
  "delegation_pool",
  "fungible_asset",
  "object",
  "stake",
  "transaction_fee",
];

export const CATALOG_PATH = fileURLToPath(new URL("../../core/src/framework.ts", import.meta.url));

const SNAPSHOTS = new URL("./framework-abi/", import.meta.url);
const FROM = 'import { defineEvent } from "@streamd/core";';

/** The catalog source, generated offline from the committed snapshots. */
export async function generateCatalog(): Promise<string> {
  const load = async (address: string, module: string) => {
    if (normalizeAddress(address) !== normalizeAddress("0x1")) {
      throw new StreamdError(`No snapshot for ${address}::${module}`);
    }
    const file = new URL(`0x1-${module}.json`, SNAPSHOTS);
    return parseModuleAbi(JSON.parse(readFileSync(file, "utf8")));
  };
  const source = await generateEventTypes(
    FRAMEWORK_MODULES.map((module) => ({ address: "0x1", module })),
    load,
  );
  if (!source.includes(FROM)) throw new StreamdError("Generated source lacks the expected import");
  return [
    "// Regenerate with `bun packages/cli/scripts/framework.ts`; ABI snapshots are in packages/cli/scripts/framework-abi.",
    source.replace(FROM, 'import { defineEvent } from "./define.js";'),
  ].join("\n");
}

if (import.meta.main) writeFileSync(CATALOG_PATH, await generateCatalog());
