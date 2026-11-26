// Writes packages/aptos/src/framework.ts, the `@streamd/aptos/framework` catalog,
// from the ABI snapshots in ./framework-abi: `bun packages/cli/scripts/framework.ts`.
// To refresh a snapshot, save `/v1/accounts/<address>/module/<name>` from a mainnet
// fullnode as `<address>-<name>.json`, keeping only `abi.address`, `abi.name` and `abi.structs`.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { generateEventTypes, parseModuleAbi } from "@streamd/codegen";
import { normalizeAddress, StreamdError } from "@streamd/core";

export const FRAMEWORK_MODULES = {
  "0x1": [
    "account",
    "account_abstraction",
    "aptos_account",
    "aptos_governance",
    "block",
    "chunky_dkg",
    "code",
    "coin",
    "confidential_asset",
    "delegation_pool",
    "dkg",
    "fungible_asset",
    "jwks",
    "multisig_account",
    "object",
    "object_code_deployment",
    "randomness",
    "reconfiguration",
    "reconfiguration_with_dkg",
    "stake",
    "staking_contract",
    "transaction_fee",
    "vesting",
    "voting",
  ],
  "0x3": ["token"],
  "0x4": ["collection", "token"],
};

export const CATALOG_PATH = fileURLToPath(new URL("../../aptos/src/framework.ts", import.meta.url));

// Exports that were unqualified until another module shared their names.
const KEPT_NAMES = `
export {
  DelegationPool_CreateProposal as CreateProposal,
  DelegationPool_SetBeneficiaryForOperator as SetBeneficiaryForOperator,
  DelegationPool_Vote as Vote,
};
`;

const SNAPSHOTS = new URL("./framework-abi/", import.meta.url);
const FROM = 'import { defineEvent, defineEvents } from "@streamd/core";';

/** The catalog source, generated offline from the committed snapshots. */
export async function generateCatalog(): Promise<string> {
  const load = async (address: string, module: string) => {
    const short = Object.keys(FRAMEWORK_MODULES).find((a) => normalizeAddress(a) === normalizeAddress(address));
    if (short === undefined) throw new StreamdError(`No snapshot for ${address}::${module}`);
    const file = new URL(`${short}-${module}.json`, SNAPSHOTS);
    return parseModuleAbi(JSON.parse(readFileSync(file, "utf8")));
  };
  const source = await generateEventTypes(
    Object.entries(FRAMEWORK_MODULES).flatMap(([address, modules]) =>
      modules.map((module) => ({ address, module })),
    ),
    load,
    { handles: false },
  );
  if (!source.includes(FROM)) throw new StreamdError("Generated source lacks the expected import");
  return [
    "// Regenerate with `bun packages/cli/scripts/framework.ts`; ABI snapshots are in packages/cli/scripts/framework-abi.",
    source.trimEnd(),
    KEPT_NAMES,
  ].join("\n");
}

if (import.meta.main) writeFileSync(CATALOG_PATH, await generateCatalog());
