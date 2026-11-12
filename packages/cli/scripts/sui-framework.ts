// Writes packages/sui/src/framework.ts, the `@streamd/sui/framework` catalog, from the
// package snapshots in ./sui-framework-abi: `bun packages/cli/scripts/sui-framework.ts`.
// `--refresh` first rewrites the snapshots from Sui mainnet's package service, keeping
// only each package's ids and its modules' datatypes.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { normalizeAddress, StreamdError } from "@streamd/core";
import { generateEventTypes } from "../src/codegen.js";
import { SUI_DIALECT, suiCodegen } from "../src/sui-abi.js";
import type { SuiPackageService } from "../src/sui-abi.js";

const FRAMEWORK_MODULES = {
  "0x2": ["coin", "deny_list", "display", "kiosk", "token", "transfer_policy"],
  "0x3": ["sui_system_state_inner", "validator", "validator_set"],
};
// The catalog's types reference only these packages' types.
const SNAPSHOTTED = ["0x1", "0x2", "0x3"];

export const SUI_CATALOG_PATH = fileURLToPath(new URL("../../sui/src/framework.ts", import.meta.url));

const SNAPSHOTS = new URL("./sui-framework-abi/", import.meta.url);
const snapshot = (short: string) => new URL(`${short}.json`, SNAPSHOTS);

const snapshots: SuiPackageService = {
  async getPackage({ packageId }) {
    const short = SNAPSHOTTED.find((a) => normalizeAddress(a) === normalizeAddress(packageId));
    if (short === undefined) throw new StreamdError(`No snapshot for package ${packageId}`);
    return { response: { package: JSON.parse(readFileSync(snapshot(short), "utf8")) } };
  },
  // Framework packages are upgraded in place, so no type needs another version.
  async listPackageVersions({ packageId }) {
    throw new StreamdError(`No snapshot of the versions of package ${packageId}`);
  },
};

/** The catalog source, generated offline from the committed snapshots. */
export async function generateSuiCatalog(): Promise<string> {
  const { targets, load } = suiCodegen(snapshots);
  const refs = Object.entries(FRAMEWORK_MODULES).flatMap(([address, modules]) => modules.map((module) => ({ address, module })));
  const source = await generateEventTypes(await targets(refs), load, { dialect: SUI_DIALECT });
  return [
    "// Regenerate with `bun packages/cli/scripts/sui-framework.ts`; package snapshots are in packages/cli/scripts/sui-framework-abi.",
    source,
  ].join("\n");
}

async function refresh(): Promise<void> {
  const { SuiGrpcClient } = await import("@mysten/sui/grpc");
  const { networks } = await import("@streamd/sui");
  const grpc = new SuiGrpcClient(networks.mainnet);
  for (const short of SNAPSHOTTED) {
    const pkg = (await grpc.movePackageService.getPackage({ packageId: short })).response.package;
    if (pkg === undefined) throw new StreamdError(`Sui mainnet returned no package ${short}`);
    const trimmed = {
      storageId: pkg.storageId,
      originalId: pkg.originalId,
      modules: pkg.modules.map((m) => ({
        name: m.name,
        datatypes: m.datatypes.map(({ name, definingId, abilities, typeParameters, kind, fields, variants }) => ({
          name, definingId, abilities, typeParameters, kind, fields, variants,
        })),
      })),
    };
    writeFileSync(snapshot(short), `${JSON.stringify(trimmed, null, 1)}\n`);
  }
}

if (import.meta.main) {
  if (process.argv.includes("--refresh")) await refresh();
  writeFileSync(SUI_CATALOG_PATH, await generateSuiCatalog());
}
