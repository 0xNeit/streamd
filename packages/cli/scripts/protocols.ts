// Writes packages/aptos/src/protocols.ts, the `@streamd/aptos/protocols` catalog, from the
// ABI snapshots in ./protocols-abi, with framework types read from ./framework-abi:
// `bun packages/cli/scripts/protocols.ts`. `--refresh` first rewrites every protocol module
// snapshot the catalog reads from Aptos mainnet, one request at a time, keeping only
// `abi.address`, `abi.name` and `abi.structs`.
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { generateEventTypes, parseModuleAbi } from "@streamd/codegen";
import type { LoadModule } from "@streamd/codegen";
import { normalizeAddress, StreamdError } from "@streamd/core";

const HYPERION = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c";
const THALA_CLAMM = "0x75b4890de3e312d9425408c43d9a9752b64ab3562a30e89a55bdc568c645920";
const THALA_V2 = "0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5";
const CELLANA = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1";
const TAPP = "0x487e905f899ccb6d46fdaec56ba1e0c4cf119862a16c409904b8c78fab1f5e8a";
const AAVE = "0x39ddcd9e1a39fa14f25e3f9ec8a86074d05cc0881cbf667df8a6ee70942016fb";
const ECHELON = "0xc6bc659f1649553c1a3fa05d9727433dc03843baac29473c817d06d39e7621ba";

/**
 * The modules the swap and lending decoders read that declare `#[event]` structs. The coin-generic
 * pools (LiquidSwap, PancakeSwap, ThalaSwap v1) and both marketplaces emit only handle events.
 */
export const PROTOCOL_MODULES: Record<string, string[]> = {
  [HYPERION]: ["pool_v3"],
  [THALA_CLAMM]: ["pool"],
  [THALA_V2]: ["pool"],
  [CELLANA]: ["liquidity_pool"],
  [TAPP]: ["router"],
  [AAVE]: ["borrow_logic", "emode_logic", "events", "liquidation_logic", "supply_logic"],
  [ECHELON]: ["lending"],
};

export const PROTOCOLS_PATH = fileURLToPath(new URL("../../aptos/src/protocols.ts", import.meta.url));

const SNAPSHOTS = new URL("./protocols-abi/", import.meta.url);
const FRAMEWORK = new URL("./framework-abi/", import.meta.url);
const FROM = 'import { defineEvent, defineEvents } from "@streamd/core";';
const NODE = "https://api.mainnet.aptoslabs.com/v1";
const FRAMEWORK_ADDRESSES = new Set(["0x1", "0x3", "0x4"].map((a) => normalizeAddress(a)));

const keyOf = (address: string, module: string) => `${normalizeAddress(address) ?? address}::${module}`;

function snapshots(dir: URL): Map<string, URL> {
  const files = new Map<string, URL>();
  for (const file of readdirSync(dir)) {
    const match = /^(0x[0-9a-f]+)-(\w+)\.json$/.exec(file);
    if (match !== null) files.set(keyOf(match[1]!, match[2]!), new URL(file, dir));
  }
  return files;
}

const offline: LoadModule = async (address, module) => {
  const file = snapshots(SNAPSHOTS).get(keyOf(address, module)) ?? snapshots(FRAMEWORK).get(keyOf(address, module));
  if (file === undefined) throw new StreamdError(`No snapshot for ${address}::${module}; run with --refresh`);
  return parseModuleAbi(JSON.parse(readFileSync(file, "utf8")));
};

const generate = (load: LoadModule) =>
  generateEventTypes(
    Object.entries(PROTOCOL_MODULES).flatMap(([address, modules]) => modules.map((module) => ({ address, module }))),
    load,
    { handles: false },
  );

/** The catalog source, generated offline from the committed snapshots. */
export async function generateProtocols(): Promise<string> {
  const source = await generate(offline);
  if (!source.includes(FROM)) throw new StreamdError("Generated source lacks the expected import");
  return [
    "// Regenerate with `bun packages/cli/scripts/protocols.ts`; ABI snapshots are in packages/cli/scripts/protocols-abi.",
    source,
  ].join("\n");
}

// Anonymous mainnet reads share one rate limit per IP, so reads run one at a time.
let queue: Promise<unknown> = Promise.resolve();

const fetched: LoadModule = (address, module) => {
  if (FRAMEWORK_ADDRESSES.has(normalizeAddress(address))) return offline(address, module);
  const read = queue.then(async () => {
    const response = await fetch(`${NODE}/accounts/${address}/module/${module}`, { signal: AbortSignal.timeout(15_000) });
    if (!response.ok) throw new StreamdError(`Mainnet answered ${response.status} for ${address}::${module}`);
    const { abi } = (await response.json()) as { abi: { address: string; name: string; structs: unknown[] } };
    const trimmed = { abi: { address: abi.address, name: abi.name, structs: abi.structs } };
    writeFileSync(new URL(`${address}-${module}.json`, SNAPSHOTS), `${JSON.stringify(trimmed, null, 1)}\n`);
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    return parseModuleAbi(trimmed);
  });
  queue = read.catch(() => {});
  return read;
};

if (import.meta.main) {
  if (process.argv.includes("--refresh")) await generate(fetched);
  writeFileSync(PROTOCOLS_PATH, await generateProtocols());
}
