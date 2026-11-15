import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { StreamdError, requireAddress } from "@streamd/core";
import { spawnError } from "./process.js";

/** The two arguments of `0x1::code::publish_package_txn`, as hex. */
export interface PublishPayload {
  metadataBytes: string;
  moduleBytecode: string[];
}

const MOVE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const HEX_BYTES = /^0x(?:[0-9a-fA-F]{2})+$/;
const OUTPUT_CHARS = 16_000;
// The CLI compiles into the package's own build/, so two compiles of one package must not overlap.
const compiling = new Map<string, Promise<unknown>>();

/**
 * Compiles the package at `packageDir` with the CLI and returns its publish
 * payload. `--skip-fetch-latest-git-deps` keeps a dependency on the CLI's own
 * cached framework rev offline; the CLI writes `build/` inside the package.
 */
export async function buildPublishPayload(
  cli: string,
  packageDir: string,
  namedAddresses: Readonly<Record<string, string>> = {},
): Promise<PublishPayload> {
  const named = Object.entries(namedAddresses).map(([name, value]) => {
    if (!MOVE_IDENTIFIER.test(name)) throw new StreamdError(`Named address ${JSON.stringify(name)} is not a Move identifier`);
    return `${name}=${requireAddress(value)}`;
  });
  const key = resolve(packageDir);
  const previous = compiling.get(key) ?? Promise.resolve();
  const current = previous.catch(() => {}).then(() => compile(cli, packageDir, named));
  compiling.set(key, current);
  try {
    return await current;
  } finally {
    if (compiling.get(key) === current) compiling.delete(key);
  }
}

async function compile(cli: string, packageDir: string, named: string[]): Promise<PublishPayload> {
  const dir = await mkdtemp(join(tmpdir(), "streamd-publish-"));
  try {
    const out = join(dir, "payload.json");
    const args = ["move", "build-publish-payload", "--package-dir", packageDir, "--skip-fetch-latest-git-deps", "--assume-yes", "--json-output-file", out];
    if (named.length > 0) args.push("--named-addresses", named.join(","));
    await run(cli, args, `Compiling ${packageDir} failed`);
    let json: unknown;
    try {
      json = JSON.parse(await readFile(out, "utf8"));
    } catch (err) {
      throw new StreamdError(`The aptos CLI wrote no readable publish payload for ${packageDir}`, err);
    }
    return parsePublishPayload(json);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

/** Checks the CLI's payload JSON is a package publish and takes its arguments. */
export function parsePublishPayload(json: unknown): PublishPayload {
  const payload = json as { function_id?: unknown; args?: unknown } | null;
  if (payload?.function_id !== "0x1::code::publish_package_txn" || !Array.isArray(payload.args) || payload.args.length !== 2) {
    throw new StreamdError("The publish payload is not a 0x1::code::publish_package_txn call with two arguments");
  }
  const [metadata, modules] = payload.args as { type?: unknown; value?: unknown }[];
  if (metadata?.type !== "hex" || typeof metadata.value !== "string" || !HEX_BYTES.test(metadata.value)) {
    throw new StreamdError("The publish payload's package metadata is not hex");
  }
  const code = modules?.value;
  if (modules?.type !== "hex" || !Array.isArray(code) || code.length === 0 || !code.every((m) => typeof m === "string" && HEX_BYTES.test(m))) {
    throw new StreamdError("The publish payload's modules are not a list of hex");
  }
  return { metadataBytes: metadata.value, moduleBytecode: code as string[] };
}

function run(cli: string, args: string[], failure: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(cli, args, { stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    const keep = (chunk: Buffer) => (output = (output + chunk).slice(-OUTPUT_CHARS));
    child.stdout?.on("data", keep);
    child.stderr?.on("data", keep);
    child.on("error", (err) => reject(spawnError(cli, err)));
    child.once("close", (code) => (code === 0 ? resolve() : reject(new StreamdError(`${failure}:\n${output.trim()}`))));
  });
}
