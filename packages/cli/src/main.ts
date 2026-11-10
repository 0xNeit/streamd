import { once } from "node:events";
import { writeFile } from "node:fs/promises";
import { AptosConfig, getAptosFullNode, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient, StreamdError } from "@streamd/core";
import { parseModuleAbi } from "./abi.js";
import type { ModuleAbi } from "./abi.js";
import { parseCommand, USAGE, UsageError } from "./args.js";
import type {
  CodegenOptions,
  ForwardOptions,
  ParsedCommand,
  SourceOptions,
  StoreOptions,
  TailOptions,
} from "./args.js";
import { generateEventTypes } from "./codegen.js";
import { message } from "./format.js";
import { runForward } from "./forward.js";
import { openDatabase, runStore } from "./store.js";
import { runTail } from "./tail.js";

const NETWORKS = {
  mainnet: Network.MAINNET,
  testnet: Network.TESTNET,
  devnet: Network.DEVNET,
} as const;

function parse(argv: string[]): ParsedCommand | undefined {
  try {
    return parseCommand(argv);
  } catch (err) {
    if (!(err instanceof UsageError)) throw err;
    process.stderr.write(`streamd: ${err.message}\nRun 'streamd --help' for usage.\n`);
    return undefined;
  }
}

/** A client for `options` and a signal that the first SIGINT or a SIGTERM aborts. */
function setup(options: SourceOptions): { client: StreamdClient; controller: AbortController } {
  const controller = new AbortController();
  // The first SIGINT stops cleanly; a second one means stop now.
  process.on("SIGINT", () => (controller.signal.aborted ? process.exit(130) : controller.abort()));
  process.once("SIGTERM", () => controller.abort());
  const client = new StreamdClient({
    aptos: new AptosConfig({ network: NETWORKS[options.network], fullnode: options.nodeUrl }),
  });
  return { client, controller };
}

async function forward(options: ForwardOptions): Promise<number> {
  const { client, controller } = setup(options);
  try {
    return await runForward(options, {
      client,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
  } finally {
    client.destroy();
  }
}

async function store(options: StoreOptions): Promise<number> {
  const { client, controller } = setup(options);
  try {
    return await runStore(options, {
      client,
      open: openDatabase,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
  } finally {
    client.destroy();
  }
}

async function tail(options: TailOptions): Promise<number> {
  const { client, controller } = setup(options);
  const { signal } = controller;
  let stdoutError: Error | undefined;
  // A closed pipe (EPIPE, as when piped into `head`) ends the tail quietly.
  process.stdout.on("error", (err: NodeJS.ErrnoException) => {
    if (err.code !== "EPIPE") stdoutError ??= err;
    controller.abort();
  });

  try {
    const code = await runTail(options, {
      client,
      write: async (line) => {
        if (!process.stdout.write(line)) await once(process.stdout, "drain", { signal });
      },
      error: (message) => process.stderr.write(`${message}\n`),
      signal,
    });
    if (stdoutError === undefined) return code;
    process.stderr.write(`streamd: ${stdoutError.message}\n`);
    return 1;
  } finally {
    client.destroy();
  }
}

async function codegen(options: CodegenOptions): Promise<number> {
  const aptosConfig = new AptosConfig({
    network: NETWORKS[options.network],
    fullnode: options.nodeUrl,
  });
  // Both parts were validated as an address and an identifier before reaching the path.
  const load = async (address: string, module: string): Promise<ModuleAbi> => {
    let data: object;
    try {
      ({ data } = await getAptosFullNode<{}, object>({
        aptosConfig,
        originMethod: "codegen",
        path: `accounts/${address}/module/${module}`,
      }));
    } catch (err) {
      if ((err as { status?: unknown } | null)?.status === 404) {
        throw new StreamdError(`Module ${address}::${module} not found`);
      }
      throw new StreamdError(`Fullnode request for module ${address}::${module} failed: ${message(err)}`);
    }
    return parseModuleAbi(data);
  };
  const source = await generateEventTypes(options.modules, load);
  if (options.out === undefined) process.stdout.write(source);
  else await writeFile(options.out, source);
  return 0;
}

async function main(argv: string[]): Promise<number> {
  const parsed = parse(argv);
  if (parsed === undefined) return 2;
  if (parsed.command === "help") {
    process.stdout.write(USAGE);
    return 0;
  }
  if (parsed.command === "codegen") return codegen(parsed.options);
  if (parsed.command === "store") return store(parsed.options);
  return parsed.command === "tail" ? tail(parsed.options) : forward(parsed.options);
}

// Exit explicitly once stdout has flushed, so an in-flight request does not
// hold the process open after the tail has ended.
function exit(code: number): void {
  if (process.stdout.destroyed || process.stdout.writableEnded) process.exit(code);
  process.stdout.write("", () => process.exit(code));
}

main(process.argv.slice(2)).then(exit, (err: unknown) => {
  process.stderr.write(`streamd: ${err instanceof Error ? err.message : String(err)}\n`);
  exit(1);
});
