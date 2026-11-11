import { once } from "node:events";
import { writeFile } from "node:fs/promises";
import { Aptos, AptosConfig, getAptosFullNode, Network } from "@aptos-labs/ts-sdk";
import { FullnodeScanAdapter, StreamdClient, StreamdError, checkDescriptor, networks } from "@streamd/core";
import { parseModuleAbi } from "./abi.js";
import type { ModuleAbi } from "./abi.js";
import { parseCommand, USAGE, UsageError } from "./args.js";
import type {
  CheckOptions,
  CodegenOptions,
  ForwardOptions,
  ParsedCommand,
  QueryOptions,
  RecordOptions,
  SourceOptions,
  StoreOptions,
  TailOptions,
  TxOptions,
} from "./args.js";
import { runCheck } from "./check.js";
import { generateEventTypes } from "./codegen.js";
import { message } from "./format.js";
import { runForward } from "./forward.js";
import { runQuery } from "./query.js";
import { runRecord, writeAtomically } from "./record.js";
import { openDatabase, runStore } from "./store.js";
import { runTail } from "./tail.js";
import { runTx } from "./tx.js";

// The SDK logs notes for custom networks with console.info, which would corrupt JSON lines on stdout.
console.info = console.error;

const NETWORKS = {
  mainnet: { network: Network.MAINNET },
  testnet: { network: Network.TESTNET },
  devnet: { network: Network.DEVNET },
  "movement-mainnet": networks.movementMainnet,
  "movement-testnet": networks.movementTestnet,
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

function aptosConfig(options: Pick<SourceOptions, "network" | "nodeUrl">): AptosConfig {
  return new AptosConfig({
    ...NETWORKS[options.network],
    ...(options.nodeUrl !== undefined && { fullnode: options.nodeUrl }),
    // The SDK's default HTTP/2 hangs under Bun, with no error.
    ...("Bun" in globalThis && { clientConfig: { http2: false } }),
  });
}

/** A client for `options` and a signal that the first SIGINT or a SIGTERM aborts. */
function setup(options: Pick<SourceOptions, "network" | "nodeUrl">): { client: StreamdClient; controller: AbortController } {
  const controller = new AbortController();
  // The first SIGINT stops cleanly; a second one means stop now.
  process.on("SIGINT", () => (controller.signal.aborted ? process.exit(130) : controller.abort()));
  process.once("SIGTERM", () => controller.abort());
  const client = new StreamdClient({ aptos: aptosConfig(options) });
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

/** Writes to stdout with backpressure; a closed pipe (EPIPE, as when piped into `head`) aborts quietly. */
function stdout(controller: AbortController) {
  const { signal } = controller;
  let failure: Error | undefined;
  process.stdout.on("error", (err: NodeJS.ErrnoException) => {
    if (err.code !== "EPIPE") failure ??= err;
    controller.abort();
  });
  return {
    write: async (line: string) => {
      if (!process.stdout.write(line)) await once(process.stdout, "drain", { signal });
    },
    /** `code`, or 1 with a message if stdout failed for any reason but a closed pipe. */
    exitCode(code: number): number {
      if (failure === undefined) return code;
      process.stderr.write(`streamd: ${failure.message}\n`);
      return 1;
    },
  };
}

async function tail(options: TailOptions): Promise<number> {
  const { client, controller } = setup(options);
  const out = stdout(controller);
  try {
    const code = await runTail(options, {
      client,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

async function query(options: QueryOptions): Promise<number> {
  const { client, controller } = setup(options);
  const out = stdout(controller);
  try {
    const code = await runQuery(options, {
      client,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

async function tx(options: TxOptions): Promise<number> {
  const { client, controller } = setup(options);
  const out = stdout(controller);
  try {
    const code = await runTx(options, {
      client,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

function record(options: RecordOptions): Promise<number> {
  const controller = new AbortController();
  process.once("SIGINT", () => controller.abort());
  process.once("SIGTERM", () => controller.abort());
  return runRecord(options, {
    adapter: new FullnodeScanAdapter(new Aptos(aptosConfig(options))),
    write: (text) =>
      options.out === undefined ? void process.stdout.write(text) : writeAtomically(options.out, text),
    error: (text) => process.stderr.write(`${text}\n`),
    signal: controller.signal,
  });
}

function check(options: CheckOptions): Promise<number> {
  const aptos = new Aptos(aptosConfig(options));
  return runCheck(options, {
    check: (descriptor) => checkDescriptor(aptos, descriptor),
    write: (line) => process.stdout.write(line),
    error: (text) => process.stderr.write(`${text}\n`),
  });
}

async function codegen(options: CodegenOptions): Promise<number> {
  const config = aptosConfig(options);
  // Both parts were validated as an address and an identifier before reaching the path.
  const load = async (address: string, module: string): Promise<ModuleAbi> => {
    let data: object;
    try {
      ({ data } = await getAptosFullNode<{}, object>({
        aptosConfig: config,
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
  const list = async (address: string): Promise<ModuleAbi[]> => {
    const abis: ModuleAbi[] = [];
    let start: string | undefined;
    do {
      let response: { data: unknown[]; headers: Record<string, unknown> };
      try {
        response = await getAptosFullNode<{}, unknown[]>({
          aptosConfig: config,
          originMethod: "codegen",
          path: `accounts/${address}/modules`,
          params: { limit: 100, ...(start === undefined ? {} : { start }) },
        });
      } catch (err) {
        throw new StreamdError(`Fullnode request for the modules of ${address} failed: ${message(err)}`);
      }
      if (!Array.isArray(response.data)) throw new StreamdError(`Malformed module list for ${address}`);
      abis.push(...response.data.map(parseModuleAbi));
      const next = response.headers["x-aptos-cursor"];
      start = typeof next === "string" && next !== "" && next !== start ? next : undefined;
    } while (start !== undefined);
    return abis;
  };
  const source = await generateEventTypes(options.modules, load, list);
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
  if (parsed.command === "check") return check(parsed.options);
  if (parsed.command === "record") return record(parsed.options);
  if (parsed.command === "query") return query(parsed.options);
  if (parsed.command === "tx") return tx(parsed.options);
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
