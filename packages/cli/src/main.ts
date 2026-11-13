import { once } from "node:events";
import { writeFile } from "node:fs/promises";
import { Aptos, getAptosFullNode } from "@aptos-labs/ts-sdk";
import { listEventHandles } from "@streamd/aptos";
import { StreamdError } from "@streamd/core";
import { parseModuleAbi } from "./abi.js";
import type { ModuleAbi } from "./abi.js";
import { parseCommand, USAGE, UsageError } from "./args.js";
import type {
  BalancesCommandOptions,
  CheckOptions,
  CodegenOptions,
  CompareOptions,
  Endpoint,
  ForwardOptions,
  HandlesOptions,
  NameOptions,
  ParsedCommand,
  QueryOptions,
  RecordOptions,
  StoreOptions,
  TailOptions,
  TransfersOptions,
  TxOptions,
  WatchCommandOptions,
} from "./args.js";
import { runBalances } from "./balances.js";
import { openCache } from "./cache.js";
import type { PageCache } from "./cache.js";
import { aptosConfig, chainFor } from "./chain.js";
import { runCheck } from "./check.js";
import { runCompare } from "./compare.js";
import { generateEventTypes } from "./codegen.js";
import { SUI_DIALECT, suiCodegen } from "./sui-abi.js";
import { loadSui } from "./sui.js";
import type { SuiNetworkPreset } from "@streamd/sui";
import { message } from "./format.js";
import { runForward } from "./forward.js";
import { runHandles } from "./handles.js";
import { runName } from "./name.js";
import { runQuery } from "./query.js";
import { runRecord, writeAtomically } from "./record.js";
import { connectPostgres, openDatabase, runStore } from "./store.js";
import { runTail } from "./tail.js";
import { runTransfers } from "./transfers.js";
import { runTx } from "./tx.js";
import { runWatch } from "./watch.js";

// The SDK logs notes for custom networks with console.info, which would corrupt JSON lines on stdout.
console.info = console.error;

function parse(argv: string[]): ParsedCommand | undefined {
  try {
    return parseCommand(argv);
  } catch (err) {
    if (!(err instanceof UsageError)) throw err;
    process.stderr.write(`streamd: ${err.message}\nRun 'streamd --help' for usage.\n`);
    return undefined;
  }
}

/** The endpoint's chain, a client over it, and a signal that the first SIGINT or a SIGTERM aborts. */
async function setup(endpoint: Endpoint, cache?: PageCache) {
  const controller = new AbortController();
  // The first SIGINT stops cleanly; a second one means stop now.
  process.on("SIGINT", () => (controller.signal.aborted ? process.exit(130) : controller.abort()));
  process.once("SIGTERM", () => controller.abort());
  const chain = await chainFor(endpoint, cache);
  return { chain, client: chain.client(), controller };
}

async function forward(options: ForwardOptions): Promise<number> {
  const { chain, client, controller } = await setup(options);
  try {
    return await runForward(options, {
      client,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
      headVersion: () => chain.adapter().getHeadVersion(),
    });
  } finally {
    client.destroy();
  }
}

async function store(options: StoreOptions): Promise<number> {
  const { client, controller } = await setup(options);
  try {
    return await runStore(options, {
      client,
      open: openDatabase,
      connect: connectPostgres,
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
  const { client, controller } = await setup(options);
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

/** Runs `run` over the `--cache` file when one is given, exiting 1 if it cannot be opened. */
async function withCache(options: QueryOptions | RecordOptions, run: (cache?: PageCache) => Promise<number>): Promise<number> {
  if (options.cache === undefined) return run();
  let cache: PageCache;
  try {
    cache = await openCache(options.cache, options, openDatabase, (text) => process.stderr.write(`${text}\n`));
  } catch (err) {
    process.stderr.write(`streamd: cannot open ${options.cache}: ${message(err)}\n`);
    return 1;
  }
  try {
    return await run(cache);
  } finally {
    cache.close();
  }
}

async function query(options: QueryOptions, cache?: PageCache): Promise<number> {
  const { client, controller } = await setup(options, cache);
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
  const { client, controller } = await setup(options);
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

async function transfers(options: TransfersOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runTransfers(options, {
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

async function balances(options: BalancesCommandOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runBalances(options, {
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

async function watch(options: WatchCommandOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runWatch(options, {
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

async function handles(options: HandlesOptions): Promise<number> {
  const controller = new AbortController();
  const out = stdout(controller);
  const aptos = new Aptos(aptosConfig(options));
  const code = await runHandles(options, {
    list: (address) => listEventHandles(aptos, address),
    write: out.write,
    error: (message) => process.stderr.write(`${message}\n`),
    signal: controller.signal,
  });
  return out.exitCode(code);
}

async function name(options: NameOptions): Promise<number> {
  const controller = new AbortController();
  const out = stdout(controller);
  const chain = await chainFor(options);
  const code = await runName(options, {
    resolveName: (value) => chain.resolveName(value),
    primaryName: (address) => chain.primaryName(address),
    write: out.write,
    error: (message) => process.stderr.write(`${message}\n`),
    signal: controller.signal,
  });
  return out.exitCode(code);
}

async function record(options: RecordOptions, cache?: PageCache): Promise<number> {
  const controller = new AbortController();
  process.once("SIGINT", () => controller.abort());
  process.once("SIGTERM", () => controller.abort());
  return runRecord(options, {
    adapter: (await chainFor(options, cache)).adapter(),
    write: (text) =>
      options.out === undefined ? void process.stdout.write(text) : writeAtomically(options.out, text),
    error: (text) => process.stderr.write(`${text}\n`),
    signal: controller.signal,
  });
}

async function check(options: CheckOptions): Promise<number> {
  const chain = await chainFor(options);
  return runCheck(options, {
    check: (descriptor) => chain.check(descriptor),
    write: (line) => process.stdout.write(line),
    error: (text) => process.stderr.write(`${text}\n`),
  });
}

async function compare(options: CompareOptions): Promise<number> {
  const chain = await chainFor(options);
  return runCompare(options, {
    a: chain.adapter(),
    // A custom node, so it gets no API key.
    b: chain.adapter(options.against),
    write: (line) => process.stdout.write(line),
    error: (text) => process.stderr.write(`${text}\n`),
  });
}

async function suiSource(options: CodegenOptions): Promise<string> {
  const [{ SuiGrpcClient }, { networks: suiNetworks }] = await loadSui();
  const preset = suiNetworks[options.network as SuiNetworkPreset["network"]];
  const grpc = new SuiGrpcClient({ ...preset, ...(options.nodeUrl !== undefined && { baseUrl: options.nodeUrl }) });
  const { targets, load } = suiCodegen(grpc.movePackageService);
  return generateEventTypes(await targets(options.modules), load, { dialect: SUI_DIALECT, validators: options.validators === true });
}

async function write(options: CodegenOptions, source: string): Promise<number> {
  if (options.out === undefined) process.stdout.write(source);
  else await writeFile(options.out, source);
  return 0;
}

async function codegen(options: CodegenOptions): Promise<number> {
  if (options.chain === "sui") return write(options, await suiSource(options));
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
  return write(options, await generateEventTypes(options.modules, load, { list, validators: options.validators === true }));
}

async function main(argv: string[]): Promise<number> {
  const parsed = parse(argv);
  if (parsed === undefined) return 2;
  if (parsed.command === "help") {
    process.stdout.write(USAGE);
    return 0;
  }
  if (parsed.command === "codegen") return codegen(parsed.options);
  if (parsed.command === "compare") return compare(parsed.options);
  if (parsed.command === "store") return store(parsed.options);
  if (parsed.command === "check") return check(parsed.options);
  if (parsed.command === "record") return withCache(parsed.options, (cache) => record(parsed.options, cache));
  if (parsed.command === "query") return withCache(parsed.options, (cache) => query(parsed.options, cache));
  if (parsed.command === "tx") return tx(parsed.options);
  if (parsed.command === "transfers") return transfers(parsed.options);
  if (parsed.command === "balances") return balances(parsed.options);
  if (parsed.command === "watch") return watch(parsed.options);
  if (parsed.command === "handles") return handles(parsed.options);
  if (parsed.command === "name") return name(parsed.options);
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
