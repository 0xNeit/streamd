import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { once } from "node:events";
import { existsSync, readFileSync } from "node:fs";
import { constants as osConstants } from "node:os";
import { writeFile } from "node:fs/promises";
import { Aptos } from "@aptos-labs/ts-sdk";
import { AptosClient, listEventHandles } from "@streamd/aptos";
import { parseCommand, USAGE, UsageError } from "./args.js";
import type {
  ActivityCommandOptions,
  AlertOptions,
  BalancesCommandOptions,
  CheckOptions,
  CodegenOptions,
  CompareOptions,
  CoverageOptions,
  Endpoint,
  ExplainCommandOptions,
  ForwardOptions,
  HoldersCommandOptions,
  MonitorCommandOptions,
  PositionsCommandOptions,
  ProvenanceCommandOptions,
  HandlesOptions,
  InspectOptions,
  MonitorOptions,
  NameOptions,
  ParsedCommand,
  QueryOptions,
  RecordOptions,
  SalesCommandOptions,
  ServeOptions,
  StoreAdminOptions,
  StoreOptions,
  StoreRecordsOptions,
  SwapsCommandOptions,
  TailOptions,
  TraceCommandOptions,
  TransfersOptions,
  TxOptions,
  UpgradesCommandOptions,
  WatchCommandOptions,
} from "./args.js";
import { runAlert } from "./alert.js";
import { parseRules } from "./alert-rules.js";
import type { AlertRule } from "./alert-rules.js";
import { runBalances } from "./balances.js";
import { openCache } from "./cache.js";
import type { PageCache } from "./cache.js";
import { aptosCodegenLoader, aptosConfig, chainFor, resourceReader } from "./chain.js";
import { runCheck } from "./check.js";
import { runCompare } from "./compare.js";
import { runCoverage } from "./coverage.js";
import { checkGenerated } from "./codegen-check.js";
import { runCodegenWatch } from "./codegen-watch.js";
import { SUI_DIALECT, suiCodegen } from "./sui-abi.js";
import { loadSui } from "./sui.js";
import { generateEventTypes } from "@streamd/codegen";
import type { SuiNetworkPreset } from "@streamd/sui";
import { startLocalnet } from "@streamd/testing";
import { message } from "./format.js";
import { runExplain } from "./explain.js";
import { runForward } from "./forward.js";
import { runHandles } from "./handles.js";
import { runInspect } from "./inspect.js";
import { runMonitor, serveMetrics } from "./monitor.js";
import { runName } from "./name.js";
import { runQuery } from "./query.js";
import { runRecord, writeAtomically } from "./record.js";
import { runActivity, runProvenance, runSales, runSwaps, runUpgrades } from "./records.js";
import type { RecordsIO } from "./records.js";
import { runServe } from "./serve.js";
import { sqlTables } from "./sql.js";
import { RequestStats, onStatsSignal } from "./stats.js";
import { connectPostgres, openDatabase, runStore, runStoreAdmin, runStoreRecords } from "./store.js";
import { runTail } from "./tail.js";
import { runHolders } from "./holders.js";
import { runPositions } from "./positions.js";
import { runMonitorState } from "./monitors.js";
import { runTrace } from "./trace.js";
import { runTransfers } from "./transfers.js";
import { runTx } from "./tx.js";
import { runWatch } from "./watch.js";

// The SDK logs notes for custom networks with console.info, which would corrupt JSON lines on stdout.
console.info = console.error;

/** Set for the whole run when `--stats` is given. */
let stats: RequestStats | undefined;

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
async function setup(endpoint: Endpoint, cache?: PageCache, counts = stats) {
  const controller = new AbortController();
  // The first SIGINT stops cleanly; a second one means stop now.
  process.on("SIGINT", () => (controller.signal.aborted ? process.exit(130) : controller.abort()));
  process.once("SIGTERM", () => controller.abort());
  const chain = await chainFor(endpoint, cache, counts);
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

async function alert(options: AlertOptions): Promise<number> {
  let text: string;
  let rules: AlertRule[];
  try {
    text = readFileSync(options.rules, "utf8");
  } catch (err) {
    process.stderr.write(`streamd: cannot read rules file: ${message(err)}\n`);
    return 1;
  }
  try {
    rules = parseRules(text, options.rules, options.chain === "sui");
  } catch (err) {
    process.stderr.write(`streamd: ${message(err)}\n`);
    return 1;
  }
  const { client, controller } = await setup(options);
  try {
    return await runAlert(options, rules, {
      client,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
  } finally {
    client.destroy();
  }
}

async function serve(options: ServeOptions): Promise<number> {
  // Always counted, for /metrics; the --stats counts when given, so both report the same calls.
  const counts = stats ?? new RequestStats();
  const { chain, client, controller } = await setup(options, undefined, counts);
  try {
    return await runServe(options, {
      client,
      adapter: chain.adapter(),
      metrics: () => counts.sources(),
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
  } finally {
    client.destroy();
  }
}

async function store<O extends StoreOptions | StoreRecordsOptions>(
  options: O,
  run: (options: O, io: Parameters<typeof runStore>[1]) => Promise<number>,
): Promise<number> {
  const { client, controller } = await setup(options);
  try {
    return await run(options, {
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

async function storeAdmin(options: StoreAdminOptions): Promise<number> {
  const controller = new AbortController();
  const out = stdout(controller);
  const code = await runStoreAdmin(options, {
    // Upkeep of a file that is not there would only create an empty one.
    open: (path) => {
      if (!existsSync(path)) throw new Error("no such file");
      return openDatabase(path);
    },
    connect: connectPostgres,
    write: out.write,
    error: (message) => process.stderr.write(`${message}\n`),
    signal: controller.signal,
  });
  return out.exitCode(code);
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

async function explain(options: ExplainCommandOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    // The parser refuses --chain sui, so this is always an AptosClient.
    if (!(client instanceof AptosClient)) throw new Error("explain reads Aptos only");
    const code = await runExplain(options, {
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

async function records<O extends SwapsCommandOptions | SalesCommandOptions | UpgradesCommandOptions>(
  options: O,
  run: (options: O, io: RecordsIO<AptosClient>) => Promise<number>,
): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    // The parser refuses --chain sui, so this is always an AptosClient.
    if (!(client instanceof AptosClient)) throw new Error("swaps, sales and upgrades read Aptos only");
    const code = await run(options, {
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

/** `activity` and `trace`: an `AptosClient` whose account reads go to the Indexer. */
async function indexed<O extends ActivityCommandOptions | TraceCommandOptions>(
  options: O,
  run: (options: O, io: { client: AptosClient; write(line: string): Promise<void>; error(message: string): void; signal: AbortSignal }) => Promise<number>,
): Promise<number> {
  const { chain, client: plain, controller } = await setup(options);
  plain.destroy();
  let client: AptosClient;
  try {
    // The parser refuses --chain sui, so the chain is Aptos's, which has indexed().
    client = chain.indexed!(options.indexer);
  } catch (err) {
    process.stderr.write(`streamd: ${message(err)}\n`);
    return 1;
  }
  const out = stdout(controller);
  try {
    const code = await run(options, {
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

async function holders(options: HoldersCommandOptions): Promise<number> {
  const { chain, client, controller } = await setup(options);
  let indexer;
  try {
    // The parser refuses --chain sui, so the chain is Aptos's, which has indexer().
    indexer = chain.indexer!(options.indexer);
  } catch (err) {
    client.destroy();
    process.stderr.write(`streamd: ${message(err)}\n`);
    return 1;
  }
  const out = stdout(controller);
  try {
    const code = await runHolders(options, {
      client: client as AptosClient,
      indexer,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      summary: (line) => process.stderr.write(`${line}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

async function provenance(options: ProvenanceCommandOptions): Promise<number> {
  const { chain, client, controller } = await setup(options);
  let indexer;
  try {
    // The parser refuses --chain sui, so the chain is Aptos's, which has indexer().
    indexer = chain.indexer!(options.indexer);
  } catch (err) {
    client.destroy();
    process.stderr.write(`streamd: ${message(err)}\n`);
    return 1;
  }
  const out = stdout(controller);
  try {
    const code = await runProvenance(options, {
      // The parser refuses --chain sui, so this is always an AptosClient.
      client: client as AptosClient,
      indexer,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

async function positions(options: PositionsCommandOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runPositions(options, {
      // The parser refuses --chain sui, so this is always an AptosClient.
      client: client as AptosClient,
      write: out.write,
      error: (message) => process.stderr.write(`${message}\n`),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    client.destroy();
  }
}

async function monitorState(options: MonitorCommandOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runMonitorState(options, {
      // The parser refuses --chain sui, so this is always an AptosClient.
      client: client as AptosClient,
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

async function inspect(options: InspectOptions): Promise<number> {
  const { client, controller } = await setup(options);
  const out = stdout(controller);
  try {
    const code = await runInspect(options, {
      client,
      read: resourceReader(options, options.address, options.resourceType),
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
    adapter: (await chainFor(options, cache, stats)).adapter(),
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

async function monitor(options: MonitorOptions): Promise<number> {
  const controller = new AbortController();
  process.once("SIGINT", () => controller.abort());
  process.once("SIGTERM", () => controller.abort());
  let latest = "";
  let server: Awaited<ReturnType<typeof serveMetrics>> | undefined;
  if (options.listen !== undefined) {
    const { host, port } = options.listen;
    try {
      server = await serveMetrics(options.listen, () => latest);
    } catch (err) {
      process.stderr.write(`streamd: cannot listen on ${host}:${port}: ${(err as { code?: string }).code ?? message(err)}\n`);
      return 1;
    }
  }
  const out = stdout(controller);
  try {
    const code = await runMonitor(options, {
      nodes: (await chainFor(options)).nodes(options.interval),
      write: out.write,
      publish: (text) => void (latest = text),
      signal: controller.signal,
    });
    return out.exitCode(code);
  } finally {
    server?.closeAllConnections();
    server?.close();
  }
}

async function suiSource(options: CodegenOptions): Promise<string> {
  const [{ SuiGrpcClient }, { networks: suiNetworks }] = await loadSui();
  const preset = suiNetworks[options.network as SuiNetworkPreset["network"]];
  const grpc = new SuiGrpcClient({ ...preset, ...(options.nodeUrl !== undefined && { baseUrl: options.nodeUrl }) });
  const { targets, load } = suiCodegen(grpc.movePackageService);
  return generateEventTypes(await targets(options.modules), load, {
    dialect: SUI_DIALECT,
    validators: options.validators === true,
    arbitraries: options.arbitraries === true,
    ...(options.sql !== undefined && { tables: sqlTables(options.sql) }),
  });
}

async function write(options: CodegenOptions, source: string): Promise<number> {
  if (options.check === true && options.out !== undefined) {
    const drift = await checkGenerated(options.out, source);
    if (drift === undefined) return 0;
    process.stderr.write(`streamd: ${drift}\n`);
    return 1;
  }
  if (options.out === undefined) process.stdout.write(source);
  else await writeFile(options.out, source);
  return 0;
}

async function codegen(options: CodegenOptions): Promise<number> {
  if (options.chain === "sui") return write(options, await suiSource(options));
  const { load, list } = aptosCodegenLoader(aptosConfig(options));
  const generate = () =>
    generateEventTypes(options.modules, load, {
      list,
      validators: options.validators === true,
      arbitraries: options.arbitraries === true,
      ...(options.sql !== undefined && { tables: sqlTables(options.sql) }),
    });
  if (options.watch === true && options.out !== undefined) return codegenWatch({ ...options, out: options.out }, generate);
  return write(options, await generate());
}

async function codegenWatch(options: CodegenOptions & { out: string }, generate: () => Promise<string>): Promise<number> {
  const { client, controller } = await setup(options);
  try {
    // The parser refuses --watch with --chain sui, so this is always an AptosClient.
    if (!(client instanceof AptosClient)) throw new Error("codegen --watch reads Aptos only");
    return await runCodegenWatch(options, {
      client,
      generate,
      report: (line) => process.stderr.write(`${line}\n`),
      signal: controller.signal,
    });
  } finally {
    client.destroy();
  }
}

async function coverage(options: CoverageOptions): Promise<number> {
  const out = stdout(new AbortController());
  const interrupted = new AbortController();
  let child: ChildProcess | undefined;
  // The first signal is waited out so the localnet is stopped rather than left running. Ctrl-C
  // already reaches the command through the process group, so only SIGTERM and SIGHUP are
  // passed on. A second signal kills the command, or with none running exits, where the
  // testing kit's exit hook kills the node.
  const interrupt = (signal: NodeJS.Signals) => {
    if (!interrupted.signal.aborted) {
      interrupted.abort();
      if (signal !== "SIGINT") child?.kill(signal);
    } else if (child !== undefined) child.kill("SIGKILL");
    else process.exit(130);
  };
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as const) process.on(signal, interrupt);
  const code = await runCoverage(options, {
    start: () => startLocalnet(),
    // The command's stdout goes to stderr, so it cannot mix with the report.
    run: (command, env) =>
      new Promise((resolve, reject) => {
        child = spawn(command[0]!, command.slice(1), { stdio: ["inherit", 2, "inherit"], env: { ...process.env, ...env } });
        child.once("error", reject);
        child.once("close", (exit, signal) => {
          child = undefined;
          resolve(exit ?? 128 + (signal === null ? 0 : osConstants.signals[signal]));
        });
      }),
    write: out.write,
    error: (message) => process.stderr.write(`${message}\n`),
    signal: interrupted.signal,
  });
  return out.exitCode(code);
}

async function run(parsed: Exclude<ParsedCommand, { command: "help" }>): Promise<number> {
  if (parsed.command === "codegen") return codegen(parsed.options);
  if (parsed.command === "coverage") return coverage(parsed.options);
  if (parsed.command === "compare") return compare(parsed.options);
  if (parsed.command === "monitor") return monitor(parsed.options);
  if (parsed.command === "serve") return serve(parsed.options);
  if (parsed.command === "store") return store(parsed.options, runStore);
  if (parsed.command === "store-records") return store(parsed.options, runStoreRecords);
  if (parsed.command === "store-admin") return storeAdmin(parsed.options);
  if (parsed.command === "alert") return alert(parsed.options);
  if (parsed.command === "check") return check(parsed.options);
  if (parsed.command === "record") return withCache(parsed.options, (cache) => record(parsed.options, cache));
  if (parsed.command === "query") return withCache(parsed.options, (cache) => query(parsed.options, cache));
  if (parsed.command === "tx") return tx(parsed.options);
  if (parsed.command === "explain") return explain(parsed.options);
  if (parsed.command === "transfers") return transfers(parsed.options);
  if (parsed.command === "swaps") return records(parsed.options, runSwaps);
  if (parsed.command === "sales") return records(parsed.options, runSales);
  if (parsed.command === "activity") return indexed(parsed.options, runActivity);
  if (parsed.command === "upgrades") return records(parsed.options, runUpgrades);
  if (parsed.command === "trace") return indexed(parsed.options, runTrace);
  if (parsed.command === "holders") return holders(parsed.options);
  if (parsed.command === "positions") return positions(parsed.options);
  if (parsed.command === "monitor-state") return monitorState(parsed.options);
  if (parsed.command === "provenance") return provenance(parsed.options);
  if (parsed.command === "balances") return balances(parsed.options);
  if (parsed.command === "watch") return watch(parsed.options);
  if (parsed.command === "inspect") return inspect(parsed.options);
  if (parsed.command === "handles") return handles(parsed.options);
  if (parsed.command === "name") return name(parsed.options);
  return parsed.command === "tail" ? tail(parsed.options) : forward(parsed.options);
}

async function main(argv: string[]): Promise<number> {
  const parsed = parse(argv);
  if (parsed === undefined) return 2;
  if (parsed.command === "help") {
    process.stdout.write(USAGE);
    return 0;
  }
  if (!("stats" in parsed.options) || parsed.options.stats !== true) return run(parsed);
  const counted = (stats = new RequestStats());
  const stop = onStatsSignal(() => process.stderr.write(counted.line()));
  try {
    return await run(parsed);
  } finally {
    stop();
    // However the command ended; flushed first, since exit() does not wait for stderr.
    await new Promise((resolve) => process.stderr.write(counted.line(), resolve));
  }
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
