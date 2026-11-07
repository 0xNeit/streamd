import { once } from "node:events";
import { AptosConfig, Network } from "@aptos-labs/ts-sdk";
import { StreamdClient } from "@streamd/core";
import { parseCommand, USAGE, UsageError } from "./args.js";
import type { ForwardOptions, ParsedCommand, SourceOptions, TailOptions } from "./args.js";
import { runForward } from "./forward.js";
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

async function main(argv: string[]): Promise<number> {
  const parsed = parse(argv);
  if (parsed === undefined) return 2;
  if (parsed.command === "help") {
    process.stdout.write(USAGE);
    return 0;
  }
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
