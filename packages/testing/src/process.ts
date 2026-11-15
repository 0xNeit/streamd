import { spawn, type ChildProcess } from "node:child_process";
import { StreamdError, StreamdTimeoutError, sleep } from "@streamd/core";

const OUTPUT_LINES = 40;
const READY_POLL = 250;
const STOP_GRACE = 20_000;

/** A running `aptos node run-localnet`. */
export interface NodeProcess {
  /** SIGINT, then SIGKILL after the grace period; resolves once the process has exited. */
  stop(): Promise<void>;
}

/** Spawns `cli` with `args` and resolves once `readyUrl` reports nothing left to start. */
export async function startNode(
  cli: string,
  args: string[],
  readyUrl: string,
  readyTimeout: number,
  stopGrace = STOP_GRACE,
): Promise<NodeProcess> {
  // Anything already answering would pass the ready check in place of this node.
  if (await isReady(readyUrl, AbortSignal.timeout(2000))) {
    throw new StreamdError(`Something is already serving a localnet ready check at ${readyUrl}`);
  }
  const child = spawn(cli, args, { stdio: ["ignore", "pipe", "pipe"] });
  const output: string[] = [];
  const keep = (chunk: Buffer) => {
    output.push(...chunk.toString().split("\n").filter((line) => line.trim() !== ""));
    output.splice(0, output.length - OUTPUT_LINES);
  };
  child.stdout?.on("data", keep);
  child.stderr?.on("data", keep);

  // Best effort for a process that exits without stop(); it does not run when
  // the process is killed by a signal, and test runners may not exit normally.
  const killOnExit = () => child.kill("SIGKILL");
  process.on("exit", killOnExit);
  const exited = new Promise<void>((resolve) => child.once("close", () => resolve()));
  exited.then(() => process.off("exit", killOnExit));

  const failed = new Promise<never>((_, reject) => {
    child.on("error", (err) => reject(spawnError(cli, err)));
    exited.then(() => reject(new StreamdError(`The localnet exited before it was ready:\n${output.join("\n")}`)));
  });

  const stop = async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGINT");
    const timer = new AbortController();
    await Promise.race([exited, sleep(stopGrace, timer.signal).catch(() => {})]);
    timer.abort();
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await exited;
    }
  };

  const deadline = new AbortController();
  try {
    await Promise.race([
      failed,
      waitReady(readyUrl, deadline.signal),
      sleep(readyTimeout, deadline.signal).then(() => {
        throw new StreamdTimeoutError(`The localnet was not ready after ${readyTimeout}ms:\n${output.join("\n")}`, readyTimeout);
      }),
    ]);
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new StreamdError(`The localnet exited before it was ready:\n${output.join("\n")}`);
    }
  } catch (err) {
    await kill(child, exited);
    throw err;
  } finally {
    deadline.abort();
  }
  return { stop };
}

/** The error for a CLI that could not be spawned, naming the install when it is missing. */
export function spawnError(cli: string, err: NodeJS.ErrnoException): StreamdError {
  return err.code === "ENOENT"
    ? new StreamdError(`The aptos CLI was not found at "${cli}"; install it, or pass its path as \`cli\``, err)
    : new StreamdError(`Could not start the aptos CLI: ${err.message}`, err);
}

async function kill(child: ChildProcess, exited: Promise<void>): Promise<void> {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGKILL");
  await exited;
}

async function waitReady(url: string, signal: AbortSignal): Promise<void> {
  while (!signal.aborted) {
    if (await isReady(url, signal)) return;
    await sleep(READY_POLL, signal).catch(() => {});
  }
}

async function isReady(url: string, signal: AbortSignal): Promise<boolean> {
  try {
    const response = await fetch(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(2000)]) });
    const body = (await response.json()) as { not_ready?: unknown };
    return Array.isArray(body?.not_ready) && body.not_ready.length === 0;
  } catch {
    return false;
  }
}
