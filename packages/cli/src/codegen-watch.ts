import type { AbiChanges, AptosClient, PackageUpgrade } from "@streamd/aptos";
import { normalizeAddress } from "@streamd/core";
import type { StreamdEvent, Subscription } from "@streamd/core";
import type { CodegenOptions } from "./args.js";
import { checkGenerated } from "./codegen-check.js";
import { message } from "./format.js";
import { writeAtomically } from "./record.js";

export interface CodegenWatchIO {
  client: Pick<AptosClient, "upgrades">;
  /** Generates the whole file from the ABIs at the head. */
  generate(): Promise<string>;
  /** Writes one stderr line, without a trailing newline. */
  report(line: string): void;
  /** Aborting ends the command with exit code 0. */
  signal: AbortSignal;
}

const names = <T extends { name: string }>(changes: AbiChanges<T>) => ({
  added: changes.added.map((s) => s.name),
  changed: changes.changed.map((c) => c.after.name),
  removed: changes.removed.map((s) => s.name),
});

/** The stderr line for one rewrite a publish caused, the ABI changes by name. */
export function rewriteLine(out: string, { version, data }: StreamdEvent<PackageUpgrade>): string {
  return JSON.stringify({
    rewrite: {
      out,
      version: version.toString(),
      address: data.address,
      isUpgrade: data.isUpgrade,
      events: names(data.events),
      entryFunctions: names(data.entryFunctions),
      views: names(data.views),
    },
  });
}

/**
 * Writes `options.out` once every address's publish walk has caught up, then
 * regenerates on each publish and rewrites the file atomically when its bytes
 * change. Resolves to 0 on the signal, 1 when a walk gives up or a generation
 * or write fails.
 */
export function runCodegenWatch(options: CodegenOptions & { out: string }, io: CodegenWatchIO): Promise<number> {
  const { out } = options;
  if (io.signal.aborted) return Promise.resolve(0);
  const addresses = [...new Set(options.modules.map((m) => normalizeAddress(m.address) ?? m.address))];
  return new Promise<number>((resolve) => {
    const subs: Subscription[] = [];
    let done = false;
    const finish = (code: number) => {
      if (done) return;
      done = true;
      io.signal.removeEventListener("abort", aborted);
      for (const sub of subs) sub.unsubscribe();
      resolve(code);
    };
    const aborted = () => finish(0);
    io.signal.addEventListener("abort", aborted);
    const fail = (err: unknown) => {
      if (done) return;
      io.report(`streamd: ${message(err)}`);
      finish(1);
    };
    /** True when the file was rewritten. */
    const regenerate = async (): Promise<boolean> => {
      const source = await io.generate();
      if (done || (await checkGenerated(out, source)) === undefined) return false;
      writeAtomically(out, source);
      return true;
    };
    // One regeneration at a time, so two publishes in one page never race on the file.
    let queue: Promise<unknown> = Promise.resolve();
    const enqueue = (task: () => Promise<void>) => (queue = queue.then(task).catch(fail));
    const publish = (record: StreamdEvent<PackageUpgrade>) =>
      enqueue(async () => {
        if (await regenerate()) io.report(rewriteLine(out, record));
      });
    try {
      for (const address of addresses) {
        const sub = io.client.upgrades(address, publish, {
          signal: io.signal,
          onError: (err) => {
            io.report(`streamd: ${message(err)}`);
            if (!sub.active) finish(1);
          },
        });
        subs.push(sub);
      }
    } catch (err) {
      return fail(err);
    }
    // Written only once every walk holds a position, so no publish after the read is missed.
    Promise.all(subs.map((s) => s.caughtUp)).then(
      () => enqueue(async () => void (await regenerate())),
      () => finish(1),
    );
  });
}
