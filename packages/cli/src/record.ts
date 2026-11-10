import { renameSync, rmSync, writeFileSync } from "node:fs";
import { StreamdClient, recordingAdapter } from "@streamd/core";
import type { EventAdapter, EventFilter } from "@streamd/core";
import type { RecordOptions } from "./args.js";
import { message } from "./format.js";

export interface RecordIO {
  adapter: EventAdapter;
  /** Receives the whole recording, once, only after the query has succeeded. */
  write(text: string): void;
  /** Reports one diagnostic message, without a trailing newline. */
  error(message: string): void;
  signal: AbortSignal;
}

const RETRIES = 5;

/** Writes `path` by rename, so a reader never sees half a recording. */
export function writeAtomically(path: string, text: string): void {
  const temporary = `${path}.${process.pid}.tmp`;
  try {
    writeFileSync(temporary, text);
    renameSync(temporary, path);
  } catch (err) {
    rmSync(temporary, { force: true });
    throw err;
  }
}

/**
 * Runs the query through `recordingAdapter` and writes the recording, and
 * resolves to the process exit code: 0 once written, 1 on a failure, 130 if
 * interrupted. Nothing is written unless the whole query succeeds.
 */
export async function runRecord(options: RecordOptions, io: RecordIO): Promise<number> {
  const recorder = recordingAdapter(io.adapter);
  const client = new StreamdClient({ adapter: recorder });
  const { fromVersion, toVersion, limit } = options;
  const filter: EventFilter = {
    fromVersion,
    toVersion,
    ...(limit !== undefined && { limit }),
  };
  try {
    const events = await client.getEvents(options.descriptor, { ...filter, retries: RETRIES, signal: io.signal });
    const recording = recorder.toJSON();
    io.write(`${JSON.stringify(recording)}\n`);
    const shown = limit === undefined ? "" : `, limit: ${limit}`;
    io.error(
      `streamd: recorded ${events.length} events in ${recording.calls.length} calls; ` +
        `replay with { fromVersion: ${fromVersion}n, toVersion: ${toVersion}n${shown} }`,
    );
    return 0;
  } catch (err) {
    if (io.signal.aborted) return 130;
    io.error(`streamd: ${message(err)}`);
    return 1;
  } finally {
    client.destroy();
  }
}
