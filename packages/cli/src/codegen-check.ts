import { readFile } from "node:fs/promises";

const MAX_SHOWN = 120;

const shown = (line: string | undefined) =>
  line === undefined ? "(end of file)" : JSON.stringify(line.length > MAX_SHOWN ? `${line.slice(0, MAX_SHOWN)}...` : line);

/**
 * Compares `generated` byte for byte with the file at `path`, writing nothing.
 * Resolves to `undefined` on a match, else a report naming the file and the first line that differs.
 */
export async function checkGenerated(path: string, generated: string): Promise<string | undefined> {
  let onDisk: Buffer;
  try {
    onDisk = await readFile(path);
  } catch (err) {
    if ((err as { code?: unknown } | null)?.code === "ENOENT") return `${path} does not exist`;
    throw err;
  }
  if (onDisk.equals(Buffer.from(generated, "utf8"))) return undefined;
  const have = onDisk.toString("utf8").split("\n");
  const want = generated.split("\n");
  const at = want.findIndex((line, i) => line !== have[i]);
  const index = at === -1 && have.length > want.length ? want.length : at;
  const report = `${path} differs from the generated output`;
  if (index === -1) return report;
  return `${report} at line ${index + 1}\n  on disk:   ${shown(have[index])}\n  generated: ${shown(want[index])}`;
}
