import { readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";

/** The saved cursor, or `undefined` when the file does not exist yet. */
export function readCursor(path: string): string | undefined {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw err;
  }
  const cursor = text.trim();
  if (cursor === "") throw new Error(`cursor file is empty: ${path}`);
  return cursor;
}

// A rename replaces the file whole, so a crash mid-write never leaves a torn cursor.
export function writeCursor(path: string, cursor: string): void {
  const temp = `${path}.${process.pid}.tmp`;
  // Exclusive create: a link planted at the predictable name fails the write
  // instead of redirecting it. A stale temp file from this pid is ours to drop.
  rmSync(temp, { force: true });
  writeFileSync(temp, `${cursor}\n`, { flag: "wx", mode: 0o600 });
  renameSync(temp, path);
}
