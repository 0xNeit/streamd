import { createHash } from "node:crypto";
import type { CachingAdapterOptions } from "@streamd/core";
import { SqlitePageStore } from "@streamd/store-sqlite";
import type { Endpoint } from "./args.js";
import { message } from "./format.js";
import type { OpenDatabase } from "./store.js";

/** What `chainFor` wraps its reads in, and the file to close once the command ends. */
export type PageCache = Required<Pick<CachingAdapterOptions, "store" | "onStoreError">> & { close(): void };

/**
 * Where a cache file keeps one source's pages. A custom node may serve any
 * chain whatever `--network` says, so its URLs are part of the name, sorted
 * because a pool's order does not change what it serves, and hashed because a
 * URL can carry a provider's key. Aptos mainnet's preset archive is the
 * network's own, so only an archive URL is named.
 */
export function cacheNamespace(endpoint: Pick<Endpoint, "network" | "nodeUrl" | "extraNodeUrls" | "archive">): string {
  const nodes = endpoint.nodeUrl === undefined ? [] : [endpoint.nodeUrl, ...(endpoint.extraNodeUrls ?? [])].sort();
  const archive = endpoint.archive === undefined || endpoint.archive === "default" ? [] : [endpoint.archive];
  const urls = [...nodes, ...archive];
  const network = `aptos:${endpoint.network}`;
  return urls.length === 0 ? network : `${network} ${createHash("sha256").update(JSON.stringify(urls)).digest("hex")}`;
}

/**
 * Opens `path` as a page cache for `endpoint`, rejecting when it cannot be
 * opened as SQLite. A failure after that is a miss, reported once.
 */
export async function openCache(
  path: string,
  endpoint: Pick<Endpoint, "network" | "nodeUrl" | "extraNodeUrls" | "archive">,
  open: (path: string) => OpenDatabase | Promise<OpenDatabase>,
  report: (text: string) => void,
): Promise<PageCache> {
  const db = await open(path);
  try {
    // As store does: WAL and a busy timeout let two runs share the file.
    db.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
    const store = new SqlitePageStore(db, { namespace: cacheNamespace(endpoint) });
    let reported = false;
    const onStoreError = (err: unknown) => {
      if (reported) return;
      reported = true;
      report(`streamd: --cache: ${message(err)}; reading from the node`);
    };
    return { store, onStoreError, close: () => db.close?.() };
  } catch (err) {
    db.close?.();
    throw err;
  }
}
