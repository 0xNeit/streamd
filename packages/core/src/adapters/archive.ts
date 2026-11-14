import type { EventAdapter, EventDescriptor, EventPage, FetchOptions } from "../types.js";
import { StreamdNetworkError } from "../errors.js";
import { requireAdapter } from "./validate.js";

const pruned = (err: unknown) => err instanceof StreamdNetworkError && err.statusCode === 410;

/** `primary`'s answer, or the archive's when the primary refused the call as pruned. */
async function orArchive<T>(primary: () => Promise<T>, archive: (() => Promise<T>) | undefined): Promise<T> {
  try {
    return await primary();
  } catch (err) {
    if (archive === undefined || !pruned(err)) throw err;
    return archive();
  }
}

/**
 * Reads from `primary`, a fullnode, and sends only what it refuses as pruned
 * (a 410) to `archive`, a node that keeps full history, such as
 * `networks.aptosMainnetArchive`. Rate limits and outages are not reasons to
 * switch: they are rethrown, so backoff applies and the archive's tighter
 * budget goes only on history the fullnode no longer has. Each call starts at
 * the fullnode, so a walk returns to it once it passes the pruned edge;
 * `FullnodeScanAdapter` cursors mean the same on both. The head and
 * simulations come from the fullnode, which the archive trails; the earliest
 * version and oldest block come from the archive.
 */
export class ArchiveFallbackAdapter implements EventAdapter {
  private readonly primary: EventAdapter;
  private readonly archive: EventAdapter;

  declare getHeadVersion?: EventAdapter["getHeadVersion"];
  declare getEarliestVersion?: EventAdapter["getEarliestVersion"];
  declare getBlock?: EventAdapter["getBlock"];
  declare getBlockHeights?: EventAdapter["getBlockHeights"];
  declare getTransactionEvents?: EventAdapter["getTransactionEvents"];
  declare getTransactionEventsByVersion?: EventAdapter["getTransactionEventsByVersion"];
  declare simulateTransactionEvents?: EventAdapter["simulateTransactionEvents"];
  declare view?: EventAdapter["view"];
  declare readObjects?: EventAdapter["readObjects"];

  constructor(primary: EventAdapter, archive: EventAdapter) {
    const p = (this.primary = requireAdapter(primary, "ArchiveFallbackAdapter primary"));
    const a = (this.archive = requireAdapter(archive, "ArchiveFallbackAdapter archive"));

    if (p.getHeadVersion) this.getHeadVersion = (d) => p.getHeadVersion!(d);
    if (p.simulateTransactionEvents) {
      this.simulateTransactionEvents = (s, d) => p.simulateTransactionEvents!(s, d);
    }
    const oldest = a.getEarliestVersion ? a : p.getEarliestVersion ? p : undefined;
    if (oldest) this.getEarliestVersion = (d) => oldest.getEarliestVersion!(d);
    if (p.getBlock) {
      this.getBlock = (d, h) => orArchive(() => p.getBlock!(d, h), a.getBlock && (() => a.getBlock!(d, h)));
    }
    if (p.getBlockHeights) {
      this.getBlockHeights = async (d) => {
        const newest = await p.getBlockHeights!(d);
        if (!a.getBlockHeights) return newest;
        return { oldest: (await a.getBlockHeights(d)).oldest, newest: newest.newest };
      };
    }
    if (p.getTransactionEvents) {
      this.getTransactionEvents = (hash, d) =>
        orArchive(
          () => p.getTransactionEvents!(hash, d),
          a.getTransactionEvents && (() => a.getTransactionEvents!(hash, d)),
        );
    }
    if (p.getTransactionEventsByVersion) {
      this.getTransactionEventsByVersion = (version, d) =>
        orArchive(
          () => p.getTransactionEventsByVersion!(version, d),
          a.getTransactionEventsByVersion && (() => a.getTransactionEventsByVersion!(version, d)),
        );
    }
    if (p.view) {
      this.view = (request, version, d) =>
        orArchive(() => p.view!(request, version, d), a.view && (() => a.view!(request, version, d)));
    }
    if (p.readObjects) {
      this.readObjects = (request, at, d) =>
        orArchive(() => p.readObjects!(request, at, d), a.readObjects && (() => a.readObjects!(request, at, d)));
    }
  }

  fetchEvents(descriptor: EventDescriptor, options: FetchOptions = {}): Promise<EventPage> {
    return orArchive(
      () => this.primary.fetchEvents(descriptor, { ...options }),
      () => this.archive.fetchEvents(descriptor, { ...options }),
    );
  }
}
